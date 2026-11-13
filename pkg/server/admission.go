package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/duravela/djob/pkg/db"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AdmissionConfig bounds local growth without deleting permanent retry identities.
// Ordinary storage pressure preserves cleanup; clock and critical storage holds
// pause all mutations. Limits are admission thresholds, not filesystem quotas.
type AdmissionConfig struct {
	Disabled          bool          // Explicit opt-out for trusted embeddings using synthetic time.
	MinAvailableBytes uint64        // Zero selects 256 MiB.
	MaxDatabaseBytes  int64         // Zero means no separate file-length ceiling.
	MaxClockSkew      time.Duration // Zero selects 5 seconds; also bounds future command times.
}

func ValidateAdmissionConfig(c AdmissionConfig) error {
	if c.MaxDatabaseBytes < 0 || c.MaxClockSkew < time.Millisecond || c.MaxClockSkew > time.Minute {
		return fmt.Errorf("admission requires a nonnegative database ceiling and clock skew in 1ms..1m")
	}
	return nil
}

func validatePublicCommandTime(at time.Time, skew time.Duration) error {
	if at.After(time.Now().Add(skew)) {
		return status.Error(codes.InvalidArgument, "command timestamp exceeds the allowed future clock skew")
	}
	return nil
}
func (s *topLevelServer) validateCommandTime(at time.Time) error {
	if s.cfg.Admission.Disabled {
		return nil
	}
	return validatePublicCommandTime(at, s.cfg.Admission.MaxClockSkew)
}

type admissionPolicy struct {
	s                     *topLevelServer
	mu                    sync.Mutex
	clockHeld             bool
	peerOK                bool
	peerAt                time.Time
	peerConfiguration     string
	recoveryHeld          bool
	running               bool
	haveCheck             bool
	checkedAt             time.Time
	checkedReason         string
	peerChecks            [2]uint64
	clockHoldRecorded     bool
	clockHoldRecordErrors uint64
	recordClockHold       func() error
}

const (
	admissionSampleInterval    = time.Second
	admissionClockProbeTimeout = 750 * time.Millisecond
	admissionClockSampleMaxAge = 3 * time.Second
)

type admissionObservation struct {
	enabled, running, haveCheck bool
	state                       string
	checkAge, peerAge           time.Duration
	havePeer, peerFresh         bool
	clockHeld, clockRecorded    bool
	recoveryHeld                bool
	peerChecks                  [2]uint64
	clockRecordErrors           uint64
}

func newAdmissionPolicy(s *topLevelServer) (*admissionPolicy, error) {
	p := &admissionPolicy{s: s}
	p.recordClockHold = func() error {
		return writeOperationalFile(s.cfg.Raft.DataDir, ".djob-clock-hold", []byte("clock discontinuity; verify UTC and reset while stopped\n"))
	}
	return p, p.loadHolds()
}

// Load again after acquiring the member lock: construction can precede startup
// while a previous process is still recording a hold in this data directory.
func (p *admissionPolicy) loadHolds() error {
	if p.s.cfg.Admission.Disabled {
		return nil
	}
	var clockHeld, recoveryHeld bool
	for path, flag := range map[string]*bool{".djob-clock-hold": &clockHeld, ".djob-recovery-hold": &recoveryHeld} {
		info, err := os.Lstat(filepath.Join(p.s.cfg.Raft.DataDir, path))
		if err == nil {
			if !info.Mode().IsRegular() {
				return fmt.Errorf("admission hold must be a regular file")
			}
			*flag = true
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	p.mu.Lock()
	p.clockHeld = p.clockHeld || clockHeld
	p.clockHoldRecorded = p.clockHoldRecorded || clockHeld
	p.recoveryHeld = p.recoveryHeld || recoveryHeld
	p.mu.Unlock()
	return nil
}

func (p *admissionPolicy) checkClock() {
	m := p.s.clock
	m.mu.Lock()
	now := m.read()
	delta := now.wall.Sub(m.base.wall.Add(now.elapsed - m.base.elapsed))
	jump := delta >= m.cfg.JumpThreshold || delta <= -m.cfg.JumpThreshold || m.state.forward > 0 || m.state.backward > 0
	m.mu.Unlock()
	if !jump {
		return
	}
	p.mu.Lock()
	if p.clockHeld {
		p.mu.Unlock()
		return
	}
	p.clockHeld = true
	p.haveCheck, p.checkedAt, p.checkedReason = true, time.Now(), "clock_untrusted"
	p.mu.Unlock()
	// Latch in memory even if recording the hold fails because storage is full.
	// Release the observation lock before I/O so collection and other admissions
	// can observe the hold even if the filesystem stalls.
	err := p.recordClockHold()
	p.mu.Lock()
	if err != nil {
		p.clockHoldRecordErrors++
	} else {
		p.clockHoldRecorded = true
	}
	p.mu.Unlock()
}

// observation reads only cached policy state and monotonic freshness. It never
// evaluates admission, reads the monitored clock, probes peers, or touches disk.
func (p *admissionPolicy) observation() admissionObservation {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	o := admissionObservation{
		enabled: !p.s.cfg.Admission.Disabled, running: p.running, haveCheck: p.haveCheck,
		havePeer: p.peerOK, clockHeld: p.clockHeld, clockRecorded: p.clockHoldRecorded,
		recoveryHeld: p.recoveryHeld, peerChecks: p.peerChecks, clockRecordErrors: p.clockHoldRecordErrors,
	}
	if o.haveCheck {
		o.checkAge = max(0, now.Sub(p.checkedAt))
	}
	if o.havePeer {
		o.peerAge = max(0, now.Sub(p.peerAt))
		o.peerFresh = o.peerAge <= admissionClockSampleMaxAge
		if p.s.application != nil {
			m := p.s.application.Raft.Membership()
			if m.Enabled && p.peerConfiguration != m.Configuration {
				o.peerFresh = false
			}
		}
	}
	switch {
	case !o.enabled:
		o.state = "disabled"
	case o.clockHeld:
		o.state = "clock_untrusted"
	case !o.haveCheck:
		o.state = "starting"
	case !o.peerFresh:
		o.state = "clock_untrusted"
	case p.checkedReason == "":
		o.state = "healthy"
	default:
		o.state = p.checkedReason
	}
	return o
}

func writeOperationalFile(directory, name string, data []byte) error {
	f, err := os.CreateTemp(directory, ".admission-pending-")
	if err != nil {
		return err
	}
	path := f.Name()
	defer os.Remove(path)
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(path, filepath.Join(directory, name)); err != nil {
		return err
	}
	d, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (p *admissionPolicy) reason() (reason string) {
	if p == nil || p.s.cfg.Admission.Disabled {
		return ""
	}
	defer func() {
		p.mu.Lock()
		p.haveCheck, p.checkedAt, p.checkedReason = true, time.Now(), reason
		p.mu.Unlock()
	}()
	p.checkClock()
	p.mu.Lock()
	clockHeld, peerOK, peerAt, recoveryHeld := p.clockHeld, p.peerOK, p.peerAt, p.recoveryHeld
	peerConfiguration := p.peerConfiguration
	p.mu.Unlock()
	if p.s.application != nil {
		m := p.s.application.Raft.Membership()
		if m.Enabled && peerConfiguration != m.Configuration {
			peerOK = false
		}
	}
	if clockHeld || !peerOK || time.Since(peerAt) > admissionClockSampleMaxAge {
		return "clock_untrusted"
	}
	storage := p.s.storage.observation()
	pressure := false
	for _, component := range []storageComponent{storageStore, storageWAL, storageSnapshots} {
		v := storage.components[component]
		if v.haveSample && v.lastOK && v.value.space.available <= p.s.cfg.Admission.MinAvailableBytes/4 {
			return "storage_exhausted"
		}
		if !v.haveSample || !v.lastOK || time.Since(v.sampledAt) > 3*p.s.cfg.StorageSampleInterval || v.value.space.available < p.s.cfg.Admission.MinAvailableBytes {
			pressure = true
		}
	}
	if maximum := p.s.cfg.Admission.MaxDatabaseBytes; maximum > 0 && storage.components[storageStore].value.fileSize >= maximum {
		if storage.components[storageStore].value.fileSize-maximum >= 64<<20 {
			return "storage_exhausted"
		}
		pressure = true
	}
	if recoveryHeld {
		return "recovery_hold"
	}
	if pressure {
		return "storage_pressure"
	}
	return ""
}

func (p *admissionPolicy) admit(c db.StateCommand) error {
	if p.s.cfg.Admission.Disabled {
		return nil
	}
	if c.At.After(time.Now().Add(p.s.cfg.Admission.MaxClockSkew)) {
		return fmt.Errorf("%w: future command timestamp", db.ErrAdmissionHeld)
	}
	reason := p.reason()
	if reason == "clock_untrusted" || reason == "storage_exhausted" {
		return fmt.Errorf("%w: %s", db.ErrAdmissionHeld, reason)
	}
	if !c.NeedsAdmission() {
		return nil
	}
	if reason != "" {
		return fmt.Errorf("%w: %s", db.ErrAdmissionHeld, reason)
	}
	return nil
}

func (p *admissionPolicy) run(ctx context.Context) {
	if p.s.cfg.Admission.Disabled || ctx.Err() != nil {
		return
	}
	p.mu.Lock()
	p.running = true
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.running = false
		p.mu.Unlock()
	}()
	ticker := time.NewTicker(admissionSampleInterval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		p.checkClock()
		probe, cancel := context.WithTimeout(ctx, admissionClockProbeTimeout)
		configuration := p.s.application.Raft.Membership().Configuration
		ok := p.s.application.Raft.PeerClockWithin(probe, p.s.cfg.Admission.MaxClockSkew)
		ok = ok && configuration == p.s.application.Raft.Membership().Configuration
		cancel()
		p.mu.Lock()
		if ok {
			p.peerOK, p.peerAt = true, time.Now()
			p.peerConfiguration = configuration
			p.peerChecks[0]++
		} else {
			p.peerChecks[1]++
		}
		p.mu.Unlock()
		if ctx.Err() != nil {
			return
		}
		p.reason()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
