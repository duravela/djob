package scheduler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duravela/djob/pkg/db"
)

type ScheduleBackend interface {
	ProposeStateCommand(context.Context, db.StateCommand) (db.StateResult, error)
	ListDueSchedules(time.Time, []byte, int) (db.SchedulePage, error)
	PendingRunCount(string) (uint64, error)
}

type MaterializerConfig struct {
	PollInterval  time.Duration
	BatchSize     int
	CommitTimeout time.Duration
	OnError       func(error)
	// OnMaterialized observes each created calendar occurrence after a verified
	// acknowledgement, including expired legacy runs. It excludes skipped,
	// deferred and rejected occurrences and must be quick and concurrency-safe.
	OnMaterialized func(MaterializationObservation)
}

type Materializer struct {
	cfg           MaterializerConfig
	backend       ScheduleBackend
	isLeader      func() bool
	sweepMu       sync.Mutex
	proposalMu    sync.Mutex
	cursor        []byte
	requestPrefix string
	sequence      atomic.Uint64
}

type MaterializationStats struct {
	Advanced  int
	Created   int
	Conflicts int
	Deferred  int
}

func newPrefix() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

func NewMaterializer(cfg MaterializerConfig, backend ScheduleBackend, isLeader func() bool) (*Materializer, error) {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 64
	}
	if cfg.CommitTimeout == 0 {
		cfg.CommitTimeout = 2 * time.Second
	}
	if backend == nil || isLeader == nil || cfg.PollInterval <= 0 || cfg.BatchSize < 1 || cfg.BatchSize > db.MaxQueryPage || cfg.CommitTimeout <= 0 {
		return nil, fmt.Errorf("invalid materializer configuration")
	}
	prefix, err := newPrefix()
	if err != nil {
		return nil, err
	}
	return &Materializer{cfg: cfg, backend: backend, isLeader: isLeader, requestPrefix: "materializer/" + prefix}, nil
}

func (m *Materializer) submit(ctx context.Context, command db.StateCommand) (db.StateResult, error) {
	command.RequestID = fmt.Sprintf("%s/%d", m.requestPrefix, m.sequence.Add(1))
	var result db.StateResult
	var err error
	for i := 0; i < 2; i++ {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		commitCtx, cancel := context.WithTimeout(ctx, m.cfg.CommitTimeout)
		result, err = m.backend.ProposeStateCommand(commitCtx, command)
		cancel()
		var rejection *db.CommandRejection
		if err == nil || errors.As(err, &rejection) {
			break
		}
	}
	if err != nil {
		return result, err
	}
	if err := result.Err(); err != nil {
		return result, err
	}
	fingerprint, err := command.Fingerprint()
	if err != nil {
		return result, err
	}
	if result.RequestID != command.RequestID || result.Fingerprint != fingerprint {
		return result, fmt.Errorf("materialization receipt does not match request")
	}
	return result, nil
}

func (m *Materializer) Put(ctx context.Context, definition db.ScheduleDefinition, expectedRevision uint64) (*db.Schedule, error) {
	m.proposalMu.Lock()
	defer m.proposalMu.Unlock()
	at := time.Now().UTC()
	put, err := PreparePut(definition, expectedRevision, at)
	if err != nil {
		return nil, err
	}
	result, err := m.submit(ctx, db.StateCommand{At: at, PutSchedule: &put})
	return result.Schedule, err
}

func (m *Materializer) Pause(ctx context.Context, id string, expectedRevision uint64, paused bool) (*db.Schedule, error) {
	m.proposalMu.Lock()
	defer m.proposalMu.Unlock()
	result, err := m.submit(ctx, db.StateCommand{At: time.Now().UTC(), PauseSchedule: &db.PauseSchedule{ID: id, ExpectedRevision: expectedRevision, Paused: paused}})
	return result.Schedule, err
}

// Tick may race a prior leader's proposals. Revision/cursor comparison and the
// occurrence ledger, rather than local leadership alone, prevent duplicates.
func (m *Materializer) Tick(ctx context.Context, at time.Time) (MaterializationStats, error) {
	m.sweepMu.Lock()
	defer m.sweepMu.Unlock()
	stats := MaterializationStats{}
	if !m.isLeader() {
		m.cursor = nil
		return stats, nil
	}
	page, err := m.backend.ListDueSchedules(at, m.cursor, m.cfg.BatchSize)
	if err != nil {
		return stats, err
	}
	m.cursor = page.NextCursor
	var failures []error
	for _, schedule := range page.Schedules {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if !m.isLeader() {
			m.cursor = nil
			break
		}
		plan, err := Plan(schedule, at)
		if err != nil {
			failures = append(failures, fmt.Errorf("plan %s: %w", schedule.ID, err))
			continue
		}
		count, err := m.backend.PendingRunCount(schedule.ChannelID)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		ready := 0
		for _, occurrence := range plan.Occurrences {
			if schedule.StartWindow == 0 || at.Before(occurrence.Add(schedule.StartWindow)) {
				ready++
			}
		}
		if ready != 0 && count+uint64(ready) > db.MaxReadyRunsPerChannel {
			stats.Deferred++
			continue
		}
		m.proposalMu.Lock()
		result, err := m.submit(ctx, db.StateCommand{At: at.UTC(), Materialize: &plan})
		m.proposalMu.Unlock()
		if err != nil {
			var rejection *db.CommandRejection
			if errors.As(err, &rejection) && rejection.Code == "backpressure" {
				stats.Deferred++
				continue
			}
			if errors.As(err, &rejection) && (rejection.Code == "schedule_conflict" || rejection.Code == "schedule_inactive" || rejection.Code == "not_due") {
				stats.Conflicts++
				continue
			}
			failures = append(failures, err)
			continue
		}
		stats.Advanced++
		stats.Created += len(result.Runs)
		m.observeMaterialized(plan, result)
	}
	return stats, errors.Join(failures...)
}

func (m *Materializer) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.cfg.PollInterval)
	defer ticker.Stop()
	for {
		_, err := m.Tick(ctx, time.Now().UTC())
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && m.cfg.OnError != nil {
			m.cfg.OnError(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
