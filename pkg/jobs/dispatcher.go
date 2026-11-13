package jobs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/duravela/djob/pkg/db"
)

type DispatchBackend interface {
	Backend
	ListDispatchJobs([]byte, int, []string) (db.JobRecordPage, error)
}
type DispatchConfig struct {
	PollInterval  time.Duration
	ScanLimit     int
	MaxConcurrent int // Per process; replicated tenant/group quotas remain authoritative.
	MaxPerTenant  int
	OnError       func(error) // May run concurrently for independent jobs.
}
type dispatchKey struct{ tenant, run string }

// Dispatcher is one bounded execution policy for the step runtime. Its discovery
// cursor is rebuildable; durable readiness, claims, quotas and resource admission
// live in the FSM. Other workers and policies can safely contend for those claims.
type Dispatcher struct {
	cfg           DispatchConfig
	backend       DispatchBackend
	worker        *Worker
	life          context.Context
	cancel        context.CancelFunc
	tickMu        sync.Mutex
	mu            sync.Mutex
	pending       map[dispatchKey]bool
	activeTenants map[string]int
	cursor        []byte
	closed        bool
	paused        bool
	tasks         sync.WaitGroup
	closeOnce     sync.Once
	done          chan struct{}
	wake          chan struct{}
}

func NewDispatcher(backend DispatchBackend, cfg DispatchConfig, execution Config, registry Registry) (*Dispatcher, error) {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.ScanLimit == 0 {
		cfg.ScanLimit = 64
	}
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = 16
	}
	if cfg.MaxPerTenant == 0 {
		cfg.MaxPerTenant = 1
	}
	if cfg.PollInterval <= 0 || cfg.ScanLimit < 1 || cfg.ScanLimit > db.MaxQueryPage || cfg.MaxConcurrent < 1 || cfg.MaxConcurrent > 1024 || cfg.MaxPerTenant < 1 || cfg.MaxPerTenant > cfg.MaxConcurrent {
		return nil, fmt.Errorf("invalid generic job dispatcher configuration")
	}
	w, err := NewWorker(backend, execution, registry)
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(context.Background())
	return &Dispatcher{cfg: cfg, backend: backend, worker: w, life: life, cancel: cancel, pending: make(map[dispatchKey]bool), activeTenants: make(map[string]int), done: make(chan struct{}), wake: make(chan struct{}, 1)}, nil
}

// Tick examines one bounded page. The cursor advances over unavailable or
// unsupported work, and full tenants are skipped as complete key ranges.
func (d *Dispatcher) Tick(ctx context.Context) (int, error) {
	d.tickMu.Lock()
	defer d.tickMu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return 0, ErrDispatcherClosed
	}
	if d.paused || len(d.pending) >= d.cfg.MaxConcurrent {
		d.mu.Unlock()
		return 0, nil
	}
	var excluded []string
	for id, count := range d.activeTenants {
		if count >= d.cfg.MaxPerTenant {
			excluded = append(excluded, id)
		}
	}
	d.mu.Unlock()
	page, err := d.backend.ListDispatchJobs(d.cursor, d.cfg.ScanLimit, excluded)
	if err != nil {
		return 0, err
	}
	started := 0
	for _, entry := range page.Entries {
		if err := ctx.Err(); err != nil {
			return started, err
		}
		run := entry.Record.Run
		if run == nil {
			return started, fmt.Errorf("job discovery returned an empty run")
		}
		d.mu.Lock()
		if d.closed {
			d.mu.Unlock()
			return started, ErrDispatcherClosed
		}
		if d.paused {
			d.mu.Unlock()
			return started, nil
		}
		if len(d.pending) >= d.cfg.MaxConcurrent {
			d.mu.Unlock()
			return started, nil
		}
		d.cursor = bytes.Clone(entry.Cursor)
		key := dispatchKey{run.TenantID, run.ID}
		if d.pending[key] || d.activeTenants[run.TenantID] >= d.cfg.MaxPerTenant || !d.worker.Supports(*run) {
			d.mu.Unlock()
			continue
		}
		d.pending[key] = true
		d.activeTenants[run.TenantID]++
		d.tasks.Add(1) // Serialized with closing; no new task can appear after Wait.
		d.mu.Unlock()
		started++
		go d.execute(key)
	}
	d.cursor = bytes.Clone(page.NextCursor)
	return started, nil
}

var ErrDispatcherClosed = errors.New("generic job dispatcher closed")

func expectedAdmission(err error) bool {
	if errors.Is(err, db.ErrAdmissionHeld) {
		return true
	}
	var rejection *db.CommandRejection
	if !errors.As(err, &rejection) {
		return false
	}
	switch rejection.Code {
	case "not_ready", "resource_busy", "channel_busy", "quota_exceeded", "stale_owner", "not_found":
		return true
	}
	return false
}

func (d *Dispatcher) execute(key dispatchKey) {
	defer d.tasks.Done()
	result, err := d.worker.Execute(d.life, key.tenant, key.run)
	if err == nil && result.Run != nil && result.Run.Status == db.JobUnknown {
		err = fmt.Errorf("job %q in tenant %q requires outcome resolution", key.run, key.tenant)
	}
	if err != nil && !expectedAdmission(err) && !errors.Is(err, context.Canceled) && d.cfg.OnError != nil {
		d.cfg.OnError(err)
	}
	d.mu.Lock()
	delete(d.pending, key)
	d.activeTenants[key.tenant]--
	if d.activeTenants[key.tenant] == 0 {
		delete(d.activeTenants, key.tenant)
	}
	d.mu.Unlock()
	if result.Run != nil {
		select {
		case d.wake <- struct{}{}:
		default:
		}
	}
}

func (d *Dispatcher) Active() int { d.mu.Lock(); defer d.mu.Unlock(); return len(d.pending) }

// InspectTenant reports local tasks for one tenant. Counts can include a pending
// claim; discovery grants no execution authority and exposes no other tenant's
// load. Registry contents stay immutable after startup.
func (d *Dispatcher) InspectTenant(tenant string) (active int, paused, closed bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.activeTenants[tenant], d.paused, d.closed
}

// Pause stops new admission while existing conversations retain their sessions.
func (d *Dispatcher) Pause() { d.mu.Lock(); d.paused = true; d.mu.Unlock() }

// Stop prevents new admission and requests cancellation of active executions.
// It does not detach driver callbacks. Close still joins those executions.
func (d *Dispatcher) Stop() {
	d.closeOnce.Do(func() {
		d.mu.Lock()
		d.closed = true
		d.cancel()
		d.mu.Unlock()
		go func() { d.tasks.Wait(); close(d.done) }()
	})
}

func (d *Dispatcher) Close(ctx context.Context) error {
	d.Stop()
	select {
	case <-d.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *Dispatcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()
	defer d.Close(context.Background())
	for {
		_, err := d.Tick(ctx)
		if err != nil && !errors.Is(err, ErrDispatcherClosed) && ctx.Err() == nil && d.cfg.OnError != nil {
			d.cfg.OnError(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.life.Done():
			return nil
		case <-ticker.C:
		case <-d.wake:
		}
	}
}
