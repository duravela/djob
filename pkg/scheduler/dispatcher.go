package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
)

var ErrDispatcherClosed = errors.New("dispatcher closed")

type DispatchBackend interface {
	executor.Backend
	ListReadyRuns([]byte, int) (db.RunPage, error)
	FirstReadyRun(string) (*db.Run, error)
}

type DispatcherConfig struct {
	PollInterval      time.Duration
	ScanLimit         int
	MaxChannels       int
	ShutdownTimeout   time.Duration
	SharedIdleTimeout time.Duration   // Idle legacy owners yield gateways registered as generic resources.
	Executor          executor.Config // ChannelID is filled from the ready run.
	OnError           func(error)     // May be called concurrently by independent gateways.
}

type channelWorker struct {
	executor *executor.Executor
	pending  int
	lastUsed time.Time
	closing  bool
}

type Dispatcher struct {
	cfg       DispatcherConfig
	backend   DispatchBackend
	factory   executor.Factory
	life      context.Context
	cancel    context.CancelFunc
	tickMu    sync.Mutex
	mu        sync.Mutex
	workers   map[string]*channelWorker
	pending   map[string]struct{}
	cursor    []byte
	wake      chan struct{}
	tasks     sync.WaitGroup
	closeOnce sync.Once
	done      chan struct{}
	closeErr  error
}

func NewDispatcher(cfg DispatcherConfig, backend DispatchBackend, factory executor.Factory) (*Dispatcher, error) {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.ScanLimit == 0 {
		cfg.ScanLimit = 128
	}
	if cfg.MaxChannels == 0 {
		cfg.MaxChannels = 8
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 5 * time.Second
	}
	if cfg.SharedIdleTimeout == 0 {
		cfg.SharedIdleTimeout = 500 * time.Millisecond
	}
	if backend == nil || factory == nil || cfg.PollInterval <= 0 || cfg.ScanLimit < 1 || cfg.ScanLimit > db.MaxQueryPage || cfg.MaxChannels < 1 || cfg.ShutdownTimeout <= 0 || cfg.SharedIdleTimeout <= 0 || cfg.Executor.QueueSize < 0 {
		return nil, fmt.Errorf("invalid dispatcher configuration")
	}
	life, cancel := context.WithCancel(context.Background())
	return &Dispatcher{cfg: cfg, backend: backend, factory: factory, life: life, cancel: cancel, workers: make(map[string]*channelWorker),
		pending: make(map[string]struct{}), wake: make(chan struct{}, 1), done: make(chan struct{})}, nil
}

func executorDone(e *executor.Executor) bool {
	select {
	case <-e.Done():
		return true
	default:
		return false
	}
}

// Tick scans a bounded page and starts at most one new channel owner. The cursor
// advances past busy channels, so a full gateway does not starve later pages.
func (d *Dispatcher) Tick(ctx context.Context) (int, error) {
	d.tickMu.Lock()
	defer d.tickMu.Unlock()
	if d.life.Err() != nil {
		return 0, ErrDispatcherClosed
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	d.mu.Lock()
	for id, worker := range d.workers {
		if worker.pending == 0 && executorDone(worker.executor) {
			delete(d.workers, id)
		}
	}
	d.mu.Unlock()
	if err := d.yieldSharedIdle(ctx); err != nil {
		return 0, err
	}
	page, err := d.backend.ListReadyRuns(d.cursor, d.cfg.ScanLimit)
	if err != nil {
		return 0, err
	}
	d.cursor = page.NextCursor
	opened, submitted := false, 0
	var failures []error
	for _, run := range page.Runs {
		if err := ctx.Err(); err != nil {
			return submitted, err
		}
		if d.life.Err() != nil {
			return submitted, ErrDispatcherClosed
		}
		d.mu.Lock()
		_, pending := d.pending[run.ID]
		worker := d.workers[run.ChannelID]
		d.mu.Unlock()
		if pending {
			continue
		}
		// Discovery scans rotate across pages for fairness. Selection always uses
		// the oldest ready row for the channel, so page rotation cannot reorder it.
		first, err := d.backend.FirstReadyRun(run.ChannelID)
		if err != nil {
			var rejection *db.CommandRejection
			if !errors.As(err, &rejection) {
				failures = append(failures, err)
			}
			continue
		}
		run = *first
		if worker == nil {
			if opened {
				continue
			}
			channel, err := d.backend.GetChannel(run.ChannelID)
			if err != nil {
				failures = append(failures, err)
				continue
			}
			if channel.State == db.ChannelBlocked || ((channel.State == db.ChannelOwned || channel.State == db.ChannelDraining) && time.Now().Before(channel.LeaseUntil)) {
				continue
			}
			d.mu.Lock()
			full := len(d.workers) >= d.cfg.MaxChannels
			d.mu.Unlock()
			if full {
				if err := d.evictIdle(ctx); err != nil {
					failures = append(failures, err)
				}
			}
			d.mu.Lock()
			full = len(d.workers) >= d.cfg.MaxChannels
			d.mu.Unlock()
			if full {
				continue
			}
			opened = true
			cfg := d.cfg.Executor
			cfg.ChannelID = run.ChannelID
			acquireCtx, cancel := context.WithCancel(ctx)
			stop := context.AfterFunc(d.life, cancel)
			e, err := executor.New(acquireCtx, cfg, d.backend, d.factory)
			stop()
			cancel()
			if err != nil {
				var rejection *db.CommandRejection
				if !errors.As(err, &rejection) {
					failures = append(failures, err)
				}
				continue
			}
			worker = &channelWorker{executor: e, lastUsed: time.Now()}
			d.mu.Lock()
			d.workers[run.ChannelID] = worker
			d.mu.Unlock()
		}
		d.mu.Lock()
		if worker.closing || worker.pending != 0 || d.life.Err() != nil {
			d.mu.Unlock()
			continue
		}
		if _, exists := d.pending[run.ID]; exists {
			d.mu.Unlock()
			continue
		}
		d.pending[run.ID] = struct{}{}
		worker.pending++
		worker.lastUsed = time.Now()
		d.tasks.Add(1)
		d.mu.Unlock()
		submitted++
		go d.execute(worker, run.ID)
	}
	return submitted, errors.Join(failures...)
}

func (d *Dispatcher) yieldSharedIdle(ctx context.Context) error {
	catalog, ok := d.backend.(interface {
		GetJobResource(string) (*db.JobResource, error)
	})
	if !ok {
		return nil
	}
	d.mu.Lock()
	var candidates []string
	for id, worker := range d.workers {
		if worker.pending == 0 && !worker.closing && time.Since(worker.lastUsed) >= d.cfg.SharedIdleTimeout {
			candidates = append(candidates, id)
		}
	}
	d.mu.Unlock()
	for _, id := range candidates {
		resource, err := catalog.GetJobResource(id)
		if err != nil {
			var rejection *db.CommandRejection
			if errors.As(err, &rejection) && rejection.Code == "not_found" {
				continue
			}
			return err
		}
		if resource.Spec.ChannelID != id {
			continue
		}
		d.mu.Lock()
		worker := d.workers[id]
		if worker == nil || worker.pending != 0 || worker.closing {
			d.mu.Unlock()
			continue
		}
		worker.closing = true
		d.mu.Unlock()
		closing, cancel := context.WithTimeout(ctx, d.cfg.ShutdownTimeout)
		err = worker.executor.Close(closing)
		cancel()
		d.mu.Lock()
		if executorDone(worker.executor) {
			delete(d.workers, id)
		}
		d.mu.Unlock()
		return err // Bound each tick to one orderly handover.
	}
	return nil
}

func (d *Dispatcher) execute(worker *channelWorker, id string) {
	defer d.tasks.Done()
	_, err := worker.executor.Execute(d.life, id)
	d.mu.Lock()
	delete(d.pending, id)
	worker.pending--
	worker.lastUsed = time.Now()
	d.mu.Unlock()
	select {
	case d.wake <- struct{}{}:
	default:
	}
	stoppedSafely := (errors.Is(err, executor.ErrRunCancelled) || errors.Is(err, executor.ErrRunExpired) || errors.Is(err, executor.ErrRetryScheduled)) && !errors.Is(err, executor.ErrRecovery) && !errors.Is(err, executor.ErrResultUncertain) && !errors.Is(err, executor.ErrOwnershipLost)
	if err != nil && d.life.Err() == nil && !errors.Is(err, executor.ErrQueueFull) && !errors.Is(err, db.ErrAdmissionHeld) && !stoppedSafely && d.cfg.OnError != nil {
		d.cfg.OnError(err)
	}
}

func (d *Dispatcher) evictIdle(ctx context.Context) error {
	d.mu.Lock()
	var victim *channelWorker
	var victimID string
	for id, worker := range d.workers {
		if worker.pending == 0 && !worker.closing && (victim == nil || worker.lastUsed.Before(victim.lastUsed)) {
			victim, victimID = worker, id
		}
	}
	if victim == nil {
		d.mu.Unlock()
		return nil
	}
	victim.closing = true
	d.mu.Unlock()
	closeCtx, cancel := context.WithTimeout(ctx, d.cfg.ShutdownTimeout)
	err := victim.executor.Close(closeCtx)
	cancel()
	d.mu.Lock()
	if executorDone(victim.executor) {
		delete(d.workers, victimID)
	}
	d.mu.Unlock()
	return err
}

func (d *Dispatcher) Pending() int { d.mu.Lock(); defer d.mu.Unlock(); return len(d.pending) }

func (d *Dispatcher) Close(ctx context.Context) error {
	d.closeOnce.Do(func() { d.cancel(); go d.shutdown() })
	select {
	case <-d.done:
		return d.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *Dispatcher) shutdown() {
	// Tick may be acquiring an executor. Wait until it has registered that owner
	// before taking the shutdown snapshot; cancellation already prevents new I/O.
	d.tickMu.Lock()
	d.mu.Lock()
	workers := make([]*channelWorker, 0, len(d.workers))
	for _, worker := range d.workers {
		worker.closing = true
		workers = append(workers, worker)
	}
	d.mu.Unlock()
	d.tickMu.Unlock()
	results := make(chan error, len(workers))
	for _, worker := range workers {
		go func() { results <- worker.executor.Close(context.Background()) }()
	}
	var failures []error
	for range workers {
		if err := <-results; err != nil {
			failures = append(failures, err)
		}
	}
	d.tasks.Wait()
	d.closeErr = errors.Join(failures...)
	close(d.done)
}

func (d *Dispatcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()
	for {
		_, err := d.Tick(ctx)
		if ctx.Err() != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), d.cfg.ShutdownTimeout)
			closeErr := d.Close(closeCtx)
			cancel()
			return errors.Join(ctx.Err(), closeErr)
		}
		if errors.Is(err, ErrDispatcherClosed) {
			return err
		}
		if err != nil && !errors.Is(err, db.ErrAdmissionHeld) && d.cfg.OnError != nil {
			d.cfg.OnError(err)
		}
		select {
		case <-ctx.Done():
		case <-d.life.Done():
			return ErrDispatcherClosed
		case <-d.wake:
		case <-ticker.C:
		}
	}
}
