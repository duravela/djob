package jobs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/duravela/djob/pkg/db"
)

type Worker struct {
	backend  Backend
	cfg      Config
	registry Registry
}

type actionAuthorityKey struct{}

// ActionAuthorityDeadline exposes only a currently verified local lease bound.
// Remote adapters use it to bound/renew a worker's permission; it is not a claim
// token and cannot create or extend durable ownership.
func ActionAuthorityDeadline(ctx context.Context) (time.Time, bool) {
	read, ok := ctx.Value(actionAuthorityKey{}).(func() (time.Time, bool))
	if !ok || ctx.Err() != nil {
		return time.Time{}, false
	}
	return read()
}

func NewWorker(backend Backend, cfg Config, registry Registry) (*Worker, error) {
	if cfg.WorkerAdmission != nil {
		guard := *cfg.WorkerAdmission
		cfg.WorkerAdmission = &guard
	}
	if backend == nil {
		return nil, fmt.Errorf("job backend is required")
	}
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	copy := Registry{Handlers: make(map[Version]Handler), Drivers: make(map[Version]DriverFactory)}
	for key, handler := range registry.Handlers {
		if key.Name == "" || key.Version == "" || handler == nil {
			return nil, fmt.Errorf("invalid versioned handler registration")
		}
		copy.Handlers[key] = handler
	}
	for key, factory := range registry.Drivers {
		if key.Name == "" || key.Version == "" || factory == nil {
			return nil, fmt.Errorf("invalid versioned driver registration")
		}
		copy.Drivers[key] = factory
	}
	return &Worker{backend: backend, cfg: cfg, registry: copy}, nil
}

// Supports is capability discovery only. A matching registration cannot grant
// execution authority; Execute still commits a fresh claim and action permission.
func (w *Worker) Supports(run db.JobRun) bool {
	if run.Definition.WorkerRelease != "" && (w.cfg.WorkerAdmission == nil || w.cfg.WorkerAdmission.DeploymentID != run.Definition.WorkerRelease) {
		return false
	}
	if run.Runtime != db.StepRuntime || w.registry.Handlers[Version{run.Definition.Handler, run.Definition.HandlerVersion}] == nil {
		return false
	}
	return run.Binding == nil || w.registry.Drivers[Version{run.Binding.Driver, run.Binding.DriverVersion}] != nil
}

func newID() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(data[:]), nil
}

func errorText(value string) string {
	value = strings.ToValidUTF8(value, "\uFFFD")
	if len(value) > 4096 {
		value = value[:4096]
		for !utf8.ValidString(value) {
			value = value[:len(value)-1]
		}
	}
	return value
}

func receipt(command db.StateCommand, result db.StateResult) error {
	fingerprint, err := command.Fingerprint()
	if err != nil {
		return err
	}
	if result.RequestID != command.RequestID || result.Fingerprint != fingerprint {
		return fmt.Errorf("job receipt does not match proposed command")
	}
	return result.Err()
}

// Retrying an unacknowledged proposal retains the exact envelope, timestamp,
// and identities. A persisted duplicate receipt alone is never new I/O authority.
func propose(ctx context.Context, backend Backend, timeout time.Duration, command db.StateCommand) (db.StateResult, error) {
	var result db.StateResult
	var err error
	for range 2 {
		commit, cancel := context.WithTimeout(ctx, timeout)
		result, err = backend.ProposeStateCommand(commit, command)
		cancel()
		var rejection *db.CommandRejection
		if err == nil {
			return result, receipt(command, result)
		}
		if errors.As(err, &rejection) || ctx.Err() != nil {
			break
		}
	}
	return result, err
}

type execution struct {
	w             *Worker
	owner         db.JobOwner
	life          context.Context
	stopLife      context.CancelFunc
	work          context.Context
	stopWork      context.CancelFunc
	proposal      sync.Mutex
	lastAt        time.Time // Guarded by proposal; never used as local execution authority.
	mu            sync.Mutex
	deadline      time.Time // Contains a local monotonic clock; never persisted.
	leaseUntil    time.Time // Last confirmed durable lease, guarded by mu.
	lost          atomic.Bool
	durableCancel atomic.Bool
	wg            sync.WaitGroup
}

func (e *execution) lose() { e.lost.Store(true); e.stopWork() }
func (e *execution) safe() bool {
	e.mu.Lock()
	deadline := e.deadline
	e.mu.Unlock()
	if e.lost.Load() || deadline.IsZero() || !time.Now().Before(deadline) {
		e.lose()
		return false
	}
	return true
}

func (e *execution) commit(build func(time.Time) db.JobCommand, renew bool) (db.StateResult, error) {
	e.proposal.Lock()
	defer e.proposal.Unlock()
	e.mu.Lock()
	hadLease := !e.deadline.IsZero()
	e.mu.Unlock()
	if e.lost.Load() || (hadLease && !e.safe()) {
		return db.StateResult{}, ErrOwnershipLost
	}
	started := time.Now()
	id, err := newID()
	if err != nil {
		return db.StateResult{}, err
	}
	at := started.UTC()
	if at.Before(e.lastAt) {
		at = e.lastAt
	}
	job := build(at)
	command := db.StateCommand{RequestID: "job/" + id, At: at, Job: &job}
	if job.Claim != nil && e.w.cfg.WorkerAdmission != nil {
		guard := *e.w.cfg.WorkerAdmission
		command.WorkerAdmission = &guard
	}
	result, err := propose(e.life, e.w.backend, e.w.cfg.CommitTimeout, command)
	if err != nil {
		return result, err
	}
	if result.Job == nil || result.Job.Run == nil || result.Job.Run.TenantID != e.owner.TenantID || result.Job.Run.ID != e.owner.RunID {
		e.lose()
		return result, ErrOwnershipLost
	}
	if result.Job.Run.LastTransition.After(e.lastAt) {
		e.lastAt = result.Job.Run.LastTransition
	}
	if hadLease && !e.safe() {
		return result, ErrOwnershipLost
	}
	if renew {
		deadline := started.Add(e.w.cfg.LeaseDuration - e.w.cfg.SafetyMargin)
		e.mu.Lock()
		// A clamped wall timestamp must not refresh local authority when the
		// durable lease did not advance. Bound every extension by the actual
		// confirmed durable extension as well as the local monotonic budget.
		if hadLease {
			bound := e.deadline.Add(result.Job.Run.LeaseUntil.Sub(e.leaseUntil))
			if bound.Before(deadline) {
				deadline = bound
			}
		}
		if !time.Now().Before(deadline) || e.lost.Load() {
			e.mu.Unlock()
			e.lose()
			return result, ErrOwnershipLost
		}
		e.deadline, e.leaseUntil = deadline, result.Job.Run.LeaseUntil
		e.mu.Unlock()
	}
	if !e.safe() {
		return result, ErrOwnershipLost
	}
	return result, nil
}

func (e *execution) start() {
	e.wg.Add(3)
	go func() {
		defer e.wg.Done()
		ticker := time.NewTicker(e.w.cfg.RenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-e.life.Done():
				return
			case <-ticker.C:
			}
			if e.lost.Load() {
				return
			}
			_, err := e.commit(func(at time.Time) db.JobCommand {
				return db.JobCommand{Renew: &db.RenewJob{JobOwner: e.owner, LeaseUntil: at.Add(e.w.cfg.LeaseDuration)}}
			}, true)
			if err != nil {
				e.lose()
				return
			}
		}
	}()
	go func() {
		defer e.wg.Done()
		// The watchdog is independent of blocked proposals and driver code.
		interval := max(time.Nanosecond, min(e.w.cfg.PollInterval, e.w.cfg.SafetyMargin/4))
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-e.life.Done():
				return
			case <-ticker.C:
			}
			if !e.safe() {
				return
			}
		}
	}()
	go func() {
		defer e.wg.Done()
		ticker := time.NewTicker(e.w.cfg.PollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-e.life.Done():
				return
			case <-ticker.C:
			}
			view, err := e.w.backend.GetJobView(e.owner.TenantID, e.owner.RunID)
			if err != nil {
				continue
			} // A read failure cannot extend authorization.
			if view.Run.CancelRequested {
				e.durableCancel.Store(true)
				e.stopWork()
			}
		}
	}()
}

func (e *execution) stop() { e.stopLife(); e.stopWork(); e.wg.Wait() }

// Execute claims one ready run. A resource-free wait returns to its dispatcher;
// a conversation holding resources stays here and renews through its waits.
// Callers may run different execution policies, but all claims use the same FSM.
func (w *Worker) Execute(ctx context.Context, tenant, runID string) (result Result, err error) {
	started := time.Now()
	claimConfirmed := false
	var cleanupFailure error
	if observe := w.cfg.OnExecution; observe != nil {
		observe(ExecutionObservation{Phase: ExecutionStarted})
		// Register first so driver cleanup and monitor joins finish before observation.
		defer func() {
			observe(ExecutionObservation{
				Phase: ExecutionReturned, Result: executionResult(result, err, claimConfirmed),
				Duration: time.Since(started), CleanupFailed: cleanupFailure != nil,
			})
		}()
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	view, err := w.backend.GetJobView(tenant, runID)
	if err != nil {
		return Result{}, err
	}
	if view.Run.Runtime != db.StepRuntime {
		return Result{}, fmt.Errorf("unsupported runtime %q", view.Run.Runtime)
	}
	handler := w.registry.Handlers[Version{view.Run.Definition.Handler, view.Run.Definition.HandlerVersion}]
	if handler == nil {
		return Result{}, fmt.Errorf("versioned job handler is not registered")
	}
	if view.Run.Binding != nil && w.registry.Drivers[Version{view.Run.Binding.Driver, view.Run.Binding.DriverVersion}] == nil {
		return Result{}, fmt.Errorf("versioned job driver is not registered")
	}
	incarnation, err := newID()
	if err != nil {
		return Result{}, err
	}
	attemptID, err := newID()
	if err != nil {
		return Result{}, err
	}
	life, stopLife := context.WithCancel(context.Background())
	work, stopWork := context.WithCancel(ctx)
	e := &execution{w: w, lastAt: view.Run.LastTransition, owner: db.JobOwner{TenantID: tenant, RunID: runID, AttemptID: attemptID, OwnerID: w.cfg.OwnerPrefix + "/" + incarnation}, life: life, stopLife: stopLife, work: work, stopWork: stopWork}
	defer e.stop()
	claimed, err := e.commit(func(at time.Time) db.JobCommand {
		return db.JobCommand{Claim: &db.ClaimJob{JobOwner: e.owner, LeaseUntil: at.Add(w.cfg.LeaseDuration)}}
	}, true)
	if err != nil {
		return Result{}, err
	}
	if claimed.Job == nil || claimed.Job.Run == nil || claimed.Job.Run.OwnerID != e.owner.OwnerID || claimed.Job.Run.AttemptID != e.owner.AttemptID {
		return Result{}, ErrOwnershipLost
	}
	claimConfirmed = true
	w.observeAdmission(claimed.Job.Run)
	e.start()
	var driver Driver
	var sessionLost bool
	defer func() {
		if driver != nil {
			cleanupFailure = errors.Join(cleanupFailure, w.cleanup(driver))
		}
	}()
	finish := func(run *db.JobRun, d Decision, event *db.JobEvent) (Result, error) {
		quiet := cleanupFailure == nil
		if driver != nil {
			cleanupErr := w.cleanup(driver)
			driver = nil
			cleanupFailure = errors.Join(cleanupFailure, cleanupErr)
		}
		if cleanupFailure != nil {
			quiet = false
			d.Status = db.JobUnknown
			if d.Error == "" {
				d.Error = cleanupFailure.Error()
			} else {
				d.Error = errors.Join(errors.New(d.Error), cleanupFailure).Error()
			}
		}
		if !e.safe() {
			return Result{Run: run}, ErrOwnershipLost
		}
		for range 2 {
			result, err := e.advance(run, event, d, quiet)
			if err == nil {
				return Result{Run: result.Job.Run}, nil
			}
			var rejection *db.CommandRejection
			if !errors.As(err, &rejection) || rejection.Code != "cancel_requested" {
				return Result{Run: run}, err
			}
			// Cancellation may commit during driver cleanup. Retain its intent
			// and known effect history while completing the same owned attempt.
			latest, readErr := w.backend.GetJobView(tenant, runID)
			if readErr != nil {
				return Result{Run: run}, readErr
			}
			run, event = latest.Run, nil
			d.State, d.Status = run.State, db.JobCancelled
		}
		return Result{Run: run}, fmt.Errorf("cancellation prevented durable completion")
	}
	decisions := 0
	for {
		if !e.safe() {
			return Result{}, ErrOwnershipLost
		}
		view, err = w.backend.GetJobView(tenant, runID)
		if err != nil {
			return Result{}, err
		}
		run := view.Run
		e.proposal.Lock()
		if run.LastTransition.After(e.lastAt) {
			e.lastAt = run.LastTransition
		}
		e.proposal.Unlock()
		if run.OwnerID != e.owner.OwnerID || run.AttemptID != e.owner.AttemptID {
			e.lose()
			return Result{Run: run}, ErrOwnershipLost
		}
		if sessionLost {
			return finish(run, Decision{Kind: db.JobComplete, State: run.State, Status: db.JobUnknown, Error: "driver session could not continue"}, nil)
		}
		if run.CancelRequested || e.durableCancel.Load() {
			status := db.JobCancelled
			if run.PendingAction != nil && run.PendingAction.Status == db.EffectUnknown {
				status = db.JobUnknown
			}
			return finish(run, Decision{Kind: db.JobComplete, State: run.State, Status: status, Error: run.CancelReason}, nil)
		}
		if e.work.Err() != nil {
			quietErr := w.cleanup(driver)
			driver = nil
			cleanupFailure = errors.Join(cleanupFailure, quietErr)
			if !e.safe() {
				return Result{Run: run}, ErrOwnershipLost
			}
			if quietErr == nil && len(run.Resources) == 0 && (run.PendingAction == nil || (run.PendingAction.Status != db.EffectAuthorized && run.PendingAction.Status != db.EffectUnknown)) {
				result, err := e.commit(func(time.Time) db.JobCommand { return db.JobCommand{Yield: &db.YieldJob{JobOwner: e.owner}} }, false)
				if err != nil {
					return Result{Run: run}, err
				}
				return Result{Run: result.Job.Run, Yielded: true}, ctx.Err()
			}
			return finish(run, Decision{Kind: db.JobComplete, State: run.State, Status: db.JobUnknown, Error: "worker interrupted"}, nil)
		}
		if run.PendingAction != nil && run.PendingAction.Status == db.EffectUnknown {
			return finish(run, Decision{Kind: db.JobComplete, State: run.State, Status: db.JobUnknown, Error: run.PendingAction.Error}, nil)
		}
		if run.Status == db.JobWaiting && view.Event == nil && (run.WakeAt.IsZero() || time.Now().Before(run.WakeAt)) {
			select {
			case <-e.work.Done():
			case <-time.After(w.cfg.PollInterval):
			}
			continue
		}
		if decisions >= w.cfg.MaxDecisions {
			return finish(run, Decision{Kind: db.JobComplete, State: run.State, Status: db.JobFailed, Error: "decision budget exhausted"}, view.Event)
		}
		decisions++
		decision, nextErr := handler(View{Run: *run, Event: view.Event, Now: time.Now().UTC()})
		if errors.Is(nextErr, ErrHandlerUnavailable) {
			// Reuse the interruption path: pure work can yield; any held
			// physical authority retains conservative unknown semantics.
			e.stopWork()
			continue
		}
		if nextErr != nil {
			return finish(run, Decision{Kind: db.JobComplete, State: run.State, Status: db.JobFailed, Error: nextErr.Error()}, view.Event)
		}
		if decision.Kind == db.JobComplete {
			return finish(run, decision, view.Event)
		}
		if decision.Kind == db.JobWait && len(run.Resources) == 0 && driver != nil {
			cleanupErr := w.cleanup(driver)
			driver = nil
			cleanupFailure = errors.Join(cleanupFailure, cleanupErr)
			if cleanupErr != nil {
				return finish(run, Decision{Kind: db.JobComplete, State: run.State, Status: db.JobUnknown, Error: cleanupErr.Error()}, view.Event)
			}
		}
		result, err := e.advance(run, view.Event, decision, false)
		if err != nil {
			var rejection *db.CommandRejection
			if errors.As(err, &rejection) && rejection.Code == "cancel_requested" {
				continue
			}
			return Result{Run: run}, err
		}
		run = result.Job.Run
		if decision.Kind == db.JobWait && run.OwnerID == "" {
			return Result{Run: run, Yielded: true}, nil
		}
		if decision.Kind != db.JobDoAction {
			continue
		}
		if !e.safe() {
			return Result{Run: run}, ErrOwnershipLost
		}
		action := result.Job.Action
		if action == nil || action.TenantID != tenant || action.RunID != runID || action.AttemptID != e.owner.AttemptID || action.Status != db.EffectAuthorized || action.Sequence != run.ActionCount {
			e.lose()
			return Result{Run: run}, ErrOwnershipLost
		}
		effect := Effect{Status: db.EffectFailed}
		var driverDuration time.Duration
		var driverInvoked bool
		if e.work.Err() != nil {
			effect.Error = e.work.Err().Error()
		} else {
			if driver == nil {
				factory := w.registry.Drivers[Version{run.Binding.Driver, run.Binding.DriverVersion}]
				driver, err = factory(*run)
				if err == nil && driver == nil {
					err = fmt.Errorf("driver factory returned nil")
				}
			}
			if err != nil {
				effect.Error = err.Error()
			} else if !e.safe() {
				return Result{Run: run}, ErrOwnershipLost
			} else {
				operation, cancel := context.WithTimeout(e.work, w.cfg.OperationTimeout)
				operation = context.WithValue(operation, actionAuthorityKey{}, func() (time.Time, bool) {
					if !e.safe() {
						return time.Time{}, false
					}
					e.mu.Lock()
					defer e.mu.Unlock()
					return e.deadline, true
				})
				key := sha256.Sum256([]byte(fmt.Sprintf("%d:%s/%d:%s/%d", len(tenant), tenant, len(runID), runID, action.Sequence)))
				started := time.Now()
				effect = driver.Execute(operation, Action{TenantID: tenant, RunID: runID, AttemptID: e.owner.AttemptID, Sequence: action.Sequence, Operation: action.Operation, Input: action.Input, IdempotencyKey: hex.EncodeToString(key[:])})
				driverDuration, driverInvoked = time.Since(started), true
				cancel()
			}
		}
		if effect.Status != db.EffectSucceeded && effect.Status != db.EffectFailed && effect.Status != db.EffectUnknown {
			effect.Status, effect.Error = db.EffectUnknown, "driver returned an invalid effect outcome"
		}
		sessionLost = effect.SessionLost
		if len(effect.Data) > 1<<20 {
			effect.Data = nil
			effect.Error += "; driver result exceeds 1 MiB"
			sessionLost = true
		}
		recordStarted := time.Now()
		_, recordErr := e.commit(func(time.Time) db.JobCommand {
			return db.JobCommand{CompleteAction: &db.CompleteJobAction{JobOwner: e.owner, Sequence: action.Sequence, Status: effect.Status, Result: effect.Data, Error: errorText(effect.Error)}}
		}, false)
		if observe := w.cfg.OnAction; observe != nil {
			observe(ActionObservation{
				Status: effect.Status, DriverInvoked: driverInvoked, DriverDuration: driverDuration,
				RecordDuration: time.Since(recordStarted), RecordConfirmed: recordErr == nil,
			})
		}
		if recordErr != nil {
			return Result{Run: run}, recordErr
		}
	}
}

func (e *execution) advance(run *db.JobRun, event *db.JobEvent, d Decision, quiet bool) (db.StateResult, error) {
	if d.WaitFor < 0 || (d.WaitFor != 0 && (d.Kind != db.JobWait || !d.WakeAt.IsZero())) {
		return db.StateResult{}, fmt.Errorf("relative wait duration must be positive and exclude an absolute deadline")
	}
	result, err := e.commit(func(at time.Time) db.JobCommand {
		c := &db.AdvanceJob{JobOwner: e.owner, ExpectedStep: run.Step, Kind: d.Kind, State: d.State, Operation: d.Operation, Input: d.Input, WaitKey: d.WaitKey, WakeAt: d.WakeAt, Status: d.Status, Result: d.Result, Error: errorText(d.Error), Quiescent: quiet}
		if d.WaitFor != 0 {
			c.WakeAt = at.Add(d.WaitFor)
		}
		if event != nil {
			c.EventID = event.ID
		}
		return db.JobCommand{Advance: c}
	}, false)
	if err == nil {
		e.observeEventConsumed(run, event, result, time.Now())
	}
	return result, err
}

func (w *Worker) cleanup(driver Driver) error {
	if driver == nil {
		return nil
	}
	recovery, cancel := context.WithTimeout(context.Background(), w.cfg.RecoveryTimeout)
	recoverErr := driver.Quiesce(recovery)
	cancel()
	closing, cancel := context.WithTimeout(context.Background(), w.cfg.RecoveryTimeout)
	closeErr := driver.Close(closing)
	cancel()
	return errors.Join(recoverErr, closeErr)
}
