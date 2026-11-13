package executor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duravela/djob/pkg/db"
)

type task struct {
	ctx   context.Context
	runID string
	reply chan taskResult
}

type taskResult struct {
	result Result
	err    error
}

// Executor owns one whole gateway. Constructing another instance always creates
// a new owner incarnation; a restarted process cannot adopt an old session.
type Executor struct {
	cfg            Config
	backend        Backend
	factory        Factory
	owner          db.ChannelOwner
	sequence       atomic.Uint64
	queue          chan task
	life           context.Context
	cancel         context.CancelCauseFunc
	done           chan struct{}
	stopMonitoring chan struct{}
	monitorWG      sync.WaitGroup
	proposalMu     sync.Mutex
	mu             sync.Mutex
	leaseUntil     time.Time
	deadline       time.Time // Carries monotonic time from the proposal's start.
	leaseChanged   chan struct{}
	fault          error
	closeErr       error
	releaseSafe    bool
}

func randomID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

// New acquires ownership through Raft before starting any transport. An expired
// prior owner is marked blocked; it is never automatically replaced.
func New(ctx context.Context, cfg Config, backend Backend, factory Factory) (*Executor, error) {
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	if backend == nil || factory == nil {
		return nil, fmt.Errorf("backend and transport factory are required")
	}
	id, err := randomID()
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancelCause(context.Background())
	e := &Executor{cfg: cfg, backend: backend, factory: factory, owner: db.ChannelOwner{ChannelID: cfg.ChannelID, OwnerID: "executor/" + id},
		queue: make(chan task, cfg.QueueSize), life: life, cancel: cancel, done: make(chan struct{}),
		stopMonitoring: make(chan struct{}), leaseChanged: make(chan struct{}, 1), releaseSafe: true}
	channel, err := backend.GetChannel(cfg.ChannelID)
	if err != nil {
		cancel(err)
		return nil, err
	}
	if (channel.State == db.ChannelOwned || channel.State == db.ChannelDraining) && !time.Now().Before(channel.LeaseUntil) {
		_, err = e.propose(ctx, func(c *db.StateCommand) { c.Expire = &db.ExpireChannel{ChannelID: cfg.ChannelID, Epoch: channel.Epoch} })
		cancel(ErrOwnershipLost)
		return nil, errors.Join(ErrOwnershipLost, err)
	}
	started := time.Now()
	result, err := e.propose(ctx, func(c *db.StateCommand) {
		c.Acquire = &db.AcquireChannel{ChannelID: cfg.ChannelID, ExpectedEpoch: channel.Epoch, OwnerID: e.owner.OwnerID, LeaseUntil: c.At.Add(cfg.LeaseDuration)}
	})
	if err != nil {
		cancel(err)
		return nil, errors.Join(ErrOwnershipLost, err)
	}
	if result.Channel == nil || result.Channel.ID != cfg.ChannelID || result.Channel.OwnerID != e.owner.OwnerID || result.Channel.State != db.ChannelOwned || result.Channel.Epoch != channel.Epoch+1 {
		cancel(ErrOwnershipLost)
		return nil, fmt.Errorf("%w: invalid acquisition receipt", ErrOwnershipLost)
	}
	e.owner.Epoch = result.Channel.Epoch
	e.confirmLease(started, result.Channel.LeaseUntil)
	if err := e.authorized(); err != nil {
		cancel(err)
		return nil, err
	}
	e.monitorWG.Add(2)
	go e.renewLoop()
	go e.watchLease()
	go e.run()
	return e, nil
}

func (e *Executor) Owner() db.ChannelOwner { return e.owner }

// Pending is the bounded queue depth; the active attempt is excluded.
func (e *Executor) Pending() int { return len(e.queue) }

// Done closes after transport cleanup and the ownership shutdown attempt.
func (e *Executor) Done() <-chan struct{} { return e.done }

// Execute queues an unclaimed run. A caller may return on cancellation while the
// executor continues recovering the transport and recording the result.
func (e *Executor) Execute(ctx context.Context, runID string) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if runID == "" {
		return Result{}, fmt.Errorf("run ID is required")
	}
	t := task{ctx: ctx, runID: runID, reply: make(chan taskResult, 1)}
	e.mu.Lock()
	if e.life.Err() != nil {
		err := e.reasonLocked()
		e.mu.Unlock()
		return Result{}, err
	}
	select {
	case e.queue <- t:
		e.mu.Unlock()
	default:
		e.mu.Unlock()
		return Result{}, ErrQueueFull
	}
	select {
	case reply := <-t.reply:
		return reply.result, reply.err
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

// Close cancels active work and rejects queued work. Returning because ctx ended
// does not release a channel whose transport is still running or recovering.
func (e *Executor) Close(ctx context.Context) error {
	e.mu.Lock()
	e.cancel(ErrClosed)
	e.mu.Unlock()
	select {
	case <-e.done:
		e.mu.Lock()
		defer e.mu.Unlock()
		return e.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Executor) fail(err error) {
	e.mu.Lock()
	if e.fault == nil {
		e.fault = err
	}
	e.cancel(err)
	e.mu.Unlock()
}

func (e *Executor) reasonLocked() error {
	if e.fault != nil {
		return e.fault
	}
	return ErrClosed
}

func (e *Executor) reason() error { e.mu.Lock(); defer e.mu.Unlock(); return e.reasonLocked() }

func (e *Executor) propose(ctx context.Context, fill func(*db.StateCommand)) (db.StateResult, error) {
	// Serialize timestamps with proposal submission, including heartbeats.
	e.proposalMu.Lock()
	defer e.proposalMu.Unlock()
	n := e.sequence.Add(1)
	if n == 0 {
		return db.StateResult{}, fmt.Errorf("request sequence exhausted")
	}
	command := db.StateCommand{RequestID: e.owner.OwnerID + "/request/" + strconv.FormatUint(n, 10), At: time.Now().UTC()}
	fill(&command)
	// One bounded retry uses the identical envelope. A lost acknowledgement must
	// not become a second transition with a fresh timestamp or request ID.
	var result db.StateResult
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		commitCtx, cancel := context.WithTimeout(ctx, e.cfg.CommitTimeout)
		result, err = e.backend.ProposeStateCommand(commitCtx, command)
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
		return result, fmt.Errorf("%w: receipt does not match the submitted request", ErrOwnershipLost)
	}
	return result, nil
}

func (e *Executor) confirmLease(started time.Time, until time.Time) {
	// A long commit cannot buy back elapsed time, and a backward wall clock jump
	// cannot extend the monotonic authorization deadline.
	deadline := started.Add(e.cfg.LeaseDuration - e.cfg.LeaseSafetyMargin)
	if wallDeadline := time.Now().Add(time.Until(until) - e.cfg.LeaseSafetyMargin); wallDeadline.Before(deadline) {
		deadline = wallDeadline
	}
	e.mu.Lock()
	if until.Before(e.leaseUntil) {
		e.mu.Unlock()
		return
	}
	if e.deadline.After(deadline) {
		deadline = e.deadline
	}
	e.leaseUntil, e.deadline = until, deadline
	e.mu.Unlock()
	select {
	case e.leaseChanged <- struct{}{}:
	default:
	}
}

func (e *Executor) authorized() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.life.Err() != nil {
		return e.reasonLocked()
	}
	now := time.Now()
	if !now.Before(e.deadline) || !now.Before(e.leaseUntil) {
		return ErrLeaseExpiring
	}
	return nil
}

func (e *Executor) renew() (db.StateResult, error) {
	started := time.Now()
	result, err := e.propose(e.life, func(c *db.StateCommand) {
		c.Renew = &db.RenewChannel{ChannelID: e.owner.ChannelID, ExpectedEpoch: e.owner.Epoch, OwnerID: e.owner.OwnerID, LeaseUntil: c.At.Add(e.cfg.LeaseDuration)}
	})
	if err != nil {
		return result, errors.Join(ErrOwnershipLost, err)
	}
	if result.Channel == nil || result.Channel.ID != e.owner.ChannelID || result.Channel.OwnerID != e.owner.OwnerID || result.Channel.Epoch != e.owner.Epoch ||
		(result.Channel.State != db.ChannelOwned && result.Channel.State != db.ChannelDraining) {
		return result, fmt.Errorf("%w: invalid renewal receipt", ErrOwnershipLost)
	}
	e.confirmLease(started, result.Channel.LeaseUntil)
	return result, e.authorized()
}

func (e *Executor) renewLoop() {
	defer e.monitorWG.Done()
	ticker := time.NewTicker(e.cfg.RenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if _, err := e.renew(); err != nil {
				if e.life.Err() == nil {
					e.fail(err)
				}
				return
			}
		case <-e.life.Done():
			return
		case <-e.stopMonitoring:
			return
		}
	}
}

func (e *Executor) watchLease() {
	defer e.monitorWG.Done()
	for {
		e.mu.Lock()
		deadline := e.deadline
		e.mu.Unlock()
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-timer.C:
			if err := e.authorized(); err != nil {
				if e.life.Err() == nil {
					e.fail(err)
				}
				return
			}
		case <-e.leaseChanged:
		case <-e.life.Done():
			timer.Stop()
			return
		case <-e.stopMonitoring:
			timer.Stop()
			return
		}
		timer.Stop()
	}
}

func (e *Executor) run() {
	defer func() {
		close(e.stopMonitoring)
		e.monitorWG.Wait()
		e.finishOwnership()
		e.mu.Lock()
		e.closeErr = errors.Join(e.fault, e.closeErr)
		for {
			select {
			case t := <-e.queue:
				t.reply <- taskResult{err: e.reasonLocked()}
			default:
				e.mu.Unlock()
				close(e.done)
				return
			}
		}
	}()
	for {
		select {
		case <-e.life.Done():
			return
		case t := <-e.queue:
			if err := t.ctx.Err(); err != nil {
				t.reply <- taskResult{err: err}
				continue
			}
			if err := e.authorized(); err != nil {
				if e.life.Err() == nil {
					e.fail(err)
				}
				t.reply <- taskResult{err: err}
				return
			}
			result, err := e.execute(t)
			t.reply <- taskResult{result: result, err: err}
		}
	}
}

func (e *Executor) finishOwnership() {
	var result db.StateResult
	var err error
	e.mu.Lock()
	safe := e.releaseSafe
	e.mu.Unlock()
	if safe {
		result, err = e.propose(context.Background(), func(c *db.StateCommand) { c.Drain = &e.owner })
	}
	if safe && err == nil && result.Channel != nil && result.Channel.ActiveAttemptID == "" {
		_, err = e.propose(context.Background(), func(c *db.StateCommand) { c.Release = &db.ReleaseChannel{ChannelOwner: e.owner, Quiescent: true} })
		if err == nil {
			return
		}
	}
	// If authority has already expired, record blocked state when consensus is
	// reachable. Otherwise the unreleased owner continues to prevent takeover.
	e.mu.Lock()
	expired := !time.Now().Before(e.leaseUntil)
	e.mu.Unlock()
	if expired {
		_, expireErr := e.propose(context.Background(), func(c *db.StateCommand) {
			c.Expire = &db.ExpireChannel{ChannelID: e.owner.ChannelID, Epoch: e.owner.Epoch}
		})
		if expireErr == nil {
			return
		}
		err = errors.Join(err, expireErr)
	}
	e.mu.Lock()
	e.closeErr = err
	e.mu.Unlock()
}
