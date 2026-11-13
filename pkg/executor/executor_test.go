package executor_test

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/stretchr/testify/require"
)

type hold struct {
	started       chan struct{}
	release       chan struct{}
	once          sync.Once
	ignoreContext bool
}

func newHold(ignoreContext bool) *hold {
	return &hold{started: make(chan struct{}), release: make(chan struct{}), ignoreContext: ignoreContext}
}

func (h *hold) wait(ctx context.Context) error {
	h.once.Do(func() { close(h.started) })
	var canceled <-chan struct{}
	if !h.ignoreContext {
		canceled = ctx.Done()
	}
	select {
	case <-h.release:
		return nil
	case <-canceled:
		return ctx.Err()
	}
}

type backend struct {
	mu            sync.Mutex
	q             *db.DB
	index         uint64
	commands      []db.StateCommand
	renewGate     atomic.Pointer[hold]
	finishGate    atomic.Pointer[hold]
	startGate     atomic.Pointer[hold]
	staleRun      atomic.Pointer[db.Run]
	loseStartAck  atomic.Bool
	failRenew     atomic.Bool
	failFinish    atomic.Bool
	loseFinishAck atomic.Bool
	staleRenew    atomic.Bool
	lastClaim     db.StateResult
}

func (b *backend) GetChannel(id string) (*db.CommunicationChannel, error) { return b.q.GetChannel(id) }
func (b *backend) GetRun(id string) (*db.Run, error) {
	if stale := b.staleRun.Load(); stale != nil && stale.ID == id {
		v := *stale
		v.Payload = bytes.Clone(v.Payload)
		return &v, nil
	}
	return b.q.GetRun(id)
}

func (b *backend) ProposeStateCommand(ctx context.Context, command db.StateCommand) (db.StateResult, error) {
	if err := ctx.Err(); err != nil {
		return db.StateResult{}, err
	}
	if command.Renew != nil {
		if b.failRenew.Load() {
			return db.StateResult{}, errors.New("consensus unavailable")
		}
		if gate := b.renewGate.Load(); gate != nil {
			if err := gate.wait(ctx); err != nil {
				return db.StateResult{}, err
			}
		}
	}
	if command.Finish != nil {
		if b.failFinish.Load() {
			return db.StateResult{}, errors.New("consensus unavailable")
		}
		if gate := b.finishGate.Load(); gate != nil {
			if err := gate.wait(ctx); err != nil {
				return db.StateResult{}, err
			}
		}
	}
	if command.StartAttempt != nil {
		if gate := b.startGate.Load(); gate != nil {
			if err := gate.wait(ctx); err != nil {
				return db.StateResult{}, err
			}
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if command.Renew != nil && b.staleRenew.Load() && b.lastClaim.RequestID != "" {
		return b.lastClaim.Clone(), nil
	}
	var data bytes.Buffer
	if err := gob.NewEncoder(&data).Encode(db.Command{Version: command.CommandVersion(), Type: db.CmdState, State: &command}); err != nil {
		return db.StateResult{}, err
	}
	b.index++
	result := b.q.ApplyEntries([]commit.Entry{{Index: b.index, Data: data.Bytes()}})
	if result.Error != nil {
		return db.StateResult{}, result.Error
	}
	if len(result.StateResults) != 1 {
		return db.StateResult{}, fmt.Errorf("missing state result")
	}
	b.commands = append(b.commands, command)
	receipt := result.StateResults[0]
	if command.Claim != nil {
		b.lastClaim = receipt.Clone()
	}
	if command.Finish != nil && b.loseFinishAck.CompareAndSwap(true, false) {
		return db.StateResult{}, context.DeadlineExceeded
	}
	if command.StartAttempt != nil && b.loseStartAck.CompareAndSwap(true, false) {
		return db.StateResult{}, context.DeadlineExceeded
	}
	return receipt, receipt.Err()
}

type fixture struct {
	t        *testing.T
	backend  *backend
	gateway  *fake.Gateway
	executor *executor.Executor
	cfg      executor.Config
}

func newFixture(t *testing.T, options fake.Options, customize ...func(*executor.Config)) *fixture {
	t.Helper()
	q, err := db.NewDB(filepath.Join(t.TempDir(), "state.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, q.Close()) })
	f := &fixture{t: t, backend: &backend{q: q}, gateway: fake.New(options), cfg: executor.Config{ChannelID: "gateway", QueueSize: 2,
		LeaseDuration: 2 * time.Second, LeaseSafetyMargin: 300 * time.Millisecond, RenewInterval: 300 * time.Millisecond, CommitTimeout: 100 * time.Millisecond,
		OperationTimeout: time.Second, RecoveryTimeout: time.Second, CloseTimeout: time.Second}}
	for _, customize := range customize {
		customize(&f.cfg)
	}
	f.state(db.StateCommand{Configure: &db.ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "gateway:502"}})
	for _, id := range []string{"slave-3", "slave-7", "third"} {
		f.state(db.StateCommand{CreateRun: &db.CreateRun{ID: id, ChannelID: "gateway", Executor: "fake", Payload: []byte{0, 0xff, 0x10}}})
	}
	f.executor, err = executor.New(context.Background(), f.cfg, f.backend, f.gateway.Factory)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, gate := range []*hold{f.backend.renewGate.Load(), f.backend.finishGate.Load(), f.backend.startGate.Load()} {
			if gate != nil {
				select {
				case <-gate.release:
				default:
					close(gate.release)
				}
			}
		}
		stop := make(chan struct{})
		go func() {
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			for {
				f.gateway.ReleaseAll()
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = f.executor.Close(ctx)
		close(stop)
	})
	return f
}

func (f *fixture) state(command db.StateCommand) db.StateResult {
	f.t.Helper()
	command.RequestID, command.At = fmt.Sprintf("test-%d", testIDs.Add(1)), time.Now().UTC()
	result, err := f.backend.ProposeStateCommand(context.Background(), command)
	require.NoError(f.t, err)
	return result
}

var testIDs atomic.Uint64

type reply struct {
	result executor.Result
	err    error
}

func executeAsync(e *executor.Executor, ctx context.Context, run string) <-chan reply {
	replies := make(chan reply, 1)
	go func() { result, err := e.Execute(ctx, run); replies <- reply{result: result, err: err} }()
	return replies
}

func await(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("phase did not start")
	}
}
func receiveCall(t *testing.T, gateway *fake.Gateway) *fake.Call {
	t.Helper()
	select {
	case call := <-gateway.Calls:
		return call
	case <-time.After(2 * time.Second):
		t.Fatal("exchange did not start")
		return nil
	}
}
func receiveReply(t *testing.T, replies <-chan reply) reply {
	t.Helper()
	select {
	case result := <-replies:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("executor did not finish")
		return reply{}
	}
}
func noCall(t *testing.T, gateway *fake.Gateway) {
	t.Helper()
	select {
	case call := <-gateway.Calls:
		t.Fatalf("unexpected exchange for %s", call.Request.RunID)
	case <-time.After(20 * time.Millisecond):
	}
}
func finishCall(t *testing.T, call *fake.Call, outcome executor.Outcome) {
	t.Helper()
	call.Reply <- fake.Response{Outcome: outcome}
	await(t, call.RecoveryStarted)
	call.Recover <- nil
	await(t, call.CloseStarted)
	call.Close <- nil
}

func TestSameGatewayStaysOccupiedThroughRecoveryCloseAndCommit(t *testing.T) {
	f := newFixture(t, fake.Options{})
	first := executeAsync(f.executor, context.Background(), "slave-3")
	call := receiveCall(t, f.gateway)
	require.Equal(t, []byte{0, 0xff, 0x10}, call.Request.Payload)
	second := executeAsync(f.executor, context.Background(), "slave-7")
	call.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded, Data: []byte("first result")}}
	await(t, call.RecoveryStarted)
	noCall(t, f.gateway)
	run, err := f.backend.GetRun("slave-7")
	require.NoError(t, err)
	require.Equal(t, db.RunReady, run.State)
	require.Zero(t, run.AttemptCount, "queued work must remain unclaimed")
	call.Recover <- nil
	await(t, call.CloseStarted)
	noCall(t, f.gateway)
	gate := newHold(false)
	f.backend.finishGate.Store(gate)
	call.Close <- nil
	await(t, gate.started)
	require.Zero(t, f.gateway.Stats().Active)
	noCall(t, f.gateway)
	run, err = f.backend.GetRun("slave-3")
	require.NoError(t, err)
	require.Equal(t, db.RunRunning, run.State, "physical completion alone must not release the committed claim")
	close(gate.release)
	firstReply := receiveReply(t, first)
	require.NoError(t, firstReply.err)
	require.True(t, firstReply.result.Committed)
	call = receiveCall(t, f.gateway)
	require.Equal(t, "slave-7", call.Request.RunID)
	finishCall(t, call, executor.Outcome{Status: db.AttemptSucceeded})
	require.NoError(t, receiveReply(t, second).err)
	require.Equal(t, 1, f.gateway.Stats().MaxActive)
	_, err = f.executor.Execute(context.Background(), "slave-3")
	require.ErrorContains(t, err, "run_unavailable")
	noCall(t, f.gateway)
}

func TestCallerCancellationDoesNotCancelRecoveryOrReleaseBus(t *testing.T) {
	f := newFixture(t, fake.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	first := executeAsync(f.executor, ctx, "slave-3")
	call := receiveCall(t, f.gateway)
	second := executeAsync(f.executor, context.Background(), "slave-7")
	cancel()
	require.ErrorIs(t, receiveReply(t, first).err, context.Canceled)
	await(t, call.RecoveryStarted)
	require.NoError(t, call.RecoveryContext.Err(), "cleanup must have its own live context")
	noCall(t, f.gateway)
	call.Recover <- nil
	await(t, call.CloseStarted)
	require.NoError(t, call.CloseContext.Err())
	noCall(t, f.gateway)
	call.Close <- nil
	call = receiveCall(t, f.gateway)
	run, err := f.backend.GetRun("slave-3")
	require.NoError(t, err)
	require.Equal(t, db.RunUnknown, run.State)
	attempt, err := f.backend.q.GetAttempt(run.LastAttemptID)
	require.NoError(t, err)
	require.Contains(t, attempt.Error, "canceled")
	require.True(t, attempt.Quiescent)
	finishCall(t, call, executor.Outcome{Status: db.AttemptSucceeded})
	require.NoError(t, receiveReply(t, second).err)
	require.Equal(t, 1, f.gateway.Stats().MaxActive)
}

func TestOperationTimeoutRecoversBeforeNextTransaction(t *testing.T) {
	f := newFixture(t, fake.Options{}, func(c *executor.Config) { c.OperationTimeout = 50 * time.Millisecond })
	first := executeAsync(f.executor, context.Background(), "slave-3")
	call := receiveCall(t, f.gateway)
	second := executeAsync(f.executor, context.Background(), "slave-7")
	await(t, call.RecoveryStarted)
	require.ErrorIs(t, call.Context.Err(), context.DeadlineExceeded)
	noCall(t, f.gateway)
	call.Recover <- nil
	await(t, call.CloseStarted)
	call.Close <- nil
	reply := receiveReply(t, first)
	require.ErrorIs(t, reply.err, context.DeadlineExceeded)
	require.True(t, reply.result.Committed)
	require.Equal(t, db.AttemptUnknown, reply.result.Outcome)
	call = receiveCall(t, f.gateway)
	finishCall(t, call, executor.Outcome{Status: db.AttemptSucceeded})
	require.NoError(t, receiveReply(t, second).err)
}

func TestFailedRecoveryBlocksChannelEvenAfterConfirmedSuccess(t *testing.T) {
	f := newFixture(t, fake.Options{})
	first := executeAsync(f.executor, context.Background(), "slave-3")
	call := receiveCall(t, f.gateway)
	second := executeAsync(f.executor, context.Background(), "slave-7")
	call.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded}}
	await(t, call.RecoveryStarted)
	call.Recover <- errors.New("bus did not become idle")
	await(t, call.CloseStarted)
	call.Close <- nil
	result := receiveReply(t, first)
	require.ErrorContains(t, result.err, "bus did not become idle")
	require.True(t, result.result.Committed)
	require.False(t, result.result.Quiescent)
	require.Error(t, receiveReply(t, second).err)
	channel, err := f.backend.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, db.ChannelBlocked, channel.State)
	run, err := f.backend.GetRun("slave-3")
	require.NoError(t, err)
	require.Equal(t, db.RunSucceeded, run.State)
	attempt, err := f.backend.q.GetAttempt(run.LastAttemptID)
	require.NoError(t, err)
	require.Contains(t, attempt.Error, "bus did not become idle")
	noCall(t, f.gateway)
}

func TestNoncooperativeExchangeCannotBeDetachedByClose(t *testing.T) {
	f := newFixture(t, fake.Options{IgnoreExchangeCancellation: true})
	ctx, cancel := context.WithCancel(context.Background())
	first := executeAsync(f.executor, ctx, "slave-3")
	call := receiveCall(t, f.gateway)
	cancel()
	require.ErrorIs(t, receiveReply(t, first).err, context.Canceled)
	await(t, call.Context.Done())
	closeCtx, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stop()
	require.ErrorIs(t, f.executor.Close(closeCtx), context.DeadlineExceeded)
	require.Equal(t, 1, f.gateway.Stats().Active)
	select {
	case <-call.RecoveryStarted:
		t.Fatal("recovery began while exchange still ran")
	default:
	}
	other, err := executor.New(context.Background(), f.cfg, f.backend, f.gateway.Factory)
	require.ErrorIs(t, err, executor.ErrOwnershipLost)
	require.Nil(t, other)
	noCall(t, f.gateway)
	call.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptUnknown}, Err: context.Canceled}
	await(t, call.RecoveryStarted)
	call.Recover <- nil
	await(t, call.CloseStarted)
	call.Close <- nil
	closedCtx, closedCancel := context.WithTimeout(context.Background(), time.Second)
	defer closedCancel()
	require.NoError(t, f.executor.Close(closedCtx))
}

func TestTimedOutRecoveryStillHoldsBusUntilItReturns(t *testing.T) {
	f := newFixture(t, fake.Options{IgnoreRecoveryCancellation: true}, func(c *executor.Config) { c.RecoveryTimeout = 40 * time.Millisecond })
	first := executeAsync(f.executor, context.Background(), "slave-3")
	call := receiveCall(t, f.gateway)
	second := executeAsync(f.executor, context.Background(), "slave-7")
	call.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded}}
	await(t, call.RecoveryStarted)
	await(t, call.RecoveryContext.Done())
	noCall(t, f.gateway)
	select {
	case <-call.CloseStarted:
		t.Fatal("close raced with an unfinished recovery")
	default:
	}
	call.Recover <- nil
	await(t, call.CloseStarted)
	call.Close <- nil
	require.ErrorIs(t, receiveReply(t, first).err, context.DeadlineExceeded)
	require.Error(t, receiveReply(t, second).err)
	channel, err := f.backend.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, db.ChannelBlocked, channel.State)
}

func TestLostResultAcknowledgementRetriesIdenticalEnvelope(t *testing.T) {
	f := newFixture(t, fake.Options{})
	f.backend.loseFinishAck.Store(true)
	first := executeAsync(f.executor, context.Background(), "slave-3")
	call := receiveCall(t, f.gateway)
	finishCall(t, call, executor.Outcome{Status: db.AttemptSucceeded, Data: []byte("answer")})
	result := receiveReply(t, first)
	require.NoError(t, result.err)
	require.True(t, result.result.Committed)
	f.backend.mu.Lock()
	var finishes []db.StateCommand
	for _, command := range f.backend.commands {
		if command.Finish != nil {
			finishes = append(finishes, command)
		}
	}
	f.backend.mu.Unlock()
	require.Len(t, finishes, 2)
	require.Equal(t, finishes[0], finishes[1])
	require.Equal(t, 1, f.gateway.Stats().Started)
}

func TestUnacknowledgedResultStopsQueuedWork(t *testing.T) {
	f := newFixture(t, fake.Options{})
	first := executeAsync(f.executor, context.Background(), "slave-3")
	call := receiveCall(t, f.gateway)
	second := executeAsync(f.executor, context.Background(), "slave-7")
	f.backend.failFinish.Store(true)
	finishCall(t, call, executor.Outcome{Status: db.AttemptSucceeded})
	result := receiveReply(t, first)
	require.ErrorIs(t, result.err, executor.ErrResultUncertain)
	require.False(t, result.result.Committed)
	require.Error(t, receiveReply(t, second).err)
	channel, err := f.backend.GetChannel("gateway")
	require.NoError(t, err)
	require.NotEqual(t, db.ChannelAvailable, channel.State)
	require.NotEmpty(t, channel.ActiveAttemptID)
	noCall(t, f.gateway)
}

func TestLeaseWatchdogCancelsEvenWhenRenewalIsStuck(t *testing.T) {
	f := newFixture(t, fake.Options{}, func(c *executor.Config) {
		c.LeaseDuration = 600 * time.Millisecond
		c.LeaseSafetyMargin = 150 * time.Millisecond
		c.RenewInterval = 50 * time.Millisecond
		c.CommitTimeout = 50 * time.Millisecond
		c.OperationTimeout = 2 * time.Second
	})
	first := executeAsync(f.executor, context.Background(), "slave-3")
	call := receiveCall(t, f.gateway)
	gate := newHold(true)
	f.backend.renewGate.Store(gate)
	second := executeAsync(f.executor, context.Background(), "slave-7")
	await(t, gate.started)
	await(t, call.Context.Done())
	await(t, call.RecoveryStarted)
	call.Recover <- nil
	await(t, call.CloseStarted)
	call.Close <- nil
	noCall(t, f.gateway)
	close(gate.release)
	require.Error(t, receiveReply(t, first).err)
	require.Error(t, receiveReply(t, second).err)
	require.Equal(t, 1, f.gateway.Stats().Started)
}

func TestRenewalsKeepLongExchangeAuthorized(t *testing.T) {
	f := newFixture(t, fake.Options{}, func(c *executor.Config) {
		c.LeaseDuration = 600 * time.Millisecond
		c.LeaseSafetyMargin = 100 * time.Millisecond
		c.RenewInterval = 100 * time.Millisecond
		c.CommitTimeout = 50 * time.Millisecond
		c.OperationTimeout = 2 * time.Second
	})
	first := executeAsync(f.executor, context.Background(), "slave-3")
	call := receiveCall(t, f.gateway)
	initial, err := f.backend.GetChannel("gateway")
	require.NoError(t, err)
	<-time.After(time.Until(initial.LeaseUntil) + 30*time.Millisecond)
	require.NoError(t, call.Context.Err())
	current, err := f.backend.GetChannel("gateway")
	require.NoError(t, err)
	require.True(t, current.LeaseUntil.After(initial.LeaseUntil))
	finishCall(t, call, executor.Outcome{Status: db.AttemptSucceeded})
	require.NoError(t, receiveReply(t, first).err)
}

func TestCachedClaimReceiptCannotAuthorizeTransport(t *testing.T) {
	f := newFixture(t, fake.Options{})
	f.backend.staleRenew.Store(true)
	result := receiveReply(t, executeAsync(f.executor, context.Background(), "slave-3"))
	require.ErrorIs(t, result.err, executor.ErrOwnershipLost)
	require.Equal(t, 0, f.gateway.Stats().Started)
}

func TestQueueIsBoundedAndCanceledQueuedRunRemainsReady(t *testing.T) {
	f := newFixture(t, fake.Options{}, func(c *executor.Config) { c.QueueSize = 1 })
	first := executeAsync(f.executor, context.Background(), "slave-3")
	call := receiveCall(t, f.gateway)
	ctx, cancel := context.WithCancel(context.Background())
	second := executeAsync(f.executor, ctx, "slave-7")
	require.Eventually(t, func() bool { return f.executor.Pending() == 1 }, time.Second, time.Millisecond)
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer probeCancel()
	_, err := f.executor.Execute(probeCtx, "third")
	require.ErrorIs(t, err, executor.ErrQueueFull)
	cancel()
	require.ErrorIs(t, receiveReply(t, second).err, context.Canceled)
	finishCall(t, call, executor.Outcome{Status: db.AttemptSucceeded})
	require.NoError(t, receiveReply(t, first).err)
	run, err := f.backend.GetRun("slave-7")
	require.NoError(t, err)
	require.Equal(t, db.RunReady, run.State)
	require.Zero(t, run.AttemptCount)
	noCall(t, f.gateway)
}

func TestIndependentGatewayExecutorsProgressTogether(t *testing.T) {
	f := newFixture(t, fake.Options{})
	f.state(db.StateCommand{Configure: &db.ConfigureChannel{ID: "other", GatewayID: "other", Protocol: "fake", Endpoint: "other:502"}})
	f.state(db.StateCommand{CreateRun: &db.CreateRun{ID: "other-run", ChannelID: "other", Executor: "fake"}})
	otherGateway := fake.New(fake.Options{})
	configuration := f.cfg
	configuration.ChannelID = "other"
	other, err := executor.New(context.Background(), configuration, f.backend, otherGateway.Factory)
	require.NoError(t, err)
	t.Cleanup(func() {
		otherGateway.ReleaseAll()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = other.Close(ctx)
	})
	first := executeAsync(f.executor, context.Background(), "slave-3")
	second := executeAsync(other, context.Background(), "other-run")
	firstCall := receiveCall(t, f.gateway)
	secondCall := receiveCall(t, otherGateway)
	require.Equal(t, 1, f.gateway.Stats().Active)
	require.Equal(t, 1, otherGateway.Stats().Active)
	finishCall(t, firstCall, executor.Outcome{Status: db.AttemptSucceeded})
	finishCall(t, secondCall, executor.Outcome{Status: db.AttemptSucceeded})
	require.NoError(t, receiveReply(t, first).err)
	require.NoError(t, receiveReply(t, second).err)
}

func TestGracefulReleaseLetsNewIncarnationAcquireHigherEpoch(t *testing.T) {
	f := newFixture(t, fake.Options{})
	previous := f.executor.Owner()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, f.executor.Close(ctx))
	channel, err := f.backend.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, db.ChannelAvailable, channel.State)
	next, err := executor.New(context.Background(), f.cfg, f.backend, f.gateway.Factory)
	require.NoError(t, err)
	t.Cleanup(func() { _ = next.Close(context.Background()) })
	require.NotEqual(t, previous.OwnerID, next.Owner().OwnerID)
	require.Greater(t, next.Owner().Epoch, previous.Epoch)
	_, err = f.executor.Execute(context.Background(), "slave-3")
	require.ErrorIs(t, err, executor.ErrClosed)
	require.Zero(t, f.gateway.Stats().Started)
}

func TestFailedCloseDoesNotAssertQuiescence(t *testing.T) {
	f := newFixture(t, fake.Options{})
	first := executeAsync(f.executor, context.Background(), "slave-3")
	call := receiveCall(t, f.gateway)
	call.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded}}
	await(t, call.RecoveryStarted)
	call.Recover <- nil
	await(t, call.CloseStarted)
	call.Close <- errors.New("session close is uncertain")
	result := receiveReply(t, first)
	require.ErrorContains(t, result.err, "session close is uncertain")
	require.False(t, result.result.Quiescent)
	channel, err := f.backend.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, db.ChannelBlocked, channel.State)
	require.Equal(t, 1, f.gateway.Stats().Active)
}

func TestCloseRejectsQueuedWorkAndFinishesActiveCleanup(t *testing.T) {
	f := newFixture(t, fake.Options{})
	first := executeAsync(f.executor, context.Background(), "slave-3")
	call := receiveCall(t, f.gateway)
	second := executeAsync(f.executor, context.Background(), "slave-7")
	require.Eventually(t, func() bool { return f.executor.Pending() == 1 }, time.Second, time.Millisecond)
	closed := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		closed <- f.executor.Close(ctx)
	}()
	await(t, call.RecoveryStarted)
	select {
	case <-closed:
		t.Fatal("close returned before cleanup")
	default:
	}
	call.Recover <- nil
	await(t, call.CloseStarted)
	call.Close <- nil
	require.Error(t, receiveReply(t, first).err)
	require.ErrorIs(t, receiveReply(t, second).err, executor.ErrClosed)
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("executor did not close")
	}
	channel, err := f.backend.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, db.ChannelAvailable, channel.State)
	run, err := f.backend.GetRun("slave-7")
	require.NoError(t, err)
	require.Equal(t, db.RunReady, run.State)
	require.Zero(t, run.AttemptCount)
}

func TestNewExecutorBlocksExpiredOwnerInsteadOfTakingOver(t *testing.T) {
	q, err := db.NewDB(filepath.Join(t.TempDir(), "state.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, q.Close()) })
	b := &backend{q: q}
	oldTime := time.Now().UTC().Add(-2 * time.Hour)
	for i, command := range []db.StateCommand{
		{Configure: &db.ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "gateway:502"}},
		{Acquire: &db.AcquireChannel{ChannelID: "gateway", OwnerID: "crashed-process", LeaseUntil: oldTime.Add(time.Hour)}},
		{CreateRun: &db.CreateRun{ID: "unresolved", ChannelID: "gateway", Executor: "fake"}},
		{Claim: &db.ClaimRun{ChannelOwner: db.ChannelOwner{ChannelID: "gateway", Epoch: 1, OwnerID: "crashed-process"}, RunID: "unresolved", AttemptID: "old-attempt", ClaimToken: "old-token"}},
	} {
		command.RequestID, command.At = fmt.Sprintf("old-%d", i), oldTime
		_, err := b.ProposeStateCommand(context.Background(), command)
		require.NoError(t, err)
	}
	gateway := fake.New(fake.Options{})
	e, err := executor.New(context.Background(), executor.Config{ChannelID: "gateway"}, b, gateway.Factory)
	require.ErrorIs(t, err, executor.ErrOwnershipLost)
	require.Nil(t, e)
	channel, err := b.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, db.ChannelBlocked, channel.State)
	require.Equal(t, uint64(1), channel.Epoch)
	run, err := b.GetRun("unresolved")
	require.NoError(t, err)
	require.Equal(t, db.RunUnknown, run.State)
	require.Zero(t, gateway.Stats().Started)
}
