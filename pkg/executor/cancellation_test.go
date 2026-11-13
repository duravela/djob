package executor_test

import (
	"context"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/stretchr/testify/require"
)

func TestDurableCancellationOfQueuedRunNeverCreatesAttempt(t *testing.T) {
	f := newFixture(t, fake.Options{})
	first := executeAsync(f.executor, context.Background(), "slave-3")
	call := receiveCall(t, f.gateway)
	second := executeAsync(f.executor, context.Background(), "slave-7")
	require.Eventually(t, func() bool { return f.executor.Pending() == 1 }, time.Second, time.Millisecond)
	f.state(db.StateCommand{CancelRun: &db.CancelRun{RunID: "slave-7", Reason: "stopped queued poll"}})
	finishCall(t, call, executor.Outcome{Status: db.AttemptSucceeded})
	require.NoError(t, receiveReply(t, first).err)
	require.ErrorIs(t, receiveReply(t, second).err, executor.ErrRunCancelled)
	run, err := f.backend.GetRun("slave-7")
	require.NoError(t, err)
	require.Equal(t, db.RunCancelled, run.State)
	require.Zero(t, run.AttemptCount)
	noCall(t, f.gateway)
}

func TestCommittedStartRejectsCancellationDespiteStaleLocalRead(t *testing.T) {
	f := newFixture(t, fake.Options{})
	stale, err := f.backend.GetRun("slave-3")
	require.NoError(t, err)
	f.backend.staleRun.Store(stale)
	gate := newHold(true)
	f.backend.startGate.Store(gate)
	first := executeAsync(f.executor, context.Background(), "slave-3")
	await(t, gate.started)
	f.state(db.StateCommand{CancelRun: &db.CancelRun{RunID: "slave-3", Reason: "cancel before authorization"}})
	close(gate.release)
	result := receiveReply(t, first)
	require.ErrorIs(t, result.err, executor.ErrRunCancelled)
	require.True(t, result.result.Committed)
	require.Equal(t, db.AttemptCancelled, result.result.Outcome)
	run, err := f.backend.q.GetRun("slave-3")
	require.NoError(t, err)
	require.Equal(t, db.RunCancelled, run.State)
	attempt, err := f.backend.q.GetAttempt(run.LastAttemptID)
	require.NoError(t, err)
	require.True(t, attempt.NoDelivery)
	require.True(t, attempt.Quiescent)
	require.True(t, attempt.AuthorizedAt.IsZero())
	require.Zero(t, f.gateway.Stats().Started)
}

func TestDurableCancellationInterruptsExchangeButWaitsForCleanup(t *testing.T) {
	f := newFixture(t, fake.Options{})
	first := executeAsync(f.executor, context.Background(), "slave-3")
	call := receiveCall(t, f.gateway)
	second := executeAsync(f.executor, context.Background(), "slave-7")
	f.state(db.StateCommand{CancelRun: &db.CancelRun{RunID: "slave-3", Reason: "stop active write"}})
	await(t, call.Context.Done())
	await(t, call.RecoveryStarted)
	require.NoError(t, call.RecoveryContext.Err())
	channel, err := f.backend.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, call.Request.AttemptID, channel.ActiveAttemptID)
	run, err := f.backend.GetRun("slave-3")
	require.NoError(t, err)
	require.Equal(t, db.RunRunning, run.State)
	noCall(t, f.gateway)
	call.Recover <- nil
	await(t, call.CloseStarted)
	noCall(t, f.gateway)
	call.Close <- nil
	result := receiveReply(t, first)
	require.ErrorIs(t, result.err, executor.ErrCancellationRequested)
	require.ErrorIs(t, result.err, executor.ErrUnknownOutcome)
	require.True(t, result.result.Committed)
	require.Equal(t, db.AttemptUnknown, result.result.Outcome)
	next := receiveCall(t, f.gateway)
	require.Equal(t, "slave-7", next.Request.RunID)
	finishCall(t, next, executor.Outcome{Status: db.AttemptSucceeded})
	require.NoError(t, receiveReply(t, second).err)
	run, err = f.backend.GetRun("slave-3")
	require.NoError(t, err)
	require.True(t, run.CancellationRequested())
	require.Equal(t, db.RunUnknown, run.State)
	require.Equal(t, 1, f.gateway.Stats().MaxActive)
}

func TestCancellationCannotDetachExchangeOrEraseItsConfirmedSuccess(t *testing.T) {
	f := newFixture(t, fake.Options{IgnoreExchangeCancellation: true})
	first := executeAsync(f.executor, context.Background(), "slave-3")
	call := receiveCall(t, f.gateway)
	second := executeAsync(f.executor, context.Background(), "slave-7")
	f.state(db.StateCommand{CancelRun: &db.CancelRun{RunID: "slave-3"}})
	await(t, call.Context.Done())
	noCall(t, f.gateway)
	select {
	case <-call.RecoveryStarted:
		t.Fatal("exchange was detached")
	default:
	}
	finishCall(t, call, executor.Outcome{Status: db.AttemptSucceeded, Data: []byte("write applied")})
	result := receiveReply(t, first)
	require.NoError(t, result.err)
	require.Equal(t, db.AttemptSucceeded, result.result.Outcome)
	next := receiveCall(t, f.gateway)
	finishCall(t, next, executor.Outcome{Status: db.AttemptSucceeded})
	require.NoError(t, receiveReply(t, second).err)
	run, err := f.backend.GetRun("slave-3")
	require.NoError(t, err)
	require.Equal(t, db.RunSucceeded, run.State)
	require.True(t, run.CancellationRequested())
	require.Equal(t, 1, f.gateway.Stats().MaxActive)
}

func TestLostStartAcknowledgementRetriesIdenticalAuthorizationOnce(t *testing.T) {
	f := newFixture(t, fake.Options{})
	f.backend.loseStartAck.Store(true)
	first := executeAsync(f.executor, context.Background(), "slave-3")
	call := receiveCall(t, f.gateway)
	finishCall(t, call, executor.Outcome{Status: db.AttemptSucceeded})
	require.NoError(t, receiveReply(t, first).err)
	f.backend.mu.Lock()
	defer f.backend.mu.Unlock()
	var starts []db.StateCommand
	for _, c := range f.backend.commands {
		if c.StartAttempt != nil {
			starts = append(starts, c)
		}
	}
	require.Len(t, starts, 2)
	require.Equal(t, starts[0], starts[1])
	require.Equal(t, 1, f.gateway.Stats().Started)
}

func TestCancellationRacingNoDeliveryCompletionIsDerivedAtomically(t *testing.T) {
	f := newFixture(t, fake.Options{})
	renew, finish := newHold(true), newHold(true)
	f.backend.renewGate.Store(renew)
	f.backend.finishGate.Store(finish)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := executeAsync(f.executor, ctx, "slave-3")
	await(t, renew.started)
	cancel()
	require.ErrorIs(t, receiveReply(t, first).err, context.Canceled)
	close(renew.release)
	await(t, finish.started)
	// Finish was proposed as a failed, unstarted exchange. Its final outcome
	// must still account for a cancellation committed before that finish applies.
	f.state(db.StateCommand{CancelRun: &db.CancelRun{RunID: "slave-3", Reason: "late no-delivery cancellation"}})
	f.backend.finishGate.Store(nil)
	close(finish.release)
	require.Eventually(t, func() bool {
		run, err := f.backend.GetRun("slave-3")
		return err == nil && run.State == db.RunCancelled
	}, time.Second, time.Millisecond)
	run, err := f.backend.GetRun("slave-3")
	require.NoError(t, err)
	attempt, err := f.backend.q.GetAttempt(run.LastAttemptID)
	require.NoError(t, err)
	require.True(t, attempt.NoDelivery)
	require.Zero(t, f.gateway.Stats().Started)
	second := executeAsync(f.executor, context.Background(), "slave-7")
	call := receiveCall(t, f.gateway)
	finishCall(t, call, executor.Outcome{Status: db.AttemptSucceeded})
	require.NoError(t, receiveReply(t, second).err)
}
