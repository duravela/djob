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

type deadlineAckBackend struct {
	*backend
	claimAck, startAck *hold
}

func (b *deadlineAckBackend) ProposeStateCommand(ctx context.Context, c db.StateCommand) (db.StateResult, error) {
	result, err := b.backend.ProposeStateCommand(ctx, c)
	if err != nil {
		return result, err
	}
	if c.Claim != nil && b.claimAck != nil {
		_ = b.claimAck.wait(ctx)
	}
	if c.StartAttempt != nil && b.startAck != nil {
		_ = b.startAck.wait(ctx)
	}
	return result, nil
}

func createDeadlineRun(f *fixture, at time.Time) {
	f.state(db.StateCommand{EnqueueRun: &db.CreateRun{ID: "deadline", ChannelID: "gateway", Executor: "fake", StartDeadline: &at}})
}

func deadlinePassed(t *testing.T, at time.Time) {
	t.Helper()
	require.Eventually(t, func() bool { return !time.Now().Before(at) }, 2*time.Second, 5*time.Millisecond)
}

func TestExpiredQueuedRunDoesNotAcquireAnAttemptOrDelayLaterWork(t *testing.T) {
	f := newFixture(t, fake.Options{}, func(c *executor.Config) { c.OperationTimeout = 3 * time.Second })
	deadline := time.Now().Add(400 * time.Millisecond)
	createDeadlineRun(f, deadline)
	first := executeAsync(f.executor, context.Background(), "slave-3")
	call := receiveCall(t, f.gateway)
	queued := executeAsync(f.executor, context.Background(), "deadline")
	require.Eventually(t, func() bool { return f.executor.Pending() == 1 }, time.Second, time.Millisecond)
	deadlinePassed(t, deadline)
	finishCall(t, call, executor.Outcome{Status: db.AttemptSucceeded})
	require.NoError(t, receiveReply(t, first).err)
	reply := receiveReply(t, queued)
	require.ErrorIs(t, reply.err, executor.ErrRunExpired)
	require.Equal(t, db.AttemptExpired, reply.result.Outcome)
	require.Empty(t, reply.result.AttemptID)
	run, err := f.backend.GetRun("deadline")
	require.NoError(t, err)
	require.Equal(t, db.RunExpired, run.State)
	require.Zero(t, run.AttemptCount)
	next := executeAsync(f.executor, context.Background(), "slave-7")
	call = receiveCall(t, f.gateway)
	require.Equal(t, "slave-7", call.Request.RunID)
	finishCall(t, call, executor.Outcome{Status: db.AttemptSucceeded})
	require.NoError(t, receiveReply(t, next).err)
	require.Equal(t, 2, f.gateway.Stats().Started)
}

func TestDelayedClaimOrStartAcknowledgementCannotExtendTheStartDeadline(t *testing.T) {
	for _, phase := range []string{"claim", "start"} {
		t.Run(phase, func(t *testing.T) {
			f := newFixture(t, fake.Options{})
			require.NoError(t, f.executor.Close(context.Background()))
			gate := newHold(true)
			t.Cleanup(func() {
				select {
				case <-gate.release:
				default:
					close(gate.release)
				}
			})
			backend := &deadlineAckBackend{backend: f.backend}
			if phase == "claim" {
				backend.claimAck = gate
			} else {
				backend.startAck = gate
			}
			worker, err := executor.New(context.Background(), f.cfg, backend, f.gateway.Factory)
			require.NoError(t, err)
			f.executor = worker
			deadline := time.Now().Add(400 * time.Millisecond)
			createDeadlineRun(f, deadline)
			first := executeAsync(worker, context.Background(), "deadline")
			await(t, gate.started)
			deadlinePassed(t, deadline)
			close(gate.release)
			reply := receiveReply(t, first)
			require.ErrorIs(t, reply.err, executor.ErrRunExpired)
			require.True(t, reply.result.Committed)
			require.Equal(t, db.AttemptExpired, reply.result.Outcome)
			run, err := f.backend.GetRun("deadline")
			require.NoError(t, err)
			attempt, err := f.backend.q.GetAttempt(run.LastAttemptID)
			require.NoError(t, err)
			require.True(t, attempt.NoDelivery)
			require.True(t, attempt.Quiescent)
			require.Equal(t, phase == "claim", attempt.AuthorizedAt.IsZero())
			require.Zero(t, f.gateway.Stats().Started)
		})
	}
}

func TestDeadlineIsRecheckedAfterFactoryAndBeforeExchange(t *testing.T) {
	f := newFixture(t, fake.Options{})
	require.NoError(t, f.executor.Close(context.Background()))
	gate := newHold(true)
	t.Cleanup(func() {
		select {
		case <-gate.release:
		default:
			close(gate.release)
		}
	})
	worker, err := executor.New(context.Background(), f.cfg, f.backend, func(channel db.CommunicationChannel) (executor.Transport, error) {
		_ = gate.wait(context.Background())
		return f.gateway.Factory(channel)
	})
	require.NoError(t, err)
	f.executor = worker
	deadline := time.Now().Add(400 * time.Millisecond)
	createDeadlineRun(f, deadline)
	first := executeAsync(worker, context.Background(), "deadline")
	await(t, gate.started)
	deadlinePassed(t, deadline)
	close(gate.release)
	reply := receiveReply(t, first)
	require.ErrorIs(t, reply.err, executor.ErrRunExpired)
	require.Equal(t, db.AttemptExpired, reply.result.Outcome)
	require.True(t, reply.result.Quiescent)
	require.Zero(t, f.gateway.Stats().Started)
}

func TestPassingStartDeadlineDoesNotCancelAnExchangeAlreadyInProgress(t *testing.T) {
	f := newFixture(t, fake.Options{}, func(c *executor.Config) { c.OperationTimeout = 3 * time.Second })
	deadline := time.Now().Add(400 * time.Millisecond)
	createDeadlineRun(f, deadline)
	first := executeAsync(f.executor, context.Background(), "deadline")
	call := receiveCall(t, f.gateway)
	deadlinePassed(t, deadline)
	require.NoError(t, call.Context.Err(), "start deadline must not become an execution timeout")
	result := f.state(db.StateCommand{ExpireRun: &db.ExpireRun{RunID: "deadline", StartDeadline: deadline}})
	require.Equal(t, db.RunRunning, result.Run.State)
	finishCall(t, call, executor.Outcome{Status: db.AttemptSucceeded, Data: []byte("write acknowledged")})
	reply := receiveReply(t, first)
	require.NoError(t, reply.err)
	require.Equal(t, db.AttemptSucceeded, reply.result.Outcome)
	require.Equal(t, []byte("write acknowledged"), reply.result.Data)
}
