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

func executionObservation(t *testing.T, observations <-chan executor.ExecutionObservation) executor.ExecutionObservation {
	t.Helper()
	select {
	case observation := <-observations:
		return observation
	case <-time.After(3 * time.Second):
		t.Fatal("worker observation did not arrive")
		return executor.ExecutionObservation{}
	}
}

func noExecutionReturn(t *testing.T, observations <-chan executor.ExecutionObservation) {
	t.Helper()
	select {
	case observation := <-observations:
		t.Fatalf("worker returned before cleanup and acknowledgement: %+v", observation)
	default:
	}
}

func TestExecutionObserverWaitsPastCallerAndCleanupCancellationForAcknowledgement(t *testing.T) {
	observations := make(chan executor.ExecutionObservation, 8)
	f := newFixture(t, fake.Options{IgnoreRecoveryCancellation: true}, func(c *executor.Config) {
		c.RecoveryTimeout = 30 * time.Millisecond
		c.OnExecution = func(o executor.ExecutionObservation) { observations <- o }
	})
	gate := newHold(false)
	f.backend.finishGate.Store(gate)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replies := executeAsync(f.executor, ctx, "slave-3")
	require.Equal(t, executor.ExecutionStarted, executionObservation(t, observations).Phase)
	call := receiveCall(t, f.gateway)
	call.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded}}
	await(t, call.RecoveryStarted)
	cancel()
	require.ErrorIs(t, receiveReply(t, replies).err, context.Canceled)
	await(t, call.RecoveryContext.Done()) // The adapter ignores its cleanup deadline.
	noExecutionReturn(t, observations)
	call.Recover <- nil
	await(t, call.CloseStarted)
	noExecutionReturn(t, observations)
	call.Close <- nil
	await(t, gate.started)
	noExecutionReturn(t, observations)
	close(gate.release)
	returned := executionObservation(t, observations)
	require.Equal(t, executor.ExecutionReturned, returned.Phase)
	require.Equal(t, "succeeded", returned.Result, "a known effect survives cleanup failure")
	require.True(t, returned.CleanupFailed)
	require.GreaterOrEqual(t, returned.Duration, f.cfg.RecoveryTimeout)
	run, err := f.backend.GetRun("slave-3")
	require.NoError(t, err)
	require.Equal(t, db.RunSucceeded, run.State)
	channel, err := f.backend.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, db.ChannelBlocked, channel.State)
}

func TestExecutionObserverDistinguishesRetryAndUnacknowledgedResults(t *testing.T) {
	for _, tc := range []struct {
		name     string
		outcome  executor.Outcome
		failAck  bool
		expected string
	}{
		{"failed", executor.Outcome{Status: db.AttemptFailed}, false, "failed"},
		{"unknown", executor.Outcome{Status: db.AttemptUnknown}, false, "unknown"},
		{"retry", executor.Outcome{Status: db.AttemptFailed, RetryClass: db.RetryNotSent}, false, "retry_wait"},
		{"unacknowledged", executor.Outcome{Status: db.AttemptSucceeded}, true, "result_uncertain"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observations := make(chan executor.ExecutionObservation, 8)
			f := newFixture(t, fake.Options{}, func(c *executor.Config) {
				c.OnExecution = func(o executor.ExecutionObservation) { observations <- o }
			})
			createRetryRun(f)
			f.backend.failFinish.Store(tc.failAck)
			replies := executeAsync(f.executor, context.Background(), "retry")
			require.Equal(t, executor.ExecutionStarted, executionObservation(t, observations).Phase)
			finishCall(t, receiveCall(t, f.gateway), tc.outcome)
			reply := receiveReply(t, replies)
			returned := executionObservation(t, observations)
			require.Equal(t, executor.ExecutionReturned, returned.Phase)
			require.Equal(t, tc.expected, returned.Result)
			require.False(t, returned.CleanupFailed)
			require.Positive(t, returned.Duration)
			require.Equal(t, !tc.failAck, reply.result.Committed)
			if tc.failAck {
				require.ErrorIs(t, reply.err, executor.ErrResultUncertain)
			}
		})
	}
}
