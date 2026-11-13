package executor_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/duravela/djob/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

type retryExecutionBackend struct{ *backend }

func (b retryExecutionBackend) ListDueRetries(at time.Time, after []byte, limit int) (db.RunPage, error) {
	return b.q.ListDueRetries(at, after, limit)
}

func createRetryRun(f *fixture) {
	f.state(db.StateCommand{EnqueueRun: &db.CreateRun{ID: "retry", ChannelID: "gateway", Executor: "fake",
		RetryPolicy: &db.RetryPolicy{MaxAttempts: 3, InitialBackoff: 150 * time.Millisecond, MaxBackoff: 300 * time.Millisecond}}})
}

func TestDurableRetryWaitReleasesGatewayAndUsesNewAttemptAuthorization(t *testing.T) {
	admissions := make(chan executor.AdmissionObservation, 4)
	f := newFixture(t, fake.Options{}, func(c *executor.Config) {
		c.OnAdmission = func(o executor.AdmissionObservation) { admissions <- o }
	})
	createRetryRun(f)
	f.backend.loseFinishAck.Store(true)
	first := executeAsync(f.executor, context.Background(), "retry")
	call := receiveCall(t, f.gateway)
	call.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptFailed, RetryClass: db.RetryNotSent}, Err: errors.New("connection refused before request")}
	await(t, call.RecoveryStarted)
	run, err := f.backend.GetRun("retry")
	require.NoError(t, err)
	require.Equal(t, db.RunRunning, run.State, "retry cannot be planned before physical cleanup")
	call.Recover <- nil
	await(t, call.CloseStarted)
	call.Close <- nil
	result := receiveReply(t, first)
	require.ErrorIs(t, result.err, executor.ErrRetryScheduled)
	require.True(t, result.result.Committed)
	require.False(t, result.result.RetryAt.IsZero())
	require.Equal(t, db.AttemptFailed, result.result.Outcome)
	require.Len(t, admissions, 1)
	require.True(t, (<-admissions).QueueTimeKnown)
	// Another unit can use this gateway while the first run waits in durable backoff.
	other := executeAsync(f.executor, context.Background(), "slave-7")
	second := receiveCall(t, f.gateway)
	require.Equal(t, "slave-7", second.Request.RunID)
	finishCall(t, second, executor.Outcome{Status: db.AttemptSucceeded})
	require.NoError(t, receiveReply(t, other).err)
	require.Len(t, admissions, 1)
	require.True(t, (<-admissions).QueueTimeKnown)
	deadlinePassed(t, result.result.RetryAt)
	promoter, err := scheduler.NewRetryPromoter(scheduler.RetryPromoterConfig{}, retryExecutionBackend{f.backend}, func() bool { return true })
	require.NoError(t, err)
	n, err := promoter.Tick(context.Background(), time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, 1, n)
	last := executeAsync(f.executor, context.Background(), "retry")
	third := receiveCall(t, f.gateway)
	require.NotEqual(t, call.Request.AttemptID, third.Request.AttemptID)
	finishCall(t, third, executor.Outcome{Status: db.AttemptSucceeded})
	require.NoError(t, receiveReply(t, last).err)
	require.Empty(t, admissions, "a retry attempt is not another initial queue-age observation")
	run, err = f.backend.GetRun("retry")
	require.NoError(t, err)
	require.Equal(t, db.RunSucceeded, run.State)
	require.EqualValues(t, 2, run.AttemptCount)
	prior, err := f.backend.q.GetAttempt(call.Request.AttemptID)
	require.NoError(t, err)
	require.Equal(t, db.AttemptFailed, prior.Outcome)
	require.Equal(t, db.RetryNotSent, prior.RetryClass)
	require.Equal(t, result.result.RetryAt, prior.RetryAt)
	require.False(t, prior.NoDelivery, "no-request-bytes differs from Exchange never being invoked")
	require.Equal(t, 1, f.gateway.Stats().MaxActive)
	f.backend.mu.Lock()
	defer f.backend.mu.Unlock()
	var finishes []db.StateCommand
	for _, c := range f.backend.commands {
		if c.Finish != nil && c.Finish.AttemptID == call.Request.AttemptID {
			finishes = append(finishes, c)
		}
	}
	require.Len(t, finishes, 2)
	require.Equal(t, finishes[0], finishes[1])
}

func TestRetryEvidenceCannotBypassRecoveryFailureOrClaimAnUnknownEffectSafe(t *testing.T) {
	for _, variant := range []string{"recovery", "invalid-evidence", "unknown"} {
		t.Run(variant, func(t *testing.T) {
			f := newFixture(t, fake.Options{})
			createRetryRun(f)
			first := executeAsync(f.executor, context.Background(), "retry")
			call := receiveCall(t, f.gateway)
			outcome := executor.Outcome{Status: db.AttemptFailed, RetryClass: db.RetryNotSent}
			if variant == "invalid-evidence" {
				outcome.Status = db.AttemptSucceeded
			}
			if variant == "unknown" {
				outcome.Status = db.AttemptUnknown
				outcome.RetryClass = ""
			}
			call.Reply <- fake.Response{Outcome: outcome}
			await(t, call.RecoveryStarted)
			if variant == "recovery" {
				call.Recover <- errors.New("cannot confirm quiet bus")
			} else {
				call.Recover <- nil
			}
			await(t, call.CloseStarted)
			call.Close <- nil
			result := receiveReply(t, first)
			require.NotErrorIs(t, result.err, executor.ErrRetryScheduled)
			require.True(t, result.result.RetryAt.IsZero())
			run, err := f.backend.GetRun("retry")
			require.NoError(t, err)
			if variant == "recovery" {
				require.ErrorIs(t, result.err, executor.ErrRecovery)
				channel, err := f.backend.GetChannel("gateway")
				require.NoError(t, err)
				require.Equal(t, db.ChannelBlocked, channel.State)
				require.Equal(t, db.RunFailed, run.State)
			} else {
				require.Equal(t, db.RunUnknown, run.State)
				require.ErrorIs(t, result.err, executor.ErrUnknownOutcome)
			}
			page, err := f.backend.q.ListDueRetries(time.Now().Add(time.Hour), nil, 10)
			require.NoError(t, err)
			require.Empty(t, page.Runs)
		})
	}
}

func TestCancellationRacingRetryCompletionPreventsAutomaticRetry(t *testing.T) {
	f := newFixture(t, fake.Options{})
	createRetryRun(f)
	gate := newHold(true)
	f.backend.finishGate.Store(gate)
	first := executeAsync(f.executor, context.Background(), "retry")
	call := receiveCall(t, f.gateway)
	finishCall(t, call, executor.Outcome{Status: db.AttemptFailed, RetryClass: db.RetryNotSent})
	await(t, gate.started)
	f.state(db.StateCommand{CancelRun: &db.CancelRun{RunID: "retry", Reason: "stop before backoff is committed"}})
	close(gate.release)
	result := receiveReply(t, first)
	require.NotErrorIs(t, result.err, executor.ErrRetryScheduled)
	run, err := f.backend.GetRun("retry")
	require.NoError(t, err)
	require.True(t, run.CancellationRequested())
	require.Equal(t, db.RunFailed, run.State)
	require.True(t, run.RetryAt.IsZero())
	page, err := f.backend.q.ListDueRetries(time.Now().Add(time.Hour), nil, 10)
	require.NoError(t, err)
	require.Empty(t, page.Runs)
}
