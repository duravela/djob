package db

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func retryPolicy() *RetryPolicy {
	return &RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: 1500 * time.Millisecond}
}

func retryRun(h *channelHarness, deadline *time.Time) StateResult {
	return h.ok(StateCommand{EnqueueRun: &CreateRun{ID: "run", ChannelID: "gateway", Executor: "fake", RetryPolicy: retryPolicy(), StartDeadline: deadline}})
}

func retryStart(owner ChannelOwner, attempt string) StateCommand {
	c := deadlineStart(owner, "run", attempt)
	c.StartAttempt.RetryAware = true
	return c
}

func retryFinish(owner ChannelOwner, attempt string, outcome AttemptOutcome, class RetryClass, quiescent bool) StateCommand {
	return StateCommand{Finish: &FinishAttempt{ChannelOwner: owner, RunID: "run", AttemptID: attempt, ClaimToken: attempt + "-token", Outcome: outcome,
		Quiescent: quiescent, CancellationAware: true, DeadlineAware: true, RetryAware: true, RetryClass: class}}
}

func release(run *Run) StateCommand {
	return StateCommand{ReleaseRetry: &ReleaseRetry{RunID: run.ID, AttemptID: run.LastAttemptID, RetryAt: run.RetryAt}}
}

func TestAutomaticRetryRequiresExplicitNoSendEvidenceAndSuccessfulRecovery(t *testing.T) {
	for _, tc := range []struct {
		name      string
		policy    bool
		outcome   AttemptOutcome
		class     RetryClass
		quiescent bool
		state     RunState
	}{
		{"eligible", true, AttemptFailed, RetryNotSent, true, RunRetryWait},
		{"disabled", false, AttemptFailed, RetryNotSent, true, RunFailed},
		{"no-evidence", true, AttemptFailed, "", true, RunFailed},
		{"unknown", true, AttemptUnknown, "", true, RunUnknown},
		{"unrecovered", true, AttemptFailed, RetryNotSent, false, RunFailed},
		{"success", true, AttemptSucceeded, "", true, RunSucceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newChannelHarness(t)
			owner := h.setup("gateway")
			if tc.policy {
				retryRun(h, nil)
			} else {
				h.ok(StateCommand{EnqueueRun: &CreateRun{ID: "run", ChannelID: "gateway", Executor: "fake"}})
			}
			h.ok(claim(owner, "run", "attempt"))
			h.ok(retryStart(owner, "attempt"))
			result := h.ok(retryFinish(owner, "attempt", tc.outcome, tc.class, tc.quiescent))
			require.Equal(t, tc.state, result.Run.State)
			require.Equal(t, tc.outcome, result.Attempt.Outcome)
			require.Equal(t, tc.class, result.Attempt.RetryClass)
			if tc.state == RunRetryWait {
				require.Equal(t, h.at.Add(time.Second), result.Run.RetryAt)
				require.Equal(t, result.Run.RetryAt, result.Attempt.RetryAt)
				require.Empty(t, result.Channel.ActiveAttemptID)
				require.Equal(t, ChannelOwned, result.Channel.State)
			} else {
				require.True(t, result.Run.RetryAt.IsZero())
				require.True(t, result.Attempt.RetryAt.IsZero())
			}
			page, err := h.db.ListDueRetries(h.at.Add(time.Hour), nil, 10)
			require.NoError(t, err)
			if tc.state == RunRetryWait {
				require.Len(t, page.Runs, 1)
			} else {
				require.Empty(t, page.Runs)
			}
			if !tc.quiescent {
				require.Equal(t, ChannelBlocked, result.Channel.State)
			}
		})
	}
}

func TestRetryBackoffLimitAndStaleReleaseAreDurable(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway")
	retryRun(h, nil)
	var previous StateCommand
	for i := 1; i <= 3; i++ {
		attempt := fmt.Sprintf("attempt-%d", i)
		h.ok(claim(owner, "run", attempt))
		h.ok(retryStart(owner, attempt))
		finish := retryFinish(owner, attempt, AttemptFailed, RetryNotSent, true)
		finish.RequestID, finish.At = fmt.Sprintf("finish-%d", i), h.at
		result := h.ok(finish)
		require.Equal(t, result, h.ok(finish), "a repeated completion cannot extend backoff")
		if i == 3 {
			require.Equal(t, RunFailed, result.Run.State)
			require.True(t, result.Run.RetryAt.IsZero())
			break
		}
		expected := time.Second
		if i == 2 {
			expected = 1500 * time.Millisecond
		}
		require.Equal(t, expected, result.Run.RetryAt.Sub(h.at))
		if previous.ReleaseRetry != nil {
			h.reject(previous, "retry_conflict")
		}
		next := release(result.Run)
		h.reject(next, "not_due")
		wrong := *next.ReleaseRetry
		wrong.AttemptID = "wrong"
		h.reject(StateCommand{ReleaseRetry: &wrong}, "retry_conflict")
		h.at = result.Run.RetryAt
		before, err := h.db.GetChannel("gateway")
		require.NoError(t, err)
		promoted := h.ok(next)
		require.Equal(t, RunReady, promoted.Run.State)
		require.True(t, promoted.Run.RetryAt.IsZero())
		after, err := h.db.GetChannel("gateway")
		require.NoError(t, err)
		require.Equal(t, before, after)
		previous = next
	}
	page, err := h.db.ListDueRetries(h.at.Add(time.Hour), nil, 10)
	require.NoError(t, err)
	require.Empty(t, page.Runs)
	run, err := h.db.GetRun("run")
	require.NoError(t, err)
	require.EqualValues(t, 3, run.AttemptCount)
}

func TestRetryWaitCancellationAndDeadlineLeaveEarlierAttemptIntact(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprint(cancel), func(t *testing.T) {
			h := newChannelHarness(t)
			owner := h.setup("gateway")
			deadline := h.at.Add(500 * time.Millisecond)
			retryRun(h, &deadline)
			h.ok(claim(owner, "run", "attempt"))
			h.ok(retryStart(owner, "attempt"))
			waiting := h.ok(retryFinish(owner, "attempt", AttemptFailed, RetryNotSent, true))
			expected := RunExpired
			if cancel {
				h.ok(StateCommand{CancelRun: &CancelRun{RunID: "run", Reason: "stop pending retry"}})
				expected = RunCancelled
			} else {
				h.at = deadline
				h.reject(StateCommand{ExpireRun: &ExpireRun{RunID: "run", StartDeadline: deadline}}, "invalid")
				h.ok(StateCommand{ExpireRun: &ExpireRun{RunID: "run", StartDeadline: deadline, RetryAware: true}})
			}
			h.at = waiting.Run.RetryAt
			result := h.ok(release(waiting.Run))
			require.Equal(t, expected, result.Run.State)
			require.EqualValues(t, 1, result.Run.AttemptCount)
			attempt, err := h.db.GetAttempt("attempt")
			require.NoError(t, err)
			require.Equal(t, waiting.Attempt, attempt)
			pending, err := h.db.PendingRunCount("gateway")
			require.NoError(t, err)
			require.Zero(t, pending)
			page, err := h.db.ListExpiredReadyRuns(h.at, nil, 10)
			require.NoError(t, err)
			require.Empty(t, page.Runs)
		})
	}
}

func TestRetryEnabledAttemptReservesBacklogCapacityThroughBackoff(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway")
	retryRun(h, nil)
	for i := 1; i < MaxReadyRunsPerChannel; i++ {
		h.ok(StateCommand{EnqueueRun: &CreateRun{ID: fmt.Sprintf("other-%03d", i), ChannelID: "gateway", Executor: "fake"}})
	}
	h.ok(claim(owner, "run", "attempt"))
	h.ok(retryStart(owner, "attempt"))
	newRun := StateCommand{EnqueueRun: &CreateRun{ID: "overflow", ChannelID: "gateway", Executor: "fake"}}
	h.reject(newRun, "backpressure")
	waiting := h.ok(retryFinish(owner, "attempt", AttemptFailed, RetryNotSent, true))
	h.reject(newRun, "backpressure")
	count, err := h.db.PendingRunCount("gateway")
	require.NoError(t, err)
	require.EqualValues(t, MaxReadyRunsPerChannel, count)
	ready, err := h.db.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.EqualValues(t, MaxReadyRunsPerChannel-1, ready)
	h.at = waiting.Run.RetryAt
	h.ok(release(waiting.Run))
	ready, err = h.db.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.EqualValues(t, MaxReadyRunsPerChannel, ready)
}

func TestRetrySnapshotIndexesMarkersAndReceiptsAreAtomic(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway")
	retryRun(h, nil)
	h.ok(claim(owner, "run", "attempt"))
	h.ok(retryStart(owner, "attempt"))
	command := retryFinish(owner, "attempt", AttemptFailed, RetryNotSent, true)
	command.RequestID, command.At = "finish", h.at
	result := h.db.ApplyEntries([]commit.Entry{
		logEntry(t, h.index+1, Command{Version: RetryCommandVersion, Type: CmdState, State: &command}),
		logEntry(t, h.index+2, Command{Type: CmdAddJobs, Jobs: []Job{{ID: ""}}}),
	})
	require.Error(t, result.Error)
	run, err := h.db.GetRun("run")
	require.NoError(t, err)
	require.Equal(t, RunRunning, run.State)
	page, err := h.db.ListDueRetries(h.at.Add(time.Hour), nil, 10)
	require.NoError(t, err)
	require.Empty(t, page.Runs)
	finished := h.ok(command)
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	copy, cleanup := NewTestDB(t)
	defer cleanup()
	require.NoError(t, copy.Restore(bytes.NewReader(snapshot.Bytes())))
	page, err = copy.ListDueRetries(finished.Run.RetryAt, nil, 10)
	require.NoError(t, err)
	require.Len(t, page.Runs, 1)
	require.Equal(t, *finished.Run, page.Runs[0])
	var broken Snapshot
	require.NoError(t, gob.NewDecoder(bytes.NewReader(snapshot.Bytes())).Decode(&broken))
	for i, bucket := range broken.Buckets {
		if bytes.Equal(bucket.Name, retryDueBucket) {
			broken.Buckets = append(broken.Buckets[:i], broken.Buckets[i+1:]...)
			break
		}
	}
	data, err := encode(broken)
	require.NoError(t, err)
	require.ErrorContains(t, copy.Restore(bytes.NewReader(data)), "retry state")
	h.ok(StateCommand{CancelRun: &CancelRun{RunID: "run"}})
	require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{4}, tx.Bucket(metadataBucket).Get(channelStateVersionKey))
		return nil
	}))
}

func TestRetryPolicyValidationVersionsAndEvidenceAreStrict(t *testing.T) {
	for _, p := range []*RetryPolicy{
		{MaxAttempts: 0, InitialBackoff: time.Second, MaxBackoff: time.Second},
		{MaxAttempts: 33, InitialBackoff: time.Second, MaxBackoff: time.Second},
		{MaxAttempts: 3, InitialBackoff: 0, MaxBackoff: time.Second},
		{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: time.Millisecond},
		{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: time.Hour + 1},
	} {
		require.Error(t, p.Validate())
	}
	p := &RetryPolicy{MaxAttempts: 32, InitialBackoff: time.Nanosecond, MaxBackoff: time.Hour}
	require.NoError(t, p.Validate())
	require.Equal(t, time.Hour, p.Backoff(^uint64(0)))
	h := newChannelHarness(t)
	owner := h.setup("gateway")
	command := StateCommand{RequestID: "old-envelope", At: h.at, EnqueueRun: &CreateRun{ID: "run", ChannelID: "gateway", Executor: "fake", RetryPolicy: retryPolicy()}}
	result := h.db.ApplyEntries([]commit.Entry{logEntry(t, h.index+1, Command{Version: DeadlineCommandVersion, Type: CmdState, State: &command})})
	require.ErrorContains(t, result.Error, "version 7")
	retryRun(h, nil)
	h.ok(claim(owner, "run", "attempt"))
	h.reject(deadlineStart(owner, "run", "attempt"), "invalid")
	h.ok(retryStart(owner, "attempt"))
	h.reject(retryFinish(owner, "attempt", AttemptSucceeded, RetryNotSent, true), "invalid")
	h.reject(retryFinish(owner, "attempt", AttemptUnknown, RetryNotSent, true), "invalid")
	h.reject(retryFinish(owner, "attempt", AttemptFailed, RetryClass("read-safe"), true), "invalid")
	legacy := retryFinish(owner, "attempt", AttemptFailed, "", true)
	legacy.Finish.RetryAware = false
	h.reject(legacy, "invalid")
}

func TestScheduledRetryPoliciesAreImmutableAndClonedPerRunAndReceipt(t *testing.T) {
	h := newChannelHarness(t)
	h.setup("gateway")
	policy := retryPolicy()
	d := ScheduleDefinition{ID: "poll", ChannelID: "gateway", Executor: "fake", Spec: ScheduleSpec{Kind: ScheduleInterval, StartAt: h.at, Interval: time.Minute},
		MissedPolicy: MissedReplay, CatchUpLimit: 2, RetryPolicy: policy}
	configured := h.ok(StateCommand{PutSchedule: &PutSchedule{Definition: d, NextAt: h.at}})
	policy.MaxAttempts = 32
	h.at = h.at.Add(time.Minute)
	plan, err := IntervalPlan(*configured.Schedule, h.at)
	require.NoError(t, err)
	old := plan
	old.RetryAware = false
	h.reject(StateCommand{Materialize: &old}, "invalid")
	result := h.ok(StateCommand{Materialize: &plan})
	require.Len(t, result.Runs, 2)
	clone := result.Clone()
	clone.Runs[0].RetryPolicy.MaxAttempts = 9
	require.EqualValues(t, 3, result.Runs[0].RetryPolicy.MaxAttempts)
	require.EqualValues(t, 3, clone.Runs[1].RetryPolicy.MaxAttempts)
	d.RetryPolicy = &RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Second, MaxBackoff: time.Second}
	h.ok(StateCommand{PutSchedule: &PutSchedule{Definition: d, ExpectedRevision: 1, NextAt: h.at.Add(time.Minute)}})
	for _, run := range result.Runs {
		stored, err := h.db.GetRun(run.ID)
		require.NoError(t, err)
		require.EqualValues(t, 3, stored.RetryPolicy.MaxAttempts, "definition updates only affect future occurrences")
	}
}

func TestPreRetryFormatRebuildsCountersAfterAnOlderWriterChangesQueue(t *testing.T) {
	h := newChannelHarness(t)
	h.setup("gateway", "first")
	// A pre-v7 writer updates its known ready indexes, leaving the new derived
	// counter unchanged. This can happen after a controller-only upgrade/downgrade
	// before any v7 operation activates the stricter storage marker.
	require.NoError(t, h.db.db.Update(func(tx *bbolt.Tx) error {
		run := Run{ID: "older-writer", ChannelID: "gateway", Executor: "fake", State: RunReady, CreatedAt: h.at, LastTransition: h.at}
		if err := writeRecord(tx, runsBucket, run.ID, run); err != nil {
			return err
		}
		if err := tx.Bucket(readyRunsBucket).Put(runKey(run), []byte(run.ID)); err != nil {
			return err
		}
		if err := tx.Bucket(readyByChannelBucket).Put(channelRunKey(run), []byte(run.ID)); err != nil {
			return err
		}
		return changeReadyCount(tx, run.ChannelID, 1)
	}))
	count, err := h.db.PendingRunCount("gateway")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	copy, cleanup := NewTestDB(t)
	defer cleanup()
	require.NoError(t, copy.Restore(bytes.NewReader(snapshot.Bytes())))
	count, err = copy.PendingRunCount("gateway")
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
}
