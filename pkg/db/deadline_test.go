package db

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func deadlineRun(h *channelHarness, id string, deadline time.Time) StateResult {
	return h.ok(StateCommand{EnqueueRun: &CreateRun{ID: id, ChannelID: "gateway", Executor: "fake", StartDeadline: &deadline}})
}

func deadlineStart(owner ChannelOwner, run, attempt string) StateCommand {
	c := authorize(owner, run, attempt)
	c.StartAttempt.DeadlineAware = true
	return c
}

func deadlineFinish(owner ChannelOwner, outcome AttemptOutcome, noDelivery bool) StateCommand {
	c := finishAware(owner, outcome, noDelivery, true)
	c.Finish.DeadlineAware = true
	return c
}

func TestQueuedExpiryIsInclusiveAndDoesNotTouchAnOccupiedGateway(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway", "active", "no-deadline")
	deadline := h.at.Add(time.Minute)
	deadlineRun(h, "stale", deadline)
	h.ok(claim(owner, "active", "active-attempt"))
	before, err := h.db.GetChannel("gateway")
	require.NoError(t, err)
	expire := StateCommand{RequestID: "expire", At: deadline, ExpireRun: &ExpireRun{RunID: "stale", StartDeadline: deadline}}
	h.reject(StateCommand{ExpireRun: expire.ExpireRun}, "not_due")
	wrong := *expire.ExpireRun
	wrong.StartDeadline = deadline.Add(time.Second)
	h.reject(StateCommand{At: deadline, ExpireRun: &wrong}, "deadline_conflict")
	h.at = deadline
	h.reject(claim(owner, "stale", "too-late"), "channel_busy")
	result := h.ok(expire)
	require.Equal(t, RunExpired, result.Run.State)
	require.Zero(t, result.Run.AttemptCount)
	require.Equal(t, result, h.ok(expire))
	after, err := h.db.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, before, after)
	count, err := h.db.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	page, err := h.db.ListExpiredReadyRuns(deadline.Add(time.Hour), nil, 10)
	require.NoError(t, err)
	require.Empty(t, page.Runs)
	first, err := h.db.FirstReadyRun("gateway")
	require.NoError(t, err)
	require.Equal(t, "no-deadline", first.ID)
}

func TestDeadlineChecksClaimAndStartAndRequiresNoDeliveryForExpiredAttempt(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway")
	deadline := h.at.Add(time.Minute)
	deadlineRun(h, "run", deadline)
	lateClaim := claim(owner, "run", "late")
	lateClaim.At = deadline
	h.reject(lateClaim, "start_deadline_expired")
	h.ok(claim(owner, "run", "attempt"))
	h.reject(authorize(owner, "run", "attempt"), "invalid")
	h.at = deadline
	h.reject(deadlineStart(owner, "run", "attempt"), "start_deadline_expired")
	observed := h.ok(StateCommand{ExpireRun: &ExpireRun{RunID: "run", StartDeadline: deadline}})
	require.Equal(t, RunRunning, observed.Run.State, "expiry cannot detach a claimed attempt")
	h.reject(deadlineFinish(owner, AttemptExpired, false), "invalid")
	h.reject(finishAware(owner, AttemptFailed, true, true), "invalid")
	done := h.ok(deadlineFinish(owner, AttemptFailed, true))
	require.Equal(t, RunExpired, done.Run.State)
	require.Equal(t, AttemptExpired, done.Attempt.Outcome)
	require.True(t, done.Attempt.NoDelivery)
	require.True(t, done.Attempt.Quiescent)
	require.True(t, done.Attempt.AuthorizedAt.IsZero())
	require.Equal(t, ChannelOwned, done.Channel.State)
	require.Empty(t, done.Channel.ActiveAttemptID)
}

func TestStartDeadlineDoesNotReplaceAnEffectOrCancellation(t *testing.T) {
	for _, outcome := range []AttemptOutcome{AttemptSucceeded, AttemptFailed, AttemptUnknown} {
		t.Run(string(outcome), func(t *testing.T) {
			h := newChannelHarness(t)
			owner := h.setup("gateway")
			deadline := h.at.Add(time.Minute)
			deadlineRun(h, "run", deadline)
			h.ok(claim(owner, "run", "attempt"))
			h.ok(deadlineStart(owner, "run", "attempt"))
			h.at = deadline.Add(time.Second)
			done := h.ok(deadlineFinish(owner, outcome, false))
			require.Equal(t, outcome, done.Attempt.Outcome)
			require.False(t, done.Attempt.NoDelivery)
			late := h.ok(StateCommand{ExpireRun: &ExpireRun{RunID: "run", StartDeadline: deadline}})
			require.Equal(t, done.Run, late.Run)
			if outcome == AttemptUnknown {
				h.reject(StateCommand{ResolveRun: &ResolveRun{RunID: "run", AttemptID: "attempt", State: RunReady, Actor: "operator", Reason: "retry"}}, "start_deadline_expired")
			}
		})
	}
	h := newChannelHarness(t)
	owner := h.setup("gateway")
	deadline := h.at.Add(time.Minute)
	deadlineRun(h, "run", deadline)
	h.ok(claim(owner, "run", "attempt"))
	h.ok(StateCommand{CancelRun: &CancelRun{RunID: "run"}})
	h.at = deadline
	done := h.ok(deadlineFinish(owner, AttemptFailed, true))
	require.Equal(t, RunCancelled, done.Run.State, "committed cancellation wins for an unstarted attempt")
	require.Equal(t, AttemptCancelled, done.Attempt.Outcome)
}

func TestScheduledStartWindowIsAnchoredAndExpiredOccurrencesDoNotFillQueue(t *testing.T) {
	h := newChannelHarness(t)
	h.setup("gateway")
	start := h.at
	d := ScheduleDefinition{ID: "poll", ChannelID: "gateway", Executor: "fake", Spec: ScheduleSpec{Kind: ScheduleInterval, StartAt: start, Interval: time.Minute},
		MissedPolicy: MissedReplay, CatchUpLimit: 4, StartWindow: 30 * time.Second}
	put := h.ok(StateCommand{PutSchedule: &PutSchedule{Definition: d, NextAt: start}})
	h.at = start.Add(2*time.Minute + 15*time.Second)
	plan, err := IntervalPlan(*put.Schedule, h.at)
	require.NoError(t, err)
	require.True(t, plan.DeadlineAware)
	old := plan
	old.DeadlineAware = false
	h.reject(StateCommand{Materialize: &old}, "invalid")
	result := h.ok(StateCommand{Materialize: &plan})
	require.Len(t, result.Runs, 3)
	for i, run := range result.Runs {
		require.Equal(t, start.Add(time.Duration(i)*time.Minute+30*time.Second), run.StartDeadline)
		expected := RunExpired
		if i == 2 {
			expected = RunReady
		}
		require.Equal(t, expected, run.State)
	}
	count, err := h.db.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	// Fill the queue and materialize a fully elapsed window: terminal occurrence
	// bookkeeping can advance even when the ready queue has no remaining room.
	for i := 1; i < MaxReadyRunsPerChannel; i++ {
		h.ok(StateCommand{EnqueueRun: &CreateRun{ID: fmt.Sprintf("fill-%03d", i), ChannelID: "gateway", Executor: "fake"}})
	}
	h.at = start.Add(3*time.Minute + 31*time.Second)
	plan, err = IntervalPlan(*result.Schedule, h.at)
	require.NoError(t, err)
	result = h.ok(StateCommand{Materialize: &plan})
	require.Len(t, result.Runs, 1)
	require.Equal(t, RunExpired, result.Runs[0].State)
	require.Equal(t, start.Add(4*time.Minute), result.Schedule.NextAt)
	count, err = h.db.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.EqualValues(t, MaxReadyRunsPerChannel, count)
}

func TestDeadlineIndexSnapshotsAndMarkerRemainAtomic(t *testing.T) {
	h := newChannelHarness(t)
	h.setup("gateway")
	deadline := h.at.Add(time.Minute)
	deadlineRun(h, "run", deadline)
	command := StateCommand{RequestID: "expire-rollback", At: deadline, ExpireRun: &ExpireRun{RunID: "run", StartDeadline: deadline}}
	result := h.db.ApplyEntries([]commit.Entry{
		logEntry(t, h.index+1, Command{Version: DeadlineCommandVersion, Type: CmdState, State: &command}),
		logEntry(t, h.index+2, Command{Type: CmdAddJobs, Jobs: []Job{{ID: ""}}}),
	})
	require.Error(t, result.Error)
	require.Equal(t, h.index, h.db.LastAppliedIndex())
	page, err := h.db.ListExpiredReadyRuns(deadline, nil, 1)
	require.NoError(t, err)
	require.Len(t, page.Runs, 1)
	var data bytes.Buffer
	require.NoError(t, h.db.Snapshot(&data))
	copy, cleanup := NewTestDB(t)
	defer cleanup()
	require.NoError(t, copy.Restore(bytes.NewReader(data.Bytes())))
	restored, err := copy.ListExpiredReadyRuns(deadline, nil, 1)
	require.NoError(t, err)
	require.Equal(t, page, restored)
	var snapshot Snapshot
	require.NoError(t, gob.NewDecoder(bytes.NewReader(data.Bytes())).Decode(&snapshot))
	for i, bucket := range snapshot.Buckets {
		if bytes.Equal(bucket.Name, runDeadlinesBucket) {
			snapshot.Buckets = append(snapshot.Buckets[:i], snapshot.Buckets[i+1:]...)
			break
		}
	}
	broken, err := encode(snapshot)
	require.NoError(t, err)
	require.ErrorContains(t, copy.Restore(bytes.NewReader(broken)), "deadline state")
	stillReady, err := copy.GetRun("run")
	require.NoError(t, err)
	require.Equal(t, RunReady, stillReady.State, "failed restore must retain the prior database")
	h.ok(StateCommand{CancelRun: &CancelRun{RunID: "run"}}) // v5 must not lower the v6 marker.
	require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{3}, tx.Bucket(metadataBucket).Get(channelStateVersionKey))
		return nil
	}))
}

func TestDeadlineVersionsAndFingerprintsPreserveOlderRequests(t *testing.T) {
	h := newChannelHarness(t)
	h.setup("gateway")
	deadline := h.at.Add(time.Minute)
	c := StateCommand{RequestID: "create", At: h.at, EnqueueRun: &CreateRun{ID: "run", ChannelID: "gateway", Executor: "fake", StartDeadline: &deadline}}
	result := h.db.ApplyEntries([]commit.Entry{logEntry(t, h.index+1, Command{Version: CancellationCommandVersion, Type: CmdState, State: &c})})
	require.ErrorContains(t, result.Error, "version 6")
	require.Equal(t, h.index, h.db.LastAppliedIndex())
	fingerprint, err := c.Fingerprint()
	require.NoError(t, err)
	encoded, err := encode(c)
	require.NoError(t, err)
	var decoded StateCommand
	require.NoError(t, gob.NewDecoder(bytes.NewReader(encoded)).Decode(&decoded))
	zone := decoded.EnqueueRun.StartDeadline.In(time.FixedZone("other", 3600))
	decoded.EnqueueRun.StartDeadline = &zone
	other, err := decoded.Fingerprint()
	require.NoError(t, err)
	require.Equal(t, fingerprint, other)
	legacy := c
	run := *c.EnqueueRun
	run.StartDeadline = nil
	legacy.EnqueueRun = &run
	require.Equal(t, EnqueueCommandVersion, legacy.CommandVersion())
	text, err := json.Marshal(legacy)
	require.NoError(t, err)
	require.NotContains(t, string(text), "StartDeadline")
	require.NotContains(t, string(text), "ExpireRun")
	created := h.ok(c)
	require.Equal(t, created, h.ok(c))
	c.EnqueueRun.StartDeadline = &h.at
	h.reject(c, "request_conflict")
}

func TestExpiryOfPendingRetryPreservesEarlierUnknownAttempt(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway")
	deadline := h.at.Add(time.Minute)
	deadlineRun(h, "run", deadline)
	h.ok(claim(owner, "run", "attempt"))
	h.ok(deadlineStart(owner, "run", "attempt"))
	unknown := h.ok(deadlineFinish(owner, AttemptUnknown, false))
	h.ok(StateCommand{ResolveRun: &ResolveRun{RunID: "run", AttemptID: "attempt", State: RunReady, Actor: "operator", Reason: "explicitly authorize a safe retry"}})
	h.at = deadline
	done := h.ok(StateCommand{ExpireRun: &ExpireRun{RunID: "run", StartDeadline: deadline}})
	require.Equal(t, RunExpired, done.Run.State)
	require.EqualValues(t, 1, done.Run.AttemptCount)
	prior, err := h.db.GetAttempt("attempt")
	require.NoError(t, err)
	require.Equal(t, unknown.Attempt, prior, "expiry of a pending retry must not claim that earlier device effects were undone")
	require.False(t, prior.NoDelivery)
}
