package db

import (
	"bytes"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func authorize(owner ChannelOwner, run, attempt string) StateCommand {
	return StateCommand{StartAttempt: &StartAttempt{ChannelOwner: owner, RunID: run, AttemptID: attempt, ClaimToken: attempt + "-token"}}
}
func finishAware(owner ChannelOwner, outcome AttemptOutcome, noDelivery, quiescent bool) StateCommand {
	return StateCommand{Finish: &FinishAttempt{ChannelOwner: owner, RunID: "run", AttemptID: "attempt", ClaimToken: "attempt-token", Outcome: outcome,
		CancellationAware: true, NoDelivery: noDelivery, Quiescent: quiescent}}
}

func TestReadyCancellationFreesQueueAndPreservesOwnershipClock(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway", "run", "other")
	before, err := h.db.GetChannel("gateway")
	require.NoError(t, err)
	command := StateCommand{RequestID: "cancel", At: h.at.Add(-time.Hour), CancelRun: &CancelRun{RunID: "run", Reason: "operator stopped poll"}}
	result := h.ok(command)
	require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{2}, tx.Bucket(metadataBucket).Get(channelStateVersionKey))
		return nil
	}))
	require.Equal(t, RunCancelled, result.Run.State)
	require.True(t, result.Run.CancellationRequested())
	require.Zero(t, result.Run.AttemptCount)
	require.Equal(t, h.at, result.Run.LastTransition, "delayed timestamps cannot rewind the run clock")
	require.Equal(t, result, h.ok(command))
	count, err := h.db.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	first, err := h.db.FirstReadyRun("gateway")
	require.NoError(t, err)
	require.Equal(t, "other", first.ID)
	h.reject(claim(owner, "run", "cancelled-attempt"), "run_unavailable")
	after, err := h.db.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, before, after)
	command.CancelRun.Reason = "changed"
	h.reject(command, "request_conflict")
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	copy, cleanup := NewTestDB(t)
	defer cleanup()
	require.NoError(t, copy.Restore(bytes.NewReader(snapshot.Bytes())))
	run, err := copy.GetRun("run")
	require.NoError(t, err)
	require.Equal(t, result.Run, run)
	page, err := copy.ListReadyRuns(nil, 10)
	require.NoError(t, err)
	require.Len(t, page.Runs, 1)
	require.Equal(t, "other", page.Runs[0].ID)
}

func TestCancellationBeforeAuthorizationConfirmsNoDeliveryOnlyAtFinish(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway", "run")
	claimed := h.ok(claim(owner, "run", "attempt"))
	cancelled := h.ok(StateCommand{CancelRun: &CancelRun{RunID: "run", Reason: "do not send"}})
	require.Equal(t, RunRunning, cancelled.Run.State)
	channel, err := h.db.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, claimed.Channel, channel)
	h.reject(authorize(owner, "run", "attempt"), "cancel_requested")
	h.reject(finishAware(owner, AttemptCancelled, false, true), "invalid")
	done := h.ok(finishAware(owner, AttemptFailed, true, true))
	require.Equal(t, RunCancelled, done.Run.State)
	require.Equal(t, AttemptCancelled, done.Attempt.Outcome)
	require.True(t, done.Attempt.NoDelivery)
	require.Empty(t, done.Channel.ActiveAttemptID)
	require.Equal(t, ChannelOwned, done.Channel.State)
}

func TestCancellationCannotOverwriteKnownEffectOrReleaseUnrecoveredChannel(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway", "run")
	h.ok(claim(owner, "run", "attempt"))
	start := authorize(owner, "run", "attempt")
	start.RequestID = "start"
	first := h.ok(start)
	require.False(t, first.Attempt.AuthorizedAt.IsZero())
	require.Equal(t, first, h.ok(start))
	start.RequestID = "another-start"
	h.reject(start, "already_started")
	h.ok(StateCommand{CancelRun: &CancelRun{RunID: "run", Reason: "late request"}})
	finish := finishAware(owner, AttemptSucceeded, false, false)
	finish.Finish.Result = []byte{0, 0xff}
	done := h.ok(finish)
	require.Equal(t, RunSucceeded, done.Run.State)
	require.True(t, done.Run.CancellationRequested())
	require.Equal(t, []byte{0, 0xff}, done.Attempt.Result)
	require.False(t, done.Attempt.NoDelivery)
	require.Equal(t, ChannelBlocked, done.Channel.State)
	require.Equal(t, "attempt", done.Channel.ActiveAttemptID)
	late := h.ok(StateCommand{CancelRun: &CancelRun{RunID: "run", Reason: "after completion"}})
	require.Equal(t, done.Run, late.Run)
}

func TestUnknownCancellationRetainsIntentAndCannotAuthorizeRetry(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway", "run")
	h.ok(claim(owner, "run", "attempt"))
	h.ok(authorize(owner, "run", "attempt"))
	h.ok(finishAware(owner, AttemptUnknown, false, true))
	cancelled := h.ok(StateCommand{CancelRun: &CancelRun{RunID: "run"}})
	require.Equal(t, RunUnknown, cancelled.Run.State)
	require.True(t, cancelled.Run.CancellationRequested())
	h.reject(StateCommand{ResolveRun: &ResolveRun{RunID: "run", AttemptID: "attempt", State: RunReady, Actor: "operator", Reason: "retry"}}, "cancel_requested")
	h.ok(StateCommand{ResolveRun: &ResolveRun{RunID: "run", AttemptID: "attempt", State: RunSucceeded, Actor: "operator", Reason: "device confirms effect"}})
}

func TestCancellationVersionsAndCompletionAttestationsAreValidated(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway", "run")
	cancel := StateCommand{RequestID: "bad-version", At: h.at, CancelRun: &CancelRun{RunID: "run"}}
	result := h.db.ApplyEntries([]commit.Entry{logEntry(t, h.index+1, Command{Version: EnqueueCommandVersion, Type: CmdState, State: &cancel})})
	require.ErrorContains(t, result.Error, "version 5")
	require.Equal(t, h.index, h.db.LastAppliedIndex())
	h.ok(claim(owner, "run", "attempt"))
	h.reject(finishAware(owner, AttemptSucceeded, false, true), "invalid")
	h.reject(finishAware(owner, AttemptCancelled, true, true), "invalid")
	h.reject(finishAware(owner, AttemptUnknown, true, true), "invalid")
	wrong := authorize(owner, "run", "attempt")
	wrong.StartAttempt.ClaimToken = "wrong"
	h.reject(wrong, "stale_attempt")
	h.ok(authorize(owner, "run", "attempt"))
	// Historical v2 completions retain their meaning and request fingerprints.
	legacy := finishAware(owner, AttemptSucceeded, false, true)
	legacy.Finish.CancellationAware = false
	require.Equal(t, ChannelCommandVersion, legacy.CommandVersion())
	h.ok(legacy)
	before, err := h.db.GetRun("run")
	require.NoError(t, err)
	late := h.ok(StateCommand{CancelRun: &CancelRun{RunID: "run"}})
	require.Equal(t, before, late.Run)
	require.False(t, late.Run.CancellationRequested())
}

func TestCancelledOccurrenceIsNotRematerializedAndDoesNotPauseSchedule(t *testing.T) {
	h := newChannelHarness(t)
	h.setup("gateway")
	schedule := putInterval(h, "poll")
	plan, err := IntervalPlan(*schedule, h.at)
	require.NoError(t, err)
	materialized := StateCommand{RequestID: "materialize-before-cancel", Materialize: &plan}
	first := h.ok(materialized)
	require.Len(t, first.Runs, 1)
	h.ok(StateCommand{CancelRun: &CancelRun{RunID: first.Runs[0].ID}})
	h.ok(materialized) // Historical acknowledgement cannot recreate its ready row.
	count, err := h.db.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.Zero(t, count)
	h.at = h.at.Add(time.Minute)
	plan, err = IntervalPlan(*first.Schedule, h.at)
	require.NoError(t, err)
	next := h.ok(StateCommand{Materialize: &plan})
	require.Len(t, next.Runs, 1)
	require.NotEqual(t, first.Runs[0].ID, next.Runs[0].ID)
	require.False(t, next.Schedule.Paused)
	run, err := h.db.GetRun(first.Runs[0].ID)
	require.NoError(t, err)
	require.Equal(t, RunCancelled, run.State)
}

func TestCancellationRollbackIncludesFormatMarkerReceiptAndReadyIndex(t *testing.T) {
	h := newChannelHarness(t)
	h.setup("gateway", "run")
	command := StateCommand{RequestID: "rolled-back-cancel", At: h.at, CancelRun: &CancelRun{RunID: "run"}}
	result := h.db.ApplyEntries([]commit.Entry{
		logEntry(t, h.index+1, Command{Version: CancellationCommandVersion, Type: CmdState, State: &command}),
		logEntry(t, h.index+2, Command{Type: CmdAddJobs, Jobs: []Job{{ID: ""}}}),
	})
	require.Error(t, result.Error)
	require.Equal(t, h.index, h.db.LastAppliedIndex())
	run, err := h.db.GetRun("run")
	require.NoError(t, err)
	require.Equal(t, RunReady, run.State)
	require.False(t, run.CancellationRequested())
	require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{1}, tx.Bucket(metadataBucket).Get(channelStateVersionKey))
		require.Nil(t, tx.Bucket(requestsBucket).Get([]byte(command.RequestID)))
		return nil
	}))
	count, err := h.db.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	h.ok(command)
}

func TestCancellingAuthorizedRetryDoesNotRewritePriorUnknownAttempt(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway", "run")
	h.ok(claim(owner, "run", "attempt"))
	h.ok(authorize(owner, "run", "attempt"))
	prior := h.ok(finishAware(owner, AttemptUnknown, false, true)).Attempt
	h.ok(StateCommand{ResolveRun: &ResolveRun{RunID: "run", AttemptID: "attempt", State: RunReady, Actor: "operator", Reason: "explicitly authorize another try"}})
	cancelled := h.ok(StateCommand{CancelRun: &CancelRun{RunID: "run"}})
	require.Equal(t, RunCancelled, cancelled.Run.State)
	require.EqualValues(t, 1, cancelled.Run.AttemptCount)
	attempt, err := h.db.GetAttempt("attempt")
	require.NoError(t, err)
	require.Equal(t, prior, attempt)
	require.Equal(t, AttemptUnknown, attempt.Outcome)
	require.False(t, attempt.NoDelivery)
}
