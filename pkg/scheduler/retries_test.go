package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/stretchr/testify/require"
)

type retryAckBackend struct {
	*runtimeBackend
	mu            sync.Mutex
	commands      []db.StateCommand
	lose, corrupt bool
	before        func(db.StateCommand)
}

func (b *retryAckBackend) ProposeStateCommand(ctx context.Context, command db.StateCommand) (db.StateResult, error) {
	if b.before != nil {
		before := b.before
		b.before = nil
		before(command)
	}
	result, err := b.runtimeBackend.ProposeStateCommand(ctx, command)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.commands = append(b.commands, command)
	if b.lose {
		b.lose = false
		return db.StateResult{}, context.DeadlineExceeded
	}
	if b.corrupt {
		result.Fingerprint[0] ^= 1
	}
	return result, err
}

func TestRetryReleaseObservationExcludesCancellationAfterDiscovery(t *testing.T) {
	f := newRuntime(t, fake.Options{}, "gateway")
	waitingRetry(f, "retry", time.Nanosecond)
	backend := &retryAckBackend{runtimeBackend: f.backend, before: func(db.StateCommand) {
		f.state(db.StateCommand{CancelRun: &db.CancelRun{RunID: "retry"}})
	}}
	var observations []DueWorkObservation
	p, err := NewRetryPromoter(RetryPromoterConfig{OnReleased: func(o DueWorkObservation) { observations = append(observations, o) }}, backend, func() bool { return true })
	require.NoError(t, err)
	n, err := p.Tick(context.Background(), time.Now().UTC())
	require.NoError(t, err)
	require.Zero(t, n)
	require.Empty(t, observations)
	run, err := f.backend.GetRun("retry")
	require.NoError(t, err)
	require.Equal(t, db.RunCancelled, run.State)
}

func waitingRetry(f *runtimeFixture, id string, delay time.Duration) *db.Run {
	f.t.Helper()
	channel, err := f.backend.GetChannel("gateway")
	require.NoError(f.t, err)
	if channel.State == db.ChannelAvailable {
		f.state(db.StateCommand{Acquire: &db.AcquireChannel{ChannelID: "gateway", OwnerID: "worker", ExpectedEpoch: channel.Epoch, LeaseUntil: time.Now().Add(time.Hour)}})
	}
	channel, err = f.backend.GetChannel("gateway")
	require.NoError(f.t, err)
	owner := db.ChannelOwner{ChannelID: "gateway", OwnerID: "worker", Epoch: channel.Epoch}
	f.state(db.StateCommand{EnqueueRun: &db.CreateRun{ID: id, ChannelID: "gateway", Executor: "fake", RetryPolicy: &db.RetryPolicy{MaxAttempts: 3, InitialBackoff: delay, MaxBackoff: delay}}})
	f.state(db.StateCommand{Claim: &db.ClaimRun{ChannelOwner: owner, RunID: id, AttemptID: id + "/attempt", ClaimToken: "token"}})
	f.state(db.StateCommand{StartAttempt: &db.StartAttempt{ChannelOwner: owner, RunID: id, AttemptID: id + "/attempt", ClaimToken: "token", DeadlineAware: true, RetryAware: true}})
	result := f.state(db.StateCommand{Finish: &db.FinishAttempt{ChannelOwner: owner, RunID: id, AttemptID: id + "/attempt", ClaimToken: "token", Outcome: db.AttemptFailed,
		Quiescent: true, CancellationAware: true, DeadlineAware: true, RetryAware: true, RetryClass: db.RetryNotSent}})
	return result.Run
}

func TestRetryPromoterPagesAndRetriesExactEnvelopeWithoutTouchingGateway(t *testing.T) {
	f := newRuntime(t, fake.Options{}, "gateway")
	for i := 0; i < 5; i++ {
		waitingRetry(f, fmt.Sprintf("run-%d", i), time.Nanosecond)
	}
	at := time.Now().UTC()
	before, err := f.backend.GetChannel("gateway")
	require.NoError(t, err)
	backend := &retryAckBackend{runtimeBackend: f.backend, lose: true}
	leader := false
	var observations []DueWorkObservation
	p, err := NewRetryPromoter(RetryPromoterConfig{BatchSize: 2, OnReleased: func(o DueWorkObservation) { observations = append(observations, o) }}, backend, func() bool { return leader })
	require.NoError(t, err)
	n, err := p.Tick(context.Background(), at)
	require.NoError(t, err)
	require.Zero(t, n)
	require.Empty(t, backend.commands)
	leader = true
	for _, expected := range []int{2, 2, 1, 0} {
		n, err := p.Tick(context.Background(), at)
		require.NoError(t, err)
		require.Equal(t, expected, n)
	}
	require.Len(t, backend.commands, 6)
	require.Equal(t, backend.commands[0], backend.commands[1])
	require.Len(t, observations, 5, "retrying an identical proposal after a lost acknowledgement adds one observation")
	for _, observation := range observations {
		require.True(t, observation.TimeKnown)
		require.GreaterOrEqual(t, observation.Lateness, time.Duration(0))
	}
	after, err := f.backend.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, before, after)
	ready, err := f.backend.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.EqualValues(t, 5, ready)
	pending, err := f.backend.PendingRunCount("gateway")
	require.NoError(t, err)
	require.EqualValues(t, 5, pending)
	require.Zero(t, f.gateways["gateway"].Stats().Started)
}

func TestRetryPromoterRejectsWrongReceiptAndRebuildsDiscoveryAfterLeadershipChange(t *testing.T) {
	f := newRuntime(t, fake.Options{}, "gateway")
	waitingRetry(f, "run", time.Nanosecond)
	backend := &retryAckBackend{runtimeBackend: f.backend, corrupt: true}
	leader := true
	var observations []DueWorkObservation
	p, err := NewRetryPromoter(RetryPromoterConfig{OnReleased: func(o DueWorkObservation) { observations = append(observations, o) }}, backend, func() bool { return leader })
	require.NoError(t, err)
	_, err = p.Tick(context.Background(), time.Now().UTC())
	require.ErrorContains(t, err, "receipt")
	run, err := f.backend.GetRun("run")
	require.NoError(t, err)
	require.Equal(t, db.RunReady, run.State, "uncertain acknowledgement cannot undo committed promotion")
	leader = false
	_, err = p.Tick(context.Background(), time.Now().UTC())
	require.NoError(t, err)
	require.Empty(t, p.cursor)
	leader = true
	backend.corrupt = false
	n, err := p.Tick(context.Background(), time.Now().UTC())
	require.NoError(t, err)
	require.Zero(t, n)
	require.Empty(t, observations, "a mismatched acknowledgement cannot be reconstructed from ready state after leadership changes")
}

func TestMaterializerCountsWaitingRetriesAgainstBacklog(t *testing.T) {
	f := newRuntime(t, fake.Options{}, "gateway")
	waitingRetry(f, "waiting", time.Hour)
	for i := 1; i < db.MaxReadyRunsPerChannel; i++ {
		f.ready(fmt.Sprintf("ready-%d", i), "gateway")
	}
	m, err := NewMaterializer(MaterializerConfig{}, f.backend, func() bool { return true })
	require.NoError(t, err)
	_, err = m.Put(context.Background(), db.ScheduleDefinition{ID: "poll", ChannelID: "gateway", Executor: "fake",
		Spec: db.ScheduleSpec{Kind: db.ScheduleOnce, StartAt: time.Now().Add(-time.Second)}, MissedPolicy: db.MissedCoalesce}, 0)
	require.NoError(t, err)
	stats, err := m.Tick(context.Background(), time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, 1, stats.Deferred)
	require.Zero(t, stats.Created)
	f.state(db.StateCommand{CancelRun: &db.CancelRun{RunID: "waiting"}})
	stats, err = m.Tick(context.Background(), time.Now().UTC())
	require.NoError(t, err)
	if stats.Advanced == 0 {
		stats, err = m.Tick(context.Background(), time.Now().UTC())
		require.NoError(t, err)
	}
	require.Equal(t, 1, stats.Created)
}
