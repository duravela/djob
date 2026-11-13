package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/stretchr/testify/require"
)

type expiryBackend struct {
	*runtimeBackend
	mu       sync.Mutex
	commands []db.StateCommand
	lose     bool
	corrupt  bool
}

func (b *expiryBackend) ProposeStateCommand(ctx context.Context, command db.StateCommand) (db.StateResult, error) {
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

func TestDeadlineSweepPagesWithoutOwnershipAndRetriesAnIdenticalRequest(t *testing.T) {
	f := newRuntime(t, fake.Options{}, "gateway")
	at := time.Now().UTC()
	deadline := at.Add(-time.Second)
	// Persist the runs using their original creation time, before the deadline.
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		_, err := f.backend.ProposeStateCommand(context.Background(), db.StateCommand{RequestID: "enqueue/" + id, At: at.Add(-time.Minute),
			EnqueueRun: &db.CreateRun{ID: id, ChannelID: "gateway", Executor: "fake", StartDeadline: &deadline}})
		require.NoError(t, err)
	}
	f.ready("no-deadline", "gateway")
	before, err := f.backend.GetChannel("gateway")
	require.NoError(t, err)
	backend := &expiryBackend{runtimeBackend: f.backend, lose: true}
	leader := false
	sweeper, err := NewDeadlineSweeper(DeadlineSweeperConfig{BatchSize: 2}, backend, func() bool { return leader })
	require.NoError(t, err)
	n, err := sweeper.Tick(context.Background(), at)
	require.NoError(t, err)
	require.Zero(t, n)
	require.Empty(t, backend.commands)
	leader = true
	for _, expected := range []int{2, 2, 1, 0} {
		n, err := sweeper.Tick(context.Background(), at)
		require.NoError(t, err)
		require.Equal(t, expected, n)
	}
	require.Len(t, backend.commands, 6)
	require.Equal(t, backend.commands[0], backend.commands[1], "lost ack must retry the complete original envelope")
	after, err := f.backend.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, before, after)
	count, err := f.backend.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	require.Zero(t, f.gateways["gateway"].Stats().Started)
}

func TestDeadlineSweepRejectsMismatchedReceiptsAndRevisitsAfterLeaderChange(t *testing.T) {
	f := newRuntime(t, fake.Options{}, "gateway")
	at, deadline := time.Now().UTC(), time.Now().UTC().Add(-time.Second)
	_, err := f.backend.ProposeStateCommand(context.Background(), db.StateCommand{RequestID: "enqueue", At: at.Add(-time.Minute),
		EnqueueRun: &db.CreateRun{ID: "run", ChannelID: "gateway", Executor: "fake", StartDeadline: &deadline}})
	require.NoError(t, err)
	backend := &expiryBackend{runtimeBackend: f.backend, corrupt: true}
	leader := true
	sweeper, err := NewDeadlineSweeper(DeadlineSweeperConfig{}, backend, func() bool { return leader })
	require.NoError(t, err)
	_, err = sweeper.Tick(context.Background(), at)
	require.ErrorContains(t, err, "receipt")
	run, err := f.backend.GetRun("run")
	require.NoError(t, err)
	require.Equal(t, db.RunExpired, run.State, "bad acknowledgement must not undo committed expiry")
	leader = false
	_, err = sweeper.Tick(context.Background(), at)
	require.NoError(t, err)
	require.Empty(t, sweeper.cursor)
	backend.corrupt = false
	leader = true
	n, err := sweeper.Tick(context.Background(), at)
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestMaterializerAdvancesExpiredStartWindowsWhenGatewayQueueIsFull(t *testing.T) {
	f := newRuntime(t, fake.Options{}, "gateway")
	for i := 0; i < db.MaxReadyRunsPerChannel; i++ {
		f.ready(time.Unix(int64(i), 0).String(), "gateway")
	}
	at := time.Now().UTC()
	m, err := NewMaterializer(MaterializerConfig{}, f.backend, func() bool { return true })
	require.NoError(t, err)
	_, err = m.Put(context.Background(), db.ScheduleDefinition{ID: "old-poll", ChannelID: "gateway", Executor: "fake",
		Spec: db.ScheduleSpec{Kind: db.ScheduleOnce, StartAt: at.Add(-time.Hour)}, MissedPolicy: db.MissedCoalesce, StartWindow: time.Minute}, 0)
	require.NoError(t, err)
	stats, err := m.Tick(context.Background(), time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, 1, stats.Advanced)
	require.Equal(t, 1, stats.Created)
	require.Zero(t, stats.Deferred)
	count, err := f.backend.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.EqualValues(t, db.MaxReadyRunsPerChannel, count)
	schedule, err := f.backend.GetSchedule("old-poll")
	require.NoError(t, err)
	require.True(t, schedule.Completed)
	run, err := f.backend.GetRun(db.OccurrenceRunID(schedule.ID, schedule.Revision, schedule.Spec.StartAt))
	require.NoError(t, err)
	require.Equal(t, db.RunExpired, run.State)
}
