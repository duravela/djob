package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/duravela/djob/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

func schedulerPhase(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("gateway phase did not start")
	}
}

func schedulerCall(t *testing.T, gateway *fake.Gateway) *fake.Call {
	t.Helper()
	select {
	case call := <-gateway.Calls:
		return call
	case <-time.After(4 * time.Second):
		t.Fatal("scheduled exchange did not start")
		return nil
	}
}

func TestRaftScheduleLeaderChangeDispatchesEachOccurrenceOnce(t *testing.T) {
	nodes := startChannelCluster(t)
	proposeChannel(t, nodes[0], db.StateCommand{RequestID: "schedule-configure", At: time.Now().UTC(),
		Configure: &db.ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "gateway:502"}})
	materializers := make([]*scheduler.Materializer, len(nodes))
	for i, node := range nodes {
		var err error
		materializers[i], err = scheduler.NewMaterializer(scheduler.MaterializerConfig{CommitTimeout: time.Second}, node.store, node.raft.IsLeader)
		require.NoError(t, err)
	}
	definitions := []db.ScheduleDefinition{
		{ID: "slave-3", ChannelID: "gateway", Executor: "fake", Payload: []byte{3},
			Spec: db.ScheduleSpec{Kind: db.ScheduleOnce, StartAt: time.Now().Add(-time.Minute)}, MissedPolicy: db.MissedCoalesce},
		{ID: "slave-7", ChannelID: "gateway", Executor: "fake", Payload: []byte{7},
			Spec: db.ScheduleSpec{Kind: db.ScheduleInterval, StartAt: time.Now().Add(-time.Hour), Interval: time.Hour}, MissedPolicy: db.MissedCoalesce},
	}
	for _, definition := range definitions {
		_, err := materializers[0].Put(context.Background(), definition, 0)
		require.NoError(t, err)
	}
	var leader int
	require.Eventually(t, func() bool {
		for i, node := range nodes {
			if node.raft.IsLeader() {
				leader = i
				return true
			}
		}
		return false
	}, 3*time.Second, 10*time.Millisecond)
	// A successful proposal waits for its caller's apply, not every replica.
	// Leadership can be observed before the leader has applied that entry.
	require.Eventually(t, func() bool {
		for _, definition := range definitions {
			if schedule, err := nodes[leader].store.GetSchedule(definition.ID); err != nil || schedule == nil {
				return false
			}
		}
		return true
	}, 3*time.Second, time.Millisecond)
	stats, err := materializers[leader].Tick(context.Background(), time.Now())
	require.NoError(t, err)
	require.Equal(t, 2, stats.Created)
	require.Eventually(t, func() bool {
		for _, node := range nodes {
			count, err := node.store.ReadyRunCount("gateway")
			if err != nil || count != 2 {
				return false
			}
		}
		return true
	}, 3*time.Second, 10*time.Millisecond)
	nodes[leader].close()
	require.False(t, nodes[leader].raft.IsLeader(), "stopped materializers must relinquish leadership")
	var next, follower int
	require.Eventually(t, func() bool {
		for i, node := range nodes {
			if i != leader && node.raft.IsLeader() {
				next = i
				return true
			}
		}
		return false
	}, 3*time.Second, 10*time.Millisecond)
	for i := range nodes {
		if i != leader && i != next {
			follower = i
		}
	}
	stats, err = materializers[next].Tick(context.Background(), time.Now())
	require.NoError(t, err)
	require.Zero(t, stats.Created, "new leader resumes the committed cursor")
	page, err := nodes[next].store.ListReadyRuns(nil, 8)
	require.NoError(t, err)
	require.Len(t, page.Runs, 2)
	require.Equal(t, "slave-3", page.Runs[0].ScheduleID)
	require.Equal(t, "slave-7", page.Runs[1].ScheduleID)

	gateway := fake.New(fake.Options{})
	dispatcher, err := scheduler.NewDispatcher(scheduler.DispatcherConfig{PollInterval: 5 * time.Millisecond,
		Executor: executor.Config{LeaseDuration: 10 * time.Second, LeaseSafetyMargin: 2 * time.Second, RenewInterval: 2 * time.Second,
			CommitTimeout: time.Second, OperationTimeout: 3 * time.Second, RecoveryTimeout: 2 * time.Second, CloseTimeout: 2 * time.Second}},
		nodes[follower].store, gateway.Factory)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		stop := make(chan struct{})
		go func() {
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for {
				gateway.ReleaseAll()
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
			}
		}()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer closeCancel()
		_ = dispatcher.Close(closeCtx)
		close(stop)
	})
	answer := make(chan error, 1)
	go func() { answer <- dispatcher.Run(ctx) }()
	first := schedulerCall(t, gateway)
	require.Equal(t, page.Runs[0].ID, first.Request.RunID)
	first.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded, Data: []byte{0x12, 0x34}}}
	schedulerPhase(t, first.RecoveryStarted)
	require.Eventually(t, func() bool {
		for i, node := range nodes {
			if i == leader {
				continue
			}
			run, err := node.store.GetRun(page.Runs[1].ID)
			if err != nil || run.State != db.RunReady || run.AttemptCount != 0 {
				return false
			}
			channel, err := node.store.GetChannel("gateway")
			if err != nil || channel.ActiveAttemptID != first.Request.AttemptID {
				return false
			}
		}
		return true
	}, 3*time.Second, 10*time.Millisecond)
	first.Recover <- nil
	schedulerPhase(t, first.CloseStarted)
	require.Equal(t, 1, gateway.Stats().Started)
	first.Close <- nil
	second := schedulerCall(t, gateway)
	require.Equal(t, page.Runs[1].ID, second.Request.RunID)
	second.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded}}
	schedulerPhase(t, second.RecoveryStarted)
	second.Recover <- nil
	schedulerPhase(t, second.CloseStarted)
	second.Close <- nil
	require.Eventually(t, func() bool {
		for i, node := range nodes {
			if i == leader {
				continue
			}
			for _, expected := range page.Runs {
				run, err := node.store.GetRun(expected.ID)
				if err != nil || run.State != db.RunSucceeded || run.AttemptCount != 1 {
					return false
				}
			}
		}
		return dispatcher.Pending() == 0
	}, 3*time.Second, 10*time.Millisecond)
	cancel()
	select {
	case err := <-answer:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(4 * time.Second):
		t.Fatal("dispatcher did not shut down")
	}
	require.Equal(t, 1, gateway.Stats().MaxActive)
	require.Equal(t, 2, gateway.Stats().Started)
}

func TestRaftScheduleSnapshotRebuildRetainsCursorAndReadyIndexes(t *testing.T) {
	dir := t.TempDir()
	n := startDurableNode(t, dir, 2)
	proposeChannel(t, n, db.StateCommand{RequestID: "schedule-snapshot-configure", At: time.Now().UTC(),
		Configure: &db.ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "gateway:502"}})
	m, err := scheduler.NewMaterializer(scheduler.MaterializerConfig{}, n.store, n.raft.IsLeader)
	require.NoError(t, err)
	_, err = m.Put(context.Background(), db.ScheduleDefinition{ID: "poll", ChannelID: "gateway", Executor: "fake", Payload: []byte{0, 0xff},
		Spec: db.ScheduleSpec{Kind: db.ScheduleInterval, StartAt: time.Now().Add(-time.Hour), Interval: time.Hour}, MissedPolicy: db.MissedReplay, CatchUpLimit: 2}, 0)
	require.NoError(t, err)
	stats, err := m.Tick(context.Background(), time.Now())
	require.NoError(t, err)
	require.Equal(t, 2, stats.Created)
	want, err := n.store.GetSchedule("poll")
	require.NoError(t, err)
	runs, err := n.store.ListReadyRuns(nil, 8)
	require.NoError(t, err)
	materializedIndex := n.store.LastAppliedIndex()
	// Cross the compaction threshold without changing the recurrence cursor.
	for _, request := range []string{"snapshot-barrier-1", "snapshot-barrier-2"} {
		proposeChannel(t, n, db.StateCommand{RequestID: request, At: time.Now().UTC(),
			PauseSchedule: &db.PauseSchedule{ID: "poll", ExpectedRevision: want.Revision, Paused: false}})
	}
	require.Eventually(t, func() bool {
		snapshot, err := n.raft.Snapshotter.Load()
		return err == nil && snapshot.Metadata.Index >= materializedIndex
	}, 3*time.Second, 10*time.Millisecond)
	n.close()
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "store")))
	n = startDurableNode(t, dir, 2)
	got, err := n.store.GetSchedule("poll")
	require.NoError(t, err)
	require.Equal(t, want, got)
	gotRuns, err := n.store.ListReadyRuns(nil, 8)
	require.NoError(t, err)
	require.Equal(t, runs, gotRuns)
	first, err := n.store.FirstReadyRun("gateway")
	require.NoError(t, err)
	require.Equal(t, runs.Runs[0], *first)
	count, err := n.store.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	m, err = scheduler.NewMaterializer(scheduler.MaterializerConfig{}, n.store, n.raft.IsLeader)
	require.NoError(t, err)
	stats, err = m.Tick(context.Background(), time.Now())
	require.NoError(t, err)
	require.Zero(t, stats.Created)
	_, err = n.store.ProposeStateCommand(context.Background(), db.StateCommand{RequestID: "snapshot-pause", At: time.Now().UTC(),
		PauseSchedule: &db.PauseSchedule{ID: "poll", ExpectedRevision: want.Revision, Paused: true}})
	require.NoError(t, err)
}
