package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

func TestRaftDeadlineSweepAfterLeaderChangeKeepsActiveBusOwned(t *testing.T) {
	nodes := startChannelCluster(t)
	leader := -1
	require.Eventually(t, func() bool {
		for i, n := range nodes {
			if n.raft.IsLeader() {
				leader = i
				return true
			}
		}
		return false
	}, 3*time.Second, 5*time.Millisecond)
	at := time.Now().UTC()
	deadline := at.Add(time.Minute)
	owner := db.ChannelOwner{ChannelID: "gateway", OwnerID: "worker/incarnation", Epoch: 1}
	proposeChannel(t, nodes[0], db.StateCommand{RequestID: "configure", At: at,
		Configure: &db.ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "gateway:502"}})
	proposeChannel(t, nodes[0], db.StateCommand{RequestID: "acquire", At: at,
		Acquire: &db.AcquireChannel{ChannelID: "gateway", OwnerID: owner.OwnerID, LeaseUntil: at.Add(time.Hour)}})
	proposeChannel(t, nodes[0], db.StateCommand{RequestID: "active", At: at,
		EnqueueRun: &db.CreateRun{ID: "active", ChannelID: "gateway", Executor: "fake"}})
	proposeChannel(t, nodes[0], db.StateCommand{RequestID: "claim", At: at,
		Claim: &db.ClaimRun{ChannelOwner: owner, RunID: "active", AttemptID: "attempt", ClaimToken: "token"}})
	started := proposeChannel(t, nodes[0], db.StateCommand{RequestID: "start", At: at,
		StartAttempt: &db.StartAttempt{ChannelOwner: owner, RunID: "active", AttemptID: "attempt", ClaimToken: "token", DeadlineAware: true}})
	command := db.StateCommand{RequestID: "queued", At: at, EnqueueRun: &db.CreateRun{ID: "queued", ChannelID: "gateway", Executor: "fake", StartDeadline: &deadline}}
	receipt := proposeChannel(t, nodes[0], command)
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			page, err := n.store.ListExpiredReadyRuns(deadline, nil, 4)
			if err != nil || len(page.Runs) != 1 {
				return false
			}
		}
		return true
	}, 3*time.Second, 5*time.Millisecond)
	nodes[leader].close()
	replacement := -1
	require.Eventually(t, func() bool {
		for i, n := range nodes {
			if i != leader && n.raft.IsLeader() {
				replacement = i
				return true
			}
		}
		return false
	}, 3*time.Second, 5*time.Millisecond)
	n := nodes[replacement]
	sweeper, err := scheduler.NewDeadlineSweeper(scheduler.DeadlineSweeperConfig{}, n.store, n.raft.IsLeader)
	require.NoError(t, err)
	count, err := sweeper.Tick(context.Background(), deadline)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, receipt, proposeChannel(t, n, command))
	require.Eventually(t, func() bool {
		for i, n := range nodes {
			if i == leader {
				continue
			}
			run, err := n.store.GetRun("queued")
			if err != nil || run.State != db.RunExpired || run.AttemptCount != 0 {
				return false
			}
			channel, err := n.store.GetChannel("gateway")
			if err != nil || *channel != *started.Channel {
				return false
			}
		}
		return true
	}, 3*time.Second, 5*time.Millisecond)
}

func TestRaftDeadlineSnapshotRebuildRetainsWindowIndexAndHistoricalReceipt(t *testing.T) {
	dir := t.TempDir()
	n := startDurableNode(t, dir, 2)
	at := time.Now().UTC()
	proposeChannel(t, n, db.StateCommand{RequestID: "configure", At: at,
		Configure: &db.ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "gateway:502"}})
	put, err := scheduler.PreparePut(db.ScheduleDefinition{ID: "poll", ChannelID: "gateway", Executor: "fake",
		Spec: db.ScheduleSpec{Kind: db.ScheduleOnce, StartAt: at}, MissedPolicy: db.MissedCoalesce, StartWindow: time.Minute}, 0, at)
	require.NoError(t, err)
	configured := proposeChannel(t, n, db.StateCommand{RequestID: "schedule", At: at, PutSchedule: &put})
	plan, err := scheduler.Plan(*configured.Schedule, at)
	require.NoError(t, err)
	command := db.StateCommand{RequestID: "materialize", At: at, Materialize: &plan}
	receipt := proposeChannel(t, n, command)
	target := n.store.LastAppliedIndex()
	for range 3 {
		proposeChannel(t, n, command)
	}
	require.Eventually(t, func() bool {
		snapshot, err := n.raft.Snapshotter.Load()
		return err == nil && snapshot.Metadata.Index >= target
	}, 3*time.Second, 5*time.Millisecond)
	n.close()
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "store")))
	n = startDurableNode(t, dir, 2)
	schedule, err := n.store.GetSchedule("poll")
	require.NoError(t, err)
	require.Equal(t, time.Minute, schedule.StartWindow)
	page, err := n.store.ListExpiredReadyRuns(at.Add(time.Minute), nil, 4)
	require.NoError(t, err)
	require.Len(t, page.Runs, 1)
	require.Equal(t, receipt.Runs[0], page.Runs[0])
	sweeper, err := scheduler.NewDeadlineSweeper(scheduler.DeadlineSweeperConfig{}, n.store, n.raft.IsLeader)
	require.NoError(t, err)
	count, err := sweeper.Tick(context.Background(), at.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, receipt, proposeChannel(t, n, command))
	run, err := n.store.GetRun(receipt.Runs[0].ID)
	require.NoError(t, err)
	require.Equal(t, db.RunExpired, run.State)
	require.Zero(t, run.AttemptCount)
}
