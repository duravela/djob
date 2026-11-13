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

func seedNodeOwnership(t *testing.T, n *durableNode, at, lease time.Time) (db.StateCommand, db.StateResult) {
	t.Helper()
	owner := db.ChannelOwner{ChannelID: "gateway", OwnerID: "lost/process", Epoch: 1}
	proposeChannel(t, n, db.StateCommand{RequestID: "configure", At: at, Configure: &db.ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "gateway"}})
	proposeChannel(t, n, db.StateCommand{RequestID: "acquire", At: at, Acquire: &db.AcquireChannel{ChannelID: "gateway", OwnerID: owner.OwnerID, LeaseUntil: lease}})
	proposeChannel(t, n, db.StateCommand{RequestID: "enqueue", At: at, EnqueueRun: &db.CreateRun{ID: "run", ChannelID: "gateway", Executor: "fake"}})
	proposeChannel(t, n, db.StateCommand{RequestID: "claim", At: at, Claim: &db.ClaimRun{ChannelOwner: owner, RunID: "run", AttemptID: "attempt", ClaimToken: "old-token"}})
	command := db.StateCommand{RequestID: "start", At: at, StartAttempt: &db.StartAttempt{ChannelOwner: owner, RunID: "run", AttemptID: "attempt", ClaimToken: "old-token", DeadlineAware: true, RetryAware: true}}
	return command, proposeChannel(t, n, command)
}

func TestRaftOwnershipSweepAfterLeaderChangePreservesUncertainOccupancy(t *testing.T) {
	nodes := startChannelCluster(t)
	at := time.Now().UTC()
	lease := at.Add(time.Minute)
	command, original := seedNodeOwnership(t, nodes[0], at, lease)
	proposeChannel(t, nodes[0], db.StateCommand{RequestID: "live-config", At: at, Configure: &db.ConfigureChannel{ID: "live", GatewayID: "live", Protocol: "fake", Endpoint: "live"}})
	live := proposeChannel(t, nodes[0], db.StateCommand{RequestID: "live-owner", At: at, Acquire: &db.AcquireChannel{ChannelID: "live", OwnerID: "live/process", LeaseUntil: lease.Add(time.Hour)}})
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			channel, err := n.store.GetChannel("gateway")
			if err != nil || channel.ActiveAttemptID != "attempt" {
				return false
			}
		}
		return true
	}, 3*time.Second, 5*time.Millisecond)
	leader := -1
	for i, n := range nodes {
		if n.raft.IsLeader() {
			leader = i
			break
		}
	}
	require.NotEqual(t, -1, leader)
	follower := nodes[(leader+1)%len(nodes)]
	sweeper, err := scheduler.NewOwnershipSweeper(scheduler.OwnershipSweeperConfig{}, follower.store, follower.raft.IsLeader)
	require.NoError(t, err)
	count, err := sweeper.Tick(context.Background(), lease)
	require.NoError(t, err)
	require.Zero(t, count)
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
	sweeper, err = scheduler.NewOwnershipSweeper(scheduler.OwnershipSweeperConfig{}, n.store, n.raft.IsLeader)
	require.NoError(t, err)
	count, err = sweeper.Tick(context.Background(), lease)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, original, proposeChannel(t, n, command), "historical authorization cannot become a new attempt")
	require.Eventually(t, func() bool {
		for i, n := range nodes {
			if i == leader {
				continue
			}
			channel, err := n.store.GetChannel("gateway")
			if err != nil || channel.State != db.ChannelBlocked || channel.ActiveAttemptID != "attempt" || channel.Epoch != 1 || channel.OwnerID != "lost/process" {
				return false
			}
			run, err := n.store.GetRun("run")
			if err != nil || run.State != db.RunUnknown || run.AttemptCount != 1 {
				return false
			}
			attempt, err := n.store.GetAttempt("attempt")
			if err != nil || attempt.Outcome != db.AttemptUnknown || attempt.Quiescent || !attempt.AuthorizedAt.Equal(original.Attempt.AuthorizedAt) {
				return false
			}
			unchanged, err := n.store.GetChannel("live")
			if err != nil || *unchanged != *live.Channel {
				return false
			}
		}
		return true
	}, 3*time.Second, 5*time.Millisecond)
}

func TestRaftOwnershipSweepRebuildsDiscoveryFromRestoredSnapshot(t *testing.T) {
	dir := t.TempDir()
	n := startDurableNode(t, dir, 2)
	at := time.Now().UTC()
	lease := at.Add(time.Minute)
	command, original := seedNodeOwnership(t, n, at, lease)
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
	sweeper, err := scheduler.NewOwnershipSweeper(scheduler.OwnershipSweeperConfig{}, n.store, n.raft.IsLeader)
	require.NoError(t, err)
	count, err := sweeper.Tick(context.Background(), lease)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	channel, err := n.store.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, db.ChannelBlocked, channel.State)
	require.Equal(t, "attempt", channel.ActiveAttemptID)
	run, err := n.store.GetRun("run")
	require.NoError(t, err)
	require.Equal(t, db.RunUnknown, run.State)
	require.EqualValues(t, 1, run.AttemptCount)
	require.Equal(t, original, proposeChannel(t, n, command))
	_, err = n.store.ProposeStateCommand(context.Background(), db.StateCommand{RequestID: "forbidden-takeover", At: lease,
		Acquire: &db.AcquireChannel{ChannelID: "gateway", ExpectedEpoch: 1, OwnerID: "replacement/process", LeaseUntil: lease.Add(time.Minute)}})
	var rejection *db.CommandRejection
	require.ErrorAs(t, err, &rejection)
	require.Equal(t, "channel_busy", rejection.Code)
}
