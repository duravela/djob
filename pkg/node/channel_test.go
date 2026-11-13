package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node/config"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

func startChannelCluster(t *testing.T) []*durableNode {
	t.Helper()
	peers := make([]string, 3)
	for i := range peers {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		peers[i] = "http://" + listener.Addr().String()
		require.NoError(t, listener.Close())
	}
	nodes := make([]*durableNode, len(peers))
	for i := range nodes {
		cfg := config.Config{}
		cfg.SetDefaults()
		cfg.ID, cfg.DataDir, cfg.Peers = uint64(i+1), t.TempDir(), peers
		cfg.Logger = zap.NewNop()
		cfg.ProposeC = make(chan []byte)
		cfg.ConfChangeC = make(chan raftpb.ConfChange)
		cfg.BatchTimeout = time.Millisecond
		cfg.RaftEventLoopHeartbeatInterval = 10 * time.Millisecond
		cfg.SnapshotCount, cfg.CatchUpEntriesN = 4, 1
		nodes[i] = startRaftApplication(t, cfg)
	}
	for _, node := range nodes {
		_, err := node.raft.WaitForLeader(3 * time.Second)
		require.NoError(t, err)
	}
	return nodes
}

func proposeChannel(t *testing.T, node *durableNode, command db.StateCommand) db.StateResult {
	t.Helper()
	var result db.StateResult
	var err error
	// Startup elections can discard an uncommitted proposal. Exercise the same
	// exact-request retry contract as executors instead of assuming its first
	// acknowledgement must arrive. A state-machine rejection is not retried.
	for range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		result, err = node.store.ProposeStateCommand(ctx, command)
		cancel()
		var rejection *db.CommandRejection
		if err == nil || errors.As(err, &rejection) {
			break
		}
	}
	require.NoError(t, err)
	return result
}

func TestReplicatedClaimsAndBlockedOwnershipSurviveLeaderChange(t *testing.T) {
	nodes := startChannelCluster(t)
	at := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	for _, id := range []string{"shared-gateway", "independent-gateway"} {
		proposeChannel(t, nodes[0], db.StateCommand{RequestID: "configure-" + id, At: at,
			Configure: &db.ConfigureChannel{ID: id, GatewayID: id, Protocol: "fake", Endpoint: id + ":502"}})
		proposeChannel(t, nodes[1], db.StateCommand{RequestID: "acquire-" + id, At: at,
			Acquire: &db.AcquireChannel{ChannelID: id, OwnerID: "worker/process-1", LeaseUntil: at.Add(time.Hour)}})
	}
	for i := 0; i < 4; i++ {
		channel := "shared-gateway"
		if i == 3 {
			channel = "independent-gateway"
		}
		proposeChannel(t, nodes[2], db.StateCommand{RequestID: fmt.Sprintf("create-%d", i), At: at,
			CreateRun: &db.CreateRun{ID: fmt.Sprintf("slave-%d", i), ChannelID: channel, Executor: "fake"}})
	}
	type reply struct {
		result db.StateResult
		err    error
	}
	replies := make(chan reply, 4)
	for i := 0; i < 4; i++ {
		go func() {
			channel := "shared-gateway"
			if i == 3 {
				channel = "independent-gateway"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result, err := nodes[i%3].store.ProposeStateCommand(ctx, db.StateCommand{RequestID: fmt.Sprintf("claim-%d", i), At: at,
				Claim: &db.ClaimRun{ChannelOwner: db.ChannelOwner{ChannelID: channel, Epoch: 1, OwnerID: "worker/process-1"},
					RunID: fmt.Sprintf("slave-%d", i), AttemptID: fmt.Sprintf("attempt-%d", i), ClaimToken: fmt.Sprintf("token-%d", i)}})
			replies <- reply{result: result, err: err}
		}()
	}
	var winning db.StateResult
	sharedSuccesses, independentSuccesses, rejected := 0, 0, 0
	for i := 0; i < 4; i++ {
		select {
		case reply := <-replies:
			if reply.err != nil {
				require.NotNil(t, reply.result.Rejection, "unexpected proposal failure: %v", reply.err)
				require.Equal(t, "channel_busy", reply.result.Rejection.Code)
				rejected++
			} else if reply.result.Channel.ID == "shared-gateway" {
				sharedSuccesses++
				winning = reply.result
			} else {
				independentSuccesses++
			}
		case <-time.After(4 * time.Second):
			t.Fatal("concurrent claim did not finish")
		}
	}
	require.Equal(t, 1, sharedSuccesses)
	require.Equal(t, 1, independentSuccesses)
	require.Equal(t, 2, rejected)
	require.Eventually(t, func() bool {
		for _, node := range nodes {
			channel, err := node.store.GetChannel("shared-gateway")
			if err != nil || channel.ActiveAttemptID != winning.Attempt.ID {
				return false
			}
		}
		return true
	}, 3*time.Second, 10*time.Millisecond)
	proposeChannel(t, nodes[0], db.StateCommand{RequestID: "expire-shared", At: at.Add(time.Hour),
		Expire: &db.ExpireChannel{ChannelID: "shared-gateway", Epoch: 1}})
	require.Eventually(t, func() bool {
		for _, node := range nodes {
			channel, err := node.store.GetChannel("shared-gateway")
			if err != nil || channel.State != db.ChannelBlocked {
				return false
			}
		}
		return true
	}, 3*time.Second, 10*time.Millisecond)
	leaderID, err := nodes[0].raft.WaitForLeader(time.Second)
	require.NoError(t, err)
	nodes[leaderID-1].close()
	var next *durableNode
	require.Eventually(t, func() bool {
		for i, node := range nodes {
			if uint64(i+1) != leaderID && node.raft.node.Status().RaftState == raft.StateLeader {
				next = node
				return true
			}
		}
		return false
	}, 3*time.Second, 10*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := next.store.ProposeStateCommand(ctx, db.StateCommand{RequestID: "unsafe-takeover", At: at.Add(time.Hour),
		Acquire: &db.AcquireChannel{ChannelID: "shared-gateway", ExpectedEpoch: 1, OwnerID: "worker/process-2", LeaseUntil: at.Add(2 * time.Hour)}})
	require.Error(t, err)
	require.Equal(t, "channel_busy", result.Rejection.Code)
	run, err := next.store.GetRun(winning.Run.ID)
	require.NoError(t, err)
	require.Equal(t, db.RunUnknown, run.State)
	attempt, err := next.store.GetAttempt(winning.Attempt.ID)
	require.NoError(t, err)
	require.Equal(t, db.AttemptUnknown, attempt.Outcome)
	independent, err := next.store.GetChannel("independent-gateway")
	require.NoError(t, err)
	require.Equal(t, db.ChannelOwned, independent.State)
	require.NotEmpty(t, independent.ActiveAttemptID)
}

func TestActiveChannelAndClaimReceiptSurviveSnapshotRebuild(t *testing.T) {
	dir := t.TempDir()
	n := startDurableNode(t, dir, 2)
	at := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	proposeChannel(t, n, db.StateCommand{RequestID: "configure", At: at, Configure: &db.ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "gateway:502"}})
	proposeChannel(t, n, db.StateCommand{RequestID: "acquire", At: at, Acquire: &db.AcquireChannel{ChannelID: "gateway", OwnerID: "worker/process-1", LeaseUntil: at.Add(time.Hour)}})
	proposeChannel(t, n, db.StateCommand{RequestID: "create", At: at, CreateRun: &db.CreateRun{ID: "run", ChannelID: "gateway", Executor: "fake", Payload: []byte{0, 0xff, 0x10}}})
	claim := db.StateCommand{RequestID: "claim", At: at, Claim: &db.ClaimRun{ChannelOwner: db.ChannelOwner{ChannelID: "gateway", Epoch: 1, OwnerID: "worker/process-1"}, RunID: "run", AttemptID: "attempt", ClaimToken: "token"}}
	first := proposeChannel(t, n, claim)
	require.Eventually(t, func() bool {
		snapshot, err := n.raft.Snapshotter.Load()
		return err == nil && snapshot.Metadata.Index == n.store.LastAppliedIndex()
	}, 3*time.Second, 10*time.Millisecond)
	n.close()
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "store")))
	n = startDurableNode(t, dir, 2)
	channel, err := n.store.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, first.Channel, channel)
	run, err := n.store.GetRun("run")
	require.NoError(t, err)
	require.Equal(t, first.Run, run)
	attempt, err := n.store.GetAttempt("attempt")
	require.NoError(t, err)
	require.Equal(t, first.Attempt, attempt)
	retried := proposeChannel(t, n, claim)
	require.Equal(t, first, retried)
}
