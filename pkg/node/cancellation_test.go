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
	"github.com/stretchr/testify/require"
)

func TestRaftDurableCancellationSurvivesLeaderChangeWithoutReleasingBus(t *testing.T) {
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
	}, 3*time.Second, time.Millisecond)
	ownerIndex := (leader + 1) % len(nodes)
	proposeChannel(t, nodes[0], db.StateCommand{RequestID: "cancel/config", At: time.Now().UTC(), Configure: &db.ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "gateway:502"}})
	for _, id := range []string{"first", "next"} {
		proposeChannel(t, nodes[0], db.StateCommand{RequestID: "cancel/create/" + id, At: time.Now().UTC(), EnqueueRun: &db.CreateRun{ID: id, ChannelID: "gateway", Executor: "fake"}})
	}
	gateway := fake.New(fake.Options{})
	e, err := executor.New(context.Background(), executor.Config{ChannelID: "gateway", LeaseDuration: 10 * time.Second, LeaseSafetyMargin: 2 * time.Second, RenewInterval: 2 * time.Second,
		CommitTimeout: time.Second, OperationTimeout: 3 * time.Second, RecoveryTimeout: 2 * time.Second, CloseTimeout: 2 * time.Second}, nodes[ownerIndex].store, gateway.Factory)
	require.NoError(t, err)
	t.Cleanup(func() {
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
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		_ = e.Close(ctx)
		close(stop)
	})
	firstReply := make(chan error, 1)
	go func() { _, err := e.Execute(context.Background(), "first"); firstReply <- err }()
	first := schedulerCall(t, gateway)
	nextReply := make(chan error, 1)
	go func() { _, err := e.Execute(context.Background(), "next"); nextReply <- err }()
	cancel := db.StateCommand{RequestID: "cancel/active", At: time.Now().UTC(), CancelRun: &db.CancelRun{RunID: "first", Reason: "stop write across leader replacement"}}
	receipt := proposeChannel(t, nodes[leader], cancel)
	require.Equal(t, db.RunRunning, receipt.Run.State)
	require.True(t, receipt.Run.CancellationRequested())
	schedulerPhase(t, first.Context.Done())
	schedulerPhase(t, first.RecoveryStarted)
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			run, err := n.store.GetRun("first")
			if err != nil || !run.CancellationRequested() {
				return false
			}
		}
		return true
	}, 3*time.Second, 5*time.Millisecond)
	nodes[leader].close()
	newLeader := -1
	require.Eventually(t, func() bool {
		for i, n := range nodes {
			if i != leader && n.raft.IsLeader() {
				newLeader = i
				return true
			}
		}
		return false
	}, 3*time.Second, 5*time.Millisecond)
	retried := proposeChannel(t, nodes[newLeader], cancel)
	require.Equal(t, receipt, retried)
	for i, n := range nodes {
		if i == leader {
			continue
		}
		channel, err := n.store.GetChannel("gateway")
		require.NoError(t, err)
		require.Equal(t, first.Request.AttemptID, channel.ActiveAttemptID)
		run, err := n.store.GetRun("next")
		require.NoError(t, err)
		require.Equal(t, db.RunReady, run.State)
		require.Zero(t, run.AttemptCount)
	}
	require.Equal(t, 1, gateway.Stats().Started)
	first.Recover <- nil
	schedulerPhase(t, first.CloseStarted)
	first.Close <- nil
	select {
	case err := <-firstReply:
		require.ErrorIs(t, err, executor.ErrUnknownOutcome)
	case <-time.After(4 * time.Second):
		t.Fatal("cancelled exchange did not commit")
	}
	next := schedulerCall(t, gateway)
	require.Equal(t, "next", next.Request.RunID)
	next.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded}}
	schedulerPhase(t, next.RecoveryStarted)
	next.Recover <- nil
	schedulerPhase(t, next.CloseStarted)
	next.Close <- nil
	select {
	case err := <-nextReply:
		require.NoError(t, err)
	case <-time.After(4 * time.Second):
		t.Fatal("next run did not commit")
	}
	require.Eventually(t, func() bool {
		for i, n := range nodes {
			if i == leader {
				continue
			}
			run, err := n.store.GetRun("first")
			if err != nil || run.State != db.RunUnknown || !run.CancellationRequested() {
				return false
			}
		}
		return true
	}, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, gateway.Stats().MaxActive)
}

func TestRaftCancellationSnapshotRebuildRetainsActiveIntentAndReceipt(t *testing.T) {
	dir := t.TempDir()
	n := startDurableNode(t, dir, 2)
	at := time.Now().UTC()
	proposeChannel(t, n, db.StateCommand{RequestID: "snapshot/config", At: at, Configure: &db.ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "gateway:502"}})
	proposeChannel(t, n, db.StateCommand{RequestID: "snapshot/owner", At: at, Acquire: &db.AcquireChannel{ChannelID: "gateway", OwnerID: "old/process", LeaseUntil: at.Add(time.Hour)}})
	proposeChannel(t, n, db.StateCommand{RequestID: "snapshot/run", At: at, EnqueueRun: &db.CreateRun{ID: "run", ChannelID: "gateway", Executor: "fake"}})
	owner := db.ChannelOwner{ChannelID: "gateway", OwnerID: "old/process", Epoch: 1}
	proposeChannel(t, n, db.StateCommand{RequestID: "snapshot/claim", At: at, Claim: &db.ClaimRun{ChannelOwner: owner, RunID: "run", AttemptID: "attempt", ClaimToken: "token"}})
	proposeChannel(t, n, db.StateCommand{RequestID: "snapshot/start", At: at, StartAttempt: &db.StartAttempt{ChannelOwner: owner, RunID: "run", AttemptID: "attempt", ClaimToken: "token"}})
	command := db.StateCommand{RequestID: "snapshot/cancel", At: at, CancelRun: &db.CancelRun{RunID: "run", Reason: "pending when process stops"}}
	receipt := proposeChannel(t, n, command)
	target := n.store.LastAppliedIndex()
	for _, id := range []string{"snapshot/barrier-1", "snapshot/barrier-2"} {
		proposeChannel(t, n, db.StateCommand{RequestID: id, At: at, CancelRun: &db.CancelRun{RunID: "run"}})
	}
	require.Eventually(t, func() bool {
		snapshot, err := n.raft.Snapshotter.Load()
		return err == nil && snapshot.Metadata.Index >= target
	}, 3*time.Second, 5*time.Millisecond)
	n.close()
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "store")))
	n = startDurableNode(t, dir, 2)
	run, err := n.store.GetRun("run")
	require.NoError(t, err)
	require.Equal(t, receipt.Run, run)
	attempt, err := n.store.GetAttempt("attempt")
	require.NoError(t, err)
	require.False(t, attempt.AuthorizedAt.IsZero())
	require.Equal(t, db.AttemptRunning, attempt.Outcome)
	require.Equal(t, receipt, proposeChannel(t, n, command))
	channel, err := n.store.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, "attempt", channel.ActiveAttemptID)
	gateway := fake.New(fake.Options{})
	replacement, err := executor.New(context.Background(), executor.Config{ChannelID: "gateway"}, n.store, gateway.Factory)
	require.ErrorIs(t, err, executor.ErrOwnershipLost)
	require.Nil(t, replacement)
	require.Zero(t, gateway.Stats().Started)
}
