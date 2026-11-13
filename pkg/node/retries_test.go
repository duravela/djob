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

func nodeRetryPolicy() *db.RetryPolicy {
	return &db.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: 2 * time.Second}
}

func nodeRetryAttempt(t *testing.T, n *durableNode, at time.Time, runID string) (db.StateCommand, db.StateResult) {
	t.Helper()
	owner := db.ChannelOwner{ChannelID: "gateway", OwnerID: "worker/incarnation", Epoch: 1}
	proposeChannel(t, n, db.StateCommand{RequestID: "acquire", At: at, Acquire: &db.AcquireChannel{ChannelID: "gateway", OwnerID: owner.OwnerID, LeaseUntil: at.Add(time.Hour)}})
	proposeChannel(t, n, db.StateCommand{RequestID: "claim", At: at, Claim: &db.ClaimRun{ChannelOwner: owner, RunID: runID, AttemptID: "attempt", ClaimToken: "token"}})
	proposeChannel(t, n, db.StateCommand{RequestID: "start", At: at, StartAttempt: &db.StartAttempt{ChannelOwner: owner, RunID: runID, AttemptID: "attempt", ClaimToken: "token", DeadlineAware: true, RetryAware: true}})
	command := db.StateCommand{RequestID: "finish", At: at, Finish: &db.FinishAttempt{ChannelOwner: owner, RunID: runID, AttemptID: "attempt", ClaimToken: "token", Outcome: db.AttemptFailed,
		Quiescent: true, CancellationAware: true, DeadlineAware: true, RetryAware: true, RetryClass: db.RetryNotSent}}
	return command, proposeChannel(t, n, command)
}

func TestRaftRetryBackoffSurvivesLeaderReplacementWithoutNewExecutionAuthority(t *testing.T) {
	nodes := startChannelCluster(t)
	at := time.Now().UTC()
	proposeChannel(t, nodes[0], db.StateCommand{RequestID: "configure", At: at, Configure: &db.ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "gateway:502"}})
	proposeChannel(t, nodes[0], db.StateCommand{RequestID: "run", At: at, EnqueueRun: &db.CreateRun{ID: "run", ChannelID: "gateway", Executor: "fake", RetryPolicy: nodeRetryPolicy()}})
	command, receipt := nodeRetryAttempt(t, nodes[0], at, "run")
	require.Equal(t, db.RunRetryWait, receipt.Run.State)
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			page, err := n.store.ListDueRetries(receipt.Run.RetryAt, nil, 4)
			if err != nil || len(page.Runs) != 1 {
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
	promoter, err := scheduler.NewRetryPromoter(scheduler.RetryPromoterConfig{}, n.store, n.raft.IsLeader)
	require.NoError(t, err)
	count, err := promoter.Tick(context.Background(), receipt.Run.RetryAt)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, receipt, proposeChannel(t, n, command), "historical finish acknowledgement cannot schedule a second retry")
	require.Eventually(t, func() bool {
		for i, n := range nodes {
			if i == leader {
				continue
			}
			run, err := n.store.GetRun("run")
			if err != nil || run.State != db.RunReady || run.AttemptCount != 1 || !run.RetryAt.IsZero() {
				return false
			}
			channel, err := n.store.GetChannel("gateway")
			if err != nil || *channel != *receipt.Channel {
				return false
			}
		}
		return true
	}, 3*time.Second, 5*time.Millisecond)
}

func TestRaftRetrySnapshotRebuildRetainsPolicyPlanAndCapacity(t *testing.T) {
	dir := t.TempDir()
	n := startDurableNode(t, dir, 2)
	at := time.Now().UTC()
	proposeChannel(t, n, db.StateCommand{RequestID: "configure", At: at, Configure: &db.ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "gateway:502"}})
	put, err := scheduler.PreparePut(db.ScheduleDefinition{ID: "poll", ChannelID: "gateway", Executor: "fake", Spec: db.ScheduleSpec{Kind: db.ScheduleOnce, StartAt: at},
		MissedPolicy: db.MissedCoalesce, RetryPolicy: nodeRetryPolicy()}, 0, at)
	require.NoError(t, err)
	configured := proposeChannel(t, n, db.StateCommand{RequestID: "schedule", At: at, PutSchedule: &put})
	plan, err := scheduler.Plan(*configured.Schedule, at)
	require.NoError(t, err)
	materialized := proposeChannel(t, n, db.StateCommand{RequestID: "materialize", At: at, Materialize: &plan})
	runID := materialized.Runs[0].ID
	command, receipt := nodeRetryAttempt(t, n, at, runID)
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
	run, err := n.store.GetRun(runID)
	require.NoError(t, err)
	require.Equal(t, receipt.Run, run)
	attempt, err := n.store.GetAttempt("attempt")
	require.NoError(t, err)
	require.Equal(t, receipt.Attempt, attempt)
	count, err := n.store.PendingRunCount("gateway")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	page, err := n.store.ListDueRetries(run.RetryAt, nil, 4)
	require.NoError(t, err)
	require.Len(t, page.Runs, 1)
	schedule, err := n.store.GetSchedule("poll")
	require.NoError(t, err)
	require.Equal(t, nodeRetryPolicy(), schedule.RetryPolicy)
	promoter, err := scheduler.NewRetryPromoter(scheduler.RetryPromoterConfig{}, n.store, n.raft.IsLeader)
	require.NoError(t, err)
	ready, err := promoter.Tick(context.Background(), run.RetryAt)
	require.NoError(t, err)
	require.Equal(t, 1, ready)
	require.Equal(t, receipt, proposeChannel(t, n, command))
}
