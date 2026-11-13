package node

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

func seedPartialRaftPurge(t *testing.T, n *durableNode) (db.StateCommand, *db.JobDeletion) {
	t.Helper()
	event := seedRaftEventTrigger(t, n)
	genericProposal(t, n, "retention-limits", db.JobCommand{PutLimits: &db.PutJobLimits{Limits: db.JobLimits{TenantID: "tenant", MaxStoredRuns: 1, MaxActivations: 1}}})
	original := db.StateCommand{RequestID: "retention-original-delivery", At: time.Now().UTC(), Job: &db.JobCommand{Activate: &event}}
	accepted := proposeChannel(t, n, original).Job.Activation
	for i := range 10 {
		genericProposal(t, n, fmt.Sprintf("retention-inbox/%d", i), db.JobCommand{Signal: &db.SignalJob{TenantID: "tenant", RunID: accepted.RunID, EventID: fmt.Sprintf("reply-%d", i), Key: "reply", Data: []byte("payload")}})
	}
	cancelled := genericProposal(t, n, "retention-cancel", db.JobCommand{Cancel: &db.CancelJob{TenantID: "tenant", RunID: accepted.RunID}}).Job.Run
	at := time.Now().UTC()
	purged := proposeChannel(t, n, db.StateCommand{RequestID: "retention-start", At: at, Job: &db.JobCommand{Purge: &db.PurgeJob{TenantID: "tenant", RunID: accepted.RunID, ExpectedStep: cancelled.Step, FinishedBefore: at, BatchSize: 1, Actor: "tenant/operator", Reason: "expired test history"}}}).Job.Deletion
	require.False(t, purged.CleanupComplete)
	return original, purged
}

func finishRaftPurge(t *testing.T, n *durableNode, original db.StateCommand, initial *db.JobDeletion) {
	t.Helper()
	controller, err := scheduler.NewJobRetention(scheduler.JobRetentionConfig{BatchSize: 1, DeleteBatchSize: 2}, n.store, n.raft.IsLeader)
	require.NoError(t, err)
	for i := range 24 {
		_, err := controller.Tick(context.Background(), time.Now().UTC())
		require.NoError(t, err)
		deletion, err := n.store.GetJobDeletion("tenant", initial.RunID)
		require.NoError(t, err)
		if deletion.CleanupComplete {
			require.Equal(t, initial.Actor, deletion.Actor)
			require.Equal(t, uint64(22), deletion.RecordsDeleted)
			break
		}
		if i == 23 {
			t.Fatal("committed cleanup did not finish with retention disabled")
		}
	}
	usage, err := n.store.GetJobUsage("tenant")
	require.NoError(t, err)
	require.Equal(t, db.JobUsage{TenantID: "tenant"}, *usage)
	policy, err := n.store.GetJobRetention("tenant")
	require.NoError(t, err)
	require.False(t, policy.Enabled)
	event := original.Job.Activate
	accepted := genericProposal(t, n, "retention-redelivery", db.JobCommand{Activate: event}).Job.Activation
	require.Equal(t, initial.RunID, accepted.RunID)
	require.True(t, accepted.DataDeleted)
	sum := sha256.Sum256(event.Data)
	require.Equal(t, sum[:], accepted.DataSHA256)
	require.Empty(t, accepted.Data)
	// Exact request receipts remain stable even after the primary payload is removed.
	repeated := proposeChannel(t, n, original).Job.Activation
	require.Equal(t, event.Data, repeated.Data)
	require.False(t, repeated.DataDeleted)
	_, err = n.store.GetJobRun("tenant", initial.RunID)
	require.Error(t, err)
	r, err := n.store.ProposeStateCommand(context.Background(), db.StateCommand{RequestID: "retention-reuse", At: time.Now().UTC(), Job: &db.JobCommand{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: "hash", DefinitionRevision: 1, RunID: initial.RunID}}})
	if err == nil {
		err = r.Err()
	}
	var rejection *db.CommandRejection
	require.ErrorAs(t, err, &rejection)
	require.Equal(t, "gone", rejection.Code)
	next := *event
	next.EventID = "new-reading"
	newRun := genericProposal(t, n, "retention-new-admission", db.JobCommand{Activate: &next}).Job.Activation
	require.NotEqual(t, initial.RunID, newRun.RunID)
}

func TestRaftJobPurgeResumesOnNewLeaderWithRetentionDisabled(t *testing.T) {
	nodes := startChannelCluster(t)
	original, initial := seedPartialRaftPurge(t, nodes[0])
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			if d, err := n.store.GetJobDeletion("tenant", initial.RunID); err != nil || d.RecordsDeleted != initial.RecordsDeleted {
				return false
			}
		}
		return true
	}, 3*time.Second, 5*time.Millisecond)
	leader := -1
	for i, n := range nodes {
		if n.raft.IsLeader() {
			leader = i
		}
	}
	require.NotEqual(t, -1, leader)
	nodes[leader].close()
	next := -1
	require.Eventually(t, func() bool {
		for i, n := range nodes {
			if i != leader && n.raft.IsLeader() {
				next = i
				return true
			}
		}
		return false
	}, 3*time.Second, 5*time.Millisecond)
	finishRaftPurge(t, nodes[next], original, initial)
}

func TestRaftJobPurgeSnapshotRebuildRetainsCleanupAndDeduplication(t *testing.T) {
	dir := t.TempDir()
	n := startDurableNode(t, dir, 2)
	original, initial := seedPartialRaftPurge(t, n)
	target := n.store.LastAppliedIndex()
	barrier := db.StateCommand{RequestID: "retention-snapshot-barrier", At: time.Now().UTC(), Job: &db.JobCommand{PutTenant: &db.PutJobTenant{ExpectedRevision: 1, Tenant: db.JobTenant{ID: "tenant"}}}}
	for range 3 {
		proposeChannel(t, n, barrier)
	}
	require.Eventually(t, func() bool { s, err := n.raft.Snapshotter.Load(); return err == nil && s.Metadata.Index >= target }, 3*time.Second, time.Millisecond)
	n.close()
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "store")))
	n = startDurableNode(t, dir, 2)
	deletion, err := n.store.GetJobDeletion("tenant", initial.RunID)
	require.NoError(t, err)
	require.Equal(t, initial, deletion)
	finishRaftPurge(t, n, original, initial)
}
