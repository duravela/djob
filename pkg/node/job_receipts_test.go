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

func seedPartialRaftReceiptCompaction(t *testing.T, n *durableNode) (db.StateCommand, *db.JobReceiptCompaction) {
	t.Helper()
	original, deletion := seedPartialRaftPurge(t, n)
	cleaned := genericProposal(t, n, "receipt-primary-cleanup", db.JobCommand{Purge: &db.PurgeJob{TenantID: "tenant", RunID: deletion.RunID, ExpectedStep: deletion.Step, FinishedBefore: time.Now().UTC(), Actor: "operator", Reason: "finish primary cleanup"}}).Job.Deletion
	require.True(t, cleaned.CleanupComplete)
	progress := genericProposal(t, n, "receipt-start-compaction", db.JobCommand{CompactReceipts: &db.CompactJobReceipts{TenantID: "tenant", RunID: deletion.RunID, ExpectedStep: deletion.Step, BatchSize: 1, Actor: "tenant/operator", Reason: "compact historical copies"}}).Job.ReceiptCompaction
	require.False(t, progress.Complete)
	require.Equal(t, uint64(1), progress.ReceiptsCompacted)
	return original, progress
}

func requireRaftReceiptRejection(t *testing.T, n *durableNode, c db.StateCommand, code string) db.StateResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r, err := n.store.ProposeStateCommand(ctx, c)
	if err == nil {
		err = r.Err()
	}
	var rejection *db.CommandRejection
	require.ErrorAs(t, err, &rejection)
	require.Equal(t, code, rejection.Code)
	return r
}

func finishRaftReceiptCompaction(t *testing.T, n *durableNode, original db.StateCommand, initial *db.JobReceiptCompaction) {
	t.Helper()
	controller, err := scheduler.NewJobRetention(scheduler.JobRetentionConfig{BatchSize: 1, DeleteBatchSize: 2}, n.store, n.raft.IsLeader)
	require.NoError(t, err)
	for i := range 24 {
		_, err := controller.Tick(context.Background(), time.Now().UTC())
		require.NoError(t, err)
		progress, err := n.store.GetJobReceiptCompaction("tenant", initial.RunID)
		require.NoError(t, err)
		if progress.Complete {
			require.Equal(t, uint64(12), progress.ReceiptsCompacted)
			require.Equal(t, initial.Actor, progress.Actor)
			require.Positive(t, progress.PayloadBytesRemoved)
			break
		}
		if i == 23 {
			t.Fatal("receipt compaction did not resume")
		}
	}
	replayed := requireRaftReceiptRejection(t, n, original, db.ReceiptCompactedCode)
	require.True(t, replayed.Compacted)
	require.Nil(t, replayed.Rejection)
	require.Equal(t, initial.RunID, replayed.Job.Activation.RunID)
	require.Empty(t, replayed.Job.Activation.Data)
	changed := *original.Job.Activate
	changed.Data = []byte("conflicting event")
	conflict := original
	conflict.Job = &db.JobCommand{Activate: &changed}
	requireRaftReceiptRejection(t, n, conflict, "request_conflict")
	conflict.RequestID = "new-envelope-event-conflict"
	requireRaftReceiptRejection(t, n, conflict, "activation_conflict")
	late := db.StateCommand{RequestID: "late-event-delivery", At: time.Now().UTC(), Job: original.Job}
	accepted := proposeChannel(t, n, late).Job.Activation
	require.Equal(t, initial.RunID, accepted.RunID)
	require.True(t, accepted.DataDeleted)
	require.True(t, requireRaftReceiptRejection(t, n, late, db.ReceiptCompactedCode).Compacted)
	usage, err := n.store.GetJobUsage("tenant")
	require.NoError(t, err)
	require.Equal(t, db.JobUsage{TenantID: "tenant"}, *usage)
	pending, err := n.store.ListPendingJobReceiptCompactions(nil, 1)
	require.NoError(t, err)
	require.Empty(t, pending.Compactions)
}

func TestRaftJobReceiptCompactionResumesAfterLeaderLoss(t *testing.T) {
	nodes := startChannelCluster(t)
	original, progress := seedPartialRaftReceiptCompaction(t, nodes[0])
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			if p, err := n.store.GetJobReceiptCompaction("tenant", progress.RunID); err != nil || p.ReceiptsCompacted != progress.ReceiptsCompacted {
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
	finishRaftReceiptCompaction(t, nodes[next], original, progress)
}

func TestRaftJobReceiptCompactionSnapshotRebuildPreservesConflictGuard(t *testing.T) {
	dir := t.TempDir()
	n := startDurableNode(t, dir, 2)
	original, progress := seedPartialRaftReceiptCompaction(t, n)
	target := n.store.LastAppliedIndex()
	barrier := db.StateCommand{RequestID: "receipt-snapshot-barrier", At: time.Now().UTC(), Job: &db.JobCommand{PutTenant: &db.PutJobTenant{ExpectedRevision: 1, Tenant: db.JobTenant{ID: "tenant"}}}}
	for range 3 {
		proposeChannel(t, n, barrier)
	}
	require.Eventually(t, func() bool { s, err := n.raft.Snapshotter.Load(); return err == nil && s.Metadata.Index >= target }, 3*time.Second, time.Millisecond)
	n.close()
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "store")))
	n = startDurableNode(t, dir, 2)
	restored, err := n.store.GetJobReceiptCompaction("tenant", progress.RunID)
	require.NoError(t, err)
	require.Equal(t, progress, restored)
	finishRaftReceiptCompaction(t, n, original, progress)
}
