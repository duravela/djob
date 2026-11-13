package node

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

func seedRaftConfigurationSweep(t *testing.T, n *durableNode) (db.StateCommand, *db.JobConfigurationReceiptCompaction) {
	t.Helper()
	genericProposal(t, n, "configuration-tenant", db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "tenant"}}})
	original := db.StateCommand{RequestID: "configuration-original", At: time.Now().UTC(), Job: &db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "tenant", ID: "work", Runtime: db.StepRuntime, Handler: "sha256", HandlerVersion: "1", Input: []byte("application input")}}}}
	proposeChannel(t, n, original)
	for i := uint64(1); i < 5; i++ {
		genericProposal(t, n, fmt.Sprintf("configuration-edit/%d", i), db.JobCommand{PutDefinition: &db.PutJobDefinition{ExpectedRevision: i, Definition: db.JobDefinition{TenantID: "tenant", ID: "work", Runtime: db.StepRuntime, Handler: "sha256", HandlerVersion: "1", Input: []byte("application input")}}})
	}
	first := genericProposal(t, n, "configuration-sweep-start", db.JobCommand{CompactConfigurationReceipts: &db.CompactJobConfigurationReceipts{TenantID: "tenant", ID: "audit", BatchSize: 1, Actor: "tenant/operator", Reason: "compact historical copies"}}).Job.ConfigurationReceiptCompaction
	require.False(t, first.Complete)
	return original, first
}
func finishRaftConfigurationSweep(t *testing.T, n *durableNode, original db.StateCommand, initial *db.JobConfigurationReceiptCompaction, others ...*durableNode) {
	t.Helper()
	var materializers []*scheduler.JobRetention
	for _, member := range append([]*durableNode{n}, others...) {
		m, err := scheduler.NewJobRetention(scheduler.JobRetentionConfig{DeleteBatchSize: 1}, member.store, member.raft.IsLeader)
		require.NoError(t, err)
		materializers = append(materializers, m)
	}
	// Elections may change again after the first replacement is observed. Run
	// the guarded maintenance loops on every survivor, as the real servers do,
	// and wait for durable completion instead of assuming 16 tight ticks work.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var p *db.JobConfigurationReceiptCompaction
	for {
		for _, m := range materializers {
			_, err := m.Tick(ctx, time.Now().UTC())
			require.NoError(t, err)
		}
		var err error
		p, err = n.store.GetJobConfigurationReceiptCompaction("tenant", "audit")
		require.NoError(t, err)
		if p.Complete {
			break
		}
		require.NoError(t, ctx.Err(), "configuration receipt compaction did not complete")
		time.Sleep(5 * time.Millisecond)
	}
	require.True(t, p.Complete)
	require.Equal(t, initial.Horizon, p.ReceiptsCompacted)
	require.Equal(t, initial.Actor, p.Actor)
	replay := requireRaftReceiptRejection(t, n, original, db.ReceiptCompactedCode)
	require.True(t, replay.Compacted)
	require.Equal(t, uint64(1), replay.Job.Definition.Revision)
	require.Empty(t, replay.Job.Definition.Input)
	live, err := n.store.GetJobRecord(db.JobRecordQuery{Kind: db.JobDefinitionRecord, TenantID: "tenant", ID: "work"})
	require.NoError(t, err)
	require.Equal(t, uint64(5), live.Definition.Revision)
	require.Equal(t, "application input", string(live.Definition.Input))
	changed := original
	d := *original.Job.PutDefinition
	d.Definition.Input = []byte("different")
	changed.Job = &db.JobCommand{PutDefinition: &d}
	requireRaftReceiptRejection(t, n, changed, "request_conflict")
}
func TestRaftConfigurationReceiptCompactionResumesAfterLeaderLoss(t *testing.T) {
	nodes := startChannelCluster(t)
	original, first := seedRaftConfigurationSweep(t, nodes[0])
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			v, err := n.store.GetJobConfigurationReceiptCompaction("tenant", "audit")
			if err != nil || v.Step != first.Step {
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
	var others []*durableNode
	for i, n := range nodes {
		if i != leader && i != next {
			others = append(others, n)
		}
	}
	finishRaftConfigurationSweep(t, nodes[next], original, first, others...)
}
func TestRaftConfigurationReceiptCompactionSnapshotRebuildResumesProgress(t *testing.T) {
	dir := t.TempDir()
	n := startDurableNode(t, dir, 2)
	original, first := seedRaftConfigurationSweep(t, n)
	target := n.store.LastAppliedIndex()
	barrier := db.StateCommand{RequestID: "configuration-snapshot-barrier", At: time.Now().UTC(), Job: &db.JobCommand{PutTenant: &db.PutJobTenant{ExpectedRevision: 1, Tenant: db.JobTenant{ID: "tenant"}}}}
	for range 3 {
		proposeChannel(t, n, barrier)
	}
	require.Eventually(t, func() bool { s, err := n.raft.Snapshotter.Load(); return err == nil && s.Metadata.Index >= target }, 3*time.Second, time.Millisecond)
	n.close()
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "store")))
	n = startDurableNode(t, dir, 2)
	finishRaftConfigurationSweep(t, n, original, first)
	// The barrier was written after the captured horizon and stays a full receipt.
	require.False(t, proposeChannel(t, n, barrier).Compacted)
}
