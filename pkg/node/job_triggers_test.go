package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/jobs"
	"github.com/duravela/djob/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

func seedRaftJobTrigger(t *testing.T, n *durableNode) *db.JobTrigger {
	t.Helper()
	genericProposal(t, n, "trigger-tenant", db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "tenant"}}})
	genericProposal(t, n, "trigger-definition", db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "tenant", ID: "hash", Runtime: db.StepRuntime, Handler: "sha256", HandlerVersion: "1", Input: []byte("scheduled")}}})
	start := time.Now().UTC().Add(-time.Hour)
	put, err := scheduler.PrepareJobTrigger(db.JobTriggerDefinition{TenantID: "tenant", ID: "hourly", DefinitionID: "hash", DefinitionRevision: 1, MaxPending: 2, Timing: db.ScheduleTiming{Spec: db.ScheduleSpec{Kind: db.ScheduleInterval, StartAt: start, Interval: time.Hour}, MissedPolicy: db.MissedCoalesce}}, 0, time.Now().UTC())
	require.NoError(t, err)
	return genericProposal(t, n, "trigger-create", db.JobCommand{PutTrigger: &put}).Job.Trigger
}

func TestRaftJobTriggerLeaderReplacementKeepsOccurrenceCursor(t *testing.T) {
	nodes := startChannelCluster(t)
	trigger := seedRaftJobTrigger(t, nodes[0])
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			if _, err := n.store.GetJobTrigger("tenant", "hourly"); err != nil {
				return false
			}
		}
		return true
	}, 3*time.Second, 5*time.Millisecond)
	materializers := make([]*scheduler.JobTriggerMaterializer, len(nodes))
	leader := -1
	for i, n := range nodes {
		var err error
		materializers[i], err = scheduler.NewJobTriggerMaterializer(scheduler.MaterializerConfig{CommitTimeout: time.Second}, n.store, n.raft.IsLeader)
		require.NoError(t, err)
		if n.raft.IsLeader() {
			leader = i
		}
	}
	require.NotEqual(t, -1, leader)
	stats, err := materializers[leader].Tick(context.Background(), time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, 1, stats.Created)
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			p, err := n.store.ListJobRuns("tenant", nil, 10)
			if err != nil || len(p.Runs) != 1 {
				return false
			}
		}
		return true
	}, 3*time.Second, 5*time.Millisecond)
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
	stats, err = materializers[next].Tick(context.Background(), time.Now().UTC())
	require.NoError(t, err)
	require.Zero(t, stats.Created)
	stats, err = materializers[next].Tick(context.Background(), trigger.Timing.Spec.StartAt.Add(2*time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, stats.Created)
	page, err := nodes[next].store.ListJobRuns("tenant", nil, 10)
	require.NoError(t, err)
	require.Len(t, page.Runs, 2)
	require.NotEqual(t, page.Runs[0].ID, page.Runs[1].ID)
	for _, r := range page.Runs {
		require.Equal(t, "hourly", r.TriggerID)
		require.Equal(t, uint64(1), r.TriggerRevision)
	}
}

func TestRaftJobTriggerSnapshotRebuildPreservesTemplateAndPendingIndex(t *testing.T) {
	dir := t.TempDir()
	n := startDurableNode(t, dir, 2)
	trigger := seedRaftJobTrigger(t, n)
	m, err := scheduler.NewJobTriggerMaterializer(scheduler.MaterializerConfig{}, n.store, n.raft.IsLeader)
	require.NoError(t, err)
	stats, err := m.Tick(context.Background(), time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, 1, stats.Created)
	target := n.store.LastAppliedIndex()
	barrier := db.StateCommand{RequestID: "trigger-snapshot-barrier", At: time.Now().UTC(), Job: &db.JobCommand{PutTenant: &db.PutJobTenant{ExpectedRevision: 1, Tenant: db.JobTenant{ID: "tenant", MaxRunning: 1}}}}
	for range 3 {
		proposeChannel(t, n, barrier)
	}
	require.Eventually(t, func() bool {
		snapshot, err := n.raft.Snapshotter.Load()
		return err == nil && snapshot.Metadata.Index >= target
	}, 3*time.Second, time.Millisecond)
	n.close()
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "store")))
	n = startDurableNode(t, dir, 2)
	restored, err := n.store.GetJobTrigger("tenant", "hourly")
	require.NoError(t, err)
	require.Equal(t, trigger.Template, restored.Template)
	capacity, err := n.store.JobTriggerCapacity("tenant", "hourly")
	require.NoError(t, err)
	require.Equal(t, 1, capacity)
	m, err = scheduler.NewJobTriggerMaterializer(scheduler.MaterializerConfig{}, n.store, n.raft.IsLeader)
	require.NoError(t, err)
	stats, err = m.Tick(context.Background(), time.Now().UTC())
	require.NoError(t, err)
	require.Zero(t, stats.Created)
	page, err := n.store.ListJobRuns("tenant", nil, 10)
	require.NoError(t, err)
	require.Len(t, page.Runs, 1)
	worker, err := jobs.NewWorker(n.store, jobs.Config{}, jobs.Builtins())
	require.NoError(t, err)
	result, err := worker.Execute(context.Background(), "tenant", page.Runs[0].ID)
	require.NoError(t, err)
	require.Equal(t, db.JobSucceeded, result.Run.Status)
	capacity, err = n.store.JobTriggerCapacity("tenant", "hourly")
	require.NoError(t, err)
	require.Equal(t, 2, capacity)
}
