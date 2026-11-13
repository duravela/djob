package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/jobs"
	"github.com/stretchr/testify/require"
)

func seedRaftJobLimits(t *testing.T, n *durableNode) {
	t.Helper()
	genericProposal(t, n, "quota-tenant", db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "tenant"}}})
	genericProposal(t, n, "quota-definition", db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "tenant", ID: "hash", Runtime: db.StepRuntime, Handler: "sha256", HandlerVersion: "1", Input: []byte("input")}}})
	genericProposal(t, n, "quota-policy", db.JobCommand{PutLimits: &db.PutJobLimits{Limits: db.JobLimits{TenantID: "tenant", MaxPendingRuns: 1, MaxStoredRuns: 2, MaxInboxEvents: 1, MaxInboxBytes: 2}}})
}

func TestRaftJobLimitsSerializeConcurrentAdmissionAcrossLeaderLoss(t *testing.T) {
	nodes := startChannelCluster(t)
	seedRaftJobLimits(t, nodes[0])
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			v, err := n.store.GetJobLimits("tenant")
			if err != nil || v.Revision != 1 {
				return false
			}
		}
		return true
	}, 3*time.Second, 5*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	results := make([]db.StateResult, len(nodes))
	errs := make([]error, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(i int, n *durableNode) {
			defer wg.Done()
			results[i], errs[i] = n.store.ProposeStateCommand(ctx, db.StateCommand{RequestID: fmt.Sprintf("quota-launch/%d", i), At: time.Now().UTC(), Job: &db.JobCommand{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: "hash", DefinitionRevision: 1, RunID: fmt.Sprintf("run-%d", i)}}})
		}(i, n)
	}
	wg.Wait()
	admitted, denied := 0, 0
	runID := ""
	for i, r := range results {
		err := errs[i]
		if err == nil {
			err = r.Err()
		}
		if err == nil {
			admitted++
			runID = r.Job.Run.ID
			continue
		}
		var rejection *db.CommandRejection
		require.ErrorAs(t, err, &rejection)
		require.Equal(t, "quota_exceeded", rejection.Code)
		denied++
	}
	require.Equal(t, 1, admitted)
	require.Equal(t, 2, denied)
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			u, err := n.store.GetJobUsage("tenant")
			if err != nil || u.PendingRuns != 1 || u.StoredRuns != 1 {
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
	rejected, err := nodes[next].store.ProposeStateCommand(ctx, db.StateCommand{RequestID: "after-leader-change", At: time.Now().UTC(), Job: &db.JobCommand{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: "hash", DefinitionRevision: 1, RunID: "after-change"}}})
	if err == nil {
		err = rejected.Err()
	}
	var rejection *db.CommandRejection
	require.True(t, errors.As(err, &rejection))
	require.Equal(t, "quota_exceeded", rejection.Code)
	worker, err := jobs.NewWorker(nodes[next].store, jobs.Config{}, jobs.Builtins())
	require.NoError(t, err)
	result, err := worker.Execute(context.Background(), "tenant", runID)
	require.NoError(t, err)
	require.Equal(t, db.JobSucceeded, result.Run.Status)
	genericProposal(t, nodes[next], "after-capacity-release", db.JobCommand{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: "hash", DefinitionRevision: 1, RunID: "next"}})
	usage, err := nodes[next].store.GetJobUsage("tenant")
	require.NoError(t, err)
	require.Equal(t, uint64(1), usage.PendingRuns)
	require.Equal(t, uint64(2), usage.StoredRuns)
}

func TestRaftJobLimitsSnapshotRebuildRetainsUsageAndRejectedEnvelope(t *testing.T) {
	dir := t.TempDir()
	n := startDurableNode(t, dir, 2)
	seedRaftJobLimits(t, n)
	genericProposal(t, n, "quota-first-run", db.JobCommand{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: "hash", DefinitionRevision: 1, RunID: "first"}})
	genericProposal(t, n, "quota-inbox", db.JobCommand{Signal: &db.SignalJob{TenantID: "tenant", RunID: "first", EventID: "event", Key: "reply", Data: []byte("ab")}})
	rejected := db.StateCommand{RequestID: "quota-full-envelope", At: time.Now().UTC(), Job: &db.JobCommand{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: "hash", DefinitionRevision: 1, RunID: "second"}}}
	r, err := n.store.ProposeStateCommand(context.Background(), rejected)
	if err == nil {
		err = r.Err()
	}
	require.Error(t, err)
	target := n.store.LastAppliedIndex()
	barrier := db.StateCommand{RequestID: "quota-snapshot-barrier", At: time.Now().UTC(), Job: &db.JobCommand{PutTenant: &db.PutJobTenant{ExpectedRevision: 1, Tenant: db.JobTenant{ID: "tenant"}}}}
	for range 3 {
		proposeChannel(t, n, barrier)
	}
	require.Eventually(t, func() bool { s, err := n.raft.Snapshotter.Load(); return err == nil && s.Metadata.Index >= target }, 3*time.Second, time.Millisecond)
	n.close()
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "store")))
	n = startDurableNode(t, dir, 2)
	usage, err := n.store.GetJobUsage("tenant")
	require.NoError(t, err)
	require.Equal(t, db.JobUsage{TenantID: "tenant", PendingRuns: 1, StoredRuns: 1, InboxEvents: 1, InboxBytes: 2}, *usage)
	genericProposal(t, n, "quota-cancel", db.JobCommand{Cancel: &db.CancelJob{TenantID: "tenant", RunID: "first"}})
	r, err = n.store.ProposeStateCommand(context.Background(), rejected)
	if err == nil {
		err = r.Err()
	}
	require.Error(t, err, "durable rejection survives released capacity and snapshot rebuild")
	genericProposal(t, n, "quota-fresh-envelope", *rejected.Job)
	usage, err = n.store.GetJobUsage("tenant")
	require.NoError(t, err)
	require.Equal(t, uint64(1), usage.PendingRuns)
	require.Equal(t, uint64(2), usage.StoredRuns)
	require.Equal(t, uint64(2), usage.InboxBytes)
}
