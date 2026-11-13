package node

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/jobs"
	"github.com/duravela/djob/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

func seedRaftEventTrigger(t *testing.T, n *durableNode) db.ActivateJob {
	t.Helper()
	genericProposal(t, n, "event-tenant", db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "tenant"}}})
	genericProposal(t, n, "event-definition", db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "tenant", ID: "hash", Runtime: db.StepRuntime, Handler: "sha256-event", HandlerVersion: "1", Input: []byte("configuration")}}})
	put, err := scheduler.PrepareJobTrigger(db.JobTriggerDefinition{TenantID: "tenant", ID: "ingest", DefinitionID: "hash", DefinitionRevision: 1, MaxPending: 2, Event: &db.EventTriggerSpec{Source: "meters"}}, 0, time.Now().UTC())
	require.NoError(t, err)
	genericProposal(t, n, "event-trigger", db.JobCommand{PutTrigger: &put})
	return db.ActivateJob{TenantID: "tenant", TriggerID: "ingest", Source: "meters", EventID: "reading-1", OccurredAt: time.Now().UTC().Add(-time.Hour), Data: []byte("reading")}
}

func TestRaftJobActivationConcurrentDeliveryAndLeaderLossCreateOneRun(t *testing.T) {
	nodes := startChannelCluster(t)
	event := seedRaftEventTrigger(t, nodes[0])
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			if _, err := n.store.GetJobTrigger("tenant", "ingest"); err != nil {
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
			results[i], errs[i] = n.store.ProposeStateCommand(ctx, db.StateCommand{RequestID: fmt.Sprintf("deliver/%d", i), At: time.Now().UTC(), Job: &db.JobCommand{Activate: &event}})
		}(i, n)
	}
	wg.Wait()
	id := db.EventJobRunID("tenant", "ingest", "meters", "reading-1")
	for i := range nodes {
		require.NoError(t, errs[i])
		require.NoError(t, results[i].Err())
		require.Equal(t, id, results[i].Job.Activation.RunID)
		require.Equal(t, results[0].Job.Activation, results[i].Job.Activation)
	}
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			p, err := n.store.ListJobRuns("tenant", nil, 10)
			if err != nil || len(p.Runs) != 1 {
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
	redelivery := genericProposal(t, nodes[next], "redelivery-after-leader-loss", db.JobCommand{Activate: &event})
	require.Equal(t, results[0].Job.Activation, redelivery.Job.Activation)
	worker, err := jobs.NewWorker(nodes[next].store, jobs.Config{}, jobs.Builtins())
	require.NoError(t, err)
	r, err := worker.Execute(context.Background(), "tenant", id)
	require.NoError(t, err)
	require.Equal(t, db.JobSucceeded, r.Run.Status)
	sum := sha256.Sum256([]byte("reading"))
	require.Equal(t, sum[:], r.Run.Result)
	require.Equal(t, []byte("configuration"), r.Run.Definition.Input)
	genericProposal(t, nodes[next], "redelivery-after-completion", db.JobCommand{Activate: &event})
	page, err := nodes[next].store.ListJobRuns("tenant", nil, 10)
	require.NoError(t, err)
	require.Len(t, page.Runs, 1)
}

func TestRaftJobActivationSnapshotRebuildRetainsAcceptedPayloadAndAdmission(t *testing.T) {
	dir := t.TempDir()
	n := startDurableNode(t, dir, 2)
	event := seedRaftEventTrigger(t, n)
	first := genericProposal(t, n, "first-delivery", db.JobCommand{Activate: &event}).Job.Activation
	target := n.store.LastAppliedIndex()
	barrier := db.StateCommand{RequestID: "activation-snapshot-barrier", At: time.Now().UTC(), Job: &db.JobCommand{PutTenant: &db.PutJobTenant{ExpectedRevision: 1, Tenant: db.JobTenant{ID: "tenant", MaxRunning: 1}}}}
	for range 3 {
		proposeChannel(t, n, barrier)
	}
	require.Eventually(t, func() bool { s, err := n.raft.Snapshotter.Load(); return err == nil && s.Metadata.Index >= target }, 3*time.Second, time.Millisecond)
	n.close()
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "store")))
	n = startDurableNode(t, dir, 2)
	accepted, err := n.store.GetJobActivation("tenant", "ingest", "meters", "reading-1")
	require.NoError(t, err)
	require.Equal(t, first, accepted)
	capacity, err := n.store.JobTriggerCapacity("tenant", "ingest")
	require.NoError(t, err)
	require.Equal(t, 1, capacity)
	repeated := genericProposal(t, n, "redelivered-after-rebuild", db.JobCommand{Activate: &event}).Job.Activation
	require.Equal(t, first, repeated)
	worker, err := jobs.NewWorker(n.store, jobs.Config{}, jobs.Builtins())
	require.NoError(t, err)
	r, err := worker.Execute(context.Background(), "tenant", first.RunID)
	require.NoError(t, err)
	require.Equal(t, db.JobSucceeded, r.Run.Status)
	sum := sha256.Sum256(event.Data)
	require.Equal(t, sum[:], r.Run.Result)
	capacity, err = n.store.JobTriggerCapacity("tenant", "ingest")
	require.NoError(t, err)
	require.Equal(t, 2, capacity)
	page, err := n.store.ListJobRuns("tenant", nil, 10)
	require.NoError(t, err)
	require.Len(t, page.Runs, 1)
}
