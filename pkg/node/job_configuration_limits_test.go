package node

import (
	"context"
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

func TestRaftJobConfigurationLimitsSerializeAdmissionAcrossLeaderLoss(t *testing.T) {
	nodes := startChannelCluster(t)
	genericProposal(t, nodes[0], "config-tenant", db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "tenant"}}})
	genericProposal(t, nodes[0], "config-policy", db.JobCommand{PutConfigurationLimits: &db.PutJobConfigurationLimits{Limits: db.JobConfigurationLimits{TenantID: "tenant", MaxDefinitions: 1, MaxPayloadBytes: 3}}})
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			v, err := n.store.GetJobConfigurationLimits("tenant")
			if err != nil || v.Revision != 1 {
				return false
			}
		}
		return true
	}, 3*time.Second, 5*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	commands := make([]db.StateCommand, len(nodes))
	results := make([]db.StateResult, len(nodes))
	errs := make([]error, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		commands[i] = db.StateCommand{RequestID: fmt.Sprintf("config-admit/%d", i), At: time.Now().UTC(), Job: &db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "tenant", ID: fmt.Sprintf("hash-%d", i), Runtime: db.StepRuntime, Handler: "sha256", HandlerVersion: "1", Input: []byte("abc")}}}}
		wg.Add(1)
		go func(i int, n *durableNode) {
			defer wg.Done()
			results[i], errs[i] = n.store.ProposeStateCommand(ctx, commands[i])
		}(i, n)
	}
	wg.Wait()
	admitted, denied := 0, 0
	winner, loser := -1, -1
	for i, r := range results {
		err := errs[i]
		if err == nil {
			err = r.Err()
		}
		if err == nil {
			admitted++
			winner = i
			continue
		}
		var rejection *db.CommandRejection
		require.ErrorAs(t, err, &rejection)
		require.Equal(t, "quota_exceeded", rejection.Code)
		denied++
		loser = i
	}
	require.Equal(t, 1, admitted)
	require.Equal(t, 2, denied)
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			u, err := n.store.GetJobConfigurationUsage("tenant")
			if err != nil || u.Definitions != 1 || u.PayloadBytes != 3 {
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
	genericProposal(t, nodes[next], "config-raise", db.JobCommand{PutConfigurationLimits: &db.PutJobConfigurationLimits{ExpectedRevision: 1, Limits: db.JobConfigurationLimits{TenantID: "tenant", MaxDefinitions: 2, MaxPayloadBytes: 6}}})
	requireRaftReceiptRejection(t, nodes[next], commands[loser], "quota_exceeded")
	commands[loser].RequestID = "config-fresh-envelope"
	proposeChannel(t, nodes[next], commands[loser])
	usage, err := nodes[next].store.GetJobConfigurationUsage("tenant")
	require.NoError(t, err)
	require.Equal(t, db.JobConfigurationUsage{TenantID: "tenant", Definitions: 2, PayloadBytes: 6}, *usage)
	// Existing definitions can still launch and execute at full config capacity.
	genericProposal(t, nodes[next], "config-launch", db.JobCommand{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: commands[winner].Job.PutDefinition.Definition.ID, DefinitionRevision: 1, RunID: "run"}})
	worker, err := jobs.NewWorker(nodes[next].store, jobs.Config{}, jobs.Builtins())
	require.NoError(t, err)
	result, err := worker.Execute(context.Background(), "tenant", "run")
	require.NoError(t, err)
	require.Equal(t, db.JobSucceeded, result.Run.Status)
	actual, err := nodes[next].store.GetJobConfigurationUsage("tenant")
	require.NoError(t, err)
	require.Equal(t, usage, actual)
}

func TestRaftJobConfigurationSnapshotRebuildPreservesPinnedUsageAndRejections(t *testing.T) {
	dir := t.TempDir()
	n := startDurableNode(t, dir, 2)
	genericProposal(t, n, "config-tenant", db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "tenant"}}})
	genericProposal(t, n, "config-binding", db.JobCommand{PutBinding: &db.PutJobBinding{Binding: db.JobBinding{TenantID: "tenant", ID: "driver", Driver: "test", DriverVersion: "1", Config: []byte("xy")}}})
	genericProposal(t, n, "config-definition", db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "tenant", ID: "work", Runtime: db.StepRuntime, Handler: "sha256", HandlerVersion: "1", Input: []byte("abcd"), BindingID: "driver"}}})
	tr := db.JobCommand{PutTrigger: &db.PutJobTrigger{Definition: db.JobTriggerDefinition{TenantID: "tenant", ID: "first", DefinitionID: "work", DefinitionRevision: 1, MaxPending: 2, Event: &db.EventTriggerSpec{Source: "meters"}}}}
	genericProposal(t, n, "config-trigger", tr)
	genericProposal(t, n, "config-policy", db.JobCommand{PutConfigurationLimits: &db.PutJobConfigurationLimits{Limits: db.JobConfigurationLimits{TenantID: "tenant", MaxTriggers: 1, MaxPayloadBytes: 12}}})
	tr.PutTrigger.Definition.ID = "second"
	rejected := db.StateCommand{RequestID: "config-rejected", At: time.Now().UTC(), Job: &tr}
	requireRaftReceiptRejection(t, n, rejected, "quota_exceeded")
	target := n.store.LastAppliedIndex()
	barrier := db.StateCommand{RequestID: "config-snapshot-barrier", At: time.Now().UTC(), Job: &db.JobCommand{PutTenant: &db.PutJobTenant{ExpectedRevision: 1, Tenant: db.JobTenant{ID: "tenant"}}}}
	for range 3 {
		proposeChannel(t, n, barrier)
	}
	require.Eventually(t, func() bool { s, err := n.raft.Snapshotter.Load(); return err == nil && s.Metadata.Index >= target }, 3*time.Second, time.Millisecond)
	n.close()
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "store")))
	n = startDurableNode(t, dir, 2)
	usage, err := n.store.GetJobConfigurationUsage("tenant")
	require.NoError(t, err)
	require.Equal(t, db.JobConfigurationUsage{TenantID: "tenant", Definitions: 1, Bindings: 1, Triggers: 1, PayloadBytes: 12}, *usage)
	genericProposal(t, n, "config-raise", db.JobCommand{PutConfigurationLimits: &db.PutJobConfigurationLimits{ExpectedRevision: 1, Limits: db.JobConfigurationLimits{TenantID: "tenant", MaxTriggers: 2, MaxPayloadBytes: 18}}})
	requireRaftReceiptRejection(t, n, rejected, "quota_exceeded")
	rejected.RequestID = "config-retry-fresh"
	proposeChannel(t, n, rejected)
	usage, err = n.store.GetJobConfigurationUsage("tenant")
	require.NoError(t, err)
	require.Equal(t, uint64(2), usage.Triggers)
	require.Equal(t, uint64(18), usage.PayloadBytes)
}
