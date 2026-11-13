package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/stretchr/testify/require"
)

func terminalRetentionRun(t *testing.T, f *runtimeFixture, tenant, run string, events int) {
	t.Helper()
	if _, err := f.backend.GetJobTenant(tenant); err != nil {
		f.state(db.StateCommand{Job: &db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: tenant}}}})
		f.state(db.StateCommand{Job: &db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: tenant, ID: "work", Runtime: db.StepRuntime, Handler: "sha256", HandlerVersion: "1"}}}})
	}
	f.state(db.StateCommand{Job: &db.JobCommand{Launch: &db.LaunchJob{TenantID: tenant, DefinitionID: "work", DefinitionRevision: 1, RunID: run}}})
	for i := 0; i < events; i++ {
		f.state(db.StateCommand{Job: &db.JobCommand{Signal: &db.SignalJob{TenantID: tenant, RunID: run, EventID: string(rune('a' + i)), Key: "reply", Data: []byte("data")}}})
	}
	f.state(db.StateCommand{Job: &db.JobCommand{Cancel: &db.CancelJob{TenantID: tenant, RunID: run}}})
}
func TestJobRetentionIsOptInAndHonorsCutoff(t *testing.T) {
	f := newRuntime(t, fake.Options{})
	terminalRetentionRun(t, f, "tenant", "run", 0)
	leader := false
	m, err := NewJobRetention(JobRetentionConfig{}, f.backend, func() bool { return leader })
	require.NoError(t, err)
	before := len(f.backend.commands)
	at := time.Now().UTC()
	stats, err := m.Tick(context.Background(), at.Add(2*time.Hour))
	require.NoError(t, err)
	require.Zero(t, stats.Batches)
	leader = true
	stats, err = m.Tick(context.Background(), at.Add(2*time.Hour))
	require.NoError(t, err)
	require.Zero(t, stats.Batches)
	require.Len(t, f.backend.commands, before)
	f.state(db.StateCommand{Job: &db.JobCommand{PutRetention: &db.PutJobRetention{Policy: db.JobRetentionPolicy{TenantID: "tenant", Enabled: true, KeepFor: time.Hour}}}})
	for range 3 {
		stats, err = m.Tick(context.Background(), at.Add(30*time.Minute))
		require.NoError(t, err)
		require.Zero(t, stats.Batches)
	}
	completed := 0
	for range 3 {
		stats, err = m.Tick(context.Background(), at.Add(2*time.Hour))
		require.NoError(t, err)
		completed += stats.Completed
	}
	require.Equal(t, 1, completed)
	deletion, err := f.backend.GetJobDeletion("tenant", "run")
	require.NoError(t, err)
	require.Equal(t, "djob/retention", deletion.Actor)
	require.Equal(t, uint64(1), deletion.PolicyRevision)
}
func TestJobRetentionContinuesCommittedManualCleanupWithPolicyDisabled(t *testing.T) {
	f := newRuntime(t, fake.Options{})
	terminalRetentionRun(t, f, "tenant", "run", 8)
	at := time.Now().UTC()
	first := f.state(db.StateCommand{Job: &db.JobCommand{Purge: &db.PurgeJob{TenantID: "tenant", RunID: "run", FinishedBefore: at, BatchSize: 1, Actor: "operator", Reason: "manual cleanup"}}}).Job.Deletion
	require.False(t, first.CleanupComplete)
	m, err := NewJobRetention(JobRetentionConfig{BatchSize: 1, DeleteBatchSize: 2}, f.backend, func() bool { return true })
	require.NoError(t, err)
	for range 30 {
		_, err = m.Tick(context.Background(), at.Add(time.Second))
		require.NoError(t, err)
	}
	d, err := f.backend.GetJobDeletion("tenant", "run")
	require.NoError(t, err)
	require.True(t, d.CleanupComplete)
	require.Equal(t, "operator", d.Actor)
	policy, err := f.backend.GetJobRetention("tenant")
	require.NoError(t, err)
	require.False(t, policy.Enabled)
	usage, err := f.backend.GetJobUsage("tenant")
	require.NoError(t, err)
	require.Zero(t, usage.StoredRuns)
	require.Zero(t, usage.InboxEvents)
	require.Zero(t, usage.InboxBytes)
}
func TestJobRetentionLostAckRetriesOneExactBatch(t *testing.T) {
	f := newRuntime(t, fake.Options{})
	terminalRetentionRun(t, f, "tenant", "run", 3)
	at := time.Now().UTC()
	first := f.state(db.StateCommand{Job: &db.JobCommand{Purge: &db.PurgeJob{TenantID: "tenant", RunID: "run", FinishedBefore: at, BatchSize: 1, Actor: "operator", Reason: "manual cleanup"}}}).Job.Deletion
	m, err := NewJobRetention(JobRetentionConfig{BatchSize: 1, DeleteBatchSize: 1}, f.backend, func() bool { return true })
	require.NoError(t, err)
	f.backend.loseAck.Store(true)
	stats, err := m.Tick(context.Background(), at.Add(time.Second))
	require.NoError(t, err)
	require.Equal(t, 1, stats.Batches)
	commands := f.backend.commands
	require.Equal(t, commands[len(commands)-2], commands[len(commands)-1])
	d, err := f.backend.GetJobDeletion("tenant", "run")
	require.NoError(t, err)
	require.Equal(t, first.RecordsDeleted+1, d.RecordsDeleted)
}
func TestJobRetentionScansPastDisabledTenantsWithBoundedPages(t *testing.T) {
	f := newRuntime(t, fake.Options{})
	for _, tenant := range []string{"first", "second", "enabled"} {
		terminalRetentionRun(t, f, tenant, "run", 0)
	}
	f.state(db.StateCommand{Job: &db.JobCommand{PutRetention: &db.PutJobRetention{Policy: db.JobRetentionPolicy{TenantID: "enabled", Enabled: true, KeepFor: time.Second}}}})
	m, err := NewJobRetention(JobRetentionConfig{BatchSize: 1}, f.backend, func() bool { return true })
	require.NoError(t, err)
	for range 10 {
		_, err = m.Tick(context.Background(), time.Now().UTC().Add(2*time.Second))
		require.NoError(t, err)
	}
	_, err = f.backend.GetJobDeletion("enabled", "run")
	require.NoError(t, err)
	_, err = f.backend.GetJobRun("first", "run")
	require.NoError(t, err)
	_, err = f.backend.GetJobRun("second", "run")
	require.NoError(t, err)
}
