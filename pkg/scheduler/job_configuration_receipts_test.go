package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/stretchr/testify/require"
)

func TestConfigurationReceiptCompactionControllerResumesIntentAndRetriesExactEnvelope(t *testing.T) {
	f := newRuntime(t, fake.Options{})
	at := time.Now().UTC()
	seedGenericTrigger(t, f, genericTriggerDefinition(at))
	leader := true
	m, err := NewJobRetention(JobRetentionConfig{DeleteBatchSize: 1}, f.backend, func() bool { return leader })
	require.NoError(t, err)
	stats, err := m.Tick(context.Background(), at)
	require.NoError(t, err)
	require.Zero(t, stats.ConfigurationReceiptBatches)
	first := f.state(db.StateCommand{Job: &db.JobCommand{CompactConfigurationReceipts: &db.CompactJobConfigurationReceipts{TenantID: "tenant", ID: "audit", BatchSize: 1, Actor: "operator", Reason: "compact configuration copies"}}}).Job.ConfigurationReceiptCompaction
	require.False(t, first.Complete)
	leader = false
	_, err = m.Tick(context.Background(), at.Add(time.Second))
	require.NoError(t, err)
	leader = true
	f.backend.loseAck.Store(true)
	stats, err = m.Tick(context.Background(), at.Add(time.Second))
	require.NoError(t, err)
	require.Equal(t, 1, stats.ConfigurationReceiptBatches)
	commands := f.backend.commands
	require.Equal(t, commands[len(commands)-2], commands[len(commands)-1])
	replacement, err := NewJobRetention(JobRetentionConfig{DeleteBatchSize: 1}, f.backend, func() bool { return true })
	require.NoError(t, err)
	for range 8 {
		_, err = replacement.Tick(context.Background(), at.Add(2*time.Second))
		require.NoError(t, err)
	}
	progress, err := f.backend.GetJobConfigurationReceiptCompaction("tenant", "audit")
	require.NoError(t, err)
	require.True(t, progress.Complete)
	require.Equal(t, first.Horizon, progress.ReceiptsCompacted)
	require.Equal(t, "operator", progress.Actor)
	policy, err := f.backend.GetJobRetention("tenant")
	require.NoError(t, err)
	require.False(t, policy.Enabled)
}

type compactBeforeMaterializationRetry struct {
	*runtimeBackend
	intercept bool
}

func (b *compactBeforeMaterializationRetry) ProposeStateCommand(ctx context.Context, c db.StateCommand) (db.StateResult, error) {
	result, err := b.runtimeBackend.ProposeStateCommand(ctx, c)
	if err == nil && c.Job != nil && c.Job.Materialize != nil && b.intercept {
		b.intercept = false
		_, err = b.runtimeBackend.ProposeStateCommand(ctx, db.StateCommand{RequestID: "compact-before-retry", At: c.At, Job: &db.JobCommand{CompactConfigurationReceipts: &db.CompactJobConfigurationReceipts{TenantID: "tenant", ID: "race", Actor: "operator", Reason: "compact committed materialization"}}})
		if err != nil {
			return db.StateResult{}, err
		}
		return db.StateResult{}, context.DeadlineExceeded
	}
	return result, err
}
func TestMaterializationRetryAfterCompactionFailsClosedWithoutDuplicateOccurrences(t *testing.T) {
	f := newRuntime(t, fake.Options{})
	at := time.Now().UTC()
	initial := seedGenericTrigger(t, f, genericTriggerDefinition(at))
	at = initial.NextAt.Add(time.Second)
	backend := &compactBeforeMaterializationRetry{runtimeBackend: f.backend, intercept: true}
	m, err := NewJobTriggerMaterializer(MaterializerConfig{}, backend, func() bool { return true })
	require.NoError(t, err)
	_, err = m.Tick(context.Background(), at)
	require.ErrorContains(t, err, db.ReceiptCompactedCode)
	rows, err := f.backend.ListJobRuns("tenant", nil, 10)
	require.NoError(t, err)
	require.Len(t, rows.Runs, 1)
	_, err = m.Tick(context.Background(), at)
	require.NoError(t, err)
	rows, err = f.backend.ListJobRuns("tenant", nil, 10)
	require.NoError(t, err)
	require.Len(t, rows.Runs, 1)
	trigger, err := f.backend.GetJobTrigger("tenant", "trigger")
	require.NoError(t, err)
	require.True(t, trigger.NextAt.After(at))
}
