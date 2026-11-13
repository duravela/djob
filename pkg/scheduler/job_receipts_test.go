package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/stretchr/testify/require"
)

func TestJobReceiptCompactionResumesOnlyCommittedWorkAndRetriesExactBatch(t *testing.T) {
	f := newRuntime(t, fake.Options{})
	terminalRetentionRun(t, f, "tenant", "run", 8)
	f.state(db.StateCommand{Job: &db.JobCommand{Purge: &db.PurgeJob{TenantID: "tenant", RunID: "run", FinishedBefore: time.Now().UTC(), Actor: "operator", Reason: "expired history"}}})
	leader := true
	m, err := NewJobRetention(JobRetentionConfig{BatchSize: 1, DeleteBatchSize: 2}, f.backend, func() bool { return leader })
	require.NoError(t, err)
	stats, err := m.Tick(context.Background(), time.Now().UTC())
	require.NoError(t, err)
	require.Zero(t, stats.ReceiptBatches, "primary deletion alone does not opt into receipt compaction")
	_, err = f.backend.GetJobReceiptCompaction("tenant", "run")
	require.Error(t, err)
	first := f.state(db.StateCommand{Job: &db.JobCommand{CompactReceipts: &db.CompactJobReceipts{TenantID: "tenant", RunID: "run", BatchSize: 1, Actor: "tenant/operator", Reason: "compact receipt copies"}}}).Job.ReceiptCompaction
	require.False(t, first.Complete)
	leader = false
	before := len(f.backend.commands)
	stats, err = m.Tick(context.Background(), time.Now().UTC())
	require.NoError(t, err)
	require.Zero(t, stats.ReceiptBatches)
	require.Len(t, f.backend.commands, before)
	leader = true
	f.backend.loseAck.Store(true)
	at := time.Now().UTC().Add(time.Second)
	stats, err = m.Tick(context.Background(), at)
	require.NoError(t, err)
	require.Equal(t, 1, stats.ReceiptBatches)
	commands := f.backend.commands
	require.Equal(t, commands[len(commands)-2], commands[len(commands)-1], "lost acknowledgement retries exactly one immutable envelope")
	progress, err := f.backend.GetJobReceiptCompaction("tenant", "run")
	require.NoError(t, err)
	require.Equal(t, first.ReceiptsCompacted+2, progress.ReceiptsCompacted)
	// A replacement controller has no process-local cursor or request history.
	m, err = NewJobRetention(JobRetentionConfig{BatchSize: 1, DeleteBatchSize: 2}, f.backend, func() bool { return true })
	require.NoError(t, err)
	for range 16 {
		_, err := m.Tick(context.Background(), at)
		require.NoError(t, err)
	}
	progress, err = f.backend.GetJobReceiptCompaction("tenant", "run")
	require.NoError(t, err)
	require.True(t, progress.Complete)
	require.Equal(t, uint64(10), progress.ReceiptsCompacted)
	require.Equal(t, "tenant/operator", progress.Actor)
	policy, err := f.backend.GetJobRetention("tenant")
	require.NoError(t, err)
	require.False(t, policy.Enabled)
}
