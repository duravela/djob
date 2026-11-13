package db

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestQueueStatsFollowReadyTransitionsAndPreserveDurableState(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway", "first", "second", "third")
	h.ok(claim(owner, "first", "attempt"))
	h.ok(StateCommand{CancelRun: &CancelRun{RunID: "third"}})
	h.job("alpha", "", "job", "")
	h.job("beta", "", "job", "")
	h.launch("alpha", "job", "ready")
	h.launch("beta", "job", "ready")
	h.launch("alpha", "job", "waiting")
	jobOwner := h.claimJob("alpha", "waiting", "wait")
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: jobOwner, Kind: JobWait, WaitKey: "reply"}}))
	var before, after bytes.Buffer
	require.NoError(t, h.db.Snapshot(&before))
	stats, err := h.db.QueueStats(context.Background(), 10)
	require.NoError(t, err)
	require.Equal(t, h.index, stats.AppliedIndex)
	require.True(t, stats.Gateway.Complete && stats.Generic.Complete)
	require.EqualValues(t, 1, stats.Gateway.Observed)
	require.EqualValues(t, 2, stats.Generic.Observed, "unsupported handlers and tenant scope do not hide durable ready rows")
	require.Equal(t, h.at, stats.Gateway.OldestCreatedAt)
	require.Equal(t, h.at, stats.Generic.OldestCreatedAt)
	require.Positive(t, stats.Gateway.DecodedBytes)
	require.Positive(t, stats.Generic.DecodedBytes)
	require.Zero(t, h.db.db.Stats().OpenTxN)
	require.NoError(t, h.db.Snapshot(&after))
	require.Equal(t, before.Bytes(), after.Bytes(), "inventory probing writes no receipts, indexes or metadata")
	h.ok(jc(JobCommand{Signal: &SignalJob{TenantID: "alpha", RunID: "waiting", EventID: "reply", Key: "reply"}}))
	stats, err = h.db.QueueStats(context.Background(), 10)
	require.NoError(t, err)
	require.EqualValues(t, 3, stats.Generic.Observed, "a resource-free conversation re-enters the ready inventory")
}

func TestQueueStatsDistinguishExactBoundaryFromLimitedPrefixAndValidateIndexes(t *testing.T) {
	h := newChannelHarness(t)
	h.setup("gateway", "first", "second")
	limited, err := h.db.QueueStats(context.Background(), 1)
	require.NoError(t, err)
	require.False(t, limited.Gateway.Complete)
	require.EqualValues(t, 1, limited.Gateway.Observed)
	require.True(t, limited.Generic.Complete)
	exact, err := h.db.QueueStats(context.Background(), 2)
	require.NoError(t, err)
	require.True(t, exact.Gateway.Complete, "an index with exactly limit rows is complete")
	require.EqualValues(t, 2, exact.Gateway.Observed)
	require.NoError(t, h.db.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(runsBucket).Put([]byte("second"), []byte("corrupt private payload"))
	}))
	_, err = h.db.QueueStats(context.Background(), 1)
	require.NoError(t, err, "the bounded probe must not decode a row beyond its cap")
	stats, err := h.db.QueueStats(context.Background(), 2)
	require.Error(t, err)
	require.Equal(t, QueueStats{}, stats, "a corrupt partial query must not be published")
}

func TestQueueStatsCapSerializedBytesBeforeDecodingAndFindOldestCreation(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	oldest := time.Now().UTC().Add(-time.Hour)
	require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error {
		var changes jobChanges
		for i := range 8 {
			run := &JobRun{TenantID: "tenant", ID: fmt.Sprintf("r%d", i), Status: JobReady, CreatedAt: oldest.Add(-time.Duration(i) * time.Minute),
				State: bytes.Repeat([]byte("s"), 1<<20), Definition: JobDefinition{Input: bytes.Repeat([]byte("i"), 1<<20)}}
			saveJobRun(&changes, nil, run)
		}
		return changes.apply(tx)
	}))
	stats, err := q.QueueStats(context.Background(), 100)
	require.NoError(t, err)
	require.True(t, stats.Gateway.Complete)
	require.False(t, stats.Generic.Complete)
	require.Positive(t, stats.Generic.Observed)
	require.Less(t, stats.Generic.Observed, uint64(8))
	require.LessOrEqual(t, stats.Generic.DecodedBytes, uint64(MaxQueueSampleBytes))
	require.Equal(t, oldest.Add(-time.Duration(stats.Generic.Observed-1)*time.Minute), stats.Generic.OldestCreatedAt, "index order does not imply creation order")
}

func TestQueueStatsReportMissingAndFutureTimesWithoutInventingReadySince(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	future := time.Now().UTC().Add(time.Hour)
	require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error {
		var changes jobChanges
		saveJobRun(&changes, nil, &JobRun{TenantID: "tenant", ID: "missing", Status: JobReady})
		saveJobRun(&changes, nil, &JobRun{TenantID: "tenant", ID: "future", Status: JobReady, CreatedAt: future})
		return changes.apply(tx)
	}))
	stats, err := q.QueueStats(context.Background(), 10)
	require.NoError(t, err)
	require.True(t, stats.Generic.Complete)
	require.EqualValues(t, 2, stats.Generic.Observed)
	require.EqualValues(t, 1, stats.Generic.MissingCreatedAt)
	require.EqualValues(t, 1, stats.Generic.FutureCreatedAt)
	require.EqualValues(t, 2, stats.Generic.MissingReadySince, "older records do not inherit a guessed ready timestamp")
	require.Equal(t, future, stats.Generic.OldestCreatedAt)
}

func TestQueueStatsSkipBusyDatabaseAndRejectCancelledInvalidOrClosedProbes(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	for _, limit := range []int{0, -1, MaxQueueSampleRows + 1} {
		_, err := q.QueueStats(context.Background(), limit)
		require.Error(t, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := q.QueueStats(ctx, 10)
	require.ErrorIs(t, err, context.Canceled)
	q.mu.Lock()
	_, err = q.QueueStats(context.Background(), 10)
	q.mu.Unlock()
	require.ErrorIs(t, err, ErrQueueStatsBusy)
	require.NoError(t, q.Close())
	_, err = q.QueueStats(context.Background(), 10)
	require.ErrorIs(t, err, ErrQueueStatsUnavailable)
}
