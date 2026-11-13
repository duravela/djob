package db

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestStateInventoryFollowsDurableTransitionsAndSurvivesRestoreWithoutWrites(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway", "uncertain", "cancelled", "queued")
	h.ok(claim(owner, "uncertain", "attempt"))
	h.ok(StateCommand{CancelRun: &CancelRun{RunID: "cancelled"}})
	h.ok(StateCommand{Finish: &FinishAttempt{ChannelOwner: owner, RunID: "uncertain", AttemptID: "attempt", ClaimToken: "attempt-token", Outcome: AttemptUnknown, Quiescent: false}})
	h.job("alpha", "pollers", "work", "", "private-bus")
	h.job("beta", "", "work", "")
	h.launch("alpha", "work", "uncertain")
	h.launch("beta", "work", "waiting")
	h.launch("beta", "work", "queued")
	a := h.claimJob("alpha", "uncertain", "attempt")
	b := h.claimJob("beta", "waiting", "attempt")
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: b, Kind: JobWait, WaitKey: "reply", State: []byte("private session")}}))
	h.at = h.at.Add(time.Minute)
	h.ok(jc(JobCommand{Expire: &ExpireJob{JobOwner: a, LeaseUntil: h.at}}))
	var before, after bytes.Buffer
	require.NoError(t, h.db.Snapshot(&before))
	stats, err := h.db.StateInventory(context.Background(), 100)
	require.NoError(t, err)
	require.Equal(t, h.index, stats.AppliedIndex)
	for kind, sample := range stats.Kinds {
		require.True(t, sample.Complete)
		var total uint64
		for _, count := range sample.Counts {
			total += count
		}
		require.Equal(t, sample.Observed, total, StateInventoryKind(kind).String())
	}
	require.EqualValues(t, 1, stats.Kinds[InventoryGatewayRuns].Counts[InventoryUnknown])
	require.EqualValues(t, 1, stats.Kinds[InventoryGatewayRuns].Counts[InventoryCancelled])
	require.EqualValues(t, 1, stats.Kinds[InventoryGatewayRuns].Counts[InventoryReady])
	require.EqualValues(t, 1, stats.Kinds[InventoryGenericRuns].Counts[InventoryUnknown])
	require.EqualValues(t, 1, stats.Kinds[InventoryGenericRuns].Counts[InventoryWaiting])
	require.EqualValues(t, 1, stats.Kinds[InventoryGenericRuns].Counts[InventoryReady])
	require.EqualValues(t, 1, stats.Kinds[InventoryChannels].Counts[InventoryBlocked])
	require.EqualValues(t, 1, stats.Kinds[InventoryResources].Counts[InventoryBlocked])
	require.NoError(t, h.db.Snapshot(&after))
	require.Equal(t, before.Bytes(), after.Bytes(), "sampling appends no commands, receipts or indexes")
	require.Zero(t, h.db.db.Stats().OpenTxN)
	q, err := NewDB(filepath.Join(t.TempDir(), "restored.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	require.NoError(t, q.RestoreAt(&before, h.index))
	restored, err := q.StateInventory(context.Background(), 100)
	require.NoError(t, err)
	require.Equal(t, stats, restored)
}

func TestStateInventoryRecognizesAllFixedStatesAndKeepsKindsIndependent(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error {
		for kind := StateInventoryKind(0); kind < StateInventoryKindCount; kind++ {
			for _, state := range kind.States() {
				id := "record-" + state.String()
				switch kind {
				case InventoryGatewayRuns:
					require.NoError(t, writeRecord(tx, runsBucket, id, &Run{ID: id, State: RunState(state.String())}))
				case InventoryGenericRuns:
					require.NoError(t, writeRecord(tx, jobRunsBucket, jobKey("tenant", id), &JobRun{TenantID: "tenant", ID: id, Status: JobStatus(state.String())}))
				case InventoryChannels:
					require.NoError(t, writeRecord(tx, channelsBucket, id, &CommunicationChannel{ID: id, State: ChannelState(state.String())}))
				case InventoryResources:
					require.NoError(t, writeRecord(tx, jobResourcesBucket, id, &JobResource{Spec: ResourceSpec{ID: id}, State: ResourceState(state.String())}))
				}
			}
		}
		return nil
	}))
	stats, err := q.StateInventory(context.Background(), 100)
	require.NoError(t, err)
	for kind := StateInventoryKind(0); kind < StateInventoryKindCount; kind++ {
		require.True(t, stats.Kinds[kind].Complete)
		for _, state := range kind.States() {
			require.EqualValues(t, 1, stats.Kinds[kind].Counts[state], "%s/%s", kind, state)
		}
	}
	limited, err := q.StateInventory(context.Background(), 4)
	require.NoError(t, err)
	require.False(t, limited.Kinds[InventoryGatewayRuns].Complete)
	require.False(t, limited.Kinds[InventoryGenericRuns].Complete)
	require.True(t, limited.Kinds[InventoryChannels].Complete, "exactly the cap is complete")
	require.True(t, limited.Kinds[InventoryResources].Complete)
}

func TestStateInventoryBoundsDecodingAndRejectsCorruptIdentityOrState(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error {
		require.NoError(t, writeRecord(tx, runsBucket, "a", &Run{ID: "a", State: RunReady}))
		return tx.Bucket(runsBucket).Put([]byte("z"), []byte("private corrupt data"))
	}))
	stats, err := q.StateInventory(context.Background(), 1)
	require.NoError(t, err)
	require.False(t, stats.Kinds[InventoryGatewayRuns].Complete)
	require.EqualValues(t, 1, stats.Kinds[InventoryGatewayRuns].Observed)
	stats, err = q.StateInventory(context.Background(), 2)
	require.Error(t, err)
	require.Equal(t, StateInventory{}, stats)
	for _, run := range []Run{{ID: "different", State: RunReady}, {ID: "z", State: "private-invalid-state"}} {
		require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error { return writeRecord(tx, runsBucket, "z", &run) }))
		stats, err = q.StateInventory(context.Background(), 2)
		require.Error(t, err)
		require.Equal(t, StateInventory{}, stats)
	}
	require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.Bucket(runsBucket).Delete([]byte("z")); err != nil {
			return err
		}
		// The first oversized record must not be decoded, even if malformed.
		return tx.Bucket(jobRunsBucket).Put([]byte(jobKey("tenant", "oversized")), bytes.Repeat([]byte{0xff}, MaxStateSampleBytes+1))
	}))
	stats, err = q.StateInventory(context.Background(), 2)
	require.NoError(t, err)
	require.True(t, stats.Kinds[InventoryGatewayRuns].Complete)
	require.False(t, stats.Kinds[InventoryGenericRuns].Complete)
	require.Zero(t, stats.Kinds[InventoryGenericRuns].Observed)
	require.Zero(t, stats.Kinds[InventoryGenericRuns].DecodedBytes)
}

func TestStateInventoryCapsAggregateBytesBeforeDecodingAnotherPrimaryRecord(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error {
		for i := range 8 {
			id := fmt.Sprintf("r%d", i)
			run := &JobRun{TenantID: "tenant", ID: id, Status: JobSucceeded, State: bytes.Repeat([]byte("s"), 1<<20), Definition: JobDefinition{Input: bytes.Repeat([]byte("i"), 1<<20)}}
			if err := writeRecord(tx, jobRunsBucket, jobKey("tenant", id), run); err != nil {
				return err
			}
		}
		return nil
	}))
	stats, err := q.StateInventory(context.Background(), 100)
	require.NoError(t, err)
	sample := stats.Kinds[InventoryGenericRuns]
	require.False(t, sample.Complete)
	require.Positive(t, sample.Observed)
	require.Less(t, sample.Observed, uint64(8))
	require.LessOrEqual(t, sample.DecodedBytes, uint64(MaxStateSampleBytes))
	require.Equal(t, sample.Observed, sample.Counts[InventorySucceeded])
}

func TestStateInventorySkipsBusyAndRejectsCancelledInvalidOrClosedProbes(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	for _, limit := range []int{0, -1, MaxStateSampleRows + 1} {
		_, err := q.StateInventory(context.Background(), limit)
		require.Error(t, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := q.StateInventory(ctx, 1)
	require.ErrorIs(t, err, context.Canceled)
	q.mu.Lock()
	_, err = q.StateInventory(context.Background(), 1)
	q.mu.Unlock()
	require.ErrorIs(t, err, ErrStateInventoryBusy)
	require.NoError(t, q.Close())
	_, err = q.StateInventory(context.Background(), 1)
	require.ErrorIs(t, err, ErrStateInventoryUnavailable)
}

func TestReleaseInventoryCountsPinnedWorkWithoutPayloadsOrUnboundedScans(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "", "ordinary", "")
	h.publishedRelease("v1", "w1")
	h.ok(releaseDefinition("work", "v1"))
	h.launch("alpha", "work", "queued")
	c := deploymentCommand("v1", "retiring", 2)
	c.WorkerDeployment.WorkerID = "w1"
	h.ok(c)
	stats, err := h.db.StateInventory(context.Background(), 32)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.Releases.Retiring)
	require.EqualValues(t, 1, stats.Releases.Outstanding)
	require.True(t, stats.Releases.DeploymentsComplete)
	require.Equal(t, h.at, stats.Releases.OldestRetiring)
	h.publishedRelease("v2", "w2")
	stats, err = h.db.StateInventory(context.Background(), 1)
	require.NoError(t, err)
	require.False(t, stats.Releases.DeploymentsComplete)
}
