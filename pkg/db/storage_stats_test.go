package db

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestDatabaseStorageStatsOpenNoTransactionsAndPreserveState(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	require.NoError(t, q.ApplyEntries([]commit.Entry{{Index: 42}}).Error)
	var before, after bytes.Buffer
	require.NoError(t, q.Snapshot(&before))
	txCount := q.db.Stats().TxN
	for range 10 {
		stats, err := q.StorageStats()
		require.NoError(t, err)
		require.Equal(t, uint64(42), stats.AppliedIndex)
		require.Equal(t, q.db.Info().PageSize, stats.PageSize)
		require.Zero(t, stats.OpenReadTransactions, "sampling must not create its own read transaction")
	}
	require.Equal(t, txCount, q.db.Stats().TxN)
	require.NoError(t, q.Snapshot(&after))
	require.Equal(t, before.Bytes(), after.Bytes())
}

func TestDatabaseStorageStatsDistinguishPinnedPagesFromReusablePages(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucket([]byte("storage-fixture"))
		if err != nil {
			return err
		}
		for i := range 64 {
			if err := b.Put([]byte(fmt.Sprintf("%03d", i)), bytes.Repeat([]byte{byte(i)}, 16<<10)); err != nil {
				return err
			}
		}
		return nil
	}))
	reader, err := q.db.Begin(false)
	require.NoError(t, err)
	defer reader.Rollback()
	require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error { return tx.DeleteBucket([]byte("storage-fixture")) }))
	pinned, err := q.StorageStats()
	require.NoError(t, err)
	require.Positive(t, pinned.PendingPages)
	require.Equal(t, 1, pinned.OpenReadTransactions)
	require.NoError(t, reader.Rollback())
	require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error { _, err := tx.CreateBucket([]byte("after-release")); return err }))
	reusable, err := q.StorageStats()
	require.NoError(t, err)
	require.Zero(t, reusable.OpenReadTransactions)
	require.Positive(t, reusable.FreePages)
	require.Less(t, reusable.PendingPages, pinned.PendingPages)
}

func TestDatabaseStorageStatsSkipBusyApplicationAndRefuseClosedDatabase(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	q.mu.Lock()
	locked := true
	defer func() {
		if locked {
			q.mu.Unlock()
		}
	}()
	done := make(chan error, 1)
	go func() { _, err := q.StorageStats(); done <- err }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrStorageStatsBusy)
	case <-time.After(time.Second):
		t.Fatal("storage sampling waited behind an application lock")
	}
	q.mu.Unlock()
	locked = false
	require.NoError(t, q.Close())
	_, err := q.StorageStats()
	require.ErrorIs(t, err, ErrStorageStatsUnavailable)
}

func TestDatabaseStorageStatsFollowRestoreAndReopen(t *testing.T) {
	source, cleanup := NewTestDB(t)
	defer cleanup()
	require.NoError(t, source.ApplyEntries([]commit.Entry{{Index: 73}}).Error)
	var snapshot bytes.Buffer
	require.NoError(t, source.Snapshot(&snapshot))
	path := filepath.Join(t.TempDir(), "restored.db")
	target, err := NewDB(path)
	require.NoError(t, err)
	defer target.Close()
	require.NoError(t, target.RestoreAt(bytes.NewReader(snapshot.Bytes()), 73))
	stats, err := target.StorageStats()
	require.NoError(t, err)
	require.Equal(t, uint64(73), stats.AppliedIndex)
	require.Positive(t, stats.PageSize)
	require.NoError(t, target.Close())
	reopened, err := NewDB(path)
	require.NoError(t, err)
	defer reopened.Close()
	stats, err = reopened.StorageStats()
	require.NoError(t, err)
	require.Equal(t, uint64(73), stats.AppliedIndex)
}
