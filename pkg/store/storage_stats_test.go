package store

import (
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func TestStoreStorageStatsSkipBusyStoreAndReportLifecycle(t *testing.T) {
	var empty Store
	_, err := empty.StorageStats()
	require.ErrorIs(t, err, db.ErrStorageStatsUnavailable)
	s, _, _ := newTestJobQueue(t)
	stats, err := s.StorageStats()
	require.NoError(t, err)
	require.Positive(t, stats.PageSize)
	s.mu.Lock()
	locked := true
	defer func() {
		if locked {
			s.mu.Unlock()
		}
	}()
	done := make(chan error, 1)
	go func() { _, err := s.StorageStats(); done <- err }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, db.ErrStorageStatsBusy)
	case <-time.After(time.Second):
		t.Fatal("storage metadata sampling waited for a store writer")
	}
	s.mu.Unlock()
	locked = false
	s.Stop()
	_, err = s.StorageStats()
	require.ErrorIs(t, err, db.ErrStorageStatsUnavailable)
}
