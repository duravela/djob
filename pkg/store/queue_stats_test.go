package store

import (
	"context"
	"testing"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func TestStoreQueueStatsSkipBusyStoreAndRefuseUnavailableState(t *testing.T) {
	var empty Store
	_, err := empty.QueueStats(context.Background(), 10)
	require.ErrorIs(t, err, db.ErrQueueStatsUnavailable)
	s, _, _ := newTestJobQueue(t)
	stats, err := s.QueueStats(context.Background(), 10)
	require.NoError(t, err)
	require.True(t, stats.Gateway.Complete && stats.Generic.Complete)
	require.Zero(t, stats.Gateway.Observed)
	require.Zero(t, stats.Generic.Observed)
	s.mu.Lock()
	_, err = s.QueueStats(context.Background(), 10)
	s.mu.Unlock()
	require.ErrorIs(t, err, db.ErrQueueStatsBusy)
	s.Stop()
	_, err = s.QueueStats(context.Background(), 10)
	require.ErrorIs(t, err, db.ErrQueueStatsUnavailable)
}
