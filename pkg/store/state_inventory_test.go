package store

import (
	"context"
	"testing"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func TestStoreStateInventorySkipsBusyStoreAndRefusesUnavailableState(t *testing.T) {
	var empty Store
	_, err := empty.StateInventory(context.Background(), 1)
	require.ErrorIs(t, err, db.ErrStateInventoryUnavailable)
	s, _, _ := newTestJobQueue(t)
	stats, err := s.StateInventory(context.Background(), 1)
	require.NoError(t, err)
	for _, kind := range stats.Kinds {
		require.True(t, kind.Complete)
		require.Zero(t, kind.Observed)
	}
	s.mu.Lock()
	_, err = s.StateInventory(context.Background(), 1)
	s.mu.Unlock()
	require.ErrorIs(t, err, db.ErrStateInventoryBusy)
	s.Stop()
	_, err = s.StateInventory(context.Background(), 1)
	require.ErrorIs(t, err, db.ErrStateInventoryUnavailable)
}
