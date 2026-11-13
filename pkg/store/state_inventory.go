package store

import (
	"context"

	"github.com/duravela/djob/pkg/db"
)

// StateInventory is for background monitoring; consensus/application and restore
// locks are skipped rather than delaying a diagnostic caller behind mutations.
func (s *Store) StateInventory(ctx context.Context, limit int) (db.StateInventory, error) {
	if !s.mu.TryRLock() {
		return db.StateInventory{}, db.ErrStateInventoryBusy
	}
	defer s.mu.RUnlock()
	select {
	case <-s.stopC:
		return db.StateInventory{}, db.ErrStateInventoryUnavailable
	default:
	}
	if s.db == nil {
		return db.StateInventory{}, db.ErrStateInventoryUnavailable
	}
	return s.db.StateInventory(ctx, limit)
}
