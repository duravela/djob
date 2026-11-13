package store

import "github.com/duravela/djob/pkg/db"

// StorageStats reads only cached database metadata and never waits for a store
// writer. It does not open a read transaction, enumerate records, or use Raft.
func (s *Store) StorageStats() (db.StorageStats, error) {
	if !s.mu.TryRLock() {
		return db.StorageStats{}, db.ErrStorageStatsBusy
	}
	defer s.mu.RUnlock()
	select {
	case <-s.stopC:
		return db.StorageStats{}, db.ErrStorageStatsUnavailable
	default:
	}
	if s.db == nil {
		return db.StorageStats{}, db.ErrStorageStatsUnavailable
	}
	return s.db.StorageStats()
}
