package db

import "errors"

var (
	ErrStorageStatsBusy        = errors.New("database storage statistics busy")
	ErrStorageStatsUnavailable = errors.New("database storage statistics unavailable")
)

// StorageStats contains process-local bbolt metadata, not logical record counts
// or a measurement of payload bytes. Reading it opens no transaction or file.
type StorageStats struct {
	AppliedIndex         uint64
	PageSize             int
	FreePages            int
	PendingPages         int
	FreelistBytes        int
	OpenReadTransactions int
}

// StorageStats never waits behind application, restore or Close. Callers can
// retain their last observation and report it stale when the database is busy.
func (q *DB) StorageStats() (StorageStats, error) {
	if !q.mu.TryLock() {
		return StorageStats{}, ErrStorageStatsBusy
	}
	defer q.mu.Unlock()
	if q.closed || q.db == nil {
		return StorageStats{}, ErrStorageStatsUnavailable
	}
	stats := q.db.Stats()
	return StorageStats{
		AppliedIndex: q.lastAppliedIndex, PageSize: q.pageSize,
		FreePages: stats.FreePageN, PendingPages: stats.PendingPageN,
		FreelistBytes: stats.FreelistInuse, OpenReadTransactions: stats.OpenTxN,
	}, nil
}
