package store

import (
	"context"

	"github.com/duravela/djob/pkg/db"
)

// QueueStats samples durable ready indexes only from a background probe. Busy
// application or restore locks are skipped instead of delaying consensus work.
func (s *Store) QueueStats(ctx context.Context, limit int) (db.QueueStats, error) {
	if !s.mu.TryRLock() {
		return db.QueueStats{}, db.ErrQueueStatsBusy
	}
	defer s.mu.RUnlock()
	select {
	case <-s.stopC:
		return db.QueueStats{}, db.ErrQueueStatsUnavailable
	default:
	}
	if s.db == nil {
		return db.QueueStats{}, db.ErrQueueStatsUnavailable
	}
	return s.db.QueueStats(ctx, limit)
}
