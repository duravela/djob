package db

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

const (
	MaxQueueSampleRows  = 65536
	MaxQueueSampleBytes = 8 << 20 // Per profile, measured before decoding each record.
)

var (
	ErrQueueStatsBusy        = errors.New("database queue statistics busy")
	ErrQueueStatsUnavailable = errors.New("database queue statistics unavailable")
)

// ReadyQueueStats contains only aggregate metadata from a bounded prefix of a
// durable ready index. Observed is exact only when Complete. Otherwise both its
// count and the oldest known creation age are lower bounds for the whole index.
// CreatedAt measures run age, not time since its most recent ready transition.
type ReadyQueueStats struct {
	Observed          uint64
	Complete          bool
	DecodedBytes      uint64
	OldestCreatedAt   time.Time
	MissingCreatedAt  uint64
	FutureCreatedAt   uint64
	OldestReadySince  time.Time
	MissingReadySince uint64
	FutureReadySince  uint64
}

type QueueStats struct {
	AppliedIndex uint64
	ObservedAt   time.Time
	Gateway      ReadyQueueStats
	Generic      ReadyQueueStats
}

// QueueStats is a background diagnostic query, not an HTTP collector. It never
// waits for an application/restore/close lock. One read transaction samples both
// ready indexes, decoding at most limit rows and MaxQueueSampleBytes per profile.
// A context bounds cooperative work; an individual OS page fault is not detached.
// No job payloads or identities escape the query and no state is written.
func (q *DB) QueueStats(ctx context.Context, limit int) (QueueStats, error) {
	if limit < 1 || limit > MaxQueueSampleRows {
		return QueueStats{}, fmt.Errorf("queue sample limit must be in 1..%d", MaxQueueSampleRows)
	}
	if err := ctx.Err(); err != nil {
		return QueueStats{}, err
	}
	if !q.mu.TryLock() {
		return QueueStats{}, ErrQueueStatsBusy
	}
	defer q.mu.Unlock()
	if q.closed || q.db == nil {
		return QueueStats{}, ErrQueueStatsUnavailable
	}
	stats := QueueStats{AppliedIndex: q.lastAppliedIndex, ObservedAt: time.Now()}
	err := q.db.View(func(tx *bbolt.Tx) error {
		for _, profile := range []struct {
			index, records []byte
			generic        bool
			sample         *ReadyQueueStats
		}{
			{readyRunsBucket, runsBucket, false, &stats.Gateway},
			{jobReadyBucket, jobRunsBucket, true, &stats.Generic},
		} {
			index, records := tx.Bucket(profile.index), tx.Bucket(profile.records)
			if index == nil || records == nil {
				return ErrQueueStatsUnavailable
			}
			c := index.Cursor()
			key, value := c.First()
			for key != nil {
				if err := ctx.Err(); err != nil {
					return err
				}
				if profile.sample.Observed >= uint64(limit) {
					break
				}
				raw := records.Get(value)
				if value == nil || raw == nil {
					return fmt.Errorf("queue statistics found an inconsistent ready index")
				}
				if uint64(len(raw)) > MaxQueueSampleBytes-profile.sample.DecodedBytes {
					break
				}
				var created, readySince time.Time
				if profile.generic {
					var header struct {
						TenantID, ID string
						Status       JobStatus
						CreatedAt    time.Time
						ReadySince   time.Time
					}
					if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&header); err != nil {
						return fmt.Errorf("decode generic queue metadata: %w", err)
					}
					if header.Status != JobReady || !bytes.Equal(key, value) || string(key) != jobKey(header.TenantID, header.ID) {
						return fmt.Errorf("queue statistics found an inconsistent generic ready index")
					}
					created = header.CreatedAt
					readySince = header.ReadySince
				} else {
					var header struct {
						ID                     string
						State                  RunState
						CreatedAt, ScheduledAt time.Time
						ReadySince             time.Time
					}
					if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&header); err != nil {
						return fmt.Errorf("decode gateway queue metadata: %w", err)
					}
					if header.State != RunReady || string(value) != header.ID || !bytes.Equal(key, runKey(Run{ID: header.ID, CreatedAt: header.CreatedAt, ScheduledAt: header.ScheduledAt})) {
						return fmt.Errorf("queue statistics found an inconsistent gateway ready index")
					}
					created = header.CreatedAt
					readySince = header.ReadySince
				}
				profile.sample.Observed++
				profile.sample.DecodedBytes += uint64(len(raw))
				if readySince.IsZero() {
					profile.sample.MissingReadySince++
				} else {
					if readySince.After(stats.ObservedAt) {
						profile.sample.FutureReadySince++
					}
					if profile.sample.OldestReadySince.IsZero() || readySince.Before(profile.sample.OldestReadySince) {
						profile.sample.OldestReadySince = readySince
					}
				}
				if created.IsZero() {
					profile.sample.MissingCreatedAt++
				} else {
					if created.After(stats.ObservedAt) {
						profile.sample.FutureCreatedAt++
					}
					if profile.sample.OldestCreatedAt.IsZero() || created.Before(profile.sample.OldestCreatedAt) {
						profile.sample.OldestCreatedAt = created
					}
				}
				key, value = c.Next()
			}
			profile.sample.Complete = key == nil
		}
		return ctx.Err()
	})
	if err != nil {
		return QueueStats{}, err // Never publish half of a failed/cancelled sample.
	}
	return stats, nil
}
