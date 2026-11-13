package db

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"go.etcd.io/bbolt"
)

type ChannelPage struct {
	Channels   []CommunicationChannel
	NextCursor []byte
}

// These paginated inspection queries read one local transaction. They do not
// grant execution authority or provide a cluster-wide linearizable read.
func listRecords[T any](q *DB, bucket, after []byte, limit int) ([]T, []byte, error) {
	if err := queryLimit(limit); err != nil {
		return nil, nil, err
	}
	var records []T
	var next []byte
	err := q.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(bucket).Cursor()
		key, value := pageStart(cursor, after)
		for key != nil && len(records) < limit {
			var record T
			if err := gob.NewDecoder(bytes.NewReader(value)).Decode(&record); err != nil {
				return fmt.Errorf("decode %s: %w", bucket, err)
			}
			records = append(records, record)
			next = bytes.Clone(key)
			key, value = cursor.Next()
		}
		if key == nil {
			next = nil
		}
		return nil
	})
	return records, next, err
}

func (q *DB) ListChannels(after []byte, limit int) (ChannelPage, error) {
	rows, next, err := listRecords[CommunicationChannel](q, channelsBucket, after, limit)
	return ChannelPage{Channels: rows, NextCursor: next}, err
}

func (q *DB) ListSchedules(after []byte, limit int) (SchedulePage, error) {
	rows, next, err := listRecords[Schedule](q, schedulesBucket, after, limit)
	return SchedulePage{Schedules: rows, NextCursor: next}, err
}

func (q *DB) ListRuns(after []byte, limit int) (RunPage, error) {
	rows, next, err := listRecords[Run](q, runsBucket, after, limit)
	return RunPage{Runs: rows, NextCursor: next}, err
}
