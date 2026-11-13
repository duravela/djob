package db

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"sync/atomic"

	"go.etcd.io/bbolt"
)

const CurrentSnapshotVersion uint32 = 1

type Snapshot struct {
	Version   uint32
	LastIndex uint64
	Buckets   []SnapshotBucket
}

type SnapshotBucket struct {
	Name     []byte
	Sequence uint64
	Records  []SnapshotRecord
	Buckets  []SnapshotBucket
}

type SnapshotRecord struct {
	Key   []byte
	Value []byte
}

func (q *DB) Snapshot(w io.Writer) error { return q.snapshot(w, nil) }

// SnapshotAt rejects a state/index mismatch during WAL replay or snapshotting.
func (q *DB) SnapshotAt(w io.Writer, index uint64) error { return q.snapshot(w, &index) }

func (q *DB) snapshot(w io.Writer, expected *uint64) error {
	encode, err := q.prepareSnapshot(expected)
	if err != nil {
		return err
	}
	return encode(w)
}

// PrepareSnapshotAt pins the exact applied index without traversing or encoding
// the database. The returned function must be called exactly once; it releases
// the read transaction on success or error. Later writes use Bolt's MVCC while
// encoding continues against the pinned view.
func (q *DB) PrepareSnapshotAt(index uint64) (func(io.Writer) error, error) {
	return q.prepareSnapshot(&index)
}

func (q *DB) prepareSnapshot(expected *uint64) (func(io.Writer) error, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, bbolt.ErrDatabaseNotOpen
	}
	tx, err := q.db.Begin(false)
	if err != nil {
		return nil, err
	}
	index, err := readAppliedIndex(tx)
	if err == nil && expected != nil && index != *expected {
		err = fmt.Errorf("snapshot index mismatch: database=%d requested=%d", index, *expected)
	}
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	var consumed atomic.Bool
	return func(w io.Writer) error {
		if consumed.Swap(true) {
			return fmt.Errorf("prepared snapshot already consumed")
		}
		defer tx.Rollback()
		return writeSnapshot(w, tx, index)
	}, nil
}

// The native encoder remains the fallback for future schema changes. Both paths
// borrow Bolt values only while the prepared read transaction is still open.
func writeSnapshotNative(w io.Writer, tx *bbolt.Tx, index uint64) error {
	snapshot := Snapshot{Version: CurrentSnapshotVersion, LastIndex: index}
	if err := tx.ForEach(func(name []byte, bucket *bbolt.Bucket) error {
		b, err := snapshotBucket(name, bucket)
		snapshot.Buckets = append(snapshot.Buckets, b)
		return err
	}); err != nil {
		return err
	}
	return gob.NewEncoder(w).Encode(snapshot)
}

func snapshotBucket(name []byte, b *bbolt.Bucket) (SnapshotBucket, error) {
	result := SnapshotBucket{Name: name, Sequence: b.Sequence()}
	err := b.ForEach(func(key, value []byte) error {
		if value == nil {
			child, err := snapshotBucket(key, b.Bucket(key))
			result.Buckets = append(result.Buckets, child)
			return err
		}
		result.Records = append(result.Records, SnapshotRecord{Key: key, Value: value})
		return nil
	})
	return result, err
}

func (q *DB) Restore(r io.Reader) error { return q.restore(r, nil) }

func (q *DB) RestoreAt(r io.Reader, index uint64) error { return q.restore(r, &index) }

// RestoreBytes is the buffered equivalent of Restore. The caller must keep data
// immutable until it returns; the database retains no input bytes afterward.
func (q *DB) RestoreBytes(data []byte) error { return q.restoreBytes(data, nil) }

func (q *DB) restore(r io.Reader, expected *uint64) error {
	var snapshot Snapshot
	if err := gob.NewDecoder(r).Decode(&snapshot); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}
	if err := validateSnapshotIndex(snapshot.Version, snapshot.LastIndex, expected); err != nil {
		return err
	}
	return q.replaceSnapshot(snapshot.LastIndex, func(tx *bbolt.Tx) error {
		for _, source := range snapshot.Buckets {
			target, err := tx.CreateBucket(source.Name)
			if err != nil {
				return err
			}
			if err := restoreBucket(target, source); err != nil {
				return err
			}
		}
		return nil
	})
}

func validateSnapshotIndex(version uint32, index uint64, expected *uint64) error {
	if version != CurrentSnapshotVersion {
		return fmt.Errorf("unsupported snapshot version %d; legacy snapshots omit active jobs and require explicit migration", version)
	}
	if expected != nil && index != *expected {
		return fmt.Errorf("snapshot index mismatch: payload=%d Raft=%d", index, *expected)
	}
	return nil
}

// All restore paths replace the database in one transaction. Malformed input,
// invalid indexes and failed state validation leave the previous state intact.
func (q *DB) replaceSnapshot(index uint64, load func(*bbolt.Tx) error) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	err := q.db.Update(func(tx *bbolt.Tx) error {
		var names [][]byte
		if err := tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
			names = append(names, bytes.Clone(name))
			return nil
		}); err != nil {
			return err
		}
		for _, name := range names {
			if err := tx.DeleteBucket(name); err != nil {
				return err
			}
		}
		if err := load(tx); err != nil {
			return err
		}
		for _, name := range jobBuckets {
			if tx.Bucket(name) == nil {
				return fmt.Errorf("snapshot is missing bucket %q", name)
			}
		}
		stored, err := readAppliedIndex(tx)
		if err != nil {
			return err
		}
		if stored != index {
			return fmt.Errorf("snapshot metadata index mismatch: metadata=%d payload=%d", stored, index)
		}
		if err := initializeChannelState(tx); err != nil {
			return err
		}
		if err := initializeSchedulingState(tx); err != nil {
			return err
		}
		if err := initializeDeadlineState(tx); err != nil {
			return err
		}
		if err := initializeRetryState(tx); err != nil {
			return err
		}
		if err := initializeJobState(tx); err != nil {
			return err
		}
		if err := initializeJobTriggerState(tx); err != nil {
			return err
		}
		if err := initializeJobActivationState(tx); err != nil {
			return err
		}
		if err := initializeJobQuotaState(tx); err != nil {
			return err
		}
		if err := initializeJobRetentionState(tx); err != nil {
			return err
		}
		if err := initializeJobReceiptState(tx); err != nil {
			return err
		}
		if err := initializeJobConfigurationState(tx); err != nil {
			return err
		}
		if err := initializeJobConfigurationReceiptState(tx); err != nil {
			return err
		}
		if err := initializeJobTriggerRetirementState(tx); err != nil {
			return err
		}
		if err := initializeJobConfigurationRetirementState(tx); err != nil {
			return err
		}
		if err := initializeWorkerEnrollments(tx); err != nil {
			return err
		}
		if err := initializeWorkerDeployments(tx); err != nil {
			return err
		}
		if err := initializeWorkerReleases(tx); err != nil {
			return err
		}
		if err := initializeWorkerReleaseRunIndex(tx); err != nil {
			return err
		}
		return initializeConsoleSessions(tx)
	})
	if err != nil {
		return fmt.Errorf("restore snapshot: %w", err)
	}
	q.lastAppliedIndex = index
	_ = q.db.View(func(tx *bbolt.Tx) error { q.refreshMetrics(tx); return nil })
	return nil
}

func restoreBucket(target *bbolt.Bucket, source SnapshotBucket) error {
	if err := target.SetSequence(source.Sequence); err != nil {
		return err
	}
	keys := make(map[string]bool)
	for _, record := range source.Records {
		if keys[string(record.Key)] {
			return fmt.Errorf("duplicate snapshot key %q", record.Key)
		}
		keys[string(record.Key)] = true
		if err := target.Put(record.Key, record.Value); err != nil {
			return err
		}
	}
	for _, child := range source.Buckets {
		b, err := target.CreateBucket(child.Name)
		if err != nil {
			return err
		}
		if err := restoreBucket(b, child); err != nil {
			return err
		}
	}
	return nil
}
