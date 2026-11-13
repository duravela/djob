package db

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"path/filepath"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func logEntry(t *testing.T, index uint64, cmd Command) commit.Entry {
	t.Helper()
	data, err := encode(cmd)
	require.NoError(t, err)
	return commit.Entry{Index: index, Data: data}
}

func TestAppliedIndexSurvivesReopenAndOverlappingReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	q, err := NewDB(path)
	require.NoError(t, err)
	add := logEntry(t, 10, Command{Version: CurrentCommandVersion, Type: CmdAddJobs, Jobs: []Job{{ID: "finished"}}})
	require.NoError(t, q.ApplyEntries([]commit.Entry{add}).Error)
	acquired, err := q.Acquire("finished", "worker", time.Hour)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, q.Release("finished", "worker"))
	require.NoError(t, q.Close())

	q, err = NewDB(path)
	require.NoError(t, err)
	defer q.Close()
	require.Equal(t, uint64(10), q.LastAppliedIndex())
	newJob := logEntry(t, 13, Command{Type: CmdAddJobs, Jobs: []Job{{ID: "new"}}})
	result := q.ApplyEntries([]commit.Entry{add, {Index: 11}, {Index: 12}, newJob, {Index: 14}})
	require.NoError(t, result.Error)
	require.False(t, q.HasPendingJob("finished"), "replay resurrected completed work")
	require.True(t, q.HasPendingJob("new"))
	require.Equal(t, uint64(14), q.LastAppliedIndex())
	require.NotNil(t, q.ApplyEntries([]commit.Entry{add}), "duplicate replay must return a usable result")
	require.NoError(t, q.Close())
	q, err = NewDB(path)
	require.NoError(t, err)
	defer q.Close()
	require.Equal(t, uint64(14), q.LastAppliedIndex())
}

func TestApplyEntriesRollsBackWholeBatch(t *testing.T) {
	for _, failure := range []string{"decode", "command version", "command type", "write"} {
		t.Run(failure, func(t *testing.T) {
			q, cleanup := NewTestDB(t)
			defer cleanup()
			good := logEntry(t, 6, Command{Type: CmdAddJobs, Jobs: []Job{{ID: "keep"}}})
			require.NoError(t, q.ApplyEntries([]commit.Entry{good}).Error)
			first := logEntry(t, 7, Command{Type: CmdAddJobs, Jobs: []Job{{ID: "rollback"}}})
			bad := logEntry(t, 8, Command{Type: CmdAssignJobs})
			switch failure {
			case "decode":
				bad.Data = []byte("bad gob")
			case "command version":
				bad = logEntry(t, 8, Command{Version: CurrentCommandVersion + 1})
			case "write":
				bad = logEntry(t, 8, Command{Type: CmdAddJobs, Jobs: []Job{{ID: ""}}})
			}
			result := q.ApplyEntries([]commit.Entry{first, bad})
			require.Error(t, result.Error)
			require.Empty(t, result.JobErrors)
			require.Equal(t, uint64(6), q.LastAppliedIndex())
			require.True(t, q.HasPendingJob("keep"))
			require.False(t, q.HasPendingJob("rollback"))
			require.Equal(t, 1, q.Stats().Pending)
		})
	}
}

func TestHistoricalBatchAddCommandRetainsItsMeaning(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	// Encode exactly the old structure: it had no Version field.
	legacy := struct {
		Type CommandType
		Jobs []Job
	}{Type: 2, Jobs: []Job{{ID: "legacy"}}}
	data, err := encode(legacy)
	require.NoError(t, err)
	require.NoError(t, q.ApplyEntries([]commit.Entry{{Index: 4, Data: data}}).Error)
	require.True(t, q.HasPendingJob("legacy"))
	require.Equal(t, CommandType(3), CmdAssignJobs)
}

func TestSnapshotPreservesLeasesPayloadsAndNestedBuckets(t *testing.T) {
	source, cleanup := NewTestDB(t)
	defer cleanup()
	entry := logEntry(t, 42, Command{Type: CmdAddJobs, Jobs: []Job{
		{ID: "pending", Data: "pending payload"},
		{ID: "leased", Data: "leased payload", Priority: 1},
	}})
	require.NoError(t, source.ApplyEntries([]commit.Entry{entry}).Error)
	ok, err := source.Acquire("leased", "owner", time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, source.db.Update(func(tx *bbolt.Tx) error {
		parent, err := tx.CreateBucket([]byte("future_state"))
		if err != nil {
			return err
		}
		if err := parent.SetSequence(27); err != nil {
			return err
		}
		child, err := parent.CreateBucket([]byte("nested"))
		if err != nil {
			return err
		}
		return child.Put([]byte("key"), []byte("value"))
	}))
	var before bytes.Buffer
	require.NoError(t, source.SnapshotAt(&before, 42))
	targetPath := filepath.Join(t.TempDir(), "restored.db")
	target, err := NewDB(targetPath)
	require.NoError(t, err)
	require.NoError(t, target.RestoreAt(bytes.NewReader(before.Bytes()), 42))
	var after bytes.Buffer
	require.NoError(t, target.Snapshot(&after))
	require.Equal(t, before.Bytes(), after.Bytes(), "full logical database must round-trip")
	require.Equal(t, 1, target.Stats().Pending)
	require.Equal(t, 1, target.Stats().Progress)
	require.Error(t, target.Release("leased", "other-worker"))
	require.NoError(t, target.Close())
	target, err = NewDB(targetPath)
	require.NoError(t, err)
	defer target.Close()
	require.Equal(t, uint64(42), target.LastAppliedIndex())
	require.NoError(t, target.Release("leased", "owner"))
	require.True(t, target.HasPendingJob("pending"))
	require.False(t, target.HasPendingJob("leased"))
}

func TestInvalidSnapshotPreservesExistingState(t *testing.T) {
	source, cleanup := NewTestDB(t)
	defer cleanup()
	require.NoError(t, source.ApplyEntries([]commit.Entry{logEntry(t, 12, Command{Type: CmdAddJobs, Jobs: []Job{{ID: "source"}}})}).Error)
	var valid bytes.Buffer
	require.NoError(t, source.Snapshot(&valid))
	for _, failure := range []string{"truncated", "legacy", "future", "missing bucket", "metadata index", "Raft index", "duplicate bucket"} {
		t.Run(failure, func(t *testing.T) {
			q, cleanup := NewTestDB(t)
			defer cleanup()
			require.NoError(t, q.ApplyEntries([]commit.Entry{logEntry(t, 99, Command{Type: CmdAddJobs, Jobs: []Job{{ID: "keep"}}})}).Error)
			var snapshot Snapshot
			require.NoError(t, gob.NewDecoder(bytes.NewReader(valid.Bytes())).Decode(&snapshot))
			switch failure {
			case "legacy":
				snapshot.Version = 0
			case "future":
				snapshot.Version++
			case "missing bucket":
				snapshot.Buckets = snapshot.Buckets[1:]
			case "duplicate bucket":
				snapshot.Buckets = append(snapshot.Buckets, snapshot.Buckets[0])
			case "metadata index":
				for i := range snapshot.Buckets {
					if bytes.Equal(snapshot.Buckets[i].Name, metadataBucket) {
						for j := range snapshot.Buckets[i].Records {
							r := &snapshot.Buckets[i].Records[j]
							if bytes.Equal(r.Key, appliedKey) {
								binary.BigEndian.PutUint64(r.Value, 13)
							}
						}
					}
				}
			}
			data, err := encode(snapshot)
			require.NoError(t, err)
			if failure == "truncated" {
				data = data[:len(data)/2]
			}
			expected := uint64(12)
			if failure == "Raft index" {
				expected++
			}
			require.Error(t, q.RestoreAt(bytes.NewReader(data), expected))
			require.Equal(t, uint64(99), q.LastAppliedIndex())
			require.True(t, q.HasPendingJob("keep"))
			require.False(t, q.HasPendingJob("source"))
			require.Equal(t, 1, q.Stats().Pending)
			require.Error(t, q.RestoreBytesAt(data, expected))
			require.Equal(t, uint64(99), q.LastAppliedIndex())
			require.True(t, q.HasPendingJob("keep"))
			require.False(t, q.HasPendingJob("source"))
			require.Equal(t, 1, q.Stats().Pending)
		})
	}
}

func TestSnapshotRejectsIndexAheadOrBehindState(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	require.NoError(t, q.ApplyEntries([]commit.Entry{{Index: 8}}).Error)
	for _, index := range []uint64{7, 9} {
		var data bytes.Buffer
		require.Error(t, q.SnapshotAt(&data, index))
		require.Empty(t, data.Bytes())
	}
}

func TestLegacyDatabaseWithoutIndexRequiresExplicitRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	raw, err := bbolt.Open(path, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, raw.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucket(jobBuckets[PENDING_PRIO_NORMAL])
		if err != nil {
			return err
		}
		return b.Put([]byte("legacy"), []byte("preserve me"))
	}))
	require.NoError(t, raw.Close())
	_, err = NewDB(path)
	require.ErrorContains(t, err, "without a durable Raft index")
	raw, err = bbolt.Open(path, 0600, nil)
	require.NoError(t, err)
	defer raw.Close()
	require.NoError(t, raw.View(func(tx *bbolt.Tx) error {
		require.Nil(t, tx.Bucket(metadataBucket), "failed initialization must roll back")
		require.Equal(t, []byte("preserve me"), tx.Bucket(jobBuckets[PENDING_PRIO_NORMAL]).Get([]byte("legacy")))
		return nil
	}))
}
