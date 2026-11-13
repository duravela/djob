package db

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/stretchr/testify/require"
)

func TestPreparedSnapshotPinsIndexWhileLaterCommandsApply(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	require.NoError(t, q.ApplyEntries([]commit.Entry{logEntry(t, 10, Command{Type: CmdAddJobs, Jobs: []Job{{ID: "original", Data: "old"}}})}).Error)
	encode, err := q.PrepareSnapshotAt(10)
	require.NoError(t, err)
	consumed := false
	defer func() {
		if !consumed {
			_ = encode(io.Discard)
		}
	}()
	require.Equal(t, 1, q.db.Stats().OpenTxN)
	applied := make(chan error, 1)
	entries := []commit.Entry{
		logEntry(t, 11, Command{Type: CmdDeleteJob, Jobs: []Job{{ID: "original"}}}),
		logEntry(t, 12, Command{Type: CmdAddJobs, Jobs: []Job{{ID: "later", Data: "new"}}}),
	}
	go func() { applied <- q.ApplyEntries(entries).Error }()
	select {
	case err := <-applied:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("prepared view blocked later application")
	}
	var data bytes.Buffer
	consumed = true
	require.NoError(t, encode(&data))
	require.Zero(t, q.db.Stats().OpenTxN)
	require.ErrorContains(t, encode(io.Discard), "already consumed")
	restored, closeRestored := NewTestDB(t)
	defer closeRestored()
	require.NoError(t, restored.RestoreAt(bytes.NewReader(data.Bytes()), 10))
	require.True(t, restored.HasPendingJob("original"))
	require.False(t, restored.HasPendingJob("later"))
	require.Equal(t, uint64(12), q.LastAppliedIndex())
	require.False(t, q.HasPendingJob("original"))
	require.True(t, q.HasPendingJob("later"))
}

type failedSnapshotWriter struct{ err error }

func (w failedSnapshotWriter) Write([]byte) (int, error) { return 0, w.err }

func TestPreparedSnapshotReleasesViewOnIndexMismatchAndWriteFailure(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	_, err := q.PrepareSnapshotAt(42)
	require.ErrorContains(t, err, "index mismatch")
	require.Zero(t, q.db.Stats().OpenTxN)
	encode, err := q.PrepareSnapshotAt(0)
	require.NoError(t, err)
	failure := errors.New("injected writer failure")
	require.ErrorIs(t, encode(failedSnapshotWriter{failure}), failure)
	require.Zero(t, q.db.Stats().OpenTxN)
}

func TestDatabaseCloseWaitsForPreparedSnapshotView(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	encode, err := q.PrepareSnapshotAt(0)
	require.NoError(t, err)
	closed := make(chan error, 1)
	go func() { closed <- q.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("closed database with a pinned view: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	require.NoError(t, encode(io.Discard))
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("snapshot leaked its read transaction")
	}
}
