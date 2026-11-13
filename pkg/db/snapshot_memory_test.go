package db

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func benchmarkSnapshotDB(b *testing.B) *DB {
	b.Helper()
	q, err := NewDB(filepath.Join(b.TempDir(), "snapshot.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := q.Close(); err != nil {
			b.Error(err)
		}
	})
	value := bytes.Repeat([]byte{0x55}, 4096)
	if err := q.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte("snapshot-profile"))
		if err != nil {
			return err
		}
		for i := 0; i < 16384; i++ {
			if err := bucket.Put([]byte(fmt.Sprintf("record-%08x", i)), value); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	runtime.GC()
	return q
}

func BenchmarkSnapshotMemory(b *testing.B) {
	for _, buffered := range []bool{false, true} {
		name := "discard"
		if buffered {
			name = "buffered"
		}
		b.Run(name, func(b *testing.B) {
			q := benchmarkSnapshotDB(b)
			b.SetBytes(64 << 20)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var output io.Writer = io.Discard
				var buffer bytes.Buffer
				if buffered {
					output = &buffer
				}
				if err := q.Snapshot(output); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSnapshotRestoreMemory(b *testing.B) {
	benchmarkRestoreSnapshot(b, false)
}

func BenchmarkSnapshotRestoreBytesMemory(b *testing.B) {
	benchmarkRestoreSnapshot(b, true)
}

func benchmarkRestoreSnapshot(b *testing.B, borrowed bool) {
	b.Helper()
	source := benchmarkSnapshotDB(b)
	var encoded bytes.Buffer
	if err := source.Snapshot(&encoded); err != nil {
		b.Fatal(err)
	}
	target, err := NewDB(filepath.Join(b.TempDir(), "restored.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := target.Close(); err != nil {
			b.Error(err)
		}
	})
	runtime.GC()
	b.SetBytes(int64(encoded.Len()))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var err error
		if borrowed {
			err = target.RestoreBytesAt(encoded.Bytes(), 0)
		} else {
			err = target.Restore(bytes.NewReader(encoded.Bytes()))
		}
		if err != nil {
			b.Fatal(err)
		}
	}
}

type pausedSnapshotWriter struct {
	bytes.Buffer
	started chan struct{}
	resume  chan struct{}
	paused  bool
}

func (w *pausedSnapshotWriter) Write(p []byte) (int, error) {
	if !w.paused {
		w.paused = true
		close(w.started)
		<-w.resume
	}
	return w.Buffer.Write(p)
}

func TestSnapshotBorrowedValuesStayInsideReadTransaction(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	old := bytes.Repeat([]byte("old"), 4096)
	require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucket([]byte("borrowed"))
		if err != nil {
			return err
		}
		return b.Put([]byte("value"), old)
	}))
	writer := &pausedSnapshotWriter{started: make(chan struct{}), resume: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- q.Snapshot(writer) }()
	resumed := false
	defer func() {
		if !resumed {
			close(writer.resume)
			<-done
		}
	}()
	select {
	case <-writer.started:
	case <-time.After(3 * time.Second):
		t.Fatal("snapshot encoding did not start")
	}
	require.Equal(t, 1, q.db.Stats().OpenTxN, "encoding must retain the transaction owning borrowed values")
	// Bypass the public mutex to exercise Bolt's MVCC while encoding is paused.
	for i := range 3 {
		require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error {
			return tx.Bucket([]byte("borrowed")).Put([]byte("value"), bytes.Repeat([]byte{byte(i)}, len(old)))
		}))
	}
	close(writer.resume)
	resumed = true
	require.NoError(t, <-done)
	var snapshot Snapshot
	require.NoError(t, gob.NewDecoder(bytes.NewReader(writer.Bytes())).Decode(&snapshot))
	found := false
	for _, bucket := range snapshot.Buckets {
		if string(bucket.Name) == "borrowed" {
			require.Len(t, bucket.Records, 1)
			require.Equal(t, old, bucket.Records[0].Value)
			found = true
		}
	}
	require.True(t, found)
	require.Zero(t, q.db.Stats().OpenTxN)
}
