package db

import (
	"bytes"
	"encoding/gob"
	"math"
	"testing"

	"github.com/duravela/djob/pkg/commit"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestRestoreBytesMatchesReaderAndReleasesInput(t *testing.T) {
	source, cleanup := NewTestDB(t)
	defer cleanup()
	require.NoError(t, source.ApplyEntries([]commit.Entry{logEntry(t, 42, Command{Type: CmdAddJobs, Jobs: []Job{{ID: "original", Data: "state"}}})}).Error)
	var data bytes.Buffer
	require.NoError(t, source.Snapshot(&data))
	_, fast := snapshotGobPayload(data.Bytes())
	require.True(t, fast, "exercise the byte-view path")
	native, closeNative := NewTestDB(t)
	defer closeNative()
	require.NoError(t, native.RestoreAt(bytes.NewReader(data.Bytes()), 42))
	target, closeTarget := NewTestDB(t)
	defer closeTarget()
	input := bytes.Clone(data.Bytes())
	require.NoError(t, target.RestoreBytesAt(input, 42))
	clear(input)
	var old, restored bytes.Buffer
	require.NoError(t, native.SnapshotAt(&old, 42))
	require.NoError(t, target.SnapshotAt(&restored, 42))
	require.Equal(t, old.Bytes(), restored.Bytes())
	require.True(t, target.HasPendingJob("original"))
}

func snapshotModel(t testing.TB) Snapshot {
	t.Helper()
	q, err := NewDB(t.TempDir() + "/base.db")
	require.NoError(t, err)
	defer q.Close()
	var encoded bytes.Buffer
	require.NoError(t, q.Snapshot(&encoded))
	var model Snapshot
	require.NoError(t, gob.NewDecoder(&encoded).Decode(&model))
	return model
}

func compareRestoreReaders(t *testing.T, encoded []byte, index uint64) {
	t.Helper()
	native, closeNative := NewTestDB(t)
	defer closeNative()
	view, closeView := NewTestDB(t)
	defer closeView()
	entry := logEntry(t, 99, Command{Type: CmdAddJobs, Jobs: []Job{{ID: "preserved-on-failure", Data: "keep"}}})
	require.NoError(t, native.ApplyEntries([]commit.Entry{entry}).Error)
	require.NoError(t, view.ApplyEntries([]commit.Entry{entry}).Error)
	nativeErr := native.RestoreAt(bytes.NewReader(encoded), index)
	viewErr := view.RestoreBytesAt(encoded, index)
	require.Equal(t, nativeErr == nil, viewErr == nil, "native=%v byte-view=%v", nativeErr, viewErr)
	if viewErr != nil {
		require.Equal(t, uint64(99), view.LastAppliedIndex())
		require.True(t, view.HasPendingJob("preserved-on-failure"))
	}
	var old, actual bytes.Buffer
	require.NoError(t, native.Snapshot(&old))
	require.NoError(t, view.Snapshot(&actual))
	require.Equal(t, old.Bytes(), actual.Bytes())
}

func TestRestoreBytesAcceptsUnsortedAndRejectsMalformedRecords(t *testing.T) {
	base := snapshotModel(t)
	for _, name := range []string{"unsorted", "duplicate-empty-value", "duplicate-nonadjacent", "duplicate-child", "record-child-collision", "empty-key", "empty-bucket"} {
		t.Run(name, func(t *testing.T) {
			bucket := SnapshotBucket{Name: []byte("test"), Sequence: math.MaxUint64,
				Records: []SnapshotRecord{{Key: []byte("z")}, {Key: []byte("a"), Value: []byte{0, 0xff}}}}
			switch name {
			case "duplicate-empty-value":
				bucket.Records = append(bucket.Records, bucket.Records[0])
			case "duplicate-nonadjacent":
				bucket.Records = append(bucket.Records, bucket.Records[1])
			case "duplicate-child":
				bucket.Buckets = []SnapshotBucket{{Name: []byte("child")}, {Name: []byte("child")}}
			case "record-child-collision":
				bucket.Buckets = []SnapshotBucket{{Name: []byte("a")}}
			case "empty-key":
				bucket.Records[0].Key = nil
			case "empty-bucket":
				bucket.Name = nil
			}
			model := base
			model.Buckets = append(append([]SnapshotBucket(nil), base.Buckets...), bucket)
			data, err := encode(model)
			require.NoError(t, err)
			_, fast := snapshotGobPayload(data)
			require.True(t, fast)
			compareRestoreReaders(t, data, 0)
		})
	}
}

// These types acquire different process-local IDs, and have different names,
// while retaining the exact same Gob field graph as historical snapshots.
type alternateRecord struct{ Key, Value []byte }
type alternateBucket struct {
	Name     []byte
	Sequence uint64
	Records  []alternateRecord
	Buckets  []alternateBucket
}
type alternateSnapshot struct {
	Version   uint32
	LastIndex uint64
	Buckets   []alternateBucket
}

func TestRestoreBytesRecognizesDifferentTypeIDs(t *testing.T) {
	base := snapshotModel(t)
	var alternate alternateSnapshot
	var encoded bytes.Buffer
	require.NoError(t, gob.NewEncoder(&encoded).Encode(base))
	require.NoError(t, gob.NewDecoder(&encoded).Decode(&alternate))
	data, err := encode(alternate)
	require.NoError(t, err)
	_, fast := snapshotGobPayload(data)
	require.True(t, fast)
	compareRestoreReaders(t, data, 0)
}

func TestRestoreBytesNativeFallbackAndFirstValueSemantics(t *testing.T) {
	base := snapshotModel(t)
	for _, model := range []any{
		struct {
			Version     uint32
			LastIndex   uint64
			Buckets     []SnapshotBucket
			FutureField string
		}{base.Version, base.LastIndex, base.Buckets, "ignored by historical readers"},
		struct {
			Buckets   []SnapshotBucket
			LastIndex uint64
			Version   uint32
		}{base.Buckets, base.LastIndex, base.Version},
	} {
		data, err := encode(model)
		require.NoError(t, err)
		_, fast := snapshotGobPayload(data)
		require.False(t, fast)
		compareRestoreReaders(t, data, 0)
	}
	data, err := encode(base)
	require.NoError(t, err)
	compareRestoreReaders(t, append(bytes.Clone(data), 0, 0xff, 0), 0)
	for _, length := range []int{0, 1, len(data) / 2, len(data) - 1} {
		compareRestoreReaders(t, data[:length], 0)
	}
	// A version wider than uint32 is still Gob uint, and must overflow both
	// decoders instead of truncating to the current version.
	wide := struct {
		Version   uint64
		LastIndex uint64
		Buckets   []SnapshotBucket
	}{1<<32 | 1, 0, base.Buckets}
	data, err = encode(wide)
	require.NoError(t, err)
	_, fast := snapshotGobPayload(data)
	require.True(t, fast)
	compareRestoreReaders(t, data, 0)
}

func FuzzSnapshotByteViewRestoration(f *testing.F) {
	base := snapshotModel(f)
	f.Add([]byte{1, 2, 3, 4})
	f.Add(bytes.Repeat([]byte{0xff, 0, 3}, 32))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 2048 {
			t.Skip()
		}
		r := bytes.NewReader(data)
		next := func() byte { b, _ := r.ReadByte(); return b }
		budget := 16
		var bucket func(int) SnapshotBucket
		bucket = func(depth int) SnapshotBucket {
			b := SnapshotBucket{Name: []byte{next() + 1}, Sequence: uint64(next()) << (next() % 8 * 8)}
			for i, count := 0, int(next()%8); i < count && budget > 0; i++ {
				budget--
				if depth < 3 && next()%3 == 0 {
					b.Buckets = append(b.Buckets, bucket(depth+1))
				} else {
					key := []byte{next() % 4}
					value := make([]byte, min(int(next()), r.Len()))
					_, _ = r.Read(value)
					b.Records = append(b.Records, SnapshotRecord{Key: key, Value: value})
				}
			}
			return b
		}
		custom := bucket(0)
		custom.Name = []byte("fuzz")
		model := base
		model.Buckets = append(append([]SnapshotBucket(nil), base.Buckets...), custom)
		encoded, err := encode(model)
		require.NoError(t, err)
		_, fast := snapshotGobPayload(encoded)
		require.True(t, fast)
		compareRestoreReaders(t, encoded, 0)
		if len(data) > 0 {
			compareRestoreReaders(t, encoded[:int(data[0])*len(encoded)/256], 0)
		}
	})
}

func FuzzSnapshotGobHeader(f *testing.F) {
	base := snapshotModel(f)
	data, err := encode(base)
	require.NoError(f, err)
	f.Add(data)
	f.Add([]byte{0, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 65536 {
			t.Skip()
		}
		_, _ = snapshotGobPayload(data)
	})
}

// Verify rollback also releases the write transaction so subsequent commands
// can apply; checking only the in-memory index would miss a leaked writer.
func TestRestoreBytesFailedWriteReleasesTransaction(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	base := snapshotModel(t)
	base.Buckets = append(base.Buckets, SnapshotBucket{Name: []byte("bad"), Records: []SnapshotRecord{{}}})
	data, err := encode(base)
	require.NoError(t, err)
	require.Error(t, q.RestoreBytesAt(data, 0))
	require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error { _, err := tx.CreateBucket([]byte("after")); return err }))
}
