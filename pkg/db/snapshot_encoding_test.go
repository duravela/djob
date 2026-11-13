package db

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func assertSnapshotNativeBytes(t *testing.T, tx *bbolt.Tx, index uint64) []byte {
	t.Helper()
	require.NotNil(t, snapshotEncoding(), "exercise the streaming path, not its fallback")
	var native, streamed bytes.Buffer
	require.NoError(t, writeSnapshotNative(&native, tx, index))
	require.NoError(t, writeSnapshot(&streamed, tx, index))
	require.Equal(t, native.Bytes(), streamed.Bytes())
	var oldReader Snapshot
	require.NoError(t, gob.NewDecoder(bytes.NewReader(streamed.Bytes())).Decode(&oldReader))
	require.Equal(t, CurrentSnapshotVersion, oldReader.Version)
	require.Equal(t, index, oldReader.LastIndex)
	return streamed.Bytes()
}

func TestStreamingSnapshotMatchesNativeGob(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error {
		for _, count := range []int{0, 1, 127, 128, 255, 256} {
			bucket, err := tx.CreateBucket([]byte(fmt.Sprintf("count-%d", count)))
			if err != nil {
				return err
			}
			if err := bucket.SetSequence(uint64(count)); err != nil {
				return err
			}
			for i := range count {
				// Empty values and nested bucket names interleave with records.
				if err := bucket.Put([]byte(fmt.Sprintf("%04d-record", i)), nil); err != nil {
					return err
				}
				child, err := bucket.CreateBucket([]byte(fmt.Sprintf("%04d-child", i)))
				if err != nil {
					return err
				}
				if err := child.SetSequence(math.MaxUint64); err != nil {
					return err
				}
			}
		}
		for _, length := range []int{1, 127, 128, 255, 256, 16383, 16384} {
			bucket, err := tx.CreateBucket(bytes.Repeat([]byte{0xff}, length))
			if err != nil {
				return err
			}
			key := bytes.Repeat([]byte{0x80}, length)
			if err := bucket.Put(key, bytes.Repeat([]byte{0x00}, length)); err != nil {
				return err
			}
		}
		large, err := tx.CreateBucket([]byte("large-values"))
		if err != nil {
			return err
		}
		for _, length := range []int{65535, 65536, 65537} {
			if err := large.Put([]byte(fmt.Sprint(length)), bytes.Repeat([]byte{0xab}, length)); err != nil {
				return err
			}
		}
		return nil
	}))
	var encoded []byte
	require.NoError(t, q.db.View(func(tx *bbolt.Tx) error {
		for _, index := range []uint64{0, 1, 127, 128, 255, 256, math.MaxUint64} {
			data := assertSnapshotNativeBytes(t, tx, index)
			if index == 0 {
				encoded = data
			}
		}
		// Appending to an existing bytes.Buffer must retain its prefix.
		buffer := bytes.NewBufferString("prefix")
		require.NoError(t, writeSnapshot(buffer, tx, 0))
		require.Equal(t, append([]byte("prefix"), encoded...), buffer.Bytes())
		return nil
	}))
	restored, closeRestored := NewTestDB(t)
	defer closeRestored()
	require.NoError(t, restored.RestoreAt(bytes.NewReader(encoded), 0))
	_, fast := snapshotGobPayload(encoded)
	require.True(t, fast)
	require.NoError(t, restored.RestoreBytesAt(encoded, 0))
	require.NoError(t, restored.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, encoded, assertSnapshotNativeBytes(t, tx, 0))
		return nil
	}))
}

func TestStreamingSnapshotEmptyDatabase(t *testing.T) {
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "empty.db"), 0600, nil)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		assertSnapshotNativeBytes(t, tx, 0)
		return nil
	}))
}

type shortSnapshotWriter struct{}

func (shortSnapshotWriter) Write([]byte) (int, error) { return 0, nil }

type limitedSnapshotWriter struct{ remaining int }

func (w *limitedSnapshotWriter) Write(p []byte) (int, error) {
	if len(p) <= w.remaining {
		w.remaining -= len(p)
		return len(p), nil
	}
	n := w.remaining
	w.remaining = 0
	return n, io.ErrClosedPipe
}

func TestStreamingSnapshotPropagatesWriterFailures(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte("large"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("value"), bytes.Repeat([]byte{1}, 256<<10))
	}))
	for _, limit := range []int{0, 1, 64 << 10, 192 << 10, 256 << 10} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			require.ErrorIs(t, q.Snapshot(&limitedSnapshotWriter{remaining: limit}), io.ErrClosedPipe)
			require.Zero(t, q.db.Stats().OpenTxN)
		})
	}
	require.ErrorIs(t, q.Snapshot(shortSnapshotWriter{}), io.ErrShortWrite)
	require.Zero(t, q.db.Stats().OpenTxN)
}

type extendedSnapshotRecord struct {
	Key         []byte
	Value       []byte
	FutureField string
}

type extendedSnapshotBucket struct {
	Name     []byte
	Sequence uint64
	Records  []extendedSnapshotRecord
	Buckets  []extendedSnapshotBucket
}

type extendedSnapshot struct {
	Version   uint32
	LastIndex uint64
	Buckets   []extendedSnapshotBucket
}

type customSnapshotRecord SnapshotRecord

func (*customSnapshotRecord) GobEncode() ([]byte, error) { return nil, errors.New("custom encoding") }

type customSnapshotBucket struct {
	Name     []byte
	Sequence uint64
	Records  []customSnapshotRecord
	Buckets  []customSnapshotBucket
}

type customSnapshot struct {
	Version   uint32
	LastIndex uint64
	Buckets   []customSnapshotBucket
}

func TestSnapshotStreamSchemaGuard(t *testing.T) {
	root, bucket, record := reflect.TypeFor[Snapshot](), reflect.TypeFor[SnapshotBucket](), reflect.TypeFor[SnapshotRecord]()
	require.True(t, snapshotGobSchemaSupported(root, bucket, record))
	require.False(t, snapshotGobSchemaSupported(reflect.TypeFor[struct {
		Version     uint32
		LastIndex   uint64
		Buckets     []SnapshotBucket
		FutureField string
	}](), bucket, record))
	require.False(t, snapshotGobSchemaSupported(reflect.TypeFor[extendedSnapshot](), reflect.TypeFor[extendedSnapshotBucket](), reflect.TypeFor[extendedSnapshotRecord]()))
	require.False(t, snapshotGobSchemaSupported(reflect.TypeFor[customSnapshot](), reflect.TypeFor[customSnapshotBucket](), reflect.TypeFor[customSnapshotRecord]()))
	require.False(t, snapshotGobSchemaSupported(root, bucket, reflect.TypeFor[[]byte]()))
}

func TestSnapshotGobIntegersAndTemplate(t *testing.T) {
	for _, value := range []uint64{0, 1, 127, 128, 255, 256, 65535, 65536, 1 << 32, math.MaxUint64} {
		var encoded bytes.Buffer
		w := snapshotGobWriter{output: bufio.NewWriter(&encoded)}
		w.uint(value)
		require.NoError(t, w.output.Flush())
		decoded, n := snapshotGobUint(encoded.Bytes())
		require.Equal(t, value, decoded)
		require.Equal(t, encoded.Len(), n)
	}
	for _, invalid := range [][]byte{nil, {0x80}, {0xf7}, {0xff}, {0xfe, 1}} {
		_, n := snapshotGobUint(invalid)
		require.Zero(t, n)
	}
	var native bytes.Buffer
	require.NoError(t, gob.NewEncoder(&native).Encode(Snapshot{}))
	require.NotNil(t, parseSnapshotGobTemplate(native.Bytes()))
	for n := range native.Len() {
		require.Nil(t, parseSnapshotGobTemplate(native.Bytes()[:n]))
	}
	require.Nil(t, parseSnapshotGobTemplate(append(bytes.Clone(native.Bytes()), 0)))
	badTerminator := bytes.Clone(native.Bytes())
	badTerminator[len(badTerminator)-1] = 1
	require.Nil(t, parseSnapshotGobTemplate(badTerminator))
	count := snapshotGobWriter{size: snapshotGobMessageLimit - 1}
	count.uint(0)
	require.ErrorContains(t, count.err, "message too big")
}

func FuzzStreamingSnapshotNativeCompatibility(f *testing.F) {
	f.Add([]byte{0})
	f.Add([]byte("nested buckets, binary values and empty records"))
	f.Add(bytes.Repeat([]byte{0xff, 0, 1, 2, 3}, 256))
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 4096 {
			t.Skip()
		}
		db, err := bbolt.Open(filepath.Join(t.TempDir(), "fuzz.db"), 0600, &bbolt.Options{NoSync: true})
		require.NoError(t, err)
		defer db.Close()
		reader := bytes.NewReader(input)
		next := func() byte { value, _ := reader.ReadByte(); return value }
		budget := 32
		var populate func(*bbolt.Bucket, int) error
		populate = func(bucket *bbolt.Bucket, depth int) error {
			if err := bucket.SetSequence(uint64(next()) << (next() % 8 * 8)); err != nil {
				return err
			}
			count := int(next() % 8)
			for i := 0; i < count && budget > 0; i++ {
				budget--
				key := []byte{byte(i), next(), 0xff}
				if next()%3 == 0 && depth < 4 {
					child, err := bucket.CreateBucket(key)
					if err != nil {
						return err
					}
					if err := populate(child, depth+1); err != nil {
						return err
					}
				} else {
					length := int(next())
					length = min(length, reader.Len())
					value := make([]byte, length)
					_, _ = io.ReadFull(reader, value)
					if err := bucket.Put(key, value); err != nil {
						return err
					}
				}
			}
			return nil
		}
		require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
			for i, count := 0, int(next()%8); i < count; i++ {
				bucket, err := tx.CreateBucket([]byte{byte(i), next()})
				if err != nil {
					return err
				}
				if err := populate(bucket, 0); err != nil {
					return err
				}
			}
			return nil
		}))
		require.NoError(t, db.View(func(tx *bbolt.Tx) error {
			assertSnapshotNativeBytes(t, tx, uint64(len(input)))
			return nil
		}))
	})
}
