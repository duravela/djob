package node

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap/snappb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

func TestAtomicSnapshotPublicationNeverExposesPartialFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "snapshots")
	require.NoError(t, os.MkdirAll(dir, 0o750))
	n := &RaftNode{snapdir: dir, logger: zap.NewNop()}
	loader := snap.New(zap.NewNop(), dir)
	metadata := raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}}}
	require.NoError(t, n.saveSnapshotAtomically(raftpb.Snapshot{Metadata: metadata, Data: []byte("initial")}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reads := make(chan error, 1)
	go func() {
		for ctx.Err() == nil {
			if _, err := loader.Load(); err != nil {
				reads <- err
				return
			}
		}
		reads <- nil
	}()
	payload := bytes.Repeat([]byte{0, 0xff, 0x12, 0x34}, 1<<20)
	for index := uint64(2); index <= 7; index++ {
		metadata.Index = index
		require.NoError(t, n.saveSnapshotAtomically(raftpb.Snapshot{Metadata: metadata, Data: payload}))
	}
	cancel()
	require.NoError(t, <-reads)
	latest, err := loader.Load()
	require.NoError(t, err)
	require.EqualValues(t, 7, latest.Metadata.Index)
	require.Equal(t, payload, latest.Data)
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, file := range files {
		require.Equal(t, ".snap", filepath.Ext(file.Name()))
	}
}

func BenchmarkSnapshotFileMemory(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "snapshots")
	if err := os.MkdirAll(dir, 0700); err != nil {
		b.Fatal(err)
	}
	n := &RaftNode{snapdir: dir, logger: zap.NewNop()}
	snapshot := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}}}, Data: bytes.Repeat([]byte{0x55}, 64<<20)}
	b.SetBytes(int64(len(snapshot.Data)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := n.saveSnapshotAtomically(snapshot); err != nil {
			b.Fatal(err)
		}
	}
}

func TestSnapshotEncodingMatchesNativeWriter(t *testing.T) {
	for _, size := range []int{-1, 0, 1, 127, 128, 16383, 16384, 1 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			var payload []byte
			if size >= 0 {
				payload = bytes.Repeat([]byte{0xff}, size)
			}
			snapshot := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 123456789, Term: 987654321, ConfState: raftpb.ConfState{Voters: []uint64{1, 3, 99}, Learners: []uint64{5}, VotersOutgoing: []uint64{1, 2}, LearnersNext: []uint64{4}, AutoLeave: true}}, Data: payload}
			nativeDir := t.TempDir()
			require.NoError(t, snap.New(zap.NewNop(), nativeDir).SaveSnap(snapshot))
			files, err := os.ReadDir(nativeDir)
			require.NoError(t, err)
			require.Len(t, files, 1)
			expected, err := os.ReadFile(filepath.Join(nativeDir, files[0].Name()))
			require.NoError(t, err)
			var actual bytes.Buffer
			require.NoError(t, writeSnapshotEncoding(&actual, snapshot))
			require.Equal(t, expected, actual.Bytes(), "existing etcd readers must see identical bytes")
			target := filepath.Join(t.TempDir(), files[0].Name())
			require.NoError(t, writeSnapshotFile(target, snapshot))
			decoded, err := snap.Read(zap.NewNop(), target)
			require.NoError(t, err)
			require.Equal(t, snapshot, *decoded)
		})
	}
}

type snapshotShortWriter struct{}

func (snapshotShortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }
func TestSnapshotEncodingPropagatesShortWrite(t *testing.T) {
	require.ErrorIs(t, writeSnapshotEncoding(snapshotShortWriter{}, raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1}}), io.ErrShortWrite)
}

func TestSnapshotFileEncodingAvoidsPayloadSizedAllocations(t *testing.T) {
	snapshot := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1}, Data: bytes.Repeat([]byte{0xaa}, 16<<20)}
	require.NoError(t, writeSnapshotEncoding(io.Discard, snapshot))
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	require.NoError(t, writeSnapshotEncoding(io.Discard, snapshot))
	runtime.ReadMemStats(&after)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20), "writing an existing 16 MiB payload must not allocate another payload")
}

func TestSnapshotMetadataVerifiesCRCAndHistoricalLayouts(t *testing.T) {
	snapshot := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 123, Term: 7, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}, Data: bytes.Repeat([]byte{0x55}, 1<<20)}
	var encoded bytes.Buffer
	require.NoError(t, writeSnapshotEncoding(&encoded, snapshot))
	original := bytes.Clone(encoded.Bytes())
	var envelope snappb.Snapshot
	require.NoError(t, envelope.Unmarshal(original))
	metadata, err := snapshot.Metadata.Marshal()
	require.NoError(t, err)
	reordered := binary.AppendUvarint([]byte{0x12}, uint64(len(metadata)))
	reordered = append(reordered, metadata...)
	reordered = append(reordered, 0x0a)
	reordered = binary.AppendUvarint(reordered, uint64(len(snapshot.Data)))
	reordered = append(reordered, snapshot.Data...)
	legacy, err := (&snappb.Snapshot{Crc: crc32.Checksum(reordered, snapshotCRCTable), Data: reordered}).Marshal()
	require.NoError(t, err)
	corrupt := bytes.Clone(original)
	at := bytes.Index(corrupt, snapshot.Data[:64])
	require.Greater(t, at, 0)
	corrupt[at] ^= 0xff
	cases := map[string][]byte{"native": original, "reordered": legacy, "unknown outer field": append(bytes.Clone(original), 0x18, 1), "corrupt": corrupt, "truncated": original[:len(original)-1], "empty": nil, "bad varint": {0x08, 0x80}, "trailing garbage": append(bytes.Clone(original), 0xff)}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "checkpoint.snap")
			require.NoError(t, os.WriteFile(path, data, 0600))
			native, nativeErr := snap.Read(zap.NewNop(), path)
			got, err := verifiedSnapshotMetadata(path)
			if nativeErr != nil {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, native.Metadata, got)
		})
	}
}

func TestSnapshotMetadataAllocationsDoNotScaleWithPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.snap")
	snapshot := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1}, Data: bytes.Repeat([]byte{0xaa}, 16<<20)}
	require.NoError(t, writeSnapshotFile(path, snapshot))
	_, err := verifiedSnapshotMetadata(path)
	require.NoError(t, err)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got, err := verifiedSnapshotMetadata(path)
	runtime.ReadMemStats(&after)
	require.NoError(t, err)
	require.Equal(t, snapshot.Metadata, got)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20), "retention must verify CRC without loading full payloads")
}

func BenchmarkSnapshotMetadataMemory(b *testing.B) {
	path := filepath.Join(b.TempDir(), "checkpoint.snap")
	snapshot := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1}, Data: bytes.Repeat([]byte{0x55}, 64<<20)}
	if err := writeSnapshotFile(path, snapshot); err != nil {
		b.Fatal(err)
	}
	for _, native := range []bool{true, false} {
		name := "streamed"
		if native {
			name = "upstream"
		}
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(snapshot.Data)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var err error
				if native {
					_, err = snap.Read(zap.NewNop(), path)
				} else {
					_, err = verifiedSnapshotMetadata(path)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func FuzzSnapshotMetadataMatchesNative(f *testing.F) {
	var valid bytes.Buffer
	if err := writeSnapshotEncoding(&valid, raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 17, Term: 3, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}, Data: []byte("checkpoint payload")}); err != nil {
		f.Fatal(err)
	}
	f.Add(valid.Bytes())
	f.Add([]byte{})
	f.Add([]byte{0x08, 0x80})
	f.Add([]byte{0x08, 1, 0x12, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 65536 {
			t.Skip()
		}
		path := filepath.Join(t.TempDir(), "checkpoint.snap")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		native, nativeErr := snap.Read(zap.NewNop(), path)
		metadata, err := verifiedSnapshotMetadata(path)
		if nativeErr != nil {
			if err == nil {
				t.Fatal("metadata verifier accepted a file rejected by upstream")
			}
			return
		}
		require.NoError(t, err)
		require.Equal(t, native.Metadata, metadata)
	})
}
