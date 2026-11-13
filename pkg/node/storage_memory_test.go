package node

import (
	"go.etcd.io/raft/v3/raftpb"
	"testing"
)

func TestAccountedStorageOverwriteCompactAndSnapshot(t *testing.T) {
	s := newAccountedStorage()
	entries := []raftpb.Entry{{Index: 1, Term: 1, Data: make([]byte, 4096)}, {Index: 2, Term: 1, Data: []byte("old")}, {Index: 3, Term: 1, Data: []byte("tail")}}
	if err := s.Append(entries); err != nil {
		t.Fatal(err)
	}
	replacement := raftpb.Entry{Index: 2, Term: 2, Data: make([]byte, 8192)}
	if err := s.Append([]raftpb.Entry{replacement}); err != nil {
		t.Fatal(err)
	}
	n, b, _ := s.stats()
	if n != 2 || b != uint64(entries[0].Size()+replacement.Size()) {
		t.Fatalf("overwrite: %d entries %d bytes", n, b)
	}
	if got := s.bytesAfter(1, 2); got != uint64(replacement.Size()) {
		t.Fatalf("range: %d", got)
	}
	if got := s.compactIndex(2, 100, 4096); got != 2 {
		t.Fatalf("oversized entry must fall back to snapshot: %d", got)
	}
	if got := s.compactIndex(2, 100, uint64(replacement.Size())); got != 1 {
		t.Fatalf("exact byte boundary: %d", got)
	}
	if _, err := s.CreateSnapshot(2, nil, []byte("checkpoint")); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(1); err != nil {
		t.Fatal(err)
	}
	n, b, snap := s.stats()
	if n != 1 || b != uint64(replacement.Size()) || snap != 10 {
		t.Fatalf("compact: %d %d %d", n, b, snap)
	}
	// WAL replay can include a prefix already represented by the snapshot.
	if err := s.Append([]raftpb.Entry{entries[0], replacement, {Index: 3, Term: 2}}); err != nil {
		t.Fatal(err)
	}
	if n, _, _ := s.stats(); n != 2 {
		t.Fatalf("replay entries: %d", n)
	}
	if err := s.ApplySnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 10, Term: 3}, Data: []byte("received")}); err != nil {
		t.Fatal(err)
	}
	if n, b, snap := s.stats(); n != 0 || b != 0 || snap != 8 {
		t.Fatalf("received snapshot: %d %d %d", n, b, snap)
	}
	if got := s.bytesAfter(0, 10); got != 0 {
		t.Fatalf("empty range: %d", got)
	}
	if err := s.Append([]raftpb.Entry{{Index: 11, Term: 3, Data: []byte("next")}}); err != nil {
		t.Fatal(err)
	}
	if got := s.compactIndex(11, 0, 1000); got != 11 {
		t.Fatalf("zero count: %d", got)
	}
	if got := s.compactIndex(11, 1, 1000); got != 10 {
		t.Fatalf("one entry: %d", got)
	}
}
