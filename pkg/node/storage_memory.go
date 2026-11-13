package node

import (
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// accountedStorage tracks serialized bytes without scanning payloads during a
// metrics scrape. All mutations and accounting reads belong to the Raft loop;
// Raft's concurrent reads still use MemoryStorage's own synchronization.
type accountedStorage struct {
	*raft.MemoryStorage
	first         uint64
	prefix        []uint64
	snapshotBytes uint64
}

func newAccountedStorage() *accountedStorage {
	return &accountedStorage{MemoryStorage: raft.NewMemoryStorage(), first: 1, prefix: []uint64{0}}
}
func (s *accountedStorage) Append(entries []raftpb.Entry) error {
	if err := s.MemoryStorage.Append(entries); err != nil {
		return err
	}
	for len(entries) > 0 && entries[0].Index < s.first {
		entries = entries[1:]
	}
	if len(entries) == 0 {
		return nil
	}
	offset := int(entries[0].Index - s.first)
	s.prefix = s.prefix[:offset+1]
	for _, entry := range entries {
		s.prefix = append(s.prefix, s.prefix[len(s.prefix)-1]+uint64(entry.Size()))
	}
	return nil
}
func (s *accountedStorage) ApplySnapshot(snapshot raftpb.Snapshot) error {
	if err := s.MemoryStorage.ApplySnapshot(snapshot); err != nil {
		return err
	}
	s.first = snapshot.Metadata.Index + 1
	s.prefix = []uint64{0}
	s.snapshotBytes = uint64(len(snapshot.Data))
	return nil
}
func (s *accountedStorage) CreateSnapshot(index uint64, state *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	snapshot, err := s.MemoryStorage.CreateSnapshot(index, state, data)
	if err == nil {
		s.snapshotBytes = uint64(len(data))
	}
	return snapshot, err
}
func (s *accountedStorage) Compact(index uint64) error {
	if err := s.MemoryStorage.Compact(index); err != nil {
		return err
	}
	offset := int(index + 1 - s.first)
	base := s.prefix[offset]
	prefix := make([]uint64, len(s.prefix)-offset)
	for i := range prefix {
		prefix[i] = s.prefix[offset+i] - base
	}
	s.prefix = prefix
	s.first = index + 1
	return nil
}
func (s *accountedStorage) stats() (uint64, uint64, uint64) {
	return uint64(len(s.prefix) - 1), s.prefix[len(s.prefix)-1], s.snapshotBytes
}
func (s *accountedStorage) bytesAfter(index, through uint64) uint64 {
	if through < s.first || through <= index {
		return 0
	}
	start := s.first
	if index >= start {
		start = index + 1
	}
	last := s.first + uint64(len(s.prefix)) - 2
	if through > last {
		through = last
	}
	if start > through {
		return 0
	}
	return s.prefix[through-s.first+1] - s.prefix[start-s.first]
}
func (s *accountedStorage) compactIndex(index, count, bytes uint64) uint64 {
	// Never discard post-checkpoint entries. A lagging peer can install the
	// durable checkpoint when even one entry exceeds the catch-up byte budget.
	compact := index
	var retained uint64
	for compact >= s.first && index-compact < count {
		size := s.bytesAfter(compact-1, compact)
		if size > bytes-retained {
			break
		}
		retained += size
		compact--
	}
	return compact
}
