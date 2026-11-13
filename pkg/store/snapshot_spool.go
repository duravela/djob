package store

import (
	"fmt"
	"io"
	"os"
)

type snapshotErrorWriter struct{ err error }

func (w snapshotErrorWriter) Write([]byte) (int, error) { return 0, w.err }

// Encode against the pinned Bolt view without allocating the new full snapshot
// in the Go heap. The encoder releases its view before the payload allocation
// below, so GC pressure from old+new snapshots cannot prolong that read view and
// amplify concurrent copy-on-write growth. This file is staging, not a durable
// checkpoint: Raft still persists and syncs its complete snapshot before WAL
// compaction. On POSIX, unlink the open file immediately for crash cleanup.
func spoolSnapshot(directory string, encode func(io.Writer) error) ([]byte, error) {
	file, err := os.CreateTemp(directory, ".snapshot-encode-*")
	if err != nil {
		// The captured view must be consumed even if no staging file can be opened.
		_ = encode(snapshotErrorWriter{err})
		return nil, fmt.Errorf("create snapshot staging file: %w", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()
	_ = os.Remove(file.Name())
	if err := encode(file); err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < 0 || uint64(info.Size()) > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("snapshot exceeds addressable buffer size")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	data := make([]byte, int(info.Size()))
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, err
	}
	return data, nil
}
