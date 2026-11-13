package node

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// The upstream snapshot writer creates its final filename before writing and
// syncing the contents. A concurrent Load can mistake that partial file for a
// corrupt snapshot and rename it. Keep native encoding, but publish with rename
// only after the complete file is synced. Staging is outside the scanned directory.
func (rc *RaftNode) saveSnapshotAtomically(snapshot raftpb.Snapshot) error {
	if fault := rc.persistFault.Load(); fault != nil {
		if err := fault.before("snapshot_file"); err != nil {
			return err
		}
	}
	if raft.IsEmptySnap(snapshot) {
		return fmt.Errorf("refuse empty Raft checkpoint")
	}
	staging, err := os.MkdirTemp(filepath.Dir(rc.snapdir), ".snapshot-pending-")
	if err != nil {
		return fmt.Errorf("create snapshot staging directory: %w", err)
	}
	defer os.RemoveAll(staging)
	name := fmt.Sprintf("%016x-%016x.snap", snapshot.Metadata.Term, snapshot.Metadata.Index)
	if err := writeSnapshotFile(filepath.Join(staging, name), snapshot); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(staging, name), filepath.Join(rc.snapdir, name)); err != nil {
		return fmt.Errorf("publish snapshot: %w", err)
	}
	directory, err := os.Open(rc.snapdir)
	if err != nil {
		return err
	}
	defer directory.Close()
	// Persist the destination directory entry before the WAL can reference it.
	return directory.Sync()
}

// writeSnapshotEncoding emits exactly snappb.Snapshot{Crc, Data}, whose Data is
// the native raftpb.Snapshot encoding. Only metadata and field headers allocate;
// the already-owned Raft payload is hashed and written without copying it twice.
func writeSnapshotEncoding(w io.Writer, snapshot raftpb.Snapshot) error {
	metadata, err := snapshot.Metadata.Marshal()
	if err != nil {
		return err
	}
	var dataHeader []byte
	if snapshot.Data != nil {
		dataHeader = binary.AppendUvarint([]byte{0x0a}, uint64(len(snapshot.Data)))
	}
	metaHeader := binary.AppendUvarint([]byte{0x12}, uint64(len(metadata)))
	parts := [][]byte{dataHeader, snapshot.Data, metaHeader, metadata}
	checksum := crc32.New(snapshotCRCTable)
	size := 0
	for _, part := range parts {
		_, _ = checksum.Write(part)
		size += len(part)
	}
	header := binary.AppendUvarint([]byte{0x08}, uint64(checksum.Sum32()))
	header = append(header, 0x12)
	header = binary.AppendUvarint(header, uint64(size))
	for _, part := range append([][]byte{header}, parts...) {
		if len(part) == 0 {
			continue
		}
		n, err := w.Write(part)
		if err != nil {
			return err
		}
		if n != len(part) {
			return io.ErrShortWrite
		}
	}
	return nil
}

var snapshotCRCTable = crc32.MakeTable(crc32.Castagnoli)

func writeSnapshotFile(path string, snapshot raftpb.Snapshot) (err error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
	}()
	if err = writeSnapshotEncoding(file, snapshot); err != nil {
		return err
	}
	return file.Sync()
}
