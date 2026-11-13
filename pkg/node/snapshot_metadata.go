package node

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

var errNoncanonicalSnapshot = errors.New("snapshot uses a noncanonical protobuf layout")

// verifiedSnapshotMetadata validates the entire CRC without materializing the
// payload. Normal etcd writers emit this fixed field layout. Other historical
// protobuf layouts still use the upstream reader, preserving its compatibility.
func verifiedSnapshotMetadata(path string) (raftpb.SnapshotMetadata, error) {
	metadata, err := canonicalSnapshotMetadata(path)
	if errors.Is(err, errNoncanonicalSnapshot) {
		snapshot, err := snap.Read(zap.NewNop(), path)
		if err != nil {
			return raftpb.SnapshotMetadata{}, err
		}
		return snapshot.Metadata, nil
	}
	return metadata, err
}

func canonicalSnapshotMetadata(path string) (raftpb.SnapshotMetadata, error) {
	file, err := os.Open(path)
	if err != nil {
		return raftpb.SnapshotMetadata{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return raftpb.SnapshotMetadata{}, err
	}
	reader := io.NewSectionReader(file, 0, info.Size())
	fail := func() (raftpb.SnapshotMetadata, error) { return raftpb.SnapshotMetadata{}, errNoncanonicalSnapshot }
	tag, err := readSnapshotByte(reader)
	if err != nil || tag != 0x08 {
		return fail()
	}
	checksum, err := readSnapshotUvarint(reader)
	if err != nil || checksum > 1<<32-1 {
		return fail()
	}
	tag, err = readSnapshotByte(reader)
	if err != nil || tag != 0x12 {
		return fail()
	}
	length, err := readSnapshotUvarint(reader)
	if err != nil {
		return fail()
	}
	start, _ := reader.Seek(0, io.SeekCurrent)
	if length != uint64(info.Size()-start) {
		return fail()
	}
	tag, err = readSnapshotByte(reader)
	if err != nil {
		return fail()
	}
	if tag == 0x0a {
		size, err := readSnapshotUvarint(reader)
		if err != nil {
			return fail()
		}
		offset, _ := reader.Seek(0, io.SeekCurrent)
		if size > uint64(info.Size()-offset) {
			return fail()
		}
		if _, err := reader.Seek(int64(size), io.SeekCurrent); err != nil {
			return fail()
		}
		tag, err = readSnapshotByte(reader)
		if err != nil {
			return fail()
		}
	}
	if tag != 0x12 {
		return fail()
	}
	size, err := readSnapshotUvarint(reader)
	if err != nil {
		return fail()
	}
	offset, _ := reader.Seek(0, io.SeekCurrent)
	// Bound the fast path's metadata allocation, not the supported file format.
	// Unusually large membership metadata remains readable through the fallback.
	if size != uint64(info.Size()-offset) || size > 1<<20 {
		return fail()
	}
	encoded := make([]byte, int(size))
	if _, err := io.ReadFull(reader, encoded); err != nil {
		return raftpb.SnapshotMetadata{}, err
	}
	var metadata raftpb.SnapshotMetadata
	if err := metadata.Unmarshal(encoded); err != nil {
		return raftpb.SnapshotMetadata{}, err
	}
	hash := crc32.New(snapshotCRCTable)
	if _, err := io.CopyBuffer(hash, io.NewSectionReader(file, start, int64(length)), make([]byte, 32<<10)); err != nil {
		return raftpb.SnapshotMetadata{}, fmt.Errorf("verify snapshot CRC: %w", err)
	}
	if checksum == 0 {
		return raftpb.SnapshotMetadata{}, snap.ErrEmptySnapshot
	}
	if uint64(hash.Sum32()) != checksum {
		return raftpb.SnapshotMetadata{}, snap.ErrCRCMismatch
	}
	return metadata, nil
}

func readSnapshotByte(r io.Reader) (byte, error) {
	var buf [1]byte
	_, err := io.ReadFull(r, buf[:])
	return buf[0], err
}
func readSnapshotUvarint(r io.Reader) (uint64, error) {
	var buf [binary.MaxVarintLen64]byte
	for i := range buf {
		b, err := readSnapshotByte(r)
		if err != nil {
			return 0, err
		}
		buf[i] = b
		if b < 0x80 {
			v, n := binary.Uvarint(buf[:i+1])
			if n <= 0 {
				return 0, errNoncanonicalSnapshot
			}
			var canonical [binary.MaxVarintLen64]byte
			if binary.PutUvarint(canonical[:], v) != i+1 {
				return 0, errNoncanonicalSnapshot
			}
			return v, nil
		}
	}
	return 0, errNoncanonicalSnapshot
}
