package node

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.uber.org/zap"
)

type RaftRetentionReport struct {
	AnchorIndex      uint64 `json:"anchor_index,string"`
	SnapshotsRemoved uint64 `json:"snapshots_removed,string"`
	SegmentsRemoved  uint64 `json:"segments_removed,string"`
	BytesRemoved     uint64 `json:"bytes_removed,string"`
}

type raftDiskFile struct {
	name          string
	first, second uint64
}

func raftDiskFiles(directory, suffix string) ([]raftDiskFile, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	var result []raftDiskFile
	for _, entry := range entries {
		name := entry.Name()
		if len(name) != 33+len(suffix) || name[16] != '-' || !strings.HasSuffix(name, suffix) {
			continue
		}
		first, e1 := strconv.ParseUint(name[:16], 16, 64)
		second, e2 := strconv.ParseUint(name[17:33], 16, 64)
		if e1 != nil || e2 != nil {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("Raft maintenance refuses nonregular files")
		}
		result = append(result, raftDiskFile{name, first, second})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].name < result[j].name })
	return result, nil
}

func syncMaintenanceDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// retainRaftFiles runs with the stable member lock held and the WAL writer
// serialized. It verifies complete checkpoints against durable WAL markers,
// retains a predecessor segment, and never removes a locked WAL segment.
func retainRaftFiles(ctx context.Context, waldir, snapdir string, keep int) (r RaftRetentionReport, err error) {
	return retainRaftFilesWithRemove(ctx, waldir, snapdir, keep, os.Remove)
}

func retainRaftFilesWithRemove(ctx context.Context, waldir, snapdir string, keep int, remove func(string) error) (r RaftRetentionReport, err error) {
	if keep < 1 || keep > 100 {
		return r, fmt.Errorf("retained snapshots must be in 1..100")
	}
	if err := ctx.Err(); err != nil {
		return r, err
	}
	markers, err := wal.ValidSnapshotEntries(zap.NewNop(), waldir)
	if err != nil {
		return r, err
	}
	files, err := raftDiskFiles(snapdir, ".snap")
	if err != nil {
		return r, err
	}
	var retained []raftDiskFile
	for i := len(files) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		f := files[i]
		matched := false
		for _, marker := range markers {
			if marker.Term == f.first && marker.Index == f.second {
				matched = true
				break
			}
		}
		if !matched {
			continue
		} // Orphaned and unknown files are left for inspection.
		metadata, err := verifiedSnapshotMetadata(filepath.Join(snapdir, f.name))
		if err != nil {
			return r, fmt.Errorf("verify retained checkpoint: %w", err)
		}
		if metadata.Term != f.first || metadata.Index != f.second {
			return r, fmt.Errorf("checkpoint filename does not match metadata")
		}
		retained = append(retained, f)
		if len(retained) == keep {
			break
		}
	}
	if len(retained) == 0 {
		return r, nil
	}
	anchor := retained[len(retained)-1]
	r.AnchorIndex = anchor.second
	// Validate the complete recovery tail before deleting anything.
	state, err := wal.Verify(zap.NewNop(), waldir, walpb.Snapshot{Index: anchor.second, Term: anchor.first})
	if err != nil {
		return r, err
	}
	if state == nil || state.Commit < anchor.second {
		return r, fmt.Errorf("checkpoint exceeds durable commit index")
	}
	segments, err := raftDiskFiles(waldir, ".wal")
	if err != nil {
		return r, err
	}
	boundary := -1
	for i, f := range segments {
		if f.second <= anchor.second {
			boundary = i
		}
	}
	if boundary < 0 {
		return r, fmt.Errorf("missing checkpoint WAL prefix")
	}
	for _, f := range files {
		if f.name >= anchor.name {
			continue
		}
		matched := false
		for _, marker := range markers {
			if marker.Term == f.first && marker.Index == f.second {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		if err := ctx.Err(); err != nil {
			return r, err
		}
		path := filepath.Join(snapdir, f.name)
		info, err := os.Lstat(path)
		if err != nil {
			return r, err
		}
		if err := remove(path); err != nil {
			return r, err
		}
		r.SnapshotsRemoved++
		r.BytesRemoved += uint64(info.Size())
	}
	// Sync removal of obsolete snapshots before dropping their WAL markers.
	// An interrupted purge can then retry without manufacturing orphan files.
	if r.SnapshotsRemoved > 0 {
		if err := syncMaintenanceDirectory(snapdir); err != nil {
			return r, err
		}
	}
	// Keep the containing segment and one predecessor, plus the entire tail.
	for i := 0; i < boundary-1; i++ {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		path := filepath.Join(waldir, segments[i].name)
		lock, err := fileutil.TryLockFile(path, os.O_RDWR, 0600)
		if err != nil {
			break
		} // Keep a contiguous suffix if another user holds a segment.
		info, statErr := lock.Stat()
		if statErr == nil {
			statErr = remove(path)
		}
		closeErr := lock.Close()
		if statErr != nil {
			return r, statErr
		}
		if closeErr != nil {
			return r, closeErr
		}
		r.SegmentsRemoved++
		r.BytesRemoved += uint64(info.Size())
	}
	if r.SegmentsRemoved > 0 {
		if err := syncMaintenanceDirectory(waldir); err != nil {
			return r, err
		}
	}
	return r, nil
}

func RetainRaftStorage(ctx context.Context, directory string, id uint64, keep int) (RaftRetentionReport, error) {
	var r RaftRetentionReport
	if directory == "" || id == 0 {
		return r, fmt.Errorf("existing data directory and member ID are required")
	}
	directory, err := filepath.EvalSymlinks(directory)
	if err != nil {
		return r, err
	}
	for _, path := range []string{filepath.Join(directory, "raft"), filepath.Join(directory, "raft", fmt.Sprintf("wal-%d", id)), filepath.Join(directory, "raft", fmt.Sprintf("snap-%d", id))} {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() {
			return r, fmt.Errorf("Raft maintenance requires existing directories without symlinks")
		}
	}
	lock, err := lockMaintenanceMember(directory)
	if err != nil {
		return r, err
	}
	defer lock.Close()
	return retainRaftFiles(ctx, filepath.Join(directory, "raft", fmt.Sprintf("wal-%d", id)), filepath.Join(directory, "raft", fmt.Sprintf("snap-%d", id)), keep)
}
