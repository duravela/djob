package node

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/duravela/djob/pkg/db"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
)

func lockNodeDataDir(directory string) (*fileutil.LockedFile, error) {
	path := filepath.Join(directory, ".djob.lock")
	if info, err := os.Lstat(path); err == nil && !info.Mode().IsRegular() {
		return nil, fmt.Errorf("node data-directory lock must be a regular file")
	} else if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	lock, err := fileutil.TryLockFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("lock node data directory (stop the member before maintenance): %w", err)
	}
	return lock, nil
}

// CompactStore operates only on this stopped member's materialized database.
// Raft, APIs and workers are never started. WAL and Raft snapshots are untouched.
func CompactStore(ctx context.Context, dataDir string, options db.FileCompactionOptions) (db.FileCompactionResult, error) {
	var result db.FileCompactionResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if dataDir == "" {
		return result, fmt.Errorf("an explicit existing data directory is required")
	}
	directory, err := filepath.EvalSymlinks(dataDir)
	if err != nil {
		return result, fmt.Errorf("inspect node data directory: %w", err)
	}
	store := filepath.Join(directory, "store")
	if info, err := os.Lstat(store); err != nil || !info.IsDir() {
		return result, fmt.Errorf("compaction requires an existing store directory without a symlink")
	}
	path := filepath.Join(store, "jobs.snap")
	if info, err := os.Lstat(path); err != nil || !info.Mode().IsRegular() {
		return result, fmt.Errorf("compaction requires an existing regular store/jobs.snap file")
	}
	lock, err := lockMaintenanceMember(directory)
	if err != nil {
		return result, err
	}
	defer lock.Close()
	return db.CompactFile(ctx, path, options)
}
