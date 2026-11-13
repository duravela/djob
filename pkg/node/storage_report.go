package node

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/duravela/djob/pkg/db"
)

// ReportStoreStorage inspects only this stopped member's materialized database.
// It starts no runtime and holds the same stable lock as startup and compaction.
func ReportStoreStorage(ctx context.Context, dataDir string) (db.FileStorageReport, error) {
	var result db.FileStorageReport
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
		return result, fmt.Errorf("storage report requires an existing store directory without a symlink")
	}
	path := filepath.Join(store, "jobs.snap")
	if info, err := os.Lstat(path); err != nil || !info.Mode().IsRegular() {
		return result, fmt.Errorf("storage report requires an existing regular store/jobs.snap file")
	}
	lock, err := lockMaintenanceMember(directory)
	if err != nil {
		return result, err
	}
	defer lock.Close()
	return db.ReportStorageFile(ctx, path)
}
