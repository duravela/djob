package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"
)

type CompactionPhase string

const (
	CompactionCheck   CompactionPhase = "check"
	CompactionCopy    CompactionPhase = "copy"
	CompactionVerify  CompactionPhase = "verify"
	CompactionPublish CompactionPhase = "publish"
)

type FileCompactionOptions struct {
	KeepBackup bool
	// OnPhase runs before each phase, outside database transactions. Returning
	// an error aborts before publication; it can also report maintenance progress.
	OnPhase func(CompactionPhase) error
}

type FileCompactionResult struct {
	Path         string `json:"path"`
	Installed    bool   `json:"installed"`
	BeforeBytes  int64  `json:"before_bytes"`
	AfterBytes   int64  `json:"after_bytes"`
	SavedBytes   int64  `json:"saved_bytes"`
	AppliedIndex uint64 `json:"applied_index"`
	Format       byte   `json:"database_format"`
	Buckets      uint64 `json:"buckets"`
	Records      uint64 `json:"records"`
	SHA256       string `json:"state_sha256"`
	BackupPath   string `json:"backup_path,omitempty"`
}

type compactionFileOps struct {
	rename  func(string, string) error
	syncDir func(string) error
	remove  func(string) error
}

// CompactFile preserves all logical state and only replaces a file when smaller.
// The caller MUST hold a stable node-directory lock across this entire call:
// inode locks alone cannot exclude a new opener while a database is renamed.
// node.CompactStore provides that lock for the application's storage layout.
func CompactFile(ctx context.Context, path string, options FileCompactionOptions) (FileCompactionResult, error) {
	return compactFile(ctx, path, options, compactionFileOps{os.Rename, syncCompactionDirectory, os.Remove})
}

func compactFile(ctx context.Context, path string, options FileCompactionOptions, ops compactionFileOps) (result FileCompactionResult, err error) {
	phase := func(p CompactionPhase) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if options.OnPhase != nil {
			if err := options.OnPhase(p); err != nil {
				return err
			}
		}
		return ctx.Err()
	}
	if err := phase(CompactionCheck); err != nil {
		return result, err
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return result, err
	}
	directory, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return result, err
	}
	path = filepath.Join(directory, filepath.Base(absolute))
	info, err := os.Lstat(path)
	if err != nil {
		return result, fmt.Errorf("inspect compaction source: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return result, fmt.Errorf("compaction requires an existing nonempty regular database file")
	}
	if err := validateCompactionFile(path, info); err != nil {
		return result, err
	}
	result.Path, result.BeforeBytes, result.AfterBytes = path, info.Size(), info.Size()
	// NoFreelistSync avoids modifying a historical source simply by opening it.
	// Exclusive access also refuses standalone database readers/writers.
	source, err := bbolt.Open(path, 0600, &bbolt.Options{
		Timeout:        100 * time.Millisecond,
		NoFreelistSync: true,
		OpenFile: func(name string, flags int, mode os.FileMode) (*os.File, error) {
			return os.OpenFile(name, flags&^os.O_CREATE, mode)
		},
	})
	if err != nil {
		return result, fmt.Errorf("lock compaction source (stop all database users first): %w", err)
	}
	defer func() {
		if source != nil {
			err = errors.Join(err, source.Close())
		}
	}()
	if err := source.View(func(tx *bbolt.Tx) error {
		var e error
		result.AppliedIndex, result.Format, e = validateCompactSchema(tx)
		return e
	}); err != nil {
		return result, err
	}
	if err := checkCompactStructure(ctx, source); err != nil {
		return result, fmt.Errorf("check compaction source: %w", err)
	}
	result.SHA256, result.Buckets, result.Records, err = compactDigest(ctx, source)
	if err != nil {
		return result, err
	}
	temporary, err := os.CreateTemp(directory, ".jobs.snap.compact-")
	if err != nil {
		return result, err
	}
	tempPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		_ = os.Remove(tempPath)
		return result, err
	}
	var backup string
	defer func() {
		if e := os.Remove(tempPath); e != nil && !os.IsNotExist(e) {
			err = errors.Join(err, fmt.Errorf("remove compaction staging file: %w", e))
		}
		if !result.Installed && backup != "" {
			if e := os.Remove(backup); e != nil && !os.IsNotExist(e) {
				err = errors.Join(err, fmt.Errorf("remove unpublished compaction backup: %w", e))
			}
		}
	}()
	target, err := bbolt.Open(tempPath, 0600, &bbolt.Options{Timeout: 100 * time.Millisecond})
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, target.Close()) }()
	if err := phase(CompactionCopy); err != nil {
		return result, err
	}
	if err := copyCompactState(ctx, target, source); err != nil {
		return result, fmt.Errorf("copy compaction state: %w", err)
	}
	if err := phase(CompactionVerify); err != nil {
		return result, err
	}
	if err := checkCompactStructure(ctx, target); err != nil {
		return result, fmt.Errorf("check compacted database: %w", err)
	}
	digest, buckets, records, err := compactDigest(ctx, target)
	if err != nil {
		return result, err
	}
	if digest != result.SHA256 || buckets != result.Buckets || records != result.Records {
		return result, fmt.Errorf("compacted database state differs from its source")
	}
	compacted, err := os.Stat(tempPath)
	if err != nil {
		return result, err
	}
	if compacted.Size() >= info.Size() {
		return result, nil // Preserve the original inode when there is no saving.
	}
	if err := copyCompactionMetadata(path, tempPath, info); err != nil {
		return result, fmt.Errorf("preserve database file metadata: %w", err)
	}
	if err := target.Sync(); err != nil {
		return result, fmt.Errorf("sync compacted database: %w", err)
	}
	if err := phase(CompactionPublish); err != nil {
		return result, err
	}
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(info, current) || current.Size() != info.Size() || !current.ModTime().Equal(info.ModTime()) || current.Mode() != info.Mode() {
		return result, fmt.Errorf("compaction source changed before publication")
	}
	backup = tempPath + ".original"
	if err := os.Link(path, backup); err != nil {
		backup = "" // An existing collision is not ours to remove.
		return result, fmt.Errorf("retain compaction original: %w", err)
	}
	if err := ops.syncDir(directory); err != nil {
		return result, fmt.Errorf("sync original compaction backup: %w", err)
	}
	// Both database inode locks and the caller's stable directory lock remain
	// held through publication and directory sync. Cancellation stops here only
	// before rename; after rename, finish durability and report installed=true.
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := ops.rename(tempPath, path); err != nil {
		return result, fmt.Errorf("publish compacted database: %w", err)
	}
	result.Installed, result.AfterBytes, result.SavedBytes = true, compacted.Size(), info.Size()-compacted.Size()
	result.BackupPath = backup
	if err := ops.syncDir(directory); err != nil {
		return result, fmt.Errorf("compacted database installed; directory sync failed and original retained at %s: %w", backup, err)
	}
	if err := source.Close(); err != nil {
		source = nil
		return result, fmt.Errorf("compacted database installed; close original: %w", err)
	}
	source = nil
	if !options.KeepBackup {
		if err := ops.remove(backup); err != nil {
			return result, fmt.Errorf("compacted database installed; original backup remains at %s: %w", backup, err)
		}
		result.BackupPath, backup = "", ""
		if err := ops.syncDir(directory); err != nil {
			return result, fmt.Errorf("compacted database installed; sync backup removal: %w", err)
		}
	}
	return result, nil
}

func syncCompactionDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
