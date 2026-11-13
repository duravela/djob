package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/duravela/djob/pkg/db"
	"go.etcd.io/raft/v3/raftpb"
)

// BackupVerification describes artifact integrity, not voter freshness, external
// fencing or the outcome of replaying commands. TransferConsumed observes only
// this copy's marker; absence cannot establish custody of other copies.
type BackupVerification struct {
	ArtifactVersion   int    `json:"artifact_version"`
	MemberID          uint64 `json:"member_id,string"`
	ClusterID         uint64 `json:"cluster_id,string"`
	RecoveryClusterID uint64 `json:"recovery_cluster_id,string"`
	Format            byte   `json:"database_format"`
	AppliedIndex      uint64 `json:"applied_index,string"`
	CommittedIndex    uint64 `json:"committed_index,string"`
	SnapshotIndex     uint64 `json:"snapshot_index,string"`
	FilesVerified     uint64 `json:"files_verified,string"`
	BytesVerified     uint64 `json:"bytes_verified,string"`
	Transferred       bool   `json:"transferred"`
	TransferConsumed  bool   `json:"transfer_consumed"`
	ClockHold         bool   `json:"clock_hold"`
	RecoveryHold      bool   `json:"recovery_hold"`
	ReplayRequired    bool   `json:"replay_required"`
}

// VerifyBackup reads an immutable backup/transfer in place without creating a
// lock, staging directory, runtime or consumption marker. The caller must keep
// the artifact's contents stable; supported restore operations only add metadata.
// File checksums, bbolt structure/schema and the recoverable Raft checkpoint and
// committed log coverage are checked. Commands and snapshot state are not replayed;
// an isolated recovery drill is still necessary to validate application recovery.
func VerifyBackup(ctx context.Context, directory string) (BackupVerification, error) {
	var result BackupVerification
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if directory == "" {
		return result, fmt.Errorf("an explicit existing backup directory is required")
	}
	directory, err := filepath.EvalSymlinks(directory)
	if err != nil {
		return result, err
	}
	m, err := loadBackup(directory)
	if err != nil {
		return result, err
	}
	if err := validateBackupFiles(directory, m); err != nil {
		return result, err
	}
	var bytesVerified uint64
	for _, expected := range m.Files {
		actual, err := hashBackupFile(ctx, filepath.Join(directory, expected.Path), expected.Path)
		if err != nil {
			return result, err
		}
		if actual.Size != expected.Size || actual.SHA256 != expected.SHA256 {
			return result, fmt.Errorf("backup checksum mismatch: %s", expected.Path)
		}
		bytesVerified += uint64(actual.Size)
	}
	result, err = inspectBackupCheckpoint(ctx, directory, m)
	if err != nil {
		return BackupVerification{}, err
	}
	result.FilesVerified, result.BytesVerified = uint64(len(m.Files)), bytesVerified
	for _, file := range m.Files {
		result.ClockHold = result.ClockHold || file.Path == ".djob-clock-hold"
		result.RecoveryHold = result.RecoveryHold || file.Path == ".djob-recovery-hold"
	}
	if _, err := os.Lstat(filepath.Join(directory, ".djob-restored")); err == nil {
		result.TransferConsumed = true
	} else if !os.IsNotExist(err) {
		return BackupVerification{}, err
	}
	if err := ctx.Err(); err != nil {
		return BackupVerification{}, err
	}
	return result, nil
}

func openBackupFile(path string) (*os.File, os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, nil, fmt.Errorf("backup refuses nonregular source files")
	}
	in, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	opened, err := in.Stat()
	if err != nil || !os.SameFile(info, opened) {
		_ = in.Close()
		return nil, nil, fmt.Errorf("backup source changed while opening")
	}
	return in, info, nil
}

func hashBackupFile(ctx context.Context, path, relative string) (result BackupFile, err error) {
	result.Path = relative
	in, info, err := openBackupFile(path)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, in.Close()) }()
	hash := sha256.New()
	result.Size, err = io.Copy(hash, maintenanceReader{ctx, in})
	if err != nil {
		return result, err
	}
	if result.Size != info.Size() {
		return result, fmt.Errorf("backup source size changed")
	}
	result.SHA256 = hex.EncodeToString(hash.Sum(nil))
	return result, nil
}

func validateBackupFiles(directory string, m MemberBackup) error {
	allowed, err := backupPaths(directory, m.Identity.MemberID)
	if err != nil {
		return err
	}
	allowedSet := make(map[string]bool, len(allowed))
	for _, path := range allowed {
		allowedSet[path] = true
	}
	seen := make(map[string]bool, len(m.Files))
	for _, file := range m.Files {
		checksum, err := hex.DecodeString(file.SHA256)
		if !filepath.IsLocal(file.Path) || strings.Contains(file.Path, "\\") || !allowedSet[file.Path] || seen[file.Path] || file.Size < 0 || err != nil || len(checksum) != sha256.Size || strings.ToLower(file.SHA256) != file.SHA256 {
			return fmt.Errorf("invalid or duplicate backup path/checksum")
		}
		seen[file.Path] = true
	}
	if len(seen) != len(allowed) {
		return fmt.Errorf("backup manifest omits recovery files")
	}
	return nil
}

func inspectBackupCheckpoint(ctx context.Context, directory string, m MemberBackup) (BackupVerification, error) {
	var result BackupVerification
	if err := ctx.Err(); err != nil {
		return result, err
	}
	identity, err := readMemberIdentity(directory)
	if err != nil {
		return result, err
	}
	if !reflect.DeepEqual(identity, m.Identity) {
		return result, fmt.Errorf("backup member identity mismatch")
	}
	report, err := db.VerifyStorageFile(ctx, filepath.Join(directory, "store", "jobs.snap"))
	if err != nil {
		return result, err
	}
	checkpoint, state, entries, err := readMemberLog(directory, m.Identity.MemberID)
	if err != nil {
		return result, err
	}
	if report.AppliedIndex != m.AppliedIndex || state.Commit != m.CommittedIndex || report.AppliedIndex > state.Commit {
		return result, fmt.Errorf("backup checkpoint boundary mismatch")
	}
	if err := checkBackupLogCoverage(ctx, report.AppliedIndex, checkpoint.Metadata.Index, state.Commit, entries); err != nil {
		return result, err
	}
	return BackupVerification{
		ArtifactVersion: m.Version, MemberID: identity.MemberID, ClusterID: identity.ClusterID, RecoveryClusterID: m.RecoveryClusterID,
		Format: report.Format, AppliedIndex: report.AppliedIndex, CommittedIndex: state.Commit, SnapshotIndex: checkpoint.Metadata.Index,
		Transferred: m.Transferred, ReplayRequired: report.AppliedIndex < state.Commit,
	}, nil
}

func checkBackupLogCoverage(ctx context.Context, applied, snapshot, committed uint64, entries []raftpb.Entry) error {
	covered := max(applied, snapshot)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Index <= covered || entry.Index > committed {
			continue
		}
		if entry.Index != covered+1 {
			return fmt.Errorf("backup WAL has a gap in committed entries")
		}
		covered = entry.Index
	}
	if covered != committed {
		return fmt.Errorf("backup WAL omits committed entries")
	}
	return ctx.Err()
}
