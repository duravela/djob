package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/duravela/djob/pkg/base"
	"github.com/duravela/djob/pkg/commit"
	"github.com/duravela/djob/pkg/db"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

const backupManifestName = ".djob-backup.json"

type BackupFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size,string"`
	SHA256 string `json:"sha256"`
}
type MemberBackup struct {
	Version           int            `json:"version"`
	Transferred       bool           `json:"transferred"`
	Identity          memberIdentity `json:"identity"`
	RecoveryClusterID uint64         `json:"recovery_cluster_id,string"`
	AppliedIndex      uint64         `json:"applied_index,string"`
	CommittedIndex    uint64         `json:"committed_index,string"`
	Files             []BackupFile   `json:"files"`
}

// RestoreOptions attests custody of an exact stopped-member transfer. Excluding
// an old process alone cannot make a stale voter log safe to reuse.
type RestoreOptions struct {
	OldAccessFenced      bool
	TransferNeverStarted bool // No copy has ever run as a replacement voter.
}

type maintenanceReader struct {
	ctx context.Context
	io.Reader
}

func (r maintenanceReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

func copyBackupFile(ctx context.Context, source, target, relative string) (BackupFile, error) {
	r := BackupFile{Path: relative}
	in, info, err := openBackupFile(source)
	if err != nil {
		return r, err
	}
	defer in.Close()
	if err = os.MkdirAll(filepath.Dir(target), 0700); err != nil {
		return r, err
	}
	out, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return r, err
	}
	hash := sha256.New()
	r.Size, err = io.Copy(io.MultiWriter(out, hash), maintenanceReader{ctx, in})
	if err == nil && r.Size != info.Size() {
		err = fmt.Errorf("backup source size changed")
	}
	if err == nil {
		err = out.Sync()
	}
	err = errors.Join(err, out.Close())
	if err != nil {
		return r, err
	}
	r.SHA256 = hex.EncodeToString(hash.Sum(nil))
	return r, nil
}

func stageMaintenanceDirectory(target string) (string, string, error) {
	absolute, err := filepath.Abs(target)
	if err != nil {
		return "", "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", "", err
	}
	absolute = filepath.Join(parent, filepath.Base(absolute))
	if _, err := os.Lstat(absolute); err == nil {
		return "", "", fmt.Errorf("maintenance destination must not exist")
	} else if !os.IsNotExist(err) {
		return "", "", err
	}
	stage, err := os.MkdirTemp(parent, ".djob-maintenance-")
	return stage, absolute, err
}

func syncMaintenanceTree(directory string) error {
	return filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return syncMaintenanceDirectory(path)
		}
		return nil
	})
}

func backupPaths(directory string, id uint64) ([]string, error) {
	if info, err := os.Lstat(filepath.Join(directory, "store")); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("backup requires an existing store directory without a symlink")
	}
	paths := []string{"store/jobs.snap", ".djob-cluster.json"}
	for _, name := range []string{".djob-clock-hold", ".djob-recovery-hold"} {
		if _, err := os.Lstat(filepath.Join(directory, name)); err == nil {
			paths = append(paths, name)
		} else if !os.IsNotExist(err) {
			return nil, err
		}
	}
	for _, kind := range []struct{ prefix, suffix string }{{"wal", ".wal"}, {"snap", ".snap"}} {
		relative := fmt.Sprintf("raft/%s-%d", kind.prefix, id)
		for _, component := range []string{"raft", relative} {
			info, err := os.Lstat(filepath.Join(directory, component))
			if err != nil || !info.IsDir() {
				return nil, fmt.Errorf("backup requires existing Raft directories without symlinks")
			}
		}
		files, err := raftDiskFiles(filepath.Join(directory, relative), kind.suffix)
		if err != nil {
			return nil, err
		}
		for _, file := range files {
			paths = append(paths, relative+"/"+file.name)
		}
	}
	return paths, nil
}

func readMemberLog(directory string, id uint64) (*raftpb.Snapshot, raftpb.HardState, []raftpb.Entry, error) {
	waldir := filepath.Join(directory, "raft", fmt.Sprintf("wal-%d", id))
	snapdir := filepath.Join(directory, "raft", fmt.Sprintf("snap-%d", id))
	markers, err := wal.ValidSnapshotEntries(zap.NewNop(), waldir)
	if err != nil {
		return nil, raftpb.HardState{}, nil, err
	}
	files, err := raftDiskFiles(snapdir, ".snap")
	if err != nil {
		return nil, raftpb.HardState{}, nil, err
	}
	checkpoint := &raftpb.Snapshot{}
	for i := len(files) - 1; i >= 0; i-- {
		f := files[i]
		matched := false
		for _, marker := range markers {
			if marker.Index == f.second && marker.Term == f.first {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		candidate, err := snap.Read(zap.NewNop(), filepath.Join(snapdir, f.name))
		if err != nil {
			continue
		}
		if candidate.Metadata.Term != f.first || candidate.Metadata.Index != f.second {
			continue
		}
		checkpoint = candidate
		break
	}
	boundary := walpb.Snapshot{Index: checkpoint.Metadata.Index, Term: checkpoint.Metadata.Term}
	if _, err := wal.Verify(zap.NewNop(), waldir, boundary); err != nil {
		return nil, raftpb.HardState{}, nil, err
	}
	reader, err := wal.OpenForRead(zap.NewNop(), waldir, boundary)
	if err != nil {
		return nil, raftpb.HardState{}, nil, err
	}
	_, state, entries, err := reader.ReadAll()
	if err == nil && state.Commit < checkpoint.Metadata.Index {
		err = fmt.Errorf("checkpoint exceeds durable commit index")
	}
	return checkpoint, state, entries, err
}

// BackupMember publishes a stopped member's DB, WAL, snapshots and identity as
// one checksummed artifact. API credentials and TLS keys are deliberately external.
func BackupMember(ctx context.Context, directory, target string, id uint64) (result MemberBackup, err error) {
	return backupMember(ctx, directory, target, id, false)
}

// TransferMember permanently retires the stopped source before publishing a
// current-state transfer. This preserves its latest Raft votes and log, unlike
// rolling a live voter back to an older disaster backup.
func TransferMember(ctx context.Context, directory, target string, id uint64) (MemberBackup, error) {
	return backupMember(ctx, directory, target, id, true)
}

func backupMember(ctx context.Context, directory, target string, id uint64, transfer bool) (result MemberBackup, err error) {
	return backupMemberWithPublish(ctx, directory, target, id, transfer, publishMaintenanceDirectory)
}

func backupMemberWithPublish(ctx context.Context, directory, target string, id uint64, transfer bool, publish func(string, string) error) (result MemberBackup, err error) {
	if directory == "" || target == "" || id == 0 {
		return result, fmt.Errorf("backup requires source, destination and member ID")
	}
	directory, err = filepath.EvalSymlinks(directory)
	if err != nil {
		return result, err
	}
	lock, err := lockMaintenanceMember(directory)
	if err != nil {
		return result, err
	}
	defer lock.Close()
	paths, err := backupPaths(directory, id)
	if err != nil {
		return result, err
	}
	stage, destination, err := stageMaintenanceDirectory(target)
	if err != nil {
		return result, err
	}
	preserveStage := false
	defer func() {
		if !preserveStage {
			_ = os.RemoveAll(stage)
		}
	}()
	if relative, relErr := filepath.Rel(directory, destination); relErr != nil || relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))) {
		return result, fmt.Errorf("backup destination must be outside the member directory")
	}
	result.Version = 1
	result.Transferred = transfer
	for _, kind := range []string{"wal", "snap"} {
		if err = os.MkdirAll(filepath.Join(stage, "raft", fmt.Sprintf("%s-%d", kind, id)), 0700); err != nil {
			return result, err
		}
	}
	for _, relative := range paths {
		file, err := copyBackupFile(ctx, filepath.Join(directory, relative), filepath.Join(stage, relative), relative)
		if err != nil {
			return MemberBackup{}, err
		}
		result.Files = append(result.Files, file)
	}
	result.Identity, err = readMemberIdentity(stage)
	if err != nil {
		return result, err
	}
	if (result.Identity.Version != 1 && result.Identity.Version != 2) || result.Identity.MemberID != id || result.Identity.ClusterID == 0 || id > uint64(len(result.Identity.Peers)) {
		return result, fmt.Errorf("backup member identity mismatch")
	}
	report, err := db.VerifyStorageFile(ctx, filepath.Join(stage, "store", "jobs.snap"))
	if err != nil {
		return result, err
	}
	checkpoint, state, entries, err := readMemberLog(stage, id)
	if err != nil {
		return result, err
	}
	if report.AppliedIndex > state.Commit {
		return result, fmt.Errorf("database exceeds WAL commit boundary")
	}
	if err := checkBackupLogCoverage(ctx, report.AppliedIndex, checkpoint.Metadata.Index, state.Commit, entries); err != nil {
		return result, err
	}
	result.AppliedIndex, result.CommittedIndex = report.AppliedIndex, state.Commit
	var random [8]byte
	if _, err = rand.Read(random[:]); err != nil {
		return result, err
	}
	result.RecoveryClusterID = binary.BigEndian.Uint64(random[:])
	if result.RecoveryClusterID == 0 || result.RecoveryClusterID == result.Identity.ClusterID {
		result.RecoveryClusterID = result.Identity.ClusterID ^ 0x8000000000000001
		if result.RecoveryClusterID == 0 {
			result.RecoveryClusterID = 1
		}
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return result, err
	}
	if err = os.WriteFile(filepath.Join(stage, backupManifestName), encoded, 0600); err != nil {
		return result, err
	}
	manifest, err := os.OpenFile(filepath.Join(stage, backupManifestName), os.O_RDWR, 0)
	if err != nil {
		return result, err
	}
	err = errors.Join(manifest.Sync(), manifest.Close())
	if err != nil {
		return result, err
	}
	if err = syncMaintenanceTree(stage); err != nil {
		return result, err
	}
	checksum := sha256.Sum256(encoded)
	if err = os.WriteFile(filepath.Join(stage, ".djob-manifest.sha256"), []byte(hex.EncodeToString(checksum[:])), 0600); err != nil {
		return result, err
	}
	seal, err := os.OpenFile(filepath.Join(stage, ".djob-manifest.sha256"), os.O_RDWR, 0)
	if err != nil {
		return result, err
	}
	err = errors.Join(seal.Sync(), seal.Close())
	if err != nil {
		return result, err
	}
	if err = syncMaintenanceDirectory(stage); err != nil {
		return result, err
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	if transfer {
		// Retirement can succeed even if its directory sync reports an error.
		// Keep the complete artifact from this point through any failure.
		preserveStage = true
		if err = writeOperationAcknowledgement(directory, ".djob-transferred", hex.EncodeToString(checksum[:]), time.Now().UTC()); err != nil {
			return result, fmt.Errorf("inspect source retirement marker; transfer preserved at %s: %w", stage, err)
		}
	}
	if err = publish(stage, destination); err != nil {
		if transfer {
			return result, fmt.Errorf("source permanently retired; transfer preserved at %s; publication failed: %w", stage, err)
		}
		return result, err
	}
	err = syncMaintenanceDirectory(filepath.Dir(destination))
	if err != nil && transfer {
		return result, fmt.Errorf("source permanently retired and transfer published; destination parent sync failed: %w", err)
	}
	return result, err
}

func loadBackup(directory string) (MemberBackup, error) {
	var m MemberBackup
	path := filepath.Join(directory, backupManifestName)
	info, err := os.Lstat(path)
	if err != nil {
		return m, err
	}
	if !info.Mode().IsRegular() || info.Size() > 8<<20 {
		return m, fmt.Errorf("invalid backup manifest")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return m, err
	}
	sealPath := filepath.Join(directory, ".djob-manifest.sha256")
	info, err = os.Lstat(sealPath)
	if err != nil {
		return m, err
	}
	if !info.Mode().IsRegular() || info.Size() != 64 {
		return m, fmt.Errorf("invalid backup manifest checksum file")
	}
	seal, err := os.ReadFile(sealPath)
	if err != nil {
		return m, err
	}
	checksum := sha256.Sum256(data)
	if string(seal) != hex.EncodeToString(checksum[:]) {
		return m, fmt.Errorf("backup manifest checksum mismatch")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&m); err != nil {
		return m, err
	}
	var trailing any
	if err = decoder.Decode(&trailing); err != io.EOF {
		return m, fmt.Errorf("trailing backup manifest content")
	}
	if m.Version != 1 || (m.Identity.Version != 1 && m.Identity.Version != 2) || m.Identity.MemberID == 0 || m.Identity.MemberID > uint64(len(m.Identity.Peers)) || m.Identity.ClusterID == 0 || m.RecoveryClusterID == 0 || m.RecoveryClusterID == m.Identity.ClusterID || len(m.Files) == 0 {
		return m, fmt.Errorf("unsupported backup identity or manifest")
	}
	return m, nil
}

// RestoreMember never overwrites a destination. It accepts only an exact current
// stopped-member transfer, retaining any existing holds and voter state.
func RestoreMember(ctx context.Context, backup, target string, options RestoreOptions) (MemberBackup, error) {
	if !options.TransferNeverStarted {
		return MemberBackup{}, fmt.Errorf("confirm that no copy of this current transfer has ever started as a replacement voter")
	}
	return restoreBackup(ctx, backup, target, 0, nil, options.OldAccessFenced)
}

// RecoverCluster seeds a new Raft cluster from committed backup state. All its
// members must use the same artifact and ordered new peer list. The old cluster
// must be fenced externally. Uncommitted WAL entries are excluded from the seed.
func RecoverCluster(ctx context.Context, backup, target string, id uint64, peers []string, oldClusterFenced bool) (MemberBackup, error) {
	if id == 0 || id > uint64(len(peers)) {
		return MemberBackup{}, fmt.Errorf("recovery member ID must select the ordered peers")
	}
	if len(peers) > 100 {
		return MemberBackup{}, fmt.Errorf("recovery supports at most 100 configured peers")
	}
	peerCfg := base.RaftConfig{Peers: peers}
	if strings.HasPrefix(peers[0], "https://") {
		peerCfg.PeerTLS = &base.RaftTLSConfig{}
	}
	if err := validateRaftPeerURLs(peerCfg); err != nil {
		return MemberBackup{}, err
	}
	return restoreBackup(ctx, backup, target, id, peers, oldClusterFenced)
}

func restoreBackup(ctx context.Context, backup, target string, id uint64, peers []string, fenced bool) (m MemberBackup, err error) {
	return restoreBackupWithPublish(ctx, backup, target, id, peers, fenced, publishMaintenanceDirectory)
}

func restoreBackupWithPublish(ctx context.Context, backup, target string, id uint64, peers []string, fenced bool, publish func(string, string) error) (m MemberBackup, err error) {
	if !fenced {
		return m, fmt.Errorf("explicit confirmation that old access is fenced is required")
	}
	if backup == "" || target == "" {
		return m, fmt.Errorf("backup and new destination are required")
	}
	backup, err = filepath.EvalSymlinks(backup)
	if err != nil {
		return m, err
	}
	m, err = loadBackup(backup)
	if err != nil {
		return m, err
	}
	if id == 0 && !m.Transferred {
		return m, fmt.Errorf("an older backup cannot reuse a Raft voter identity; use a current stopped-member transfer or recover a new cluster")
	}
	if id == 0 {
		lock, err := lockNodeDataDir(backup)
		if err != nil {
			return m, err
		}
		defer lock.Close()
		if _, err := os.Lstat(filepath.Join(backup, ".djob-restored")); err == nil {
			return m, fmt.Errorf("transfer artifact already consumed; use current member state or recover a new cluster")
		} else if !os.IsNotExist(err) {
			return m, err
		}
	}
	stage, destination, err := stageMaintenanceDirectory(target)
	if err != nil {
		return m, err
	}
	preserveStage := false
	defer func() {
		if !preserveStage {
			_ = os.RemoveAll(stage)
		}
	}()
	if id == 0 {
		if err := writeOperationAcknowledgement(stage, ".djob-restore-pending", "prepared replacement is not activated", time.Now().UTC()); err != nil {
			return m, err
		}
	}
	if err := validateBackupFiles(backup, m); err != nil {
		return m, err
	}
	for _, kind := range []string{"wal", "snap"} {
		if err = os.MkdirAll(filepath.Join(stage, "raft", fmt.Sprintf("%s-%d", kind, m.Identity.MemberID)), 0700); err != nil {
			return m, err
		}
	}
	for _, file := range m.Files {
		copied, err := copyBackupFile(ctx, filepath.Join(backup, file.Path), filepath.Join(stage, file.Path), file.Path)
		if err != nil {
			return m, err
		}
		if copied.Size != file.Size || copied.SHA256 != file.SHA256 {
			return m, fmt.Errorf("backup checksum mismatch")
		}
	}
	if _, err := inspectBackupCheckpoint(ctx, stage, m); err != nil {
		return m, err
	}
	if id != 0 {
		q, state, err := replayMember(ctx, stage, m.Identity.MemberID)
		if err != nil {
			return m, err
		}
		index := state.Commit
		if index == 0 {
			index = 1
			applied := q.ApplyEntries([]commit.Entry{{Index: 1}})
			if applied.Error != nil {
				q.Close()
				return m, applied.Error
			}
		}
		var data bytes.Buffer
		err = q.ActivateRecoveryFormat()
		if err == nil {
			err = q.SnapshotAt(&data, index)
		}
		err = errors.Join(err, q.Close())
		if err != nil {
			return m, err
		}
		if state.Term == ^uint64(0) {
			return m, fmt.Errorf("Raft term exhausted")
		}
		term := state.Term + 1
		identity := memberIdentity{Version: 1, MemberID: id, ClusterID: m.RecoveryClusterID, Peers: append([]string(nil), peers...)}
		if err = os.RemoveAll(filepath.Join(stage, "raft")); err != nil {
			return m, err
		}
		if err = os.Remove(filepath.Join(stage, ".djob-cluster.json")); err != nil {
			return m, err
		}
		if err = writeIdentity(stage, identity); err != nil {
			return m, err
		}
		snapdir := filepath.Join(stage, "raft", fmt.Sprintf("snap-%d", id))
		if err = os.MkdirAll(snapdir, 0700); err != nil {
			return m, err
		}
		voters := make([]uint64, len(peers))
		for i := range voters {
			voters[i] = uint64(i + 1)
		}
		checkpoint := raftpb.Snapshot{Data: data.Bytes(), Metadata: raftpb.SnapshotMetadata{Index: index, Term: term, ConfState: raftpb.ConfState{Voters: voters}}}
		if err = snap.New(zap.NewNop(), snapdir).SaveSnap(checkpoint); err != nil {
			return m, err
		}
		writer, err := wal.Create(zap.NewNop(), filepath.Join(stage, "raft", fmt.Sprintf("wal-%d", id)), nil)
		if err != nil {
			return m, err
		}
		err = writer.Save(raftpb.HardState{Term: term, Commit: index}, nil)
		if err == nil {
			err = writer.SaveSnapshot(walpb.Snapshot{Index: index, Term: term, ConfState: &checkpoint.Metadata.ConfState})
		}
		err = errors.Join(err, writer.Close())
		if err != nil {
			return m, err
		}
	}
	if id != 0 {
		if err = os.WriteFile(filepath.Join(stage, ".djob-recovery-hold"), []byte("reconcile restored pending work and pause triggers before releasing admission\n"), 0600); err != nil {
			return m, err
		}
		hold, err := os.OpenFile(filepath.Join(stage, ".djob-recovery-hold"), os.O_RDWR, 0)
		if err != nil {
			return m, err
		}
		err = errors.Join(hold.Sync(), hold.Close())
		if err != nil {
			return m, err
		}
	}
	if err = syncMaintenanceTree(stage); err != nil {
		return m, err
	}
	if err = ctx.Err(); err != nil {
		return m, err
	}
	if id == 0 {
		preserveStage = true
		if err = writeOperationAcknowledgement(backup, ".djob-restored", "consumed for "+destination+"; prepared at "+stage, time.Now().UTC()); err != nil {
			return m, fmt.Errorf("inspect artifact consumption marker; inactive replacement preserved at %s: %w", stage, err)
		}
		if err = os.Remove(filepath.Join(stage, ".djob-restore-pending")); err == nil {
			err = syncMaintenanceDirectory(stage)
		}
		if err != nil {
			return m, fmt.Errorf("transfer consumed; inspect replacement activation at %s: %w", stage, err)
		}
	}
	if err = publish(stage, destination); err != nil {
		if id == 0 {
			return m, fmt.Errorf("transfer consumed; complete replacement preserved at %s; publication failed: %w", stage, err)
		}
		return m, err
	}
	err = syncMaintenanceDirectory(filepath.Dir(destination))
	return m, err
}

func replayMember(ctx context.Context, directory string, id uint64) (*db.DB, raftpb.HardState, error) {
	checkpoint, state, entries, err := readMemberLog(directory, id)
	if err != nil {
		return nil, state, err
	}
	q, err := db.NewDB(filepath.Join(directory, "store", "jobs.snap"))
	if err != nil {
		return nil, state, err
	}
	fail := func(err error) (*db.DB, raftpb.HardState, error) { _ = q.Close(); return nil, state, err }
	if q.LastAppliedIndex() > state.Commit {
		return fail(fmt.Errorf("database exceeds committed WAL"))
	}
	if q.LastAppliedIndex() < checkpoint.Metadata.Index {
		if err = q.RestoreBytesAt(checkpoint.Data, checkpoint.Metadata.Index); err != nil {
			return fail(err)
		}
	}
	for _, entry := range entries {
		if err = ctx.Err(); err != nil {
			return fail(err)
		}
		if entry.Index <= q.LastAppliedIndex() || entry.Index > state.Commit {
			continue
		}
		if entry.Index != q.LastAppliedIndex()+1 {
			return fail(fmt.Errorf("committed recovery log has a gap"))
		}
		applied := commit.Entry{Index: entry.Index}
		if entry.Type == raftpb.EntryNormal {
			applied.Data = entry.Data
		}
		if result := q.ApplyEntries([]commit.Entry{applied}); result.Error != nil {
			return fail(result.Error)
		}
	}
	if q.LastAppliedIndex() != state.Commit {
		return fail(fmt.Errorf("recovery does not reach committed index"))
	}
	return q, state, nil
}

func ReleaseRecovery(ctx context.Context, directory string, id uint64, reason string) error {
	if directory == "" || id == 0 || len(strings.TrimSpace(reason)) == 0 || len(reason) > 4096 {
		return fmt.Errorf("recovery release requires a reason of 1..4096 bytes")
	}
	directory, err := filepath.EvalSymlinks(directory)
	if err != nil {
		return err
	}
	lock, err := lockMaintenanceMember(directory)
	if err != nil {
		return err
	}
	defer lock.Close()
	// Check every intermediate directory and file before opening the database
	// read/write to replay committed entries. Never follow store/Raft symlinks.
	paths, err := backupPaths(directory, id)
	if err != nil {
		return err
	}
	for _, relative := range paths {
		info, err := os.Lstat(filepath.Join(directory, relative))
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("recovery release requires regular member files")
		}
	}
	identity, err := readMemberIdentity(directory)
	if err != nil {
		return err
	}
	if identity.MemberID != id {
		return fmt.Errorf("recovery release member ID differs from stored identity")
	}
	info, err := os.Lstat(filepath.Join(directory, ".djob-recovery-hold"))
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("invalid recovery hold")
	}
	q, _, err := replayMember(ctx, directory, id)
	if err != nil {
		return err
	}
	err = q.ValidateRecoveryRelease()
	err = errors.Join(err, q.Close())
	if err != nil {
		return err
	}
	if err = writeOperationAcknowledgement(directory, ".djob-recovery-release.json", reason, time.Now().UTC()); err != nil {
		return err
	}
	if err = os.Remove(filepath.Join(directory, ".djob-recovery-hold")); err != nil {
		return err
	}
	return syncMaintenanceDirectory(directory)
}

func writeOperationAcknowledgement(directory, name, reason string, at time.Time) error {
	data, err := json.Marshal(struct {
		At     time.Time `json:"at"`
		Reason string    `json:"reason"`
	}{at, reason})
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(directory, ".operation-ack-")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	if _, err = temp.Write(data); err == nil {
		err = temp.Sync()
	}
	err = errors.Join(err, temp.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(temp.Name(), filepath.Join(directory, name)); err != nil {
		return err
	}
	return syncMaintenanceDirectory(directory)
}

func ResetClockHold(directory, reason string, reference time.Time, maxSkew time.Duration) error {
	if directory == "" || strings.TrimSpace(reason) == "" || len(reason) > 4096 || reference.IsZero() || maxSkew < time.Millisecond || maxSkew > time.Minute {
		return fmt.Errorf("clock reset requires a reason, trusted UTC reference and positive skew budget at most 1m")
	}
	delta := time.Since(reference)
	if delta > maxSkew || delta < -maxSkew {
		return fmt.Errorf("local wall time is outside the trusted reference budget")
	}
	directory, err := filepath.EvalSymlinks(directory)
	if err != nil {
		return err
	}
	lock, err := lockMaintenanceMember(directory)
	if err != nil {
		return err
	}
	defer lock.Close()
	path := filepath.Join(directory, ".djob-clock-hold")
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("invalid clock hold")
	}
	if err = writeOperationAcknowledgement(directory, ".djob-clock-reset.json", reason, time.Now().UTC()); err != nil {
		return err
	}
	if err = os.Remove(path); err != nil {
		return err
	}
	return syncMaintenanceDirectory(directory)
}
