//go:build darwin || linux

package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.uber.org/zap"
)

type maintenanceFileState struct {
	info   os.FileInfo
	digest string
	link   string
}

func maintenanceTree(t *testing.T, directory string) map[string]maintenanceFileState {
	t.Helper()
	files := make(map[string]maintenanceFileState)
	require.NoError(t, filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		state := maintenanceFileState{info: info}
		if info.Mode().IsRegular() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			hash := sha256.New()
			_, err = io.Copy(hash, file)
			require.NoError(t, file.Close())
			if err != nil {
				return err
			}
			state.digest = hex.EncodeToString(hash.Sum(nil))
		} else if info.Mode()&os.ModeSymlink != 0 {
			state.link, err = os.Readlink(path)
			if err != nil {
				return err
			}
		}
		relative, err := filepath.Rel(directory, path)
		if err != nil {
			return err
		}
		files[relative] = state
		return nil
	}))
	return files
}

func assertMaintenanceTree(t *testing.T, directory string, before map[string]maintenanceFileState) {
	t.Helper()
	after := maintenanceTree(t, directory)
	require.Len(t, after, len(before), "maintenance must not create or remove files")
	for path, old := range before {
		current, ok := after[path]
		require.True(t, ok, path)
		require.True(t, os.SameFile(old.info, current.info), path)
		require.Equal(t, old.info.ModTime(), current.info.ModTime(), path)
		require.Equal(t, old.info.Mode(), current.info.Mode(), path)
		require.Equal(t, old.info.Size(), current.info.Size(), path)
		require.Equal(t, old.digest, current.digest, path)
		require.Equal(t, old.link, current.link, path)
	}
}

func verificationArtifact(t *testing.T) (string, MemberBackup) {
	t.Helper()
	directory := t.TempDir()
	n := startDurableNode(t, directory, 2)
	seedRaftEventTrigger(t, n)
	n.close()
	for _, marker := range []string{".djob-clock-hold", ".djob-recovery-hold"} {
		require.NoError(t, os.WriteFile(filepath.Join(directory, marker), []byte("private hold details"), 0600))
	}
	artifact := filepath.Join(t.TempDir(), "backup")
	m, err := BackupMember(context.Background(), directory, artifact, 1)
	require.NoError(t, err)
	return artifact, m
}

func TestVerifyBackupReadsReadOnlyArtifactWithoutChangingIt(t *testing.T) {
	artifact, m := verificationArtifact(t)
	t.Cleanup(func() {
		_ = filepath.WalkDir(artifact, func(path string, entry os.DirEntry, _ error) error {
			if entry != nil && entry.IsDir() {
				return os.Chmod(path, 0700)
			}
			return nil
		})
	})
	require.NoError(t, filepath.WalkDir(artifact, func(path string, entry os.DirEntry, _ error) error {
		if entry.IsDir() {
			return os.Chmod(path, 0500)
		}
		return os.Chmod(path, 0400)
	}))
	before := maintenanceTree(t, artifact)
	alias := filepath.Join(t.TempDir(), "backup-alias")
	require.NoError(t, os.Symlink(artifact, alias))
	report, err := VerifyBackup(context.Background(), alias)
	require.NoError(t, err)
	require.Equal(t, m.Version, report.ArtifactVersion)
	require.Equal(t, m.Identity.MemberID, report.MemberID)
	require.Equal(t, m.Identity.ClusterID, report.ClusterID)
	require.Equal(t, m.RecoveryClusterID, report.RecoveryClusterID)
	require.Equal(t, m.AppliedIndex, report.AppliedIndex)
	require.Equal(t, m.CommittedIndex, report.CommittedIndex)
	require.Equal(t, uint64(len(m.Files)), report.FilesVerified)
	var total uint64
	for _, file := range m.Files {
		total += uint64(file.Size)
	}
	require.Equal(t, total, report.BytesVerified)
	require.True(t, report.ClockHold)
	require.True(t, report.RecoveryHold)
	require.False(t, report.Transferred)
	require.False(t, report.TransferConsumed)
	require.False(t, report.ReplayRequired)
	encoded, err := json.Marshal(report)
	require.NoError(t, err)
	for _, private := range []string{"private hold details", "tenant", "ingest", m.Identity.Peers[0]} {
		require.NotContains(t, string(encoded), private)
	}
	second, err := VerifyBackup(context.Background(), artifact)
	require.NoError(t, err)
	require.Equal(t, report, second)
	assertMaintenanceTree(t, artifact, before)
}

func sealVerificationManifest(t *testing.T, artifact string, data []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(artifact, backupManifestName), data, 0600))
	checksum := sha256.Sum256(data)
	require.NoError(t, os.WriteFile(filepath.Join(artifact, ".djob-manifest.sha256"), []byte(hex.EncodeToString(checksum[:])), 0600))
}

func TestVerifyBackupRejectsDamagedOrInconsistentArtifacts(t *testing.T) {
	artifact, m := verificationArtifact(t)
	original, err := os.ReadFile(filepath.Join(artifact, backupManifestName))
	require.NoError(t, err)
	var walFile, snapFile string
	for _, file := range m.Files {
		switch filepath.Ext(file.Path) {
		case ".wal":
			walFile = file.Path
		case ".snap":
			if file.Path != "store/jobs.snap" {
				snapFile = file.Path
			}
		}
	}
	require.NotEmpty(t, walFile)
	require.NotEmpty(t, snapFile)
	for _, kind := range []string{"seal", "unknown_field", "duplicate", "traversal", "invalid_hash", "negative_length", "omitted", "extra_hold", "missing", "db_checksum", "wal_checksum", "snapshot_checksum", "identity", "peers", "applied", "committed", "store_link", "raft_link", "wal_directory_link", "snap_directory_link", "db_link", "identity_link", "wal_link"} {
		t.Run(kind, func(t *testing.T) {
			t.Cleanup(func() { sealVerificationManifest(t, artifact, original) })
			var changed MemberBackup
			require.NoError(t, json.Unmarshal(original, &changed))
			move := func(relative string, symlink bool) {
				path := filepath.Join(artifact, relative)
				saved := filepath.Join(t.TempDir(), "saved")
				require.NoError(t, os.Rename(path, saved))
				t.Cleanup(func() {
					if symlink {
						require.NoError(t, os.Remove(path))
					}
					require.NoError(t, os.Rename(saved, path))
				})
				if symlink {
					require.NoError(t, os.Symlink(saved, path))
				}
			}
			switch kind {
			case "seal":
				require.NoError(t, os.WriteFile(filepath.Join(artifact, ".djob-manifest.sha256"), make([]byte, 64), 0600))
			case "unknown_field":
				sealVerificationManifest(t, artifact, append([]byte(`{"unexpected":true,`), original[1:]...))
			case "duplicate":
				changed.Files = append(changed.Files, changed.Files[0])
			case "traversal":
				changed.Files[0].Path = "../store/jobs.snap"
			case "invalid_hash":
				changed.Files[0].SHA256 = "invalid"
			case "negative_length":
				changed.Files[0].Size = -1
			case "omitted":
				changed.Files = changed.Files[1:]
			case "extra_hold":
				// Remove an expected hold from the manifest without changing its file.
				for i, file := range changed.Files {
					if file.Path == ".djob-clock-hold" {
						changed.Files = append(changed.Files[:i], changed.Files[i+1:]...)
						break
					}
				}
			case "missing":
				move(".djob-cluster.json", false)
			case "db_checksum", "wal_checksum", "snapshot_checksum":
				relative := map[string]string{"db_checksum": "store/jobs.snap", "wal_checksum": walFile, "snapshot_checksum": snapFile}[kind]
				file, err := os.OpenFile(filepath.Join(artifact, relative), os.O_RDWR, 0600)
				require.NoError(t, err)
				var value [1]byte
				_, err = file.ReadAt(value[:], 0)
				require.NoError(t, err)
				old := value
				value[0] ^= 1
				_, err = file.WriteAt(value[:], 0)
				require.NoError(t, err)
				t.Cleanup(func() {
					_, err := file.WriteAt(old[:], 0)
					require.NoError(t, err)
					require.NoError(t, file.Close())
				})
			case "identity":
				changed.Identity.ClusterID++
			case "peers":
				changed.Identity.Peers[0] = "http://127.0.0.1:9999"
			case "applied":
				changed.AppliedIndex++
			case "committed":
				changed.CommittedIndex++
			default:
				relative := map[string]string{"store_link": "store", "raft_link": "raft", "wal_directory_link": "raft/wal-1", "snap_directory_link": "raft/snap-1", "db_link": "store/jobs.snap", "identity_link": ".djob-cluster.json", "wal_link": walFile}[kind]
				move(relative, true)
			}
			if kind != "seal" && kind != "unknown_field" {
				data, err := json.Marshal(changed)
				require.NoError(t, err)
				sealVerificationManifest(t, artifact, data)
			}
			before := maintenanceTree(t, artifact)
			report, err := VerifyBackup(context.Background(), artifact)
			require.Error(t, err)
			require.Equal(t, BackupVerification{}, report)
			assertMaintenanceTree(t, artifact, before)
		})
	}
	_, err = VerifyBackup(context.Background(), artifact)
	require.NoError(t, err, "failed verification never consumes or repairs the artifact")
}

func TestVerifyBackupCancellationReturnsNoPartialReport(t *testing.T) {
	artifact, _ := verificationArtifact(t)
	before := maintenanceTree(t, artifact)
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	checks := 0
	ctx := storageReportContext{Context: base, check: func() {
		checks++
		if checks == 15 {
			cancel()
		}
	}}
	report, err := VerifyBackup(ctx, artifact)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, BackupVerification{}, report)
	require.Equal(t, 15, checks, "cancel during streamed file hashing")
	assertMaintenanceTree(t, artifact, before)
	_, err = VerifyBackup(context.Background(), artifact)
	require.NoError(t, err)
	report, err = VerifyBackup(ctx, artifact)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, BackupVerification{}, report)
}

func TestMaintenanceRefusesArtifactsAndRetiredOrInactiveMembersWithoutWrites(t *testing.T) {
	for _, marker := range []string{backupManifestName, ".djob-manifest.sha256", ".djob-restored", ".djob-transferred", ".djob-restore-pending"} {
		for _, symlink := range []bool{false, true} {
			kind := "regular"
			if symlink {
				kind = "dangling_symlink"
			}
			t.Run(marker+"/"+kind, func(t *testing.T) {
				directory := newStorageReportDirectory(t)
				for _, path := range []string{"raft/wal-1", "raft/snap-1"} {
					require.NoError(t, os.MkdirAll(filepath.Join(directory, path), 0700))
				}
				for _, hold := range []string{".djob-clock-hold", ".djob-recovery-hold"} {
					require.NoError(t, os.WriteFile(filepath.Join(directory, hold), []byte("keep held"), 0600))
				}
				if symlink {
					require.NoError(t, os.Symlink("missing", filepath.Join(directory, marker)))
				} else {
					require.NoError(t, os.WriteFile(filepath.Join(directory, marker), []byte("reserved"), 0600))
				}
				before := maintenanceTree(t, directory)
				operations := map[string]func() error{
					"compact": func() error {
						_, err := CompactStore(context.Background(), directory, db.FileCompactionOptions{})
						return err
					},
					"report": func() error { _, err := ReportStoreStorage(context.Background(), directory); return err },
					"retain": func() error { _, err := RetainRaftStorage(context.Background(), directory, 1, 1); return err },
					"backup": func() error {
						_, err := BackupMember(context.Background(), directory, filepath.Join(t.TempDir(), "backup"), 1)
						return err
					},
					"transfer": func() error {
						_, err := TransferMember(context.Background(), directory, filepath.Join(t.TempDir(), "transfer"), 1)
						return err
					},
					"clock_reset":      func() error { return ResetClockHold(directory, "checked", time.Now().UTC(), time.Second) },
					"recovery_release": func() error { return ReleaseRecovery(context.Background(), directory, 1, "checked") },
				}
				for name, operation := range operations {
					t.Run(name, func(t *testing.T) { require.ErrorContains(t, operation(), "member maintenance refuses") })
				}
				assertMaintenanceTree(t, directory, before)
				var raft RaftNode
				require.Error(t, raft.validateMemberIdentity(directory), "damaged artifacts and reserved markers must also block startup")
			})
		}
	}
}

func TestVerifyAndRecoverRejectChecksummedWALWithoutItsCommittedTail(t *testing.T) {
	artifact, m := verificationArtifact(t)
	checkpoint, state, _, err := readMemberLog(artifact, 1)
	require.NoError(t, err)
	writer, err := wal.Open(zap.NewNop(), filepath.Join(artifact, "raft", "wal-1"), walpb.Snapshot{Index: checkpoint.Metadata.Index, Term: checkpoint.Metadata.Term})
	require.NoError(t, err)
	_, _, _, err = writer.ReadAll()
	require.NoError(t, err)
	// All native CRCs and artifact checksums are valid, but the committed tail
	// is absent. Checking only hashes and the manifest boundary would miss it.
	state.Commit += 2
	require.NoError(t, writer.Save(state, nil))
	require.NoError(t, writer.Close())
	m.CommittedIndex = state.Commit
	for i, file := range m.Files {
		m.Files[i], err = hashBackupFile(context.Background(), filepath.Join(artifact, file.Path), file.Path)
		require.NoError(t, err)
	}
	encoded, err := json.Marshal(m)
	require.NoError(t, err)
	sealVerificationManifest(t, artifact, encoded)
	before := maintenanceTree(t, artifact)
	report, err := VerifyBackup(context.Background(), artifact)
	require.ErrorContains(t, err, "omits committed entries")
	require.Equal(t, BackupVerification{}, report)
	destination := filepath.Join(t.TempDir(), "recovered")
	_, err = RecoverCluster(context.Background(), artifact, destination, 1, []string{"http://127.0.0.1:0"}, true)
	require.ErrorContains(t, err, "omits committed entries")
	_, err = os.Lstat(destination)
	require.True(t, os.IsNotExist(err))
	assertMaintenanceTree(t, artifact, before)
}
