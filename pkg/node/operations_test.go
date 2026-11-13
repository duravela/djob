//go:build darwin || linux

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node/config"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

func TestBackupRecoveryPreservesCommittedStateAndRequiresReconciliation(t *testing.T) {
	directory := t.TempDir()
	n := startDurableNode(t, directory, 2)
	seedRaftEventTrigger(t, n)
	backup := filepath.Join(t.TempDir(), "backup")
	_, err := BackupMember(context.Background(), directory, backup, 1)
	require.ErrorContains(t, err, "lock node data directory")
	n.close()
	before, err := os.ReadFile(filepath.Join(directory, "store", "jobs.snap"))
	require.NoError(t, err)
	manifest, err := BackupMember(context.Background(), directory, backup, 1)
	require.NoError(t, err)
	after, err := os.ReadFile(filepath.Join(directory, "store", "jobs.snap"))
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(before), sha256.Sum256(after))
	require.NotZero(t, manifest.CommittedIndex)
	require.NotEqual(t, manifest.Identity.ClusterID, manifest.RecoveryClusterID)
	_, err = RestoreMember(context.Background(), backup, filepath.Join(t.TempDir(), "unsafe"), RestoreOptions{OldAccessFenced: true, TransferNeverStarted: true})
	require.ErrorContains(t, err, "cannot reuse")
	recovered := filepath.Join(t.TempDir(), "recovered")
	_, err = RecoverCluster(context.Background(), backup, recovered, 1, []string{"http://127.0.0.1:0"}, false)
	require.Error(t, err)
	_, err = RecoverCluster(context.Background(), backup, recovered, 1, []string{"http://127.0.0.1:0"}, true)
	require.NoError(t, err)
	n = startDurableNode(t, recovered, 2)
	trigger, err := n.store.GetJobTrigger("tenant", "ingest")
	require.NoError(t, err)
	require.Equal(t, manifest.RecoveryClusterID, n.raft.raftCfg.ClusterID)
	_, err = n.store.ProposeStateCommand(context.Background(), db.StateCommand{RequestID: "held-launch", At: time.Now().UTC(), Job: &db.JobCommand{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: "hash", DefinitionRevision: 1, RunID: "held"}}})
	require.ErrorIs(t, err, db.ErrAdmissionHeld)
	n.close()
	require.Error(t, ReleaseRecovery(context.Background(), recovered, 1, "reviewed"), "active triggers must first be paused")
	n = startDurableNode(t, recovered, 2)
	genericProposal(t, n, "pause-restored-trigger", db.JobCommand{PauseTrigger: &db.PauseJobTrigger{TenantID: "tenant", ID: "ingest", ExpectedRevision: trigger.Revision, Paused: true}})
	n.close()
	require.NoError(t, ReleaseRecovery(context.Background(), recovered, 1, "pending work reconciled; old cluster fenced"))
	n = startDurableNode(t, recovered, 2)
	genericProposal(t, n, "new-launch", db.JobCommand{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: "hash", DefinitionRevision: 1, RunID: "new"}})
}

func TestDisasterRecoveryRebuildsThreeMembersFromCommittedWALWithoutOriginalCluster(t *testing.T) {
	directory := t.TempDir()
	n := startDurableNode(t, directory, 1000)
	seedRaftEventTrigger(t, n)
	n.close()
	checkpoint, state, _, err := readMemberLog(directory, 1)
	require.NoError(t, err)
	writer, err := wal.Open(zap.NewNop(), filepath.Join(directory, "raft", "wal-1"), walpb.Snapshot{Index: checkpoint.Metadata.Index, Term: checkpoint.Metadata.Term})
	require.NoError(t, err)
	_, _, _, err = writer.ReadAll()
	require.NoError(t, err)
	encode := func(c db.StateCommand) []byte {
		var buffer bytes.Buffer
		require.NoError(t, gob.NewEncoder(&buffer).Encode(db.Command{Version: c.CommandVersion(), Type: db.CmdState, State: &c}))
		return buffer.Bytes()
	}
	// Simulate a crash after the committed log reached disk but before the
	// materialized DB applied it. The subsequent uncommitted tail must be lost.
	state.Commit++
	committed := db.StateCommand{RequestID: "committed-before-crash", At: time.Now().UTC(), Job: &db.JobCommand{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: "hash", DefinitionRevision: 1, RunID: "restored-ready"}}}
	uncommitted := db.StateCommand{RequestID: "never-committed", At: time.Now().UTC(), Job: &db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "uncommitted"}}}}
	require.NoError(t, writer.Save(state, []raftpb.Entry{
		{Index: state.Commit, Term: state.Term, Data: encode(committed)},
		{Index: state.Commit + 1, Term: state.Term, Data: encode(uncommitted)},
	}))
	require.NoError(t, writer.Close())
	artifact := filepath.Join(t.TempDir(), "backup")
	m, err := BackupMember(context.Background(), directory, artifact, 1)
	require.NoError(t, err)
	require.Less(t, m.AppliedIndex, m.CommittedIndex)
	verification, err := VerifyBackup(context.Background(), artifact)
	require.NoError(t, err)
	require.True(t, verification.ReplayRequired)
	require.Equal(t, m.CommittedIndex, verification.CommittedIndex)
	// The source no longer exists. Recovery has only the verified artifact.
	require.NoError(t, os.RemoveAll(directory))
	peers := make([]string, 3)
	for i := range peers {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		peers[i] = "http://" + listener.Addr().String()
		require.NoError(t, listener.Close())
	}
	nodes := make([]*durableNode, 3)
	configs := make([]config.Config, 3)
	for i := range nodes {
		destination := filepath.Join(t.TempDir(), "recovered")
		_, err := RecoverCluster(context.Background(), artifact, destination, uint64(i+1), peers, true)
		require.NoError(t, err)
		report, err := db.ReportStorageFile(context.Background(), filepath.Join(destination, "store", "jobs.snap"))
		require.NoError(t, err)
		require.Equal(t, byte(12), report.Format, "older runtimes must refuse recovered state")
		cfg := crashTestConfig(destination)
		cfg.ID, cfg.Peers = uint64(i+1), peers
		cfg.ProposeC, cfg.ConfChangeC = make(chan []byte), make(chan raftpb.ConfChange)
		configs[i] = cfg
		nodes[i] = startRaftApplication(t, cfg)
	}
	for _, n := range nodes {
		requireReadBarrier(t, n, m.CommittedIndex)
		require.Equal(t, m.RecoveryClusterID, n.raft.raftCfg.ClusterID)
		view, err := n.store.GetJobView("tenant", "restored-ready")
		require.NoError(t, err)
		require.Equal(t, db.JobReady, view.Run.Status)
		_, err = n.store.GetJobRecord(db.JobRecordQuery{Kind: db.JobTenantRecord, TenantID: "uncommitted"})
		require.Error(t, err)
		_, err = n.store.ProposeStateCommand(context.Background(), uncommitted)
		require.ErrorIs(t, err, db.ErrAdmissionHeld)
	}
	trigger, err := nodes[0].store.GetJobTrigger("tenant", "ingest")
	require.NoError(t, err)
	genericProposal(t, nodes[0], "pause-after-disaster", db.JobCommand{PauseTrigger: &db.PauseJobTrigger{TenantID: "tenant", ID: "ingest", ExpectedRevision: trigger.Revision, Paused: true}})
	genericProposal(t, nodes[0], "cancel-after-disaster", db.JobCommand{Cancel: &db.CancelJob{TenantID: "tenant", RunID: "restored-ready", Reason: "reconcile external effects before resubmitting"}})
	minimum := nodes[0].store.LastAppliedIndex()
	for _, n := range nodes {
		requireReadBarrier(t, n, minimum)
	}
	for _, n := range nodes {
		n.close()
	}
	for i, cfg := range configs {
		require.NoError(t, ReleaseRecovery(context.Background(), cfg.DataDir, uint64(i+1), "old access fenced, pending work reconciled, producers remain paused"))
		cfg.ProposeC, cfg.ConfChangeC = make(chan []byte), make(chan raftpb.ConfChange)
		nodes[i] = startRaftApplication(t, cfg)
	}
	for _, n := range nodes {
		requireReadBarrier(t, n, minimum)
	}
	genericProposal(t, nodes[1], "after-disaster", db.JobCommand{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: "hash", DefinitionRevision: 1, RunID: "fresh"}})
}

func TestMemberTransferAndRollingRestartsKeepLiveClusterAvailable(t *testing.T) {
	nodes, configs := startReadBarrierCluster(t, true)
	seedRaftEventTrigger(t, nodes[0])
	command := db.StateCommand{RequestID: "rolling-retry", At: time.Now().UTC(), Job: &db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "rolling"}}}}
	expected := proposeChannel(t, nodes[0], command)
	follower := -1
	for i, n := range nodes {
		if !n.raft.IsLeader() {
			follower = i
			break
		}
	}
	require.NotEqual(t, -1, follower)
	nodes[follower].close()
	artifact := filepath.Join(t.TempDir(), "transfer")
	_, err := TransferMember(context.Background(), configs[follower].DataDir, artifact, uint64(follower+1))
	require.NoError(t, err)
	alive := (follower + 1) % len(nodes)
	genericProposal(t, nodes[alive], "while-transferring", db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "catch-up"}}})
	minimum := nodes[alive].store.LastAppliedIndex()
	replacement := filepath.Join(t.TempDir(), "replacement")
	_, err = RestoreMember(context.Background(), artifact, replacement, RestoreOptions{OldAccessFenced: true, TransferNeverStarted: true})
	require.NoError(t, err)
	configs[follower].DataDir = replacement
	for i := range nodes {
		if i != follower {
			nodes[i].close()
		}
		cfg := configs[i]
		cfg.ProposeC, cfg.ConfChangeC = make(chan []byte), make(chan raftpb.ConfChange)
		nodes[i] = startRaftApplication(t, cfg)
		requireReadBarrier(t, nodes[i], minimum)
		_, err = nodes[i].store.GetJobRecord(db.JobRecordQuery{Kind: db.JobTenantRecord, TenantID: "catch-up"})
		require.NoError(t, err)
		require.Equal(t, expected, proposeChannel(t, nodes[i], command))
	}
}

func TestCurrentMemberTransferRetiresSourceAndPreservesLatestVoterState(t *testing.T) {
	directory := t.TempDir()
	n := startDurableNode(t, directory, 2)
	seedRaftEventTrigger(t, n)
	n.close()
	artifact := filepath.Join(t.TempDir(), "transfer")
	m, err := TransferMember(context.Background(), directory, artifact, 1)
	require.NoError(t, err)
	require.True(t, m.Transferred)
	cfg := config.Config{}
	cfg.SetDefaults()
	cfg.DataDir = directory
	cfg.Peers = []string{"http://127.0.0.1:0"}
	cfg.Logger = zap.NewNop()
	_, err = OpenApplication(context.Background(), cfg)
	require.ErrorContains(t, err, "transferred")
	target := filepath.Join(t.TempDir(), "replacement")
	_, err = RestoreMember(context.Background(), artifact, target, RestoreOptions{TransferNeverStarted: true})
	require.Error(t, err)
	_, err = RestoreMember(context.Background(), artifact, target, RestoreOptions{OldAccessFenced: true})
	require.ErrorContains(t, err, "no copy")
	_, err = RestoreMember(context.Background(), artifact, target, RestoreOptions{OldAccessFenced: true, TransferNeverStarted: true})
	require.NoError(t, err)
	_, err = RestoreMember(context.Background(), artifact, filepath.Join(t.TempDir(), "reused"), RestoreOptions{OldAccessFenced: true, TransferNeverStarted: true})
	require.ErrorContains(t, err, "already consumed")
	_, err = TransferMember(context.Background(), artifact, filepath.Join(t.TempDir(), "converted"), 1)
	require.ErrorContains(t, err, "member maintenance refuses")
	_, err = TransferMember(context.Background(), directory, filepath.Join(t.TempDir(), "recaptured"), 1)
	require.ErrorContains(t, err, "member maintenance refuses")
	_, before, _, err := readMemberLog(directory, 1)
	require.NoError(t, err)
	_, after, _, err := readMemberLog(target, 1)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, os.WriteFile(filepath.Join(target, ".djob-restore-pending"), []byte("incomplete activation"), 0600))
	_, err = TransferMember(context.Background(), target, filepath.Join(t.TempDir(), "inactive-transfer"), 1)
	require.ErrorContains(t, err, "member maintenance refuses")
	cfg.DataDir = target
	_, err = OpenApplication(context.Background(), cfg)
	require.ErrorContains(t, err, "has not been activated")
	require.NoError(t, os.Remove(filepath.Join(target, ".djob-restore-pending")))
	n = startDurableNode(t, target, 2)
	_, err = n.store.GetJobTrigger("tenant", "ingest")
	require.NoError(t, err)
}

func TestRestorePublicationFailureConsumesArtifactBeforeActivatingReplacement(t *testing.T) {
	directory := t.TempDir()
	n := startDurableNode(t, directory, 1000)
	seedRaftEventTrigger(t, n)
	n.close()
	artifact := filepath.Join(t.TempDir(), "transfer")
	_, err := TransferMember(context.Background(), directory, artifact, 1)
	require.NoError(t, err)
	var prepared string
	_, err = restoreBackupWithPublish(context.Background(), artifact, filepath.Join(t.TempDir(), "replacement"), 0, nil, true, func(stage, _ string) error {
		prepared = stage
		_, err := os.Stat(filepath.Join(artifact, ".djob-restored"))
		require.NoError(t, err, "artifact consumption must be durable before a replacement can start")
		_, err = os.Stat(filepath.Join(stage, ".djob-restore-pending"))
		require.True(t, os.IsNotExist(err))
		return syscall.EIO
	})
	require.ErrorIs(t, err, syscall.EIO)
	require.ErrorContains(t, err, prepared)
	n = startDurableNode(t, prepared, 1000)
	genericProposal(t, n, "replacement-writes", db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "newer-voter-state"}}})
	n.close()
	_, err = RestoreMember(context.Background(), artifact, filepath.Join(t.TempDir(), "rollback"), RestoreOptions{OldAccessFenced: true, TransferNeverStarted: true})
	require.ErrorContains(t, err, "already consumed", "even fencing the replacement cannot justify forgetting its later votes")
}

func TestTransferPublicationFailurePreservesArtifactAfterRetiringSource(t *testing.T) {
	directory := t.TempDir()
	n := startDurableNode(t, directory, 1000)
	seedRaftEventTrigger(t, n)
	n.close()
	for _, transfer := range []bool{false, true} {
		var staged string
		destination := filepath.Join(t.TempDir(), "published")
		_, err := backupMemberWithPublish(context.Background(), directory, destination, 1, transfer, func(source, target string) error {
			staged = source
			_, err := loadBackup(source)
			require.NoError(t, err, "publication receives a complete verified artifact")
			parent, err := filepath.EvalSymlinks(filepath.Dir(destination))
			require.NoError(t, err)
			require.Equal(t, filepath.Join(parent, filepath.Base(destination)), target)
			return syscall.EIO
		})
		require.ErrorIs(t, err, syscall.EIO)
		_, statErr := os.Stat(destination)
		require.True(t, os.IsNotExist(statErr))
		if !transfer {
			_, err = os.Stat(staged)
			require.True(t, os.IsNotExist(err), "ordinary failed backup removes its unpublished copy")
			continue
		}
		require.ErrorContains(t, err, "source permanently retired")
		require.ErrorContains(t, err, staged)
		_, err = os.Stat(filepath.Join(directory, ".djob-transferred"))
		require.NoError(t, err)
		_, err = loadBackup(staged)
		require.NoError(t, err, "retirement never discards the only prepared transfer artifact")
		_, err = RestoreMember(context.Background(), staged, filepath.Join(t.TempDir(), "replacement"), RestoreOptions{OldAccessFenced: true, TransferNeverStarted: true})
		require.NoError(t, err)
	}
}

func TestRecoveryRejectsCorruptionCancellationAndExistingDestination(t *testing.T) {
	directory := t.TempDir()
	n := startDurableNode(t, directory, 1000)
	seedRaftEventTrigger(t, n)
	n.close()
	artifact := filepath.Join(t.TempDir(), "backup")
	_, err := BackupMember(context.Background(), directory, artifact, 1)
	require.NoError(t, err)
	existing := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(existing, "keep"), []byte("untouched"), 0600))
	_, err = RecoverCluster(context.Background(), artifact, existing, 1, []string{"http://127.0.0.1:0"}, true)
	require.Error(t, err)
	keep, err := os.ReadFile(filepath.Join(existing, "keep"))
	require.NoError(t, err)
	require.Equal(t, "untouched", string(keep))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	target := filepath.Join(t.TempDir(), "cancelled")
	_, err = RecoverCluster(ctx, artifact, target, 1, []string{"http://127.0.0.1:0"}, true)
	require.ErrorIs(t, err, context.Canceled)
	_, err = os.Stat(target)
	require.True(t, os.IsNotExist(err))
	f, err := os.OpenFile(filepath.Join(artifact, "store", "jobs.snap"), os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("changed"), 4096)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	_, err = RecoverCluster(context.Background(), artifact, filepath.Join(t.TempDir(), "bad"), 1, []string{"http://127.0.0.1:0"}, true)
	require.ErrorContains(t, err, "checksum")
}
