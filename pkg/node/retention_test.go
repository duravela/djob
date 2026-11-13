//go:build darwin || linux

package node

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

func retentionFixture(t *testing.T) (string, string) {
	t.Helper()
	previous := wal.SegmentSizeBytes
	wal.SegmentSizeBytes = 4096
	t.Cleanup(func() { wal.SegmentSizeBytes = previous })
	directory := t.TempDir()
	waldir, snapdir := filepath.Join(directory, "wal"), filepath.Join(directory, "snap")
	require.NoError(t, os.Mkdir(snapdir, 0700))
	writer, err := wal.Create(zap.NewNop(), waldir, nil)
	require.NoError(t, err)
	for i := uint64(1); i <= 20; i++ {
		require.NoError(t, writer.Save(raftpb.HardState{Term: 1, Commit: i}, []raftpb.Entry{{Index: i, Term: 1, Data: bytes.Repeat([]byte("x"), 8192)}}))
		if i%2 == 0 {
			checkpoint := raftpb.Snapshot{Data: []byte("complete checkpoint"), Metadata: raftpb.SnapshotMetadata{Index: i, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1}}}}
			require.NoError(t, snap.New(zap.NewNop(), snapdir).SaveSnap(checkpoint))
			require.NoError(t, writer.SaveSnapshot(walpb.Snapshot{Index: i, Term: 1, ConfState: &checkpoint.Metadata.ConfState}))
		}
	}
	require.NoError(t, writer.Close())
	return waldir, snapdir
}

func TestRaftRetentionPartialDeletionStillHasRecoverableCheckpoint(t *testing.T) {
	for _, phase := range []string{"wal", "snapshot"} {
		t.Run(phase, func(t *testing.T) {
			waldir, snapdir := retentionFixture(t)
			calls := 0
			_, err := retainRaftFilesWithRemove(context.Background(), waldir, snapdir, 3, func(path string) error {
				if (phase == "wal" && filepath.Ext(path) == ".wal") || (phase == "snapshot" && filepath.Ext(path) == ".snap") {
					calls++
					if calls == 2 {
						return syscall.EIO
					}
				}
				return os.Remove(path)
			})
			require.ErrorIs(t, err, syscall.EIO)
			state, err := wal.Verify(zap.NewNop(), waldir, walpb.Snapshot{Index: 16, Term: 1})
			require.NoError(t, err)
			require.Equal(t, uint64(20), state.Commit)
			_, err = snap.Read(zap.NewNop(), filepath.Join(snapdir, "0000000000000001-0000000000000010.snap"))
			require.NoError(t, err)
			_, err = retainRaftFiles(context.Background(), waldir, snapdir, 3)
			require.NoError(t, err, "maintenance can be retried after an interrupted prefix purge")
			remaining, err := raftDiskFiles(snapdir, ".snap")
			require.NoError(t, err)
			require.Len(t, remaining, 3, "interruption must not orphan obsolete snapshots after their WAL markers are purged")
		})
	}
}

func TestRaftRetentionKeepsVerifiedRecoveryTailAndFallbackCheckpoints(t *testing.T) {
	waldir, snapdir := retentionFixture(t)
	before, err := raftDiskFiles(waldir, ".wal")
	require.NoError(t, err)
	r, err := retainRaftFiles(context.Background(), waldir, snapdir, 3)
	require.NoError(t, err)
	require.Equal(t, uint64(16), r.AnchorIndex)
	require.Equal(t, uint64(7), r.SnapshotsRemoved)
	require.Positive(t, r.SegmentsRemoved)
	after, err := raftDiskFiles(waldir, ".wal")
	require.NoError(t, err)
	require.Less(t, len(after), len(before))
	checkpoints, err := raftDiskFiles(snapdir, ".snap")
	require.NoError(t, err)
	require.Len(t, checkpoints, 3)
	for _, checkpoint := range checkpoints {
		state, err := wal.Verify(zap.NewNop(), waldir, walpb.Snapshot{Index: checkpoint.second, Term: checkpoint.first})
		require.NoError(t, err)
		require.Equal(t, uint64(20), state.Commit)
	}
	second, err := retainRaftFiles(context.Background(), waldir, snapdir, 3)
	require.NoError(t, err)
	require.Zero(t, second.SegmentsRemoved)
	require.Zero(t, second.SnapshotsRemoved)
}

func TestRaftRetentionCorruptCheckpointOrCancellationDeletesNothing(t *testing.T) {
	for _, kind := range []string{"corrupt", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			waldir, snapdir := retentionFixture(t)
			before, err := raftDiskFiles(waldir, ".wal")
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if kind == "corrupt" {
				require.NoError(t, os.WriteFile(filepath.Join(snapdir, "0000000000000001-0000000000000014.snap"), []byte("bad"), 0600))
			} else {
				cancel()
			}
			_, err = retainRaftFiles(ctx, waldir, snapdir, 3)
			require.Error(t, err)
			after, err := raftDiskFiles(waldir, ".wal")
			require.NoError(t, err)
			require.Equal(t, before, after)
			files, err := raftDiskFiles(snapdir, ".snap")
			require.NoError(t, err)
			require.Len(t, files, 10)
		})
	}
}
