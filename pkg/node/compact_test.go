//go:build darwin || linux

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node/config"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

func bloatStoppedNodeStore(t *testing.T, directory string) {
	t.Helper()
	q, err := bbolt.Open(filepath.Join(directory, "store", "jobs.snap"), 0600, nil)
	require.NoError(t, err)
	require.NoError(t, q.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucket([]byte("temporary-space"))
		if err != nil {
			return err
		}
		for i := range 32 {
			if err := b.Put([]byte(fmt.Sprintf("%03d", i)), bytes.Repeat([]byte("discarded"), 64<<10)); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, q.Update(func(tx *bbolt.Tx) error { return tx.DeleteBucket([]byte("temporary-space")) }))
	require.NoError(t, q.Close())
}

func raftFilesBeforeCompaction(t *testing.T, directory string) map[string][32]byte {
	t.Helper()
	hashes := make(map[string][32]byte)
	require.NoError(t, filepath.WalkDir(filepath.Join(directory, defaultRaftDir), func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		hash := sha256.New()
		if _, err := io.Copy(hash, f); err != nil {
			return err
		}
		var digest [32]byte
		copy(digest[:], hash.Sum(nil))
		hashes[path] = digest
		return nil
	}))
	return hashes
}

func TestOfflineCompactionPreservesRaftRecoveryAndPendingReceiptCleanup(t *testing.T) {
	directory := t.TempDir()
	n := startDurableNode(t, directory, 2)
	original, progress := seedPartialRaftReceiptCompaction(t, n)
	_, err := CompactStore(context.Background(), directory, db.FileCompactionOptions{})
	require.ErrorContains(t, err, "lock node data directory")
	n.close()
	bloatStoppedNodeStore(t, directory)
	files := raftFilesBeforeCompaction(t, directory)
	result, err := CompactStore(context.Background(), directory, db.FileCompactionOptions{})
	require.NoError(t, err)
	require.True(t, result.Installed)
	require.Greater(t, result.SavedBytes, int64(1<<20))
	require.Equal(t, files, raftFilesBeforeCompaction(t, directory), "maintenance must not rewrite WAL or Raft snapshots")
	n = startDurableNode(t, directory, 2)
	resumed, err := n.store.GetJobReceiptCompaction("tenant", progress.RunID)
	require.NoError(t, err)
	require.Equal(t, progress, resumed)
	finishRaftReceiptCompaction(t, n, original, progress)
	target := n.store.LastAppliedIndex()
	barrier := db.StateCommand{RequestID: "physical-compaction-snapshot", At: time.Now().UTC(), Job: &db.JobCommand{PutTenant: &db.PutJobTenant{ExpectedRevision: 1, Tenant: db.JobTenant{ID: "tenant"}}}}
	for range 3 {
		proposeChannel(t, n, barrier)
	}
	require.Eventually(t, func() bool { s, err := n.raft.Snapshotter.Load(); return err == nil && s.Metadata.Index >= target }, 3*time.Second, time.Millisecond)
	n.close()
	require.NoError(t, os.RemoveAll(filepath.Join(directory, "store")))
	n = startDurableNode(t, directory, 2)
	require.True(t, requireRaftReceiptRejection(t, n, original, db.ReceiptCompactedCode).Compacted)
	rebuilt, err := n.store.GetJobReceiptCompaction("tenant", progress.RunID)
	require.NoError(t, err)
	require.True(t, rebuilt.Complete)
}

func TestOfflineCompactionExcludesRuntimeAndOtherMaintenanceUntilFinished(t *testing.T) {
	directory := t.TempDir()
	n := startDurableNode(t, directory, 1000)
	seedRaftEventTrigger(t, n)
	n.close()
	bloatStoppedNodeStore(t, directory)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	done := make(chan error, 1)
	go func() {
		_, err := CompactStore(context.Background(), directory, db.FileCompactionOptions{OnPhase: func(phase db.CompactionPhase) error {
			if phase == db.CompactionCopy {
				close(entered)
				<-release
			}
			return nil
		}})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("compaction did not enter its locked copy phase")
	}
	_, err := CompactStore(context.Background(), directory, db.FileCompactionOptions{})
	require.ErrorContains(t, err, "lock node data directory")
	_, err = ReportStoreStorage(context.Background(), directory)
	require.ErrorContains(t, err, "lock node data directory")
	cfg := config.Config{}
	cfg.SetDefaults()
	cfg.DataDir, cfg.Peers, cfg.Logger = directory, []string{"http://127.0.0.1:0"}, zap.NewNop()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	application, err := OpenApplication(ctx, cfg)
	require.ErrorContains(t, err, "lock node data directory")
	require.Nil(t, application)
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-done)
	n = startDurableNode(t, directory, 1000)
	_, err = n.store.GetJobTrigger("tenant", "ingest")
	require.NoError(t, err)
}
