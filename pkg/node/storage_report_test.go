//go:build darwin || linux

package node

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestOfflineStorageReportPreservesCheckpointAndRaftRecovery(t *testing.T) {
	directory := t.TempDir()
	n := startDurableNode(t, directory, 2)
	seedRaftEventTrigger(t, n)
	trigger, err := n.store.GetJobTrigger("tenant", "ingest")
	require.NoError(t, err)
	_, err = ReportStoreStorage(context.Background(), directory)
	require.ErrorContains(t, err, "lock node data directory")
	n.close()
	path := filepath.Join(directory, "store", "jobs.snap")
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	files := raftFilesBeforeCompaction(t, directory)
	report, err := ReportStoreStorage(context.Background(), directory)
	require.NoError(t, err)
	require.Equal(t, n.store.LastAppliedIndex(), report.AppliedIndex)
	require.Positive(t, report.Records)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(before), sha256.Sum256(after))
	require.Equal(t, files, raftFilesBeforeCompaction(t, directory))
	n = startDurableNode(t, directory, 2)
	restored, err := n.store.GetJobTrigger("tenant", "ingest")
	require.NoError(t, err)
	require.Equal(t, trigger, restored)
	require.GreaterOrEqual(t, n.store.LastAppliedIndex(), report.AppliedIndex)
}

type storageReportContext struct {
	context.Context
	check func()
}

func (c storageReportContext) Err() error {
	c.check()
	return c.Context.Err()
}

func newStorageReportDirectory(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	store := filepath.Join(directory, "store")
	require.NoError(t, os.Mkdir(store, 0750))
	q, err := db.NewDB(filepath.Join(store, "jobs.snap"))
	require.NoError(t, err)
	require.NoError(t, q.Close())
	return directory
}

func TestOfflineStorageReportExcludesRuntimeAndMaintenanceThroughoutScan(t *testing.T) {
	directory := newStorageReportDirectory(t)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	checks := 0
	ctx := storageReportContext{Context: base, check: func() {
		checks++
		if checks == 3 { // The first bucket is visited with both locks held.
			close(entered)
			<-release
		}
	}}
	var reportErr error
	go func() {
		_, reportErr = ReportStoreStorage(ctx, directory)
		close(done)
	}()
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("storage report did not finish")
		}
	})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("storage report did not start its scan")
	}
	_, err := ReportStoreStorage(context.Background(), directory)
	require.ErrorContains(t, err, "lock node data directory")
	_, err = CompactStore(context.Background(), directory, db.FileCompactionOptions{})
	require.ErrorContains(t, err, "lock node data directory")
	cfg := config.Config{}
	cfg.SetDefaults()
	cfg.DataDir, cfg.Peers, cfg.Logger = directory, []string{"http://127.0.0.1:0"}, zap.NewNop()
	application, err := OpenApplication(context.Background(), cfg)
	require.ErrorContains(t, err, "lock node data directory")
	require.Nil(t, application)
	cancel()
	once.Do(func() { close(release) })
	<-done
	require.ErrorIs(t, reportErr, context.Canceled)
	_, err = ReportStoreStorage(context.Background(), directory)
	require.NoError(t, err, "cancellation must release both storage locks")
}

func TestOfflineStorageReportRefusesSymlinkedStorageAndLock(t *testing.T) {
	for _, name := range []string{"store", "store/jobs.snap", ".djob.lock"} {
		t.Run(name, func(t *testing.T) {
			directory := newStorageReportDirectory(t)
			path := filepath.Join(directory, name)
			target := filepath.Join(t.TempDir(), "original")
			if name == ".djob.lock" {
				require.NoError(t, os.WriteFile(target, nil, 0600))
			} else {
				require.NoError(t, os.Rename(path, target))
			}
			require.NoError(t, os.Symlink(target, path))
			_, err := ReportStoreStorage(context.Background(), directory)
			require.Error(t, err)
		})
	}
	// A symlink to the whole member directory resolves before its stable lock.
	directory := newStorageReportDirectory(t)
	alias := filepath.Join(t.TempDir(), "member")
	require.NoError(t, os.Symlink(directory, alias))
	report, err := ReportStoreStorage(context.Background(), alias)
	require.NoError(t, err)
	resolved, err := filepath.EvalSymlinks(directory)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(resolved, "store", "jobs.snap"), report.Path)
}
