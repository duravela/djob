package server

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestFilesystemStorageSpaceConversionRejectsOverflowAndAcceptsFullVolume(t *testing.T) {
	for _, counts := range [][4]uint64{{0, 1, 1, 1}, {4096, math.MaxUint64, 0, 0}, {4096, 10, math.MaxUint64, 0}, {4096, 10, 1, math.MaxUint64}} {
		_, err := filesystemSpaceFromBlocks(counts[0], counts[1], counts[2], counts[3])
		require.Error(t, err)
	}
	space, err := filesystemSpaceFromBlocks(4096, 100, 10, 0)
	require.NoError(t, err)
	require.Equal(t, filesystemSpace{total: 409600, free: 40960, available: 0}, space)
}

func TestStorageMonitorUsesMemberPathsWithoutEnumeratingFiles(t *testing.T) {
	dir := t.TempDir()
	for _, child := range []string{"store", "raft/wal-7", "raft/snap-7"} {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, child), 0700))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "store/jobs.snap"), []byte("test file"), 0600))
	// Neither malformed files nor dangling links inside Raft directories are
	// interpreted or followed by a filesystem-capacity probe.
	require.NoError(t, os.Symlink(filepath.Join(dir, "missing"), filepath.Join(dir, "raft/wal-7/ignored-link")))
	m := newStorageMonitor(MinStorageSampleInterval, dir, 7)
	m.sample(context.Background())
	observed := m.observation()
	for _, component := range []storageComponent{storageStore, storageWAL, storageSnapshots} {
		sample := observed.components[component]
		require.True(t, sample.haveSample)
		require.True(t, sample.lastOK)
		require.Positive(t, sample.value.space.total)
	}
	require.EqualValues(t, 9, observed.components[storageStore].value.fileSize)
	require.False(t, observed.components[storageDatabase].haveSample)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "store/jobs.snap"), make([]byte, 64<<10), 0600))
	m.sample(context.Background())
	require.EqualValues(t, 64<<10, m.observation().components[storageStore].value.fileSize, "a subsequent probe must observe file growth")
	missing := newStorageMonitor(MinStorageSampleInterval, dir, 8)
	missing.sample(context.Background())
	require.False(t, missing.observation().components[storageWAL].lastOK, "another member's directory cannot supply this member's metric")
}

func TestStorageMonitorPreservesLastSuccessAcrossErrorsAndBusyDatabase(t *testing.T) {
	m := newStorageMonitor(MinStorageSampleInterval, t.TempDir(), 1)
	for i := range m.probes {
		m.probes[i] = func() (storageValue, error) {
			return storageValue{space: filesystemSpace{100, 40, 30}, fileSize: 50, database: db.StorageStats{AppliedIndex: 7, PageSize: 4096}}, nil
		}
	}
	m.sample(context.Background())
	before := m.observation()
	m.probes[storageStore] = func() (storageValue, error) { return storageValue{}, errors.New("private filesystem detail") }
	m.probes[storageDatabase] = func() (storageValue, error) { return storageValue{}, db.ErrStorageStatsBusy }
	m.sample(context.Background())
	after := m.observation()
	for _, component := range []storageComponent{storageStore, storageDatabase} {
		require.True(t, after.components[component].haveSample)
		require.False(t, after.components[component].lastOK)
		require.Equal(t, before.components[component].sampledAt, after.components[component].sampledAt)
		require.Equal(t, before.components[component].value, after.components[component].value)
	}
	require.EqualValues(t, 1, after.components[storageStore].results[storageError])
	require.EqualValues(t, 1, after.components[storageDatabase].results[storageBusy])
	require.EqualValues(t, 0, after.components[storageDatabase].results[storageError])
	require.EqualValues(t, 2, after.components[storageWAL].results[storageOK])
	m.probes[storageStore] = func() (storageValue, error) {
		return storageValue{space: filesystemSpace{100, 0, 0}, fileSize: 75}, nil
	}
	m.sample(context.Background())
	after = m.observation()
	require.True(t, after.components[storageStore].lastOK)
	require.Zero(t, after.components[storageStore].value.space.available, "a confirmed full volume must remain distinguishable from an error")
}

func TestStorageMonitorSingleSamplerKeepsCollectionIndependentOfBlockedIO(t *testing.T) {
	m := newStorageMonitor(MinStorageSampleInterval, t.TempDir(), 1)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var calls atomic.Uint64
	m.probes[storageStore] = func() (storageValue, error) {
		calls.Add(1)
		close(entered)
		<-release
		return storageValue{}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { m.run(ctx); close(done) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("background probe did not start")
	}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(newStorageMetricsCollector(m))
	for range 10 {
		_, err := registry.Gather()
		require.NoError(t, err)
	}
	cancel()
	select {
	case <-done:
		t.Fatal("shutdown detached a filesystem probe that is still running")
	default:
	}
	require.EqualValues(t, 1, calls.Load())
	releaseOnce.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("monitor did not join the released probe")
	}
	require.False(t, m.observation().running)
	require.False(t, m.observation().components[storageStore].haveSample, "shutdown must not publish a probe completed after cancellation")
}
