package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/duravela/djob/pkg/db"
)

const (
	DefaultStorageSampleInterval = 10 * time.Second
	MinStorageSampleInterval     = 100 * time.Millisecond
	MaxStorageSampleInterval     = 5 * time.Minute
)

func ValidateStorageSampleInterval(interval time.Duration) error {
	if interval < MinStorageSampleInterval || interval > MaxStorageSampleInterval {
		return fmt.Errorf("storage sample interval must be between %s and %s", MinStorageSampleInterval, MaxStorageSampleInterval)
	}
	return nil
}

type storageComponent int

const (
	storageStore storageComponent = iota
	storageWAL
	storageSnapshots
	storageDatabase
	storageComponents
)

var storageComponentNames = [...]string{"store", "raft_wal", "raft_snapshots", "database"}

const (
	storageOK = iota
	storageBusy
	storageError
	storageResults
)

var storageResultNames = [...]string{"ok", "busy", "error"}

type filesystemSpace struct {
	total, free, available uint64
}

// Convert block counters without wrapping a failed/unsupported capacity reading
// into a plausible byte count. Zero available blocks is a valid full volume.
func filesystemSpaceFromBlocks(blockSize, blocks, free, available uint64) (filesystemSpace, error) {
	if blockSize == 0 {
		return filesystemSpace{}, errors.New("invalid filesystem block size")
	}
	for _, count := range []uint64{blocks, free, available} {
		if count > ^uint64(0)/blockSize {
			return filesystemSpace{}, errors.New("filesystem capacity overflows bytes")
		}
	}
	return filesystemSpace{blocks * blockSize, free * blockSize, available * blockSize}, nil
}

type storageValue struct {
	space    filesystemSpace
	fileSize int64
	database db.StorageStats
}

type storageComponentObservation struct {
	value      storageValue
	haveSample bool
	lastOK     bool
	sampledAt  time.Time
	duration   time.Duration
	results    [storageResults]uint64
}

type storageObservation struct {
	running    bool
	components [storageComponents]storageComponentObservation
}

type storageMonitor struct {
	interval time.Duration
	probes   [storageComponents]func() (storageValue, error)
	mu       sync.Mutex
	state    storageObservation
}

func newStorageMonitor(interval time.Duration, dataDir string, nodeID uint64) *storageMonitor {
	m := &storageMonitor{interval: interval}
	storeDir := filepath.Join(dataDir, "store")
	m.probes[storageStore] = func() (storageValue, error) {
		space, err := readFilesystemSpace(storeDir)
		if err != nil {
			return storageValue{}, err
		}
		file, err := os.Stat(filepath.Join(storeDir, "jobs.snap"))
		if err != nil {
			return storageValue{}, err
		}
		if !file.Mode().IsRegular() || file.Size() < 0 {
			return storageValue{}, errors.New("database storage path is not a regular file")
		}
		return storageValue{space: space, fileSize: file.Size()}, nil
	}
	for component, directory := range map[storageComponent]string{
		storageWAL:       filepath.Join(dataDir, "raft", fmt.Sprintf("wal-%d", nodeID)),
		storageSnapshots: filepath.Join(dataDir, "raft", fmt.Sprintf("snap-%d", nodeID)),
	} {
		m.probes[component] = func() (storageValue, error) {
			space, err := readFilesystemSpace(directory)
			return storageValue{space: space}, err
		}
	}
	return m
}

func (m *storageMonitor) sample(ctx context.Context) {
	for component, probe := range m.probes {
		if ctx.Err() != nil {
			return
		}
		started := time.Now()
		value, err := storageValue{}, db.ErrStorageStatsUnavailable
		if probe != nil {
			value, err = probe()
		}
		if ctx.Err() != nil {
			return
		}
		result := storageOK
		if errors.Is(err, db.ErrStorageStatsBusy) {
			result = storageBusy
		} else if err != nil {
			result = storageError
		}
		m.mu.Lock()
		observed := &m.state.components[component]
		observed.results[result]++
		observed.lastOK = result == storageOK
		observed.duration = time.Since(started)
		if result == storageOK {
			observed.value = value
			observed.haveSample = true
			observed.sampledAt = started
		}
		m.mu.Unlock()
	}
}

func (m *storageMonitor) observation() storageObservation {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

func (m *storageMonitor) run(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	m.mu.Lock()
	m.state.running = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.state.running = false
		m.mu.Unlock()
	}()
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		m.sample(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
