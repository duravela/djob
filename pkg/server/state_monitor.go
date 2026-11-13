package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/duravela/djob/pkg/db"
)

const (
	DefaultStateSampleInterval = 10 * time.Second
	MinStateSampleInterval     = 100 * time.Millisecond
	MaxStateSampleInterval     = 5 * time.Minute
	DefaultStateSampleLimit    = 256
	StateProbeTimeout          = 100 * time.Millisecond
)

type StateMonitorConfig struct {
	SampleInterval time.Duration
	ScanLimit      int // Per primary bucket; serialized decoding also has an 8 MiB cap.
}

func ValidateStateMonitorConfig(cfg StateMonitorConfig) error {
	if cfg.SampleInterval < MinStateSampleInterval || cfg.SampleInterval > MaxStateSampleInterval {
		return fmt.Errorf("state sample interval must be between %s and %s", MinStateSampleInterval, MaxStateSampleInterval)
	}
	if cfg.ScanLimit < 1 || cfg.ScanLimit > db.MaxStateSampleRows {
		return fmt.Errorf("state sample limit must be in 1..%d", db.MaxStateSampleRows)
	}
	return nil
}

var stateProbeResults = [...]string{"ok", "limited", "busy", "error"}

const (
	stateProbeOK = iota
	stateProbeLimited
	stateProbeBusy
	stateProbeError
	stateProbeResultCount
)

type stateObservation struct {
	running, haveSample, lastOK bool
	sampledAt                   time.Time
	duration                    time.Duration
	results                     [stateProbeResultCount]uint64
	stats                       db.StateInventory
}

type stateMonitor struct {
	cfg   StateMonitorConfig
	probe func(context.Context, int) (db.StateInventory, error)
	mu    sync.Mutex
	state stateObservation
}

func newStateMonitor(cfg StateMonitorConfig) *stateMonitor { return &stateMonitor{cfg: cfg} }

func (m *stateMonitor) sample(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	started := time.Now()
	probeCtx, cancel := context.WithTimeout(ctx, StateProbeTimeout)
	stats, err := db.StateInventory{}, db.ErrStateInventoryUnavailable
	if m.probe != nil {
		stats, err = m.probe(probeCtx, m.cfg.ScanLimit)
	}
	err = errors.Join(err, probeCtx.Err())
	cancel()
	if ctx.Err() != nil {
		return
	}
	result := stateProbeOK
	if errors.Is(err, db.ErrStateInventoryBusy) {
		result = stateProbeBusy
	} else if err != nil {
		result = stateProbeError
	} else {
		for _, kind := range stats.Kinds {
			if !kind.Complete {
				result = stateProbeLimited
			}
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.results[result]++
	m.state.lastOK, m.state.duration = err == nil, time.Since(started)
	if err == nil {
		m.state.stats, m.state.sampledAt, m.state.haveSample = stats, started, true
	}
}

func (m *stateMonitor) observation() stateObservation {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

func (m *stateMonitor) run(ctx context.Context) {
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
	ticker := time.NewTicker(m.cfg.SampleInterval)
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
