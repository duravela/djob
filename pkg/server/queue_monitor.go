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
	DefaultQueueSampleInterval = 10 * time.Second
	MinQueueSampleInterval     = 100 * time.Millisecond
	MaxQueueSampleInterval     = 5 * time.Minute
	DefaultQueueSampleLimit    = 4096
	QueueProbeTimeout          = 100 * time.Millisecond
)

type QueueMonitorConfig struct {
	SampleInterval time.Duration
	ScanLimit      int // Per profile; serialized record decoding also has an 8 MiB cap.
}

func ValidateQueueMonitorConfig(cfg QueueMonitorConfig) error {
	if cfg.SampleInterval < MinQueueSampleInterval || cfg.SampleInterval > MaxQueueSampleInterval {
		return fmt.Errorf("queue sample interval must be between %s and %s", MinQueueSampleInterval, MaxQueueSampleInterval)
	}
	if cfg.ScanLimit < 1 || cfg.ScanLimit > db.MaxQueueSampleRows {
		return fmt.Errorf("queue sample limit must be in 1..%d", db.MaxQueueSampleRows)
	}
	return nil
}

var queueProbeResults = [...]string{"ok", "limited", "busy", "error"}

const (
	queueProbeOK = iota
	queueProbeLimited
	queueProbeBusy
	queueProbeError
	queueProbeResultCount
)

type queueObservation struct {
	running, haveSample, lastOK bool
	sampledAt                   time.Time
	duration                    time.Duration
	results                     [queueProbeResultCount]uint64
	stats                       db.QueueStats
}

type queueMonitor struct {
	cfg   QueueMonitorConfig
	probe func(context.Context, int) (db.QueueStats, error)
	mu    sync.Mutex
	state queueObservation
}

func newQueueMonitor(cfg QueueMonitorConfig) *queueMonitor { return &queueMonitor{cfg: cfg} }

func (m *queueMonitor) sample(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	started := time.Now()
	probeCtx, cancel := context.WithTimeout(ctx, QueueProbeTimeout)
	stats, err := db.QueueStats{}, db.ErrQueueStatsUnavailable
	if m.probe != nil {
		stats, err = m.probe(probeCtx, m.cfg.ScanLimit)
	}
	err = errors.Join(err, probeCtx.Err())
	cancel()
	if ctx.Err() != nil {
		return
	}
	result := queueProbeOK
	if errors.Is(err, db.ErrQueueStatsBusy) {
		result = queueProbeBusy
	} else if err != nil {
		result = queueProbeError
	} else if !stats.Gateway.Complete || !stats.Generic.Complete {
		result = queueProbeLimited
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.results[result]++
	m.state.lastOK = err == nil
	m.state.duration = time.Since(started)
	if err == nil {
		m.state.stats, m.state.sampledAt, m.state.haveSample = stats, started, true
	}
}

func (m *queueMonitor) observation() queueObservation {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

func (m *queueMonitor) run(ctx context.Context) {
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
