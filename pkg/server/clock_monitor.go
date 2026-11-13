package server

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	DefaultClockSampleInterval = time.Second
	MinClockSampleInterval     = 10 * time.Millisecond
	MaxClockSampleInterval     = time.Minute
	DefaultClockJumpThreshold  = time.Second
	MinClockJumpThreshold      = time.Millisecond
	MaxClockJumpThreshold      = time.Hour
)

// ClockMonitorConfig controls local diagnostics and the discontinuity threshold
// used by admission holds. Zero fields use defaults when constructing a server.
type ClockMonitorConfig struct {
	SampleInterval time.Duration
	JumpThreshold  time.Duration
}

// ValidateClockMonitorConfig validates explicit values, as supplied by the CLI.
// NewServer fills zero-valued fields before validation.
func ValidateClockMonitorConfig(cfg ClockMonitorConfig) error {
	if cfg.SampleInterval < MinClockSampleInterval || cfg.SampleInterval > MaxClockSampleInterval {
		return fmt.Errorf("clock sample interval must be between %s and %s", MinClockSampleInterval, MaxClockSampleInterval)
	}
	if cfg.JumpThreshold < MinClockJumpThreshold || cfg.JumpThreshold > MaxClockJumpThreshold {
		return fmt.Errorf("clock jump threshold must be between %s and %s", MinClockJumpThreshold, MaxClockJumpThreshold)
	}
	return nil
}

// Both parts come from the same time.Now call. elapsed retains its monotonic
// basis while wall deliberately strips it, so Time.Sub cannot hide a wall jump.
type clockReading struct {
	wall    time.Time
	elapsed time.Duration
}

type clockObservation struct {
	running           bool
	samples           uint64
	forward, backward uint64
	offset, step      time.Duration // Offset is relative to this monitor's baseline, not UTC.
	age               time.Duration
	lastWall          time.Time
}

type clockMonitor struct {
	cfg   ClockMonitorConfig
	read  func() clockReading
	mu    sync.Mutex
	base  clockReading
	last  clockReading
	state clockObservation
}

func newClockMonitor(cfg ClockMonitorConfig, read func() clockReading) *clockMonitor {
	if read == nil {
		start := time.Now()
		read = func() clockReading {
			now := time.Now()
			return clockReading{wall: now.Round(0), elapsed: now.Sub(start)}
		}
	}
	initial := read()
	initial.wall = initial.wall.Round(0)
	return &clockMonitor{cfg: cfg, read: read, base: initial, last: initial,
		state: clockObservation{samples: 1, lastWall: initial.wall}}
}

func (m *clockMonitor) sample() {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.read()
	now.wall = now.wall.Round(0)
	// Compare wall time to the instant predicted by monotonic elapsed time.
	// Time.Sub saturates large discrepancies without overflowing, and integer
	// durations preserve the exact inclusive jump threshold down to nanoseconds.
	m.state.step = now.wall.Sub(m.last.wall.Add(now.elapsed - m.last.elapsed))
	m.state.offset = now.wall.Sub(m.base.wall.Add(now.elapsed - m.base.elapsed))
	if m.state.step >= m.cfg.JumpThreshold {
		m.state.forward++
	} else if m.state.step <= -m.cfg.JumpThreshold {
		m.state.backward++
	}
	m.state.samples++
	m.state.lastWall = now.wall
	m.last = now
}

func (m *clockMonitor) observation() clockObservation {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := m.state
	// Scraping reads time for freshness only. It never takes another sample or
	// consumes a discontinuity before the periodic monitor can observe it.
	result.age = m.read().elapsed - m.last.elapsed
	if result.age < 0 {
		result.age = 0
	}
	return result
}

func (m *clockMonitor) run(ctx context.Context) {
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
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			// Read the clock now, rather than using a ticker's queued timestamp.
			m.sample()
		}
	}
}
