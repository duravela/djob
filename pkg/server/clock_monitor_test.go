package server

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type manualMonitorClock struct {
	mu      sync.Mutex
	reading clockReading
}

func newManualMonitorClock() *manualMonitorClock {
	return &manualMonitorClock{reading: clockReading{wall: time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)}}
}
func (c *manualMonitorClock) read() clockReading {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reading
}
func (c *manualMonitorClock) advance(wall, monotonic time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reading.wall = c.reading.wall.Add(wall)
	c.reading.elapsed += monotonic
}
func clockTestConfig() ClockMonitorConfig {
	return ClockMonitorConfig{SampleInterval: MinClockSampleInterval, JumpThreshold: time.Second}
}

func TestClockMonitorDetectsBothDirectionsWithoutCountingDelayedSamples(t *testing.T) {
	clock := newManualMonitorClock()
	m := newClockMonitor(clockTestConfig(), clock.read)
	clock.advance(37*time.Second, 37*time.Second) // A delayed worker is not a wall-clock discontinuity.
	m.sample()
	observed := m.observation()
	require.EqualValues(t, 2, observed.samples)
	require.Zero(t, observed.step)
	require.Zero(t, observed.offset)
	require.Zero(t, observed.forward)
	require.Zero(t, observed.backward)
	clock.advance(6*time.Second, time.Second)
	m.sample()
	observed = m.observation()
	require.Equal(t, 5*time.Second, observed.step)
	require.Equal(t, 5*time.Second, observed.offset)
	require.EqualValues(t, 1, observed.forward)
	clock.advance(-9*time.Second, time.Second)
	m.sample()
	observed = m.observation()
	require.Equal(t, -10*time.Second, observed.step)
	require.Equal(t, -5*time.Second, observed.offset)
	require.EqualValues(t, 1, observed.backward)
	clock.advance(time.Second, time.Second)
	m.sample()
	observed = m.observation()
	require.Zero(t, observed.step, "a stable shifted clock must not count the same change again")
	require.Equal(t, -5*time.Second, observed.offset)
	require.EqualValues(t, 1, observed.forward)
	require.EqualValues(t, 1, observed.backward)
}

func TestClockMonitorPreservesInclusiveNanosecondThresholds(t *testing.T) {
	clock := newManualMonitorClock()
	cfg := clockTestConfig()
	cfg.JumpThreshold = time.Millisecond
	m := newClockMonitor(cfg, clock.read)
	clock.advance(time.Second+time.Millisecond-time.Nanosecond, time.Second)
	m.sample()
	require.Zero(t, m.observation().forward)
	clock.advance(time.Second+time.Millisecond, time.Second)
	m.sample()
	require.EqualValues(t, 1, m.observation().forward)
	clock.advance(time.Second-time.Millisecond+time.Nanosecond, time.Second)
	m.sample()
	require.Zero(t, m.observation().backward)
	clock.advance(time.Second-time.Millisecond, time.Second)
	m.sample()
	require.EqualValues(t, 1, m.observation().backward)
}

func TestClockMonitorTracksCumulativeChangeAndMonotonicSampleAge(t *testing.T) {
	clock := newManualMonitorClock()
	m := newClockMonitor(clockTestConfig(), clock.read)
	for range 4 {
		clock.advance(1400*time.Millisecond, time.Second)
		m.sample()
	}
	observed := m.observation()
	require.Equal(t, 1600*time.Millisecond, observed.offset)
	require.Equal(t, 400*time.Millisecond, observed.step)
	require.Zero(t, observed.forward, "threshold applies to each sampled interval, not the cumulative offset")
	clock.advance(-24*time.Hour, 7*time.Second)
	for range 10 {
		observed = m.observation()
		require.Equal(t, 7*time.Second, observed.age, "backward wall changes cannot make an old sample look fresh")
		require.EqualValues(t, 5, observed.samples, "inspection does not consume an unsampled change")
		require.Zero(t, observed.backward)
	}
	m.sample()
	observed = m.observation()
	require.EqualValues(t, 1, observed.backward)
	require.Zero(t, observed.age)
}

func TestClockMonitorLargeAdjustmentsSaturateInTheCorrectDirection(t *testing.T) {
	clock := newManualMonitorClock()
	m := newClockMonitor(clockTestConfig(), clock.read)
	clock.mu.Lock()
	clock.reading.wall = time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.reading.elapsed = time.Hour
	clock.mu.Unlock()
	m.sample()
	require.Equal(t, time.Duration(math.MinInt64), m.observation().step)
	require.EqualValues(t, 1, m.observation().backward)
	clock.mu.Lock()
	clock.reading.wall = time.Date(4000, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.reading.elapsed += time.Hour
	clock.mu.Unlock()
	m.sample()
	require.Equal(t, time.Duration(math.MaxInt64), m.observation().step)
	require.EqualValues(t, 1, m.observation().forward)
}

func TestClockMonitorLifecycleCancellationAndConcurrentCollection(t *testing.T) {
	clock := newManualMonitorClock()
	m := newClockMonitor(clockTestConfig(), clock.read)
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(newClockMetricsCollector(m))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { m.run(ctx); close(done) }()
	require.Eventually(t, func() bool { return m.observation().running }, time.Second, time.Millisecond)
	var wg sync.WaitGroup
	collectionErrors := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				_, err := registry.Gather()
				if err != nil {
					collectionErrors <- err
					return
				}
			}
		}()
	}
	clock.advance(5*time.Second, time.Second)
	require.Eventually(t, func() bool { return m.observation().forward == 1 }, time.Second, time.Millisecond)
	wg.Wait()
	close(collectionErrors)
	for err := range collectionErrors {
		require.NoError(t, err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("clock monitor did not stop promptly")
	}
	before := m.observation()
	require.False(t, before.running)
	clock.advance(time.Second, time.Second)
	require.Equal(t, before.samples, m.observation().samples)
	stopped, cancelStopped := context.WithCancel(context.Background())
	cancelStopped()
	m.run(stopped)
	require.Equal(t, before.samples, m.observation().samples)
}

func TestClockMonitorConfigurationIsValidatedBeforeStorageOpens(t *testing.T) {
	for _, cfg := range []ClockMonitorConfig{
		{SampleInterval: -1}, {SampleInterval: MinClockSampleInterval - 1}, {SampleInterval: MaxClockSampleInterval + 1},
		{JumpThreshold: -1}, {JumpThreshold: MinClockJumpThreshold - 1}, {JumpThreshold: MaxClockJumpThreshold + 1},
	} {
		serverConfig := apiTestConfig(filepath.Join(t.TempDir(), "unopened"))
		serverConfig.ClockMonitor = cfg
		s, err := Start(context.Background(), serverConfig)
		require.Error(t, err)
		require.Nil(t, s)
		_, err = os.Stat(serverConfig.Raft.DataDir)
		require.True(t, os.IsNotExist(err))
	}
	for _, cfg := range []ClockMonitorConfig{
		{SampleInterval: MinClockSampleInterval, JumpThreshold: MinClockJumpThreshold},
		{SampleInterval: MaxClockSampleInterval, JumpThreshold: MaxClockJumpThreshold},
	} {
		require.NoError(t, ValidateClockMonitorConfig(cfg))
	}
	require.Error(t, ValidateClockMonitorConfig(ClockMonitorConfig{}), "explicit CLI zero values are not defaults")
}
