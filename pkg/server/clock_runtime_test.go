package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func startManualClockAPI(t *testing.T, cfg Config) (*Server, *manualMonitorClock) {
	t.Helper()
	startup, err := NewServer(cfg, stop.NewStopper())
	require.NoError(t, err)
	s := startup.(*Server)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	// Replace only the private reading source before any workers or listeners
	// start. Keep the actual constructor's baseline and registered collector.
	clock := &manualMonitorClock{reading: s.clock.last}
	s.clock.read = clock.read
	require.NoError(t, s.PreStart(context.Background()))
	return s, clock
}

func TestClockMetricsReportChangesWithoutScrapesConsumingThem(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.ClockMonitor.SampleInterval = MaxClockSampleInterval
	s, clock := startManualClockAPI(t, cfg)
	code, _, _ := probeReadiness(t, s)
	require.Equal(t, 200, code)
	index := s.application.Store.LastAppliedIndex()
	clock.advance(5*time.Second, time.Second)
	for range 3 {
		families := scrapeMetrics(t, s)
		require.Equal(t, float64(1), metricValue(t, families, "djob_clock_monitor_running"))
		require.Equal(t, float64(1), metricValue(t, families, "djob_clock_samples_total"))
		require.Equal(t, float64(1), metricValue(t, families, "djob_clock_sample_age_seconds"))
		require.Zero(t, metricValue(t, families, "djob_clock_discontinuities_total", "direction", "forward"))
	}
	s.clock.sample()
	families := scrapeMetrics(t, s)
	require.Equal(t, float64(2), metricValue(t, families, "djob_clock_samples_total"))
	require.Equal(t, float64(4), metricValue(t, families, "djob_clock_offset_seconds"))
	require.Equal(t, float64(4), metricValue(t, families, "djob_clock_step_seconds"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_clock_discontinuities_total", "direction", "forward"))
	require.Equal(t, MaxClockSampleInterval.Seconds(), metricValue(t, families, "djob_clock_sample_interval_seconds"))
	require.Equal(t, DefaultClockJumpThreshold.Seconds(), metricValue(t, families, "djob_clock_jump_threshold_seconds"))
	clock.advance(-9*time.Second, time.Second)
	s.clock.sample()
	families = scrapeMetrics(t, s)
	require.Equal(t, float64(-10), metricValue(t, families, "djob_clock_step_seconds"))
	require.Equal(t, float64(-6), metricValue(t, families, "djob_clock_offset_seconds"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_clock_discontinuities_total", "direction", "backward"))
	require.Zero(t, metricValue(t, families, "djob_clock_sample_age_seconds"))
	require.Equal(t, index, s.application.Store.LastAppliedIndex(), "clock monitoring and scraping create no entries or receipts")
	code, _, _ = probeReadiness(t, s)
	require.Equal(t, 200, code, "local clock diagnostics are separate from quorum readiness")
	otherCfg := apiTestConfig(t.TempDir())
	otherCfg.APIAuth = genericCredentials()
	other := startAPI(t, otherCfg)
	families = scrapeMetrics(t, other)
	require.Zero(t, metricValue(t, families, "djob_clock_discontinuities_total", "direction", "forward"))
	require.Zero(t, metricValue(t, families, "djob_clock_discontinuities_total", "direction", "backward"))
	require.NoError(t, s.Close(context.Background()))
	require.False(t, s.clock.observation().running)
	restarted := startAPI(t, cfg)
	families = scrapeMetrics(t, restarted)
	require.Zero(t, metricValue(t, families, "djob_clock_discontinuities_total", "direction", "forward"))
	require.Zero(t, metricValue(t, families, "djob_clock_discontinuities_total", "direction", "backward"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_clock_samples_total"))
}

func TestClockMonitorContinuesSamplingWithoutQuorum(t *testing.T) {
	members := startReadinessServers(t)
	var leader *Server
	for _, s := range members {
		if s.application.Raft.IsLeader() {
			leader = s
		}
	}
	require.NotNil(t, leader)
	for _, s := range members {
		if s != leader {
			require.NoError(t, s.Close(context.Background()))
		}
	}
	before := leader.clock.observation().samples
	require.Eventually(t, func() bool { return leader.clock.observation().samples > before }, 3*time.Second, 10*time.Millisecond)
	code, _, _ := probeReadiness(t, leader)
	require.Equal(t, 503, code)
	families := scrapeMetrics(t, leader)
	require.Equal(t, float64(1), metricValue(t, families, "djob_clock_monitor_running"))
	require.Greater(t, metricValue(t, families, "djob_clock_samples_total"), float64(before))
	require.Less(t, metricValue(t, families, "djob_clock_sample_age_seconds"), float64(3))
	require.NoError(t, leader.Close(context.Background()))
	require.False(t, leader.clock.observation().running)
}

func TestClockMonitorRunsDuringStartupAndJoinsAfterCancelledStartup(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.ClockMonitor.SampleInterval = MinClockSampleInterval
	cfg.Raft.Peers = make([]string, 3)
	for i := range cfg.Raft.Peers {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		cfg.Raft.Peers[i] = "http://" + listener.Addr().String()
		require.NoError(t, listener.Close())
	}
	startup, err := NewServer(cfg, stop.NewStopper())
	require.NoError(t, err)
	s := startup.(*Server)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.PreStart(ctx) }()
	require.Eventually(t, func() bool {
		observed := s.clock.observation()
		return observed.running && observed.samples >= 3
	}, time.Second, time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("startup unexpectedly finished without the other voters: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("startup did not honor cancellation")
	}
	require.NoError(t, s.Close(context.Background()))
	require.False(t, s.clock.observation().running)
}
