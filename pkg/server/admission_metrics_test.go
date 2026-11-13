package server

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/util/stop"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
)

func admissionRegistry(p *admissionPolicy) *prometheus.Registry {
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(newAdmissionMetricsCollector(p))
	return registry
}

func gatherAdmissionMetrics(t *testing.T, registry *prometheus.Registry) map[string]*dto.MetricFamily {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	result := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		result[family.GetName()] = family
	}
	return result
}

func TestAdmissionMetricsDistinguishNewWorkAndAllMutationHolds(t *testing.T) {
	for _, state := range admissionStates {
		t.Run(state, func(t *testing.T) {
			p, offset := healthyAdmissionFixture(t)
			switch state {
			case "disabled":
				p.s.cfg.Admission.Disabled = true
				p.peerOK = false
			case "starting":
				p.peerOK = false
			case "storage_pressure":
				p.s.storage.state.components[storageStore].value.space.available = 512
			case "storage_exhausted":
				p.s.storage.state.components[storageStore].value.space.available = 0
			case "recovery_hold":
				p.recoveryHeld = true
			case "clock_untrusted":
				offset.Store(int64(2 * time.Second))
			}
			if state != "starting" && state != "disabled" {
				p.reason()
			}
			families := gatherAdmissionMetrics(t, admissionRegistry(p))
			var active float64
			for _, candidate := range admissionStates {
				value := metricValue(t, families, "djob_admission_state", "state", candidate)
				active += value
				if state == candidate {
					require.Equal(t, float64(1), value)
				} else {
					require.Zero(t, value)
				}
			}
			require.Equal(t, float64(1), active)
			require.Len(t, families["djob_admission_state"].Metric, len(admissionStates))
			newWorkHeld := state != "healthy" && state != "disabled"
			allHeld := state == "starting" || state == "clock_untrusted" || state == "storage_exhausted"
			require.Equal(t, newWorkHeld, metricValue(t, families, "djob_admission_new_work_held") == 1)
			require.Equal(t, allHeld, metricValue(t, families, "djob_admission_all_mutations_held") == 1)
			require.Equal(t, float64(1024), metricValue(t, families, "djob_admission_min_storage_available_bytes"))
			require.Zero(t, metricValue(t, families, "djob_admission_max_database_bytes"))
			require.Equal(t, float64(5), metricValue(t, families, "djob_admission_max_clock_skew_seconds"))
			require.Equal(t, float64(3), metricValue(t, families, "djob_admission_peer_clock_max_age_seconds"))
			if state == "clock_untrusted" {
				require.Equal(t, float64(1), metricValue(t, families, "djob_admission_clock_hold_recorded"))
			}
		})
	}
}

func TestAdmissionMetricsExpireCachedPeerAgreementWithoutEvaluatingPolicy(t *testing.T) {
	p, _ := healthyAdmissionFixture(t)
	require.Empty(t, p.reason())
	p.peerAt = time.Now().Add(-4 * time.Second)
	// Collection must not call a policy or storage probe to notice staleness.
	p.s.clock.read = func() clockReading { panic("admission collection evaluated the monitored clock") }
	p.recordClockHold = func() error { panic("admission collection attempted a disk write") }
	for i := range p.s.storage.probes {
		p.s.storage.probes[i] = func() (storageValue, error) { panic("admission collection probed storage") }
	}
	for range 3 {
		families := gatherAdmissionMetrics(t, admissionRegistry(p))
		require.Equal(t, float64(1), metricValue(t, families, "djob_admission_state", "state", "clock_untrusted"))
		require.GreaterOrEqual(t, metricValue(t, families, "djob_admission_peer_clock_sample_age_seconds"), float64(4))
		require.Zero(t, metricValue(t, families, "djob_admission_peer_clock_fresh"))
		require.Equal(t, float64(1), metricValue(t, families, "djob_admission_all_mutations_held"))
	}
	require.Empty(t, p.checkedReason, "collection must not replace the actual policy evaluation")
	require.Zero(t, p.peerChecks)
}

func TestAdmissionMetricsBeforeStartupDoNotEvaluateOrPersistClockJump(t *testing.T) {
	cfg := apiTestConfig(filepath.Join(t.TempDir(), "unopened"))
	cfg.APIAuth = genericCredentials()
	cfg.Admission.Disabled = false
	startup, err := NewServer(cfg, stop.NewStopper())
	require.NoError(t, err)
	s := startup.(*Server)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	clock := &manualMonitorClock{reading: s.clock.last}
	s.clock.read = clock.read
	clock.advance(5*time.Second, time.Second)
	s.admission.recordClockHold = func() error { panic("scrape tried to persist a clock hold") }
	for component := range s.storage.probes {
		s.storage.probes[component] = func() (storageValue, error) { panic("scrape tried to probe storage") }
	}
	request := httptest.NewRequest("GET", "/metrics", nil)
	request.Header.Set("Authorization", "Bearer "+genericToken("ops/alice"))
	response := httptest.NewRecorder()
	s.metricsHTTP(response, request)
	require.Equal(t, 200, response.Code)
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(bytes.NewReader(response.Body.Bytes()))
	require.NoError(t, err)
	require.Equal(t, float64(1), metricValue(t, families, "djob_admission_enabled"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_admission_state", "state", "starting"))
	require.Zero(t, metricValue(t, families, "djob_admission_monitor_running"))
	require.True(t, math.IsInf(metricValue(t, families, "djob_admission_sample_age_seconds"), 1))
	require.True(t, math.IsInf(metricValue(t, families, "djob_admission_peer_clock_sample_age_seconds"), 1))
	require.False(t, s.admission.haveCheck)
	require.False(t, s.admission.clockHeld)
	require.Zero(t, s.admission.peerChecks)
	require.NotContains(t, response.Body.String(), cfg.Raft.DataDir)
	require.NotContains(t, response.Body.String(), genericToken("ops/alice"))
	_, err = os.Stat(cfg.Raft.DataDir)
	require.True(t, os.IsNotExist(err))
}

func TestAdmissionMetricsRemainResponsiveWhileClockHoldWriteStallsAndFails(t *testing.T) {
	p, offset := healthyAdmissionFixture(t)
	require.Empty(t, p.reason())
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	p.recordClockHold = func() error {
		close(entered)
		<-release
		return errors.New("private disk failure details")
	}
	offset.Store(int64(2 * time.Second))
	done := make(chan string, 1)
	go func() { done <- p.reason() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("clock hold recording did not start")
	}
	registry := admissionRegistry(p)
	gathered := make(chan error, 1)
	go func() { _, err := registry.Gather(); gathered <- err }()
	select {
	case err := <-gathered:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("collection blocked behind clock hold I/O")
	}
	families := gatherAdmissionMetrics(t, registry)
	require.Equal(t, float64(1), metricValue(t, families, "djob_admission_clock_hold"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_admission_state", "state", "clock_untrusted"))
	require.Zero(t, metricValue(t, families, "djob_admission_clock_hold_recorded"))
	require.Zero(t, metricValue(t, families, "djob_admission_clock_hold_record_errors_total"))
	// Another admission must see the latch rather than queue behind the write.
	require.Equal(t, "clock_untrusted", p.reason())
	releaseOnce.Do(func() { close(release) })
	select {
	case reason := <-done:
		require.Equal(t, "clock_untrusted", reason)
	case <-time.After(time.Second):
		t.Fatal("clock hold write did not complete")
	}
	families = gatherAdmissionMetrics(t, registry)
	require.Equal(t, float64(1), metricValue(t, families, "djob_admission_clock_hold_record_errors_total"))
	require.Zero(t, metricValue(t, families, "djob_admission_clock_hold_recorded"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_admission_all_mutations_held"))
}

func TestAdmissionMetricsTrackPersistedHoldAcrossPolicyRestart(t *testing.T) {
	p, offset := healthyAdmissionFixture(t)
	offset.Store(int64(2 * time.Second))
	require.Equal(t, "clock_untrusted", p.reason())
	restarted, err := newAdmissionPolicy(p.s)
	require.NoError(t, err)
	families := gatherAdmissionMetrics(t, admissionRegistry(restarted))
	require.Equal(t, float64(1), metricValue(t, families, "djob_admission_clock_hold"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_admission_clock_hold_recorded"))
	require.Zero(t, metricValue(t, families, "djob_admission_clock_hold_record_errors_total"))
	require.Zero(t, metricValue(t, families, "djob_admission_monitor_running"))
}

func TestAdmissionMetricsSampleWithoutReadinessAndStopOnShutdown(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.Admission = AdmissionConfig{MinAvailableBytes: 1, MaxDatabaseBytes: 1}
	cfg.StorageSampleInterval = 100 * time.Millisecond
	s := startAPI(t, cfg)
	require.Eventually(t, func() bool {
		o := s.admission.observation()
		return o.running && o.state == "storage_pressure"
	}, 3*time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := s.application.Raft.ReadBarrier(ctx)
	require.NoError(t, err)
	index := s.application.Store.LastAppliedIndex()
	for range 3 {
		families := scrapeMetrics(t, s)
		require.Equal(t, float64(1), metricValue(t, families, "djob_admission_monitor_running"))
		require.Equal(t, float64(1), metricValue(t, families, "djob_admission_state", "state", "storage_pressure"))
		require.Equal(t, float64(1), metricValue(t, families, "djob_admission_new_work_held"))
		require.Zero(t, metricValue(t, families, "djob_admission_all_mutations_held"))
		require.Positive(t, metricValue(t, families, "djob_admission_peer_clock_checks_total", "result", "ok"))
		require.Equal(t, float64(1), metricValue(t, families, "djob_admission_max_database_bytes"))
		require.Zero(t, metricValue(t, families, "djob_readiness_requests_total", "result", "ready"))
	}
	require.Equal(t, index, s.application.Store.LastAppliedIndex(), "monitoring and scrapes must create no log entries or receipts")
	require.NoError(t, s.Close(context.Background()))
	o := s.admission.observation()
	require.False(t, o.running)
	stopCtx, stop := context.WithCancel(context.Background())
	stop()
	s.admission.run(stopCtx)
	require.Equal(t, o.peerChecks, s.admission.observation().peerChecks)
}

func TestAdmissionMetricsObserveClockQuorumLossWithoutReadiness(t *testing.T) {
	members := startReadinessServers(t, func(c *Config) {
		c.Admission = AdmissionConfig{MinAvailableBytes: 1}
		c.StorageSampleInterval = 100 * time.Millisecond
	})
	var isolated *Server
	require.Eventually(t, func() bool {
		for _, s := range members {
			if s.admission.observation().state != "healthy" {
				return false
			}
			if s.application.Raft.IsLeader() {
				isolated = s
			}
		}
		return isolated != nil
	}, 4*time.Second, 10*time.Millisecond)
	for _, s := range members {
		if s != isolated {
			require.NoError(t, s.Close(context.Background()))
		}
	}
	require.Eventually(t, func() bool {
		o := isolated.admission.observation()
		return o.state == "clock_untrusted" && !o.peerFresh && o.peerChecks[1] > 0
	}, 6*time.Second, 10*time.Millisecond)
	families := scrapeMetrics(t, isolated)
	require.Equal(t, float64(1), metricValue(t, families, "djob_admission_monitor_running"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_admission_all_mutations_held"))
	require.Zero(t, metricValue(t, families, "djob_admission_peer_clock_fresh"))
	require.Zero(t, metricValue(t, families, "djob_admission_clock_hold"), "missing agreement does not fabricate a local discontinuity")
	require.Zero(t, metricValue(t, families, "djob_readiness_requests_total", "result", "quorum_unavailable"))
}
