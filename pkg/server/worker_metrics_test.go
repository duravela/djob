package server

import (
	"context"
	"testing"
	"time"

	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestWorkerSessionMetricsUseOnlyLocalStateAndFixedLabels(t *testing.T) {
	h := &remoteWorkers{cfg: RemoteWorkerConfig{MaxSessions: 8, MaxPerTenant: 3, IdleTimeout: 10 * time.Second, QuorumGrace: 5 * time.Second}, sessions: make(map[string]*workerSession)}
	h.probe = func(context.Context) error { panic("metrics must not probe Raft") }
	// Nil backend and dispatchers also make accidental storage/task reads fail.
	registry := prometheus.NewRegistry()
	registry.MustRegister(newWorkerSessionMetricsCollector(h))
	gather := func() map[string]*dto.MetricFamily {
		values, err := registry.Gather()
		require.NoError(t, err)
		families := make(map[string]*dto.MetricFamily)
		for _, value := range values {
			families[value.GetName()] = value
			for _, metric := range value.Metric {
				for _, label := range metric.Label {
					require.Equal(t, "state", label.GetName())
					require.Contains(t, []string{"connected", "registered", "draining", "relocating", "stale", "unavailable"}, label.GetValue())
				}
			}
			require.NotContains(t, value.String(), "private-marker")
		}
		return families
	}
	assertCounts := func(want map[string]float64) {
		families := gather()
		require.Len(t, families["djob_worker_sessions_by_state"].Metric, 6)
		for _, state := range []string{"connected", "registered", "draining", "relocating", "stale", "unavailable"} {
			require.Equal(t, want[state], metricValue(t, families, "djob_worker_sessions_by_state", "state", state), state)
		}
		require.Equal(t, float64(8), metricValue(t, families, "djob_worker_session_slot_limit"))
		require.Equal(t, float64(3), metricValue(t, families, "djob_worker_tenant_session_slot_limit"))
	}
	assertCounts(nil) // Every state is present even before the first registration.
	at := time.Now()
	for _, state := range []string{"connected", "registered", "draining", "relocating", "stale", "closed"} {
		s := &workerSession{hub: h, lastSeen: at, started: true, info: jp.WorkerInfo{WorkerID: "private-marker", TenantID: "private-marker"}}
		switch state {
		case "registered":
			s.started = false
		case "draining":
			s.draining, s.lastSeen = true, at.Add(-time.Minute)
		case "relocating":
			s.relocating, s.draining = true, true
		case "stale":
			s.lastSeen = at.Add(-time.Minute)
		case "closed":
			s.closed = true
		}
		h.sessions[state] = s
	}
	assertCounts(map[string]float64{"connected": 1, "registered": 1, "draining": 1, "relocating": 1, "stale": 1})
	h.quorumUnavailable = true
	assertCounts(map[string]float64{"unavailable": 2, "draining": 1, "relocating": 1, "stale": 1})
	h.quorumUnavailable, h.lastConfirmed = false, at.Add(-time.Minute)
	assertCounts(map[string]float64{"unavailable": 2, "draining": 1, "relocating": 1, "stale": 1})
	require.Equal(t, at, h.sessions["connected"].lastSeen, "scraping must not refresh worker contact")
	h.sessions = make(map[string]*workerSession)
	assertCounts(nil)
}
