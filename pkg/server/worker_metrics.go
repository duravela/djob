package server

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var workerSessionStates = [...]string{"connected", "registered", "draining", "relocating", "stale", "unavailable"}

type workerSessionMetricsCollector struct {
	hub                              *remoteWorkers
	states, memberLimit, tenantLimit *prometheus.Desc
}

func newWorkerSessionMetricsCollector(h *remoteWorkers) *workerSessionMetricsCollector {
	return &workerSessionMetricsCollector{
		hub:         h,
		states:      prometheus.NewDesc("djob_worker_sessions_by_state", "Open worker sessions on this member by local observation state; not execution authority or free capacity.", []string{"state"}, nil),
		memberLimit: prometheus.NewDesc("djob_worker_session_slot_limit", "Configured maximum reserved remote-worker session slots on this member.", nil, nil),
		tenantLimit: prometheus.NewDesc("djob_worker_tenant_session_slot_limit", "Configured maximum reserved remote-worker session slots per tenant on this member.", nil, nil),
	}
}

func (c *workerSessionMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.states
	ch <- c.memberLimit
	ch <- c.tenantLimit
}

func (c *workerSessionMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	counts := make(map[string]uint64, len(workerSessionStates))
	for _, state := range workerSessionStates {
		counts[state] = 0
	}
	// Reuse inventory's state precedence. These reads touch only bounded local
	// memory and cached quorum age, including while a probe or dispatcher blocks.
	at, accepting := time.Now(), c.hub.acceptingWorkers()
	for _, session := range c.hub.snapshot() {
		info, visible := session.observation(at, accepting)
		if !visible {
			continue
		}
		state := info.State
		if _, known := counts[state]; !known {
			state = "unavailable"
		}
		counts[state]++
	}
	for _, state := range workerSessionStates {
		ch <- prometheus.MustNewConstMetric(c.states, prometheus.GaugeValue, float64(counts[state]), state)
	}
	ch <- prometheus.MustNewConstMetric(c.memberLimit, prometheus.GaugeValue, float64(c.hub.cfg.MaxSessions))
	ch <- prometheus.MustNewConstMetric(c.tenantLimit, prometheus.GaugeValue, float64(c.hub.cfg.MaxPerTenant))
}
