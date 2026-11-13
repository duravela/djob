package server

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type workerQuorumMetrics struct {
	probes   *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func newWorkerQuorumMetrics(h *remoteWorkers, registry *prometheus.Registry) *workerQuorumMetrics {
	m := &workerQuorumMetrics{
		probes:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "djob_worker_quorum_probes_total", Help: "Worker routing read barriers by fixed result category."}, []string{"result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "djob_worker_quorum_probe_duration_seconds", Help: "Worker routing read-barrier time, including local durable application.", Buckets: []float64{.001, .01, .1, .5, 1, 2, 5}}, []string{"result"}),
	}
	for _, result := range []string{"confirmed", "failed"} {
		m.probes.WithLabelValues(result)
		m.duration.WithLabelValues(result)
	}
	registry.MustRegister(m.probes, m.duration,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "djob_worker_controller_available", Help: "One while a recent quorum confirmation permits worker routing; never execution authority."}, func() float64 {
			h.mu.Lock()
			defer h.mu.Unlock()
			if !h.closed && !h.lastConfirmed.IsZero() && time.Since(h.lastConfirmed) <= h.cfg.QuorumGrace && !h.quorumUnavailable {
				return 1
			}
			return 0
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "djob_worker_quorum_last_success_timestamp_seconds", Help: "Last successful worker routing read barrier; zero before confirmation."}, func() float64 {
			h.mu.Lock()
			defer h.mu.Unlock()
			if h.lastConfirmed.IsZero() {
				return 0
			}
			return float64(h.lastConfirmed.UnixNano()) / 1e9
		}),
	)
	return m
}

func (h *remoteWorkers) acceptingWorkers() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Startup may accept a registration before the first probe completes. Its
	// dispatcher still cannot obtain execution authority without a committed claim.
	return !h.closed && !h.quorumUnavailable && (h.lastConfirmed.IsZero() || time.Since(h.lastConfirmed) <= h.cfg.QuorumGrace)
}

func (h *remoteWorkers) checkQuorum(ctx context.Context) {
	started := time.Now()
	probe, stop := context.WithTimeout(ctx, h.cfg.QuorumProbeTimeout)
	err := h.probe(probe)
	stop()
	if ctx.Err() != nil {
		return
	}
	result := "failed"
	if err == nil {
		result = "confirmed"
	}
	h.quorumMetrics.probes.WithLabelValues(result).Inc()
	h.quorumMetrics.duration.WithLabelValues(result).Observe(time.Since(started).Seconds())
	now := time.Now()
	h.mu.Lock()
	h.lastProbe = now
	if err == nil {
		h.lastConfirmed = now
	}
	h.quorumUnavailable = err != nil && (h.lastConfirmed.IsZero() || now.Sub(h.lastConfirmed) >= h.cfg.QuorumGrace)
	unavailable := h.quorumUnavailable
	h.mu.Unlock()
	if unavailable {
		for _, session := range h.snapshot() {
			session.relocate()
		}
	}
}

// Relocation stops controller-side admission/work without dropping the worker
// connection. Already executing I/O receives cancellation through its existing
// lease budget; the normal kernel still joins I/O, recovery and close. Only then
// does a transient session error permit fresh registration elsewhere.
func (s *workerSession) relocate() {
	s.mu.Lock()
	if s.closed || s.relocating {
		s.mu.Unlock()
		return
	}
	s.relocating = true
	started := s.started
	s.mu.Unlock()
	s.dispatcher.Stop()
	if !started {
		s.stop()
		return
	}
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *workerSession) finished() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}
