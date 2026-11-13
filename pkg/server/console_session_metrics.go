package server

import (
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/prometheus/client_golang/prometheus"
)

// Storage values are sampled by the session maintenance loop. Scrapes only
// observe gauges and counters; they never acquire database locks or read quorum.
type consoleSessionMetrics struct {
	storage            *prometheus.GaugeVec
	rejected           *prometheus.CounterVec
	probeRejected      prometheus.Counter
	cleanupFailures    prometheus.Counter
	cleanupLastSuccess prometheus.Gauge
	sampleFailures     prometheus.Counter
	sampledAt          prometheus.Gauge
}

func newConsoleSessionMetrics(reg *prometheus.Registry) *consoleSessionMetrics {
	m := &consoleSessionMetrics{
		storage:            prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "djob_console_session_storage", Help: "Cached local session storage observations by fixed kind; see sample timestamp for freshness."}, []string{"kind"}),
		rejected:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "djob_console_session_rejections_total", Help: "Rejected session RPC endpoint requests by fixed gRPC status code."}, []string{"code"}),
		probeRejected:      prometheus.NewCounter(prometheus.CounterOpts{Name: "djob_console_session_probe_rejections_total", Help: "Authentication probes refused before quorum work because their rate or concurrency budget is exhausted."}),
		cleanupFailures:    prometheus.NewCounter(prometheus.CounterOpts{Name: "djob_console_session_cleanup_failures_total", Help: "Failed session cleanup checks, quorum probes or proposals."}),
		cleanupLastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{Name: "djob_console_session_cleanup_last_success_timestamp_seconds", Help: "Last successful cleanup proposal; zero before any cleanup is needed."}),
		sampleFailures:     prometheus.NewCounter(prometheus.CounterOpts{Name: "djob_console_session_sample_failures_total", Help: "Failed local session storage samples."}),
		sampledAt:          prometheus.NewGauge(prometheus.GaugeOpts{Name: "djob_console_session_sample_timestamp_seconds", Help: "Wall time of the last successful local session storage sample; zero before sampling."}),
	}
	for _, kind := range []string{"records", "revoked", "ciphertext_bytes", "record_limit", "ciphertext_limit", "active"} {
		m.storage.WithLabelValues(kind)
	}
	for _, code := range []string{"Unauthenticated", "PermissionDenied", "ResourceExhausted", "Unavailable", "InvalidArgument", "FailedPrecondition", "Aborted", "Internal", "DeadlineExceeded", "Canceled"} {
		m.rejected.WithLabelValues(code)
	}
	reg.MustRegister(m.storage, m.rejected, m.probeRejected, m.cleanupFailures, m.cleanupLastSuccess, m.sampleFailures, m.sampledAt)
	return m
}
func (m *consoleSessionMetrics) observe(stats db.ConsoleSessionStats) {
	for name, value := range map[string]int{"records": stats.Records, "revoked": stats.Revoked, "ciphertext_bytes": stats.CiphertextBytes, "record_limit": stats.RecordLimit, "ciphertext_limit": stats.ByteLimit} {
		m.storage.WithLabelValues(name).Set(float64(value))
	}
	active := 0.
	if stats.Active {
		active = 1
	}
	m.storage.WithLabelValues("active").Set(active)
	m.sampledAt.Set(float64(time.Now().UnixNano()) / 1e9)
}
