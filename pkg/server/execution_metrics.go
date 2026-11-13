package server

import (
	"time"

	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/jobs"
	"github.com/prometheus/client_golang/prometheus"
)

var executionResults = [...]string{
	"succeeded", "failed", "unknown", "cancelled", "expired", "retry_wait",
	"waiting", "yielded", "not_admitted", "interrupted", "result_uncertain",
	"ownership_lost", "error",
}

type executionMetrics struct {
	started        *prometheus.CounterVec
	inFlight       *prometheus.GaugeVec
	returns        *prometheus.CounterVec
	duration       *prometheus.HistogramVec
	cleanupFailure *prometheus.CounterVec
}

func newExecutionMetrics(reg prometheus.Registerer) *executionMetrics {
	m := &executionMetrics{
		started: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "djob_execution_started_total", Help: "Local worker invocations started, excluding gateway queueing",
		}, []string{"profile"}),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "djob_execution_in_flight", Help: "Local worker invocations still executing, cleaning up or awaiting result acknowledgement",
		}, []string{"profile"}),
		returns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "djob_execution_returns_total", Help: "Local worker invocation returns by fixed result category; not unique job completions",
		}, []string{"profile", "result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "djob_execution_duration_seconds", Help: "Local worker invocation duration through cleanup and result acknowledgement; excludes queueing",
			Buckets: []float64{.001, .01, .1, 1, 10, 60, 300, 1800, 7200},
		}, []string{"profile"}),
		cleanupFailure: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "djob_execution_cleanup_failures_total", Help: "Returned local worker invocations reporting a recovery or close error",
		}, []string{"profile"}),
	}
	for _, profile := range []string{"gateway", "generic"} {
		m.started.WithLabelValues(profile)
		m.inFlight.WithLabelValues(profile)
		m.duration.WithLabelValues(profile)
		m.cleanupFailure.WithLabelValues(profile)
		for _, result := range executionResults {
			m.returns.WithLabelValues(profile, result)
		}
	}
	reg.MustRegister(m.started, m.inFlight, m.returns, m.duration, m.cleanupFailure)
	return m
}

func (m *executionMetrics) start(profile string) {
	m.started.WithLabelValues(profile).Inc()
	m.inFlight.WithLabelValues(profile).Inc()
}

func (m *executionMetrics) returned(profile, result string, duration time.Duration, cleanupFailed bool) {
	// Never derive a label from an arbitrary error or an extension's data.
	bounded := "error"
	for _, known := range executionResults {
		if result == known {
			bounded = known
			break
		}
	}
	m.returns.WithLabelValues(profile, bounded).Inc()
	m.duration.WithLabelValues(profile).Observe(duration.Seconds())
	if cleanupFailed {
		m.cleanupFailure.WithLabelValues(profile).Inc()
	}
	m.inFlight.WithLabelValues(profile).Dec()
}

func (s *topLevelServer) instrumentExecutions() {
	m := s.metrics.executions
	previousGateway := s.cfg.Dispatcher.Executor.OnExecution
	s.cfg.Dispatcher.Executor.OnExecution = func(o executor.ExecutionObservation) {
		switch o.Phase {
		case executor.ExecutionStarted:
			m.start("gateway")
		case executor.ExecutionReturned:
			m.returned("gateway", o.Result, o.Duration, o.CleanupFailed)
		}
		if previousGateway != nil {
			previousGateway(o)
		}
	}
	previousGeneric := s.cfg.JobWorker.OnExecution
	s.cfg.JobWorker.OnExecution = func(o jobs.ExecutionObservation) {
		switch o.Phase {
		case jobs.ExecutionStarted:
			m.start("generic")
		case jobs.ExecutionReturned:
			m.returned("generic", o.Result, o.Duration, o.CleanupFailed)
		}
		if previousGeneric != nil {
			previousGeneric(o)
		}
	}
	previousAction := s.cfg.JobWorker.OnAction
	s.cfg.JobWorker.OnAction = func(o jobs.ActionObservation) {
		s.metrics.actions.observe(o)
		if previousAction != nil {
			previousAction(o)
		}
	}
}
