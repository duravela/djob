package server

import (
	"bytes"
	"net/http"
	"time"

	"github.com/duravela/djob/pkg/node"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxConcurrentMetricScrapes = 4

var backgroundWorkers = [...]string{
	"materializer", "deadlines", "retries", "ownership", "job_maintenance",
	"job_triggers", "job_retention", "gateway_dispatch", "job_dispatch", "worker_deployments",
}

type runtimeMetrics struct {
	consoleSessions   *consoleSessionMetrics
	registry          *prometheus.Registry
	readiness         *prometheus.CounterVec
	readinessDuration prometheus.Histogram
	backgroundErrors  *prometheus.CounterVec
	executions        *executionMetrics
	actions           *actionMetrics
	scheduling        *schedulingMetrics
	scrapes           chan struct{}
}

func newRuntimeMetrics(s *topLevelServer) *runtimeMetrics {
	m := &runtimeMetrics{
		registry: prometheus.NewRegistry(),
		readiness: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "djob_readiness_requests_total", Help: "Completed HTTP readiness probes by fixed result category",
		}, []string{"result"}),
		readinessDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "djob_readiness_duration_seconds", Help: "HTTP readiness processing time through the readiness decision",
			Buckets: []float64{.001, .01, .1, .5, 1, 2, 5},
		}),
		backgroundErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "djob_background_errors_total", Help: "Errors reported by local background workers; concurrent callbacks count separately",
		}, []string{"worker"}),
		scrapes: make(chan struct{}, maxConcurrentMetricScrapes),
	}
	for _, result := range []string{"ready", "quorum_unavailable", "busy", "starting", "stopping", "invalid_method", "invalid_request", "clock_untrusted", "storage_pressure", "storage_exhausted", "recovery_hold"} {
		m.readiness.WithLabelValues(result)
	}
	for _, worker := range backgroundWorkers {
		m.backgroundErrors.WithLabelValues(worker)
	}
	m.registry.MustRegister(m.readiness, m.readinessDuration, m.backgroundErrors, newLocalMetricsCollector(s))
	m.registry.MustRegister(newClockMetricsCollector(s.clock))
	m.registry.MustRegister(newStorageMetricsCollector(s.storage))
	m.registry.MustRegister(newQueueMetricsCollector(s.queue))
	m.registry.MustRegister(newStateMetricsCollector(s.states))
	m.registry.MustRegister(newReleaseRuntimeCollector(s))
	m.registry.MustRegister(newAdmissionMetricsCollector(s.admission))
	m.registry.MustRegister(newRuntimeResourcesCollector())
	m.consoleSessions = newConsoleSessionMetrics(m.registry)
	m.executions = newExecutionMetrics(m.registry)
	m.actions = newActionMetrics(m.registry)
	m.scheduling = newSchedulingMetrics(m.registry)
	return m
}

func (m *runtimeMetrics) observeReadiness(reason string, elapsed time.Duration) {
	if reason == "" {
		reason = "ready"
	}
	m.readiness.WithLabelValues(reason).Inc()
	m.readinessDuration.Observe(elapsed.Seconds())
}

func (m *runtimeMetrics) onError(worker string, previous func(error)) func(error) {
	counter := m.backgroundErrors.WithLabelValues(worker)
	return func(err error) {
		if err != nil {
			counter.Inc()
		}
		if previous != nil {
			previous(err)
		}
	}
}

func (s *topLevelServer) instrumentBackgroundErrors() {
	m := s.metrics
	s.cfg.Materializer.OnError = m.onError("materializer", s.cfg.Materializer.OnError)
	s.cfg.Deadlines.OnError = m.onError("deadlines", s.cfg.Deadlines.OnError)
	s.cfg.Retries.OnError = m.onError("retries", s.cfg.Retries.OnError)
	s.cfg.Ownership.OnError = m.onError("ownership", s.cfg.Ownership.OnError)
	s.cfg.JobMaintenance.OnError = m.onError("job_maintenance", s.cfg.JobMaintenance.OnError)
	s.cfg.JobTriggers.OnError = m.onError("job_triggers", s.cfg.JobTriggers.OnError)
	s.cfg.JobRetention.OnError = m.onError("job_retention", s.cfg.JobRetention.OnError)
	s.cfg.Dispatcher.OnError = m.onError("gateway_dispatch", s.cfg.Dispatcher.OnError)
	s.cfg.JobDispatch.OnError = m.onError("job_dispatch", s.cfg.JobDispatch.OnError)
}

// metricsHTTP deliberately has no ReadIndex, database read, or payload label.
// The private registry contains only bounded, in-memory collectors. Authenticate
// before collecting, including when the ordinary APIs allow anonymous access.
func (s *topLevelServer) metricsHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if _, err := s.scheduler.authorize(captureHTTPAuthorization(r).Context(), APIMonitor); err != nil {
		writeAPIError(w, err)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "use GET", http.StatusMethodNotAllowed)
		return
	}
	if r.URL.RawQuery != "" || r.URL.ForceQuery {
		writeAPIError(w, invalidHTTP("metrics query parameters are unsupported"))
		return
	}
	select {
	case s.metrics.scrapes <- struct{}{}:
		defer func() { <-s.metrics.scrapes }()
	default:
		http.Error(w, "metrics scrape capacity exhausted", http.StatusServiceUnavailable)
		return
	}
	families, err := s.metrics.registry.Gather()
	if err != nil {
		writeAPIError(w, status.Error(codes.Internal, "cannot gather metrics"))
		return
	}
	var data bytes.Buffer
	for _, family := range families {
		if _, err := expfmt.MetricFamilyToText(&data, family); err != nil {
			writeAPIError(w, status.Error(codes.Internal, "cannot encode metrics"))
			return
		}
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write(data.Bytes())
}

type localMetricsCollector struct {
	server *topLevelServer
	descs  map[string]*prometheus.Desc
}

func newLocalMetricsCollector(s *topLevelServer) *localMetricsCollector {
	c := &localMetricsCollector{server: s, descs: make(map[string]*prometheus.Desc)}
	for name, help := range map[string]string{
		"djob_server_stopping":                 "Whether local API shutdown has begun",
		"djob_raft_dynamic_membership_enabled": "Whether this member uses guarded dynamic membership",
		"djob_raft_membership_voters":          "Dynamic mode locally observed voters; zero when disabled; does not establish quorum",
		"djob_raft_membership_learners":        "Dynamic mode locally observed learners awaiting promotion or removal; zero when disabled",
		"djob_raft_membership_fresh_slots":     "Unused higher IDs in the fixed preauthorized peer catalog",
		"djob_raft_running":                    "Whether the local Raft member has not been stopped",
		"djob_raft_leader":                     "Local leader role hint; does not confirm quorum",
		"djob_raft_leader_known":               "Whether the member last observed a leader; does not confirm quorum",
		"djob_raft_term":                       "Last observed local Raft term",
		"djob_raft_commit_index":               "Last observed persisted local Raft commit index",
		"djob_raft_applied_index":              "Last locally acknowledged durable application index",
		"djob_raft_log_entries":                "Retained Raft entries excluding the compacted sentinel",
		"djob_raft_log_bytes":                  "Serialized bytes in retained Raft entries, not process heap",
		"djob_raft_snapshot_bytes":             "Serialized bytes in the installed in-memory snapshot",
		"djob_raft_uncheckpointed_bytes":       "Applied serialized entry bytes beyond the installed checkpoint",
		"djob_raft_snapshot_in_flight":         "Whether a background checkpoint encoder is active",
		"djob_raft_snapshot_index":             "Last locally installed or created Raft snapshot index",
		"djob_raft_apply_lag_entries":          "Observed persisted commit index minus locally acknowledged application index",
		"djob_raft_read_barriers_pending":      "Accepted local read barriers, including timed-out callers still awaiting Raft progress",
		"djob_raft_read_barriers_limit":        "Maximum accepted outstanding local read barriers",
	} {
		c.descs[name] = prometheus.NewDesc(name, help, nil, nil)
	}
	c.descs["djob_dispatch_enabled"] = prometheus.NewDesc("djob_dispatch_enabled", "Whether this execution profile is enabled locally", []string{"profile"}, nil)
	c.descs["djob_dispatch_active"] = prometheus.NewDesc("djob_dispatch_active", "Local dispatch callers still waiting; gateway execution may outlive a cancelled caller", []string{"profile"}, nil)
	return c
}

func (c *localMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.descs {
		ch <- desc
	}
}

func (c *localMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.server
	var raft node.LocalRaftStatus
	if s.application != nil {
		raft = s.application.Raft.LocalStatus()
	}
	emit := func(name string, value float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(c.descs[name], prometheus.GaugeValue, value, labels...)
	}
	bit := func(v bool) float64 {
		if v {
			return 1
		}
		return 0
	}
	emit("djob_server_stopping", bit(s.startupContext != nil && s.startupContext.Err() != nil))
	var membership node.MembershipStatus
	if s.application != nil {
		membership = s.application.Raft.Membership()
	}
	emit("djob_raft_dynamic_membership_enabled", bit(membership.Enabled))
	emit("djob_raft_membership_voters", float64(len(membership.Voters)))
	emit("djob_raft_membership_learners", float64(len(membership.Learners)))
	emit("djob_raft_membership_fresh_slots", float64(max(0, membership.CatalogSize-int(membership.MaxID))))
	emit("djob_raft_running", bit(raft.Running))
	emit("djob_raft_leader", bit(raft.Leader))
	emit("djob_raft_leader_known", bit(raft.LeaderKnown))
	emit("djob_raft_term", float64(raft.Term))
	emit("djob_raft_commit_index", float64(raft.CommitIndex))
	emit("djob_raft_applied_index", float64(raft.AppliedIndex))
	emit("djob_raft_log_entries", float64(raft.LogEntries))
	emit("djob_raft_log_bytes", float64(raft.LogBytes))
	emit("djob_raft_snapshot_bytes", float64(raft.SnapshotBytes))
	emit("djob_raft_uncheckpointed_bytes", float64(raft.UncheckpointedBytes))
	emit("djob_raft_snapshot_in_flight", bit(raft.SnapshotInFlight))
	emit("djob_raft_snapshot_index", float64(raft.SnapshotIndex))
	emit("djob_raft_apply_lag_entries", float64(raft.CommitIndex-raft.AppliedIndex))
	emit("djob_raft_read_barriers_pending", float64(raft.PendingReadBarriers))
	emit("djob_raft_read_barriers_limit", node.MaxPendingReadBarriers)
	emit("djob_dispatch_enabled", bit(s.dispatcher != nil), "gateway")
	emit("djob_dispatch_enabled", bit(s.jobDispatcher != nil), "generic")
	var gatewayActive, genericActive int
	if s.dispatcher != nil {
		gatewayActive = s.dispatcher.Pending()
	}
	if s.jobDispatcher != nil {
		genericActive = s.jobDispatcher.Active()
	}
	emit("djob_dispatch_active", float64(gatewayActive), "gateway")
	emit("djob_dispatch_active", float64(genericActive), "generic")
}
