package server

import (
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var admissionStates = [...]string{"disabled", "starting", "healthy", "clock_untrusted", "storage_pressure", "storage_exhausted", "recovery_hold"}

type admissionMetricsCollector struct {
	policy *admissionPolicy
	descs  map[string]*prometheus.Desc
}

func newAdmissionMetricsCollector(p *admissionPolicy) *admissionMetricsCollector {
	c := &admissionMetricsCollector{policy: p, descs: make(map[string]*prometheus.Desc)}
	for name, help := range map[string]string{
		"djob_admission_enabled":                        "Whether the local server admission policy is enabled",
		"djob_admission_monitor_running":                "Whether periodic local admission monitoring is running",
		"djob_admission_sample_age_seconds":             "Monotonic age of the last local policy evaluation; infinity before evaluation",
		"djob_admission_new_work_held":                  "Whether cached local policy holds new work; does not confirm consensus or execution authority",
		"djob_admission_all_mutations_held":             "Whether cached local policy also holds cancellation, result recording and cleanup proposals",
		"djob_admission_peer_clock_sample_age_seconds":  "Monotonic age of the last successful majority clock sample; infinity before success",
		"djob_admission_peer_clock_fresh":               "Whether a successful majority clock sample is within its freshness budget",
		"djob_admission_clock_hold":                     "Whether a local clock discontinuity hold is latched",
		"djob_admission_clock_hold_recorded":            "Whether the clock hold marker was loaded or successfully synced to disk",
		"djob_admission_clock_hold_record_errors_total": "Failures recording this instance's clock hold; errors never clear its in-memory latch",
		"djob_admission_recovery_hold":                  "Whether this server loaded a disaster recovery admission hold",
		"djob_admission_min_storage_available_bytes":    "Configured filesystem headroom threshold for holding new work",
		"djob_admission_max_database_bytes":             "Configured database file-length admission ceiling; zero disables this ceiling",
		"djob_admission_max_clock_skew_seconds":         "Configured peer uncertainty and future command timestamp tolerance",
		"djob_admission_sample_interval_seconds":        "Periodic admission monitoring interval",
		"djob_admission_peer_clock_max_age_seconds":     "Maximum age of a successful majority clock sample",
	} {
		c.descs[name] = prometheus.NewDesc(name, help, nil, nil)
	}
	c.descs["djob_admission_state"] = prometheus.NewDesc("djob_admission_state", "One active local admission state from cached observations; not quorum readiness", []string{"state"}, nil)
	c.descs["djob_admission_peer_clock_checks_total"] = prometheus.NewDesc("djob_admission_peer_clock_checks_total", "Completed majority clock probes, including unsuccessful or cancelled probes", []string{"result"}, nil)
	return c
}

func (c *admissionMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.descs {
		ch <- desc
	}
}

func (c *admissionMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	o := c.policy.observation()
	emit := func(name string, kind prometheus.ValueType, value float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(c.descs[name], kind, value, labels...)
	}
	gauge := func(name string, value float64) { emit(name, prometheus.GaugeValue, value) }
	bit := func(value bool) float64 {
		if value {
			return 1
		}
		return 0
	}
	age := func(present bool, duration time.Duration) float64 {
		if !present {
			return math.Inf(1)
		}
		return duration.Seconds()
	}
	gauge("djob_admission_enabled", bit(o.enabled))
	gauge("djob_admission_monitor_running", bit(o.running))
	gauge("djob_admission_sample_age_seconds", age(o.haveCheck, o.checkAge))
	gauge("djob_admission_new_work_held", bit(o.enabled && o.state != "healthy"))
	gauge("djob_admission_all_mutations_held", bit(o.enabled && (o.state == "starting" || o.state == "clock_untrusted" || o.state == "storage_exhausted")))
	gauge("djob_admission_peer_clock_sample_age_seconds", age(o.havePeer, o.peerAge))
	gauge("djob_admission_peer_clock_fresh", bit(o.peerFresh))
	gauge("djob_admission_clock_hold", bit(o.clockHeld))
	gauge("djob_admission_clock_hold_recorded", bit(o.clockRecorded))
	gauge("djob_admission_recovery_hold", bit(o.recoveryHeld))
	for _, state := range admissionStates {
		emit("djob_admission_state", prometheus.GaugeValue, bit(state == o.state), state)
	}
	for i, result := range []string{"ok", "unavailable"} {
		emit("djob_admission_peer_clock_checks_total", prometheus.CounterValue, float64(o.peerChecks[i]), result)
	}
	emit("djob_admission_clock_hold_record_errors_total", prometheus.CounterValue, float64(o.clockRecordErrors))
	cfg := c.policy.s.cfg.Admission
	gauge("djob_admission_min_storage_available_bytes", float64(cfg.MinAvailableBytes))
	gauge("djob_admission_max_database_bytes", float64(cfg.MaxDatabaseBytes))
	gauge("djob_admission_max_clock_skew_seconds", cfg.MaxClockSkew.Seconds())
	gauge("djob_admission_sample_interval_seconds", admissionSampleInterval.Seconds())
	gauge("djob_admission_peer_clock_max_age_seconds", admissionClockSampleMaxAge.Seconds())
}
