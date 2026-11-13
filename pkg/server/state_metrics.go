package server

import (
	"math"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/prometheus/client_golang/prometheus"
)

type stateMetricsCollector struct {
	monitor *stateMonitor
	descs   map[string]*prometheus.Desc
}

func newStateMetricsCollector(m *stateMonitor) *stateMetricsCollector {
	c := &stateMetricsCollector{monitor: m, descs: make(map[string]*prometheus.Desc)}
	for name, help := range map[string]string{
		"djob_release_sample_complete":               "Whether the cached release catalog sample is complete",
		"djob_release_oldest_retirement_age_seconds": "Age of oldest observed retiring release; absence of completion can include lower bounds",
		"djob_state_monitor_running":                 "Whether the local bounded durable-state sampler is running",
		"djob_state_sample_interval_seconds":         "Configured durable-state inventory sample interval",
		"djob_state_sample_row_limit":                "Maximum records decoded per primary bucket per inventory probe",
		"djob_state_sample_byte_limit":               "Maximum serialized bytes decoded per primary bucket per inventory probe",
		"djob_state_probe_timeout_seconds":           "Cooperative inventory probe timeout; an OS page fault is not detached",
		"djob_state_probe_success":                   "Whether the last completed probe returned valid complete or limited observations",
		"djob_state_sample_age_seconds":              "Monotonic age since the last valid probe began; positive infinity before any sample",
		"djob_state_probe_duration_seconds":          "Duration of the last completed durable-state inventory probe",
		"djob_state_stats_applied_index":             "Local application index of the cached durable-state inventory transaction",
	} {
		c.descs[name] = prometheus.NewDesc(name, help, nil, nil)
	}
	for name, help := range map[string]string{
		"djob_state_sample_complete":      "Whether the cached probe exhausted this kind's primary bucket",
		"djob_state_sample_rows_observed": "Primary rows verified in the cached prefix; a lower bound when incomplete",
		"djob_state_sample_decoded_bytes": "Serialized primary-record bytes decoded in this kind's cached sample",
	} {
		c.descs[name] = prometheus.NewDesc(name, help, []string{"kind"}, nil)
	}
	c.descs["djob_release_inventory_observed"] = prometheus.NewDesc("djob_release_inventory_observed", "Published release catalog and nonterminal pinned runs in bounded cached samples; lower bounds when corresponding samples are incomplete", []string{"state"}, nil)
	c.descs["djob_state_inventory"] = prometheus.NewDesc("djob_state_inventory", "Retained primary records by durable state at the sampled index; absent for incomplete kinds", []string{"kind", "state"}, nil)
	c.descs["djob_state_inventory_observed"] = prometheus.NewDesc("djob_state_inventory_observed", "Verified primary records by durable state in the sampled prefix; a lower bound when incomplete", []string{"kind", "state"}, nil)
	c.descs["djob_state_probe_results_total"] = prometheus.NewDesc("djob_state_probe_results_total", "Completed durable-state inventory probes by fixed result category", []string{"result"}, nil)
	return c
}

func (c *stateMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.descs {
		ch <- desc
	}
}

func (c *stateMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.monitor.observation()
	emit := func(name string, kind prometheus.ValueType, value float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(c.descs[name], kind, value, labels...)
	}
	bit := func(b bool) float64 {
		if b {
			return 1
		}
		return 0
	}
	emit("djob_state_monitor_running", prometheus.GaugeValue, bit(s.running))
	emit("djob_state_sample_interval_seconds", prometheus.GaugeValue, c.monitor.cfg.SampleInterval.Seconds())
	emit("djob_state_sample_row_limit", prometheus.GaugeValue, float64(c.monitor.cfg.ScanLimit))
	emit("djob_state_sample_byte_limit", prometheus.GaugeValue, db.MaxStateSampleBytes)
	emit("djob_state_probe_timeout_seconds", prometheus.GaugeValue, StateProbeTimeout.Seconds())
	emit("djob_state_probe_success", prometheus.GaugeValue, bit(s.lastOK))
	age := math.Inf(1)
	if s.haveSample {
		age = time.Since(s.sampledAt).Seconds()
	}
	emit("djob_state_sample_age_seconds", prometheus.GaugeValue, age)
	emit("djob_state_probe_duration_seconds", prometheus.GaugeValue, s.duration.Seconds())
	for result, count := range s.results {
		emit("djob_state_probe_results_total", prometheus.CounterValue, float64(count), stateProbeResults[result])
	}
	if !s.haveSample {
		return
	}
	emit("djob_release_sample_complete", prometheus.GaugeValue, bit(s.stats.Releases.DeploymentsComplete))
	r := s.stats.Releases
	for state, n := range map[string]uint64{"active": r.Active, "retiring": r.Retiring, "retired": r.Retired, "outstanding": r.Outstanding, "unknown": r.Unknown} {
		emit("djob_release_inventory_observed", prometheus.GaugeValue, float64(n), state)
	}
	retirementAge := 0.0
	if !r.OldestRetiring.IsZero() {
		retirementAge = max(0, time.Since(r.OldestRetiring).Seconds())
	}
	emit("djob_release_oldest_retirement_age_seconds", prometheus.GaugeValue, retirementAge)
	emit("djob_state_stats_applied_index", prometheus.GaugeValue, float64(s.stats.AppliedIndex))
	for kind := db.StateInventoryKind(0); kind < db.StateInventoryKindCount; kind++ {
		sample := s.stats.Kinds[kind]
		emit("djob_state_sample_complete", prometheus.GaugeValue, bit(sample.Complete), kind.String())
		emit("djob_state_sample_rows_observed", prometheus.GaugeValue, float64(sample.Observed), kind.String())
		emit("djob_state_sample_decoded_bytes", prometheus.GaugeValue, float64(sample.DecodedBytes), kind.String())
		for _, state := range kind.States() {
			emit("djob_state_inventory_observed", prometheus.GaugeValue, float64(sample.Counts[state]), kind.String(), state.String())
			if sample.Complete {
				emit("djob_state_inventory", prometheus.GaugeValue, float64(sample.Counts[state]), kind.String(), state.String())
			}
		}
	}
}
