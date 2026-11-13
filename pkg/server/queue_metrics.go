package server

import (
	"math"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/prometheus/client_golang/prometheus"
)

type queueMetricsCollector struct {
	monitor *queueMonitor
	descs   map[string]*prometheus.Desc
}

func newQueueMetricsCollector(m *queueMonitor) *queueMetricsCollector {
	c := &queueMetricsCollector{monitor: m, descs: make(map[string]*prometheus.Desc)}
	for name, help := range map[string]string{
		"djob_queue_monitor_running":         "Whether the local bounded ready-inventory sampler is running",
		"djob_queue_sample_interval_seconds": "Configured ready-inventory sample interval",
		"djob_queue_sample_row_limit":        "Maximum records decoded per execution profile per ready-inventory sample",
		"djob_queue_sample_byte_limit":       "Maximum serialized record bytes decoded per execution profile per sample",
		"djob_queue_probe_timeout_seconds":   "Cooperative ready-inventory probe timeout; an OS page fault is not detached",
		"djob_queue_probe_success":           "Whether the last completed probe returned valid complete or limited observations",
		"djob_queue_sample_age_seconds":      "Monotonic age since the last valid probe began; positive infinity before any sample",
		"djob_queue_probe_duration_seconds":  "Duration of the last completed ready-inventory probe",
		"djob_queue_stats_applied_index":     "Locally applied index observed with the cached ready-inventory transaction",
	} {
		c.descs[name] = prometheus.NewDesc(name, help, nil, nil)
	}
	for name, help := range map[string]string{
		"djob_queue_oldest_observed_ready_age_seconds":   "Maximum known age since latest ready transition in sampled prefix; lower bound when incomplete",
		"djob_queue_oldest_ready_age_seconds":            "Oldest latest-ready age at sampling; absent when incomplete or timestamps are unknown; zero for empty queue",
		"djob_queue_missing_ready_times_observed":        "Ready rows with unknown latest ready timestamps in sampled prefix",
		"djob_queue_future_ready_times_observed":         "Ready rows with latest ready timestamps after probe time in sampled prefix",
		"djob_queue_sample_complete":                     "Whether the cached sample exhausted this profile's ready index",
		"djob_queue_ready_runs_observed":                 "Ready rows verified in the cached prefix; a lower bound when the sample is limited",
		"djob_queue_ready_runs":                          "Ready rows at the sampled index; absent when its sample is incomplete",
		"djob_queue_sample_decoded_bytes":                "Serialized ready-record bytes decoded in this cached profile sample",
		"djob_queue_missing_creation_times_observed":     "Ready rows in the sampled prefix with missing creation times",
		"djob_queue_future_creation_times_observed":      "Ready rows in the sampled prefix created after the probe's wall timestamp",
		"djob_queue_oldest_observed_created_age_seconds": "Maximum known nonnegative creation age in the sampled ready prefix; zero for a complete empty queue",
		"djob_queue_oldest_ready_created_age_seconds":    "Oldest ready run's creation age at sampling; absent if incomplete or its origin is unknown; zero for an empty queue",
	} {
		c.descs[name] = prometheus.NewDesc(name, help, []string{"profile"}, nil)
	}
	c.descs["djob_queue_probe_results_total"] = prometheus.NewDesc("djob_queue_probe_results_total", "Completed ready-inventory probes by fixed result category", []string{"result"}, nil)
	return c
}

func (c *queueMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.descs {
		ch <- desc
	}
}

func (c *queueMetricsCollector) Collect(ch chan<- prometheus.Metric) {
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
	emit("djob_queue_monitor_running", prometheus.GaugeValue, bit(s.running))
	emit("djob_queue_sample_interval_seconds", prometheus.GaugeValue, c.monitor.cfg.SampleInterval.Seconds())
	emit("djob_queue_sample_row_limit", prometheus.GaugeValue, float64(c.monitor.cfg.ScanLimit))
	emit("djob_queue_sample_byte_limit", prometheus.GaugeValue, db.MaxQueueSampleBytes)
	emit("djob_queue_probe_timeout_seconds", prometheus.GaugeValue, QueueProbeTimeout.Seconds())
	emit("djob_queue_probe_success", prometheus.GaugeValue, bit(s.lastOK))
	age := math.Inf(1)
	if s.haveSample {
		age = time.Since(s.sampledAt).Seconds()
	}
	emit("djob_queue_sample_age_seconds", prometheus.GaugeValue, age)
	emit("djob_queue_probe_duration_seconds", prometheus.GaugeValue, s.duration.Seconds())
	for result, count := range s.results {
		emit("djob_queue_probe_results_total", prometheus.CounterValue, float64(count), queueProbeResults[result])
	}
	if !s.haveSample {
		return // Unknown inventory is not an empty queue.
	}
	emit("djob_queue_stats_applied_index", prometheus.GaugeValue, float64(s.stats.AppliedIndex))
	for _, profile := range []struct {
		name  string
		stats db.ReadyQueueStats
	}{{"gateway", s.stats.Gateway}, {"generic", s.stats.Generic}} {
		q := profile.stats
		emit("djob_queue_missing_ready_times_observed", prometheus.GaugeValue, float64(q.MissingReadySince), profile.name)
		emit("djob_queue_future_ready_times_observed", prometheus.GaugeValue, float64(q.FutureReadySince), profile.name)
		readyAge := s.stats.ObservedAt.Sub(q.OldestReadySince).Seconds()
		readyKnown := !q.OldestReadySince.IsZero() && readyAge >= 0
		if q.Complete && q.Observed == 0 {
			readyAge, readyKnown = 0, true
		}
		if readyKnown {
			emit("djob_queue_oldest_observed_ready_age_seconds", prometheus.GaugeValue, readyAge, profile.name)
			if q.Complete && q.MissingReadySince == 0 && q.FutureReadySince == 0 {
				emit("djob_queue_oldest_ready_age_seconds", prometheus.GaugeValue, readyAge, profile.name)
			}
		}
		emit("djob_queue_sample_complete", prometheus.GaugeValue, bit(q.Complete), profile.name)
		emit("djob_queue_ready_runs_observed", prometheus.GaugeValue, float64(q.Observed), profile.name)
		emit("djob_queue_sample_decoded_bytes", prometheus.GaugeValue, float64(q.DecodedBytes), profile.name)
		emit("djob_queue_missing_creation_times_observed", prometheus.GaugeValue, float64(q.MissingCreatedAt), profile.name)
		emit("djob_queue_future_creation_times_observed", prometheus.GaugeValue, float64(q.FutureCreatedAt), profile.name)
		if q.Complete {
			emit("djob_queue_ready_runs", prometheus.GaugeValue, float64(q.Observed), profile.name)
		}
		creationAge := s.stats.ObservedAt.Sub(q.OldestCreatedAt).Seconds()
		known := !q.OldestCreatedAt.IsZero() && creationAge >= 0
		if q.Complete && q.Observed == 0 {
			creationAge, known = 0, true
		}
		if known {
			emit("djob_queue_oldest_observed_created_age_seconds", prometheus.GaugeValue, creationAge, profile.name)
			if q.Complete && q.MissingCreatedAt == 0 {
				emit("djob_queue_oldest_ready_created_age_seconds", prometheus.GaugeValue, creationAge, profile.name)
			}
		}
	}
}
