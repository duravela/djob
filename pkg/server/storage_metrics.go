package server

import (
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type storageMetricsCollector struct {
	monitor *storageMonitor
	descs   map[string]*prometheus.Desc
}

func newStorageMetricsCollector(m *storageMonitor) *storageMetricsCollector {
	c := &storageMetricsCollector{monitor: m, descs: make(map[string]*prometheus.Desc)}
	for name, help := range map[string]string{
		"djob_storage_monitor_running":         "Whether the local background storage sampler is running",
		"djob_storage_sample_interval_seconds": "Configured storage sample interval",
		"djob_store_file_bytes":                "Last sampled materialized database file length; not allocated or live payload bytes",
		"djob_store_stats_applied_index":       "Applied index observed with the cached database metadata",
		"djob_store_page_size_bytes":           "Database page size",
		"djob_store_free_pages":                "Database free pages available for reuse",
		"djob_store_pending_pages":             "Database pages waiting for outstanding read transactions before reuse",
		"djob_store_freelist_bytes":            "Database freelist bookkeeping bytes",
		"djob_store_open_read_transactions":    "Open database read transactions observed without creating another transaction",
	} {
		c.descs[name] = prometheus.NewDesc(name, help, nil, nil)
	}
	for name, help := range map[string]string{
		"djob_storage_probe_success":          "Whether the last completed probe succeeded; zero before the first completion",
		"djob_storage_sample_age_seconds":     "Monotonic age since the last successful probe began; positive infinity until first success",
		"djob_storage_probe_duration_seconds": "Duration of the last completed probe",
		"djob_storage_volume_total_bytes":     "Filesystem-reported total capacity at this component's directory",
		"djob_storage_volume_free_bytes":      "Filesystem-reported free bytes, including privileged reserves",
		"djob_storage_volume_available_bytes": "Filesystem-reported free bytes available to an unprivileged user",
	} {
		c.descs[name] = prometheus.NewDesc(name, help, []string{"component"}, nil)
	}
	c.descs["djob_storage_probe_results_total"] = prometheus.NewDesc("djob_storage_probe_results_total", "Completed storage probes by fixed component and result", []string{"component", "result"}, nil)
	return c
}

func (c *storageMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.descs {
		ch <- desc
	}
}

func (c *storageMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	observed := c.monitor.observation()
	now := time.Now()
	emit := func(name string, kind prometheus.ValueType, value float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(c.descs[name], kind, value, labels...)
	}
	var running float64
	if observed.running {
		running = 1
	}
	emit("djob_storage_monitor_running", prometheus.GaugeValue, running)
	emit("djob_storage_sample_interval_seconds", prometheus.GaugeValue, c.monitor.interval.Seconds())
	for component, sample := range observed.components {
		name := storageComponentNames[component]
		var ok float64
		if sample.lastOK {
			ok = 1
		}
		emit("djob_storage_probe_success", prometheus.GaugeValue, ok, name)
		age := math.Inf(1)
		if sample.haveSample {
			age = now.Sub(sample.sampledAt).Seconds()
		}
		emit("djob_storage_sample_age_seconds", prometheus.GaugeValue, age, name)
		emit("djob_storage_probe_duration_seconds", prometheus.GaugeValue, sample.duration.Seconds(), name)
		for result, count := range sample.results {
			emit("djob_storage_probe_results_total", prometheus.CounterValue, float64(count), name, storageResultNames[result])
		}
		if !sample.haveSample {
			continue // An unavailable filesystem is not a zero-capacity volume.
		}
		if storageComponent(component) == storageDatabase {
			stats := sample.value.database
			emit("djob_store_stats_applied_index", prometheus.GaugeValue, float64(stats.AppliedIndex))
			emit("djob_store_page_size_bytes", prometheus.GaugeValue, float64(stats.PageSize))
			emit("djob_store_free_pages", prometheus.GaugeValue, float64(stats.FreePages))
			emit("djob_store_pending_pages", prometheus.GaugeValue, float64(stats.PendingPages))
			emit("djob_store_freelist_bytes", prometheus.GaugeValue, float64(stats.FreelistBytes))
			emit("djob_store_open_read_transactions", prometheus.GaugeValue, float64(stats.OpenReadTransactions))
		} else {
			space := sample.value.space
			emit("djob_storage_volume_total_bytes", prometheus.GaugeValue, float64(space.total), name)
			emit("djob_storage_volume_free_bytes", prometheus.GaugeValue, float64(space.free), name)
			emit("djob_storage_volume_available_bytes", prometheus.GaugeValue, float64(space.available), name)
			if storageComponent(component) == storageStore {
				emit("djob_store_file_bytes", prometheus.GaugeValue, float64(sample.value.fileSize))
			}
		}
	}
}
