package server

import "github.com/prometheus/client_golang/prometheus"

type clockMetricsCollector struct {
	clock *clockMonitor
	descs map[string]*prometheus.Desc
}

func newClockMetricsCollector(clock *clockMonitor) *clockMetricsCollector {
	c := &clockMetricsCollector{clock: clock, descs: make(map[string]*prometheus.Desc)}
	for name, help := range map[string]string{
		"djob_clock_monitor_running":               "Whether the local periodic clock monitor is running",
		"djob_clock_samples_total":                 "Local clock samples, including the initial baseline",
		"djob_clock_sample_age_seconds":            "Monotonic time since the last clock sample",
		"djob_clock_last_sample_timestamp_seconds": "Host wall time of the last clock sample; not a trusted UTC reference",
		"djob_clock_offset_seconds":                "Wall elapsed time minus monotonic elapsed time since this monitor's baseline; not a UTC offset",
		"djob_clock_step_seconds":                  "Wall elapsed time minus monotonic elapsed time between the last two samples",
		"djob_clock_sample_interval_seconds":       "Configured local clock sample interval",
		"djob_clock_jump_threshold_seconds":        "Absolute per-sample discrepancy that counts as a clock discontinuity",
	} {
		c.descs[name] = prometheus.NewDesc(name, help, nil, nil)
	}
	c.descs["djob_clock_discontinuities_total"] = prometheus.NewDesc("djob_clock_discontinuities_total", "Local clock sample intervals whose discrepancy reached the configured threshold", []string{"direction"}, nil)
	return c
}

func (c *clockMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.descs {
		ch <- desc
	}
}

func (c *clockMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	observed := c.clock.observation()
	emit := func(name string, kind prometheus.ValueType, value float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(c.descs[name], kind, value, labels...)
	}
	var running float64
	if observed.running {
		running = 1
	}
	emit("djob_clock_monitor_running", prometheus.GaugeValue, running)
	emit("djob_clock_samples_total", prometheus.CounterValue, float64(observed.samples))
	emit("djob_clock_sample_age_seconds", prometheus.GaugeValue, observed.age.Seconds())
	emit("djob_clock_last_sample_timestamp_seconds", prometheus.GaugeValue, float64(observed.lastWall.Unix())+float64(observed.lastWall.Nanosecond())/1e9)
	emit("djob_clock_offset_seconds", prometheus.GaugeValue, observed.offset.Seconds())
	emit("djob_clock_step_seconds", prometheus.GaugeValue, observed.step.Seconds())
	emit("djob_clock_sample_interval_seconds", prometheus.GaugeValue, c.clock.cfg.SampleInterval.Seconds())
	emit("djob_clock_jump_threshold_seconds", prometheus.GaugeValue, c.clock.cfg.JumpThreshold.Seconds())
	emit("djob_clock_discontinuities_total", prometheus.CounterValue, float64(observed.forward), "forward")
	emit("djob_clock_discontinuities_total", prometheus.CounterValue, float64(observed.backward), "backward")
}
