package server

import (
	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/jobs"
	"github.com/prometheus/client_golang/prometheus"
)

type actionMetrics struct {
	results *prometheus.CounterVec
	driver  *prometheus.HistogramVec
	record  *prometheus.HistogramVec
}

func newActionMetrics(reg prometheus.Registerer) *actionMetrics {
	buckets := []float64{.001, .01, .1, 1, 10, 60, 300, 1800, 7200}
	m := &actionMetrics{
		results: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "djob_job_action_results_total", Help: "Local generic action recording attempts by prepared effect status and acknowledgement; not unique durable effects",
		}, []string{"state", "result"}),
		driver: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "djob_job_action_driver_duration_seconds", Help: "Local generic Driver.Execute duration, including any cleanup inside that method; excludes factories and outcome recording",
			Buckets: buckets,
		}, []string{"state"}),
		record: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "djob_job_action_record_duration_seconds", Help: "Local generic action outcome recording duration through retries and receipt checks; excludes driver I/O",
			Buckets: buckets,
		}, []string{"result"}),
	}
	for _, state := range []string{string(db.EffectSucceeded), string(db.EffectFailed), string(db.EffectUnknown)} {
		m.driver.WithLabelValues(state)
		for _, result := range []string{"confirmed", "unconfirmed"} {
			m.results.WithLabelValues(state, result)
		}
	}
	for _, result := range []string{"confirmed", "unconfirmed"} {
		m.record.WithLabelValues(result)
	}
	reg.MustRegister(m.results, m.driver, m.record)
	return m
}

func (m *actionMetrics) observe(o jobs.ActionObservation) {
	state := "unknown"
	switch o.Status {
	case db.EffectSucceeded, db.EffectFailed, db.EffectUnknown:
		state = string(o.Status)
	}
	result := "unconfirmed"
	if o.RecordConfirmed {
		result = "confirmed"
	}
	m.results.WithLabelValues(state, result).Inc()
	if o.DriverInvoked {
		m.driver.WithLabelValues(state).Observe(o.DriverDuration.Seconds())
	}
	m.record.WithLabelValues(result).Observe(o.RecordDuration.Seconds())
}
