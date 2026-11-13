package server

import (
	"time"

	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/jobs"
	"github.com/duravela/djob/pkg/scheduler"
	"github.com/prometheus/client_golang/prometheus"
)

type schedulingMetrics struct {
	queueAge         *prometheus.HistogramVec
	readyAdmission   *prometheus.HistogramVec
	admission        *prometheus.HistogramVec
	materialization  *prometheus.HistogramVec
	retryRelease     *prometheus.HistogramVec
	timerWake        *prometheus.HistogramVec
	eventConsumption *prometheus.HistogramVec
	discarded        *prometheus.CounterVec
}

func newSchedulingMetrics(reg prometheus.Registerer) *schedulingMetrics {
	histogram := func(name, help string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: name, Help: help,
			Buckets: []float64{.001, .01, .1, 1, 10, 60, 300, 1800, 7200, 86400, 604800, 2592000},
		}, []string{"profile"})
	}
	m := &schedulingMetrics{
		readyAdmission: histogram("djob_ready_to_admission_seconds", "Wall time from latest durable ready transition to verified claim acknowledgement, including resumes and retries"),
		queueAge: histogram("djob_queue_age_at_first_admission_seconds",
			"Wall time from durable run creation to local acknowledgement of its first claim; excludes rejected claims and resumes"),
		admission: histogram("djob_schedule_lateness_at_first_admission_seconds",
			"Wall time from a calendar occurrence to local acknowledgement of its first claim; does not confirm I/O started"),
		materialization: histogram("djob_schedule_materialization_lateness_seconds",
			"Wall time from a created calendar occurrence to local acknowledgement of its materialization"),
		retryRelease: histogram("djob_retry_release_lateness_seconds",
			"Wall time from a persisted retry due instant to local acknowledgement of ready work; excludes subsequent admission"),
		timerWake: histogram("djob_timer_wake_lateness_seconds",
			"Wall time from a resource-free generic wait timer to local acknowledgement of its ready transition; excludes subsequent admission"),
		eventConsumption: histogram("djob_event_consumption_delay_seconds",
			"Wall time from a persisted generic inbox event to local acknowledgement of the decision consuming it; includes time queued before a wait"),
		discarded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "djob_scheduling_timing_discarded_total", Help: "Scheduling timing samples omitted because their origin timestamp is missing or in the future",
		}, []string{"profile", "component", "result"}),
	}
	for _, profile := range []string{"gateway", "generic"} {
		m.readyAdmission.WithLabelValues(profile)
		m.queueAge.WithLabelValues(profile)
		m.admission.WithLabelValues(profile)
		m.materialization.WithLabelValues(profile)
		m.retryRelease.WithLabelValues(profile)
		m.timerWake.WithLabelValues(profile)
		m.eventConsumption.WithLabelValues(profile)
		for _, component := range []string{"ready_admission", "queue_age", "scheduled_admission", "materialization", "retry_release", "timer_wake", "event_consumption"} {
			for _, result := range []string{"missing", "future"} {
				m.discarded.WithLabelValues(profile, component, result)
			}
		}
	}
	reg.MustRegister(m.readyAdmission, m.queueAge, m.admission, m.materialization, m.retryRelease, m.timerWake, m.eventConsumption, m.discarded)
	return m
}

func (m *schedulingMetrics) observe(histogram *prometheus.HistogramVec, profile, component string, age time.Duration, known bool) {
	if !known {
		m.discarded.WithLabelValues(profile, component, "missing").Inc()
		return
	}
	if age < 0 {
		m.discarded.WithLabelValues(profile, component, "future").Inc()
		return
	}
	histogram.WithLabelValues(profile).Observe(age.Seconds())
}

func (m *schedulingMetrics) admitted(profile string, age time.Duration, queueKnown, scheduled bool, lateness time.Duration, scheduleKnown bool) {
	m.observe(m.queueAge, profile, "queue_age", age, queueKnown)
	if scheduled {
		m.observe(m.admission, profile, "scheduled_admission", lateness, scheduleKnown)
	}
}

func (s *topLevelServer) instrumentScheduling() {
	m := s.metrics.scheduling
	previousReadyGateway := s.cfg.Dispatcher.Executor.OnReadyAdmission
	s.cfg.Dispatcher.Executor.OnReadyAdmission = func(age time.Duration, known bool) {
		m.observe(m.readyAdmission, "gateway", "ready_admission", age, known)
		if previousReadyGateway != nil {
			previousReadyGateway(age, known)
		}
	}
	previousReadyGeneric := s.cfg.JobWorker.OnReadyAdmission
	s.cfg.JobWorker.OnReadyAdmission = func(age time.Duration, known bool) {
		m.observe(m.readyAdmission, "generic", "ready_admission", age, known)
		if previousReadyGeneric != nil {
			previousReadyGeneric(age, known)
		}
	}
	previousGateway := s.cfg.Dispatcher.Executor.OnAdmission
	s.cfg.Dispatcher.Executor.OnAdmission = func(o executor.AdmissionObservation) {
		m.admitted("gateway", o.QueueAge, o.QueueTimeKnown, o.Scheduled, o.ScheduleLateness, o.ScheduleTimeKnown)
		if previousGateway != nil {
			previousGateway(o)
		}
	}
	previousGeneric := s.cfg.JobWorker.OnAdmission
	s.cfg.JobWorker.OnAdmission = func(o jobs.AdmissionObservation) {
		m.admitted("generic", o.QueueAge, o.QueueTimeKnown, o.Scheduled, o.ScheduleLateness, o.ScheduleTimeKnown)
		if previousGeneric != nil {
			previousGeneric(o)
		}
	}
	previousEvent := s.cfg.JobWorker.OnEventConsumed
	s.cfg.JobWorker.OnEventConsumed = func(o jobs.EventConsumptionObservation) {
		m.observe(m.eventConsumption, "generic", "event_consumption", o.Delay, o.TimeKnown)
		if previousEvent != nil {
			previousEvent(o)
		}
	}
	wrapMaterialization := func(profile string, previous func(scheduler.MaterializationObservation)) func(scheduler.MaterializationObservation) {
		return func(o scheduler.MaterializationObservation) {
			m.observe(m.materialization, profile, "materialization", o.Lateness, o.TimeKnown)
			if previous != nil {
				previous(o)
			}
		}
	}
	s.cfg.Materializer.OnMaterialized = wrapMaterialization("gateway", s.cfg.Materializer.OnMaterialized)
	s.cfg.JobTriggers.OnMaterialized = wrapMaterialization("generic", s.cfg.JobTriggers.OnMaterialized)
	previousRetry := s.cfg.Retries.OnReleased
	s.cfg.Retries.OnReleased = func(o scheduler.DueWorkObservation) {
		m.observe(m.retryRelease, "gateway", "retry_release", o.Lateness, o.TimeKnown)
		if previousRetry != nil {
			previousRetry(o)
		}
	}
	previousWake := s.cfg.JobMaintenance.OnWoken
	s.cfg.JobMaintenance.OnWoken = func(o scheduler.DueWorkObservation) {
		m.observe(m.timerWake, "generic", "timer_wake", o.Lateness, o.TimeKnown)
		if previousWake != nil {
			previousWake(o)
		}
	}
}
