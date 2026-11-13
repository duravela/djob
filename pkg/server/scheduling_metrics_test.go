package server

import (
	"testing"
	"time"

	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/duravela/djob/pkg/jobs"
	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/duravela/djob/pkg/scheduler"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func timingHistogram(t *testing.T, families map[string]*dto.MetricFamily, name, profile string) *dto.Histogram {
	t.Helper()
	family := families[name]
	require.NotNil(t, family, name)
	for _, metric := range family.Metric {
		if len(metric.Label) == 1 && metric.Label[0].GetName() == "profile" && metric.Label[0].GetValue() == profile {
			return metric.Histogram
		}
	}
	t.Fatalf("missing %s for %s", name, profile)
	return nil
}

func TestSchedulingTimingMetricsMeasureBothProfilesAndPreserveCallbacks(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	gateway := fake.New(fake.Options{})
	cfg.TransportFactory = gateway.Factory
	cfg.JobRegistry = func(JobBackend) (jobs.Registry, error) { return jobs.Builtins(), nil }
	cfg.JobTriggers.PollInterval, cfg.JobDispatch.PollInterval = 5*time.Millisecond, 5*time.Millisecond
	legacyAdmission := make(chan executor.AdmissionObservation, 4)
	genericAdmission := make(chan jobs.AdmissionObservation, 4)
	legacyMaterialization := make(chan scheduler.MaterializationObservation, 4)
	genericMaterialization := make(chan scheduler.MaterializationObservation, 4)
	cfg.Dispatcher.Executor.OnAdmission = func(o executor.AdmissionObservation) { legacyAdmission <- o }
	cfg.JobWorker.OnAdmission = func(o jobs.AdmissionObservation) { genericAdmission <- o }
	cfg.Materializer.OnMaterialized = func(o scheduler.MaterializationObservation) { legacyMaterialization <- o }
	cfg.JobTriggers.OnMaterialized = func(o scheduler.MaterializationObservation) { genericMaterialization <- o }
	s := startAPI(t, cfg, gateway)
	c := apiClient(t, s)
	generic := genericClient(t, s)
	families := scrapeMetrics(t, s)
	for _, profile := range []string{"gateway", "generic"} {
		for _, name := range []string{"djob_queue_age_at_first_admission_seconds", "djob_schedule_lateness_at_first_admission_seconds", "djob_schedule_materialization_lateness_seconds"} {
			require.Zero(t, timingHistogram(t, families, name, profile).GetSampleCount())
		}
	}
	_, err := c.ConfigureChannel(genericContext("writer"), &pb.ConfigureChannelRequest{
		Command: meta("timing-channel"), Id: "gateway", GatewayId: "gateway", Protocol: "fake", Endpoint: "private-endpoint:502",
	})
	require.NoError(t, err)
	due := time.Now().UTC().Add(-30 * time.Second)
	_, err = c.PutSchedule(genericContext("writer"), apiSchedule("timing-once", due))
	require.NoError(t, err)
	call := apiCall(t, gateway)
	select {
	case o := <-legacyAdmission:
		require.True(t, o.QueueTimeKnown && o.Scheduled && o.ScheduleTimeKnown)
		require.GreaterOrEqual(t, o.ScheduleLateness, 30*time.Second)
		require.Greater(t, o.ScheduleLateness, o.QueueAge)
	case <-time.After(3 * time.Second):
		t.Fatal("gateway admission callback did not run")
	}
	tenantAPI(t, generic, "alpha")
	definitionAPI(t, generic, "alpha", "hash", "sha256", "", []byte("private input"))
	mutateJob(t, generic, "alpha/writer", triggerAPI("timing-trigger", "hash", due))
	select {
	case o := <-genericAdmission:
		require.True(t, o.QueueTimeKnown && o.Scheduled && o.ScheduleTimeKnown)
		require.GreaterOrEqual(t, o.ScheduleLateness, 30*time.Second)
		require.Greater(t, o.ScheduleLateness, o.QueueAge)
	case <-time.After(3 * time.Second):
		t.Fatal("generic admission callback did not run")
	}
	for _, observations := range []<-chan scheduler.MaterializationObservation{legacyMaterialization, genericMaterialization} {
		select {
		case o := <-observations:
			require.True(t, o.TimeKnown)
			require.GreaterOrEqual(t, o.Lateness, 30*time.Second)
		case <-time.After(3 * time.Second):
			t.Fatal("materialization callback did not run")
		}
	}
	families = scrapeMetrics(t, s)
	for _, profile := range []string{"gateway", "generic"} {
		queue := timingHistogram(t, families, "djob_queue_age_at_first_admission_seconds", profile)
		ready := timingHistogram(t, families, "djob_ready_to_admission_seconds", profile)
		require.EqualValues(t, 1, ready.GetSampleCount())
		require.Positive(t, ready.GetSampleSum())
		admitted := timingHistogram(t, families, "djob_schedule_lateness_at_first_admission_seconds", profile)
		materialized := timingHistogram(t, families, "djob_schedule_materialization_lateness_seconds", profile)
		require.EqualValues(t, 1, queue.GetSampleCount())
		require.Positive(t, queue.GetSampleSum())
		require.EqualValues(t, 1, admitted.GetSampleCount())
		require.GreaterOrEqual(t, admitted.GetSampleSum(), float64(30))
		require.EqualValues(t, 1, materialized.GetSampleCount())
		require.GreaterOrEqual(t, materialized.GetSampleSum(), float64(30))
	}
	finishGatewayCall(call, "done")
	// Timing is recorded at admission; finishing the invocation adds no sample.
	require.Eventually(t, func() bool { return s.dispatcher.Pending() == 0 }, 3*time.Second, time.Millisecond)
	require.EqualValues(t, 1, timingHistogram(t, scrapeMetrics(t, s), "djob_queue_age_at_first_admission_seconds", "gateway").GetSampleCount())
	require.Empty(t, legacyAdmission)
	require.Empty(t, genericAdmission)
}

func TestSchedulingTimingMetricsDiscardFutureAndMissingTimesWithoutChangingHistograms(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newSchedulingMetrics(reg)
	m.admitted("generic", -time.Second, true, true, 2*time.Second, true)
	m.admitted("generic", 0, false, true, -time.Second, true)
	m.admitted("gateway", 0, true, false, 0, false)
	m.observe(m.materialization, "generic", "materialization", -time.Second, true)
	m.observe(m.materialization, "gateway", "materialization", 0, false)
	m.observe(m.materialization, "generic", "materialization", 0, true)
	gathered, err := reg.Gather()
	require.NoError(t, err)
	families := make(map[string]*dto.MetricFamily)
	for _, family := range gathered {
		families[family.GetName()] = family
	}
	require.Zero(t, timingHistogram(t, families, "djob_queue_age_at_first_admission_seconds", "generic").GetSampleCount())
	require.EqualValues(t, 1, timingHistogram(t, families, "djob_queue_age_at_first_admission_seconds", "gateway").GetSampleCount())
	require.EqualValues(t, 1, timingHistogram(t, families, "djob_schedule_lateness_at_first_admission_seconds", "generic").GetSampleCount())
	require.Equal(t, float64(2), timingHistogram(t, families, "djob_schedule_lateness_at_first_admission_seconds", "generic").GetSampleSum())
	require.Zero(t, timingHistogram(t, families, "djob_schedule_lateness_at_first_admission_seconds", "gateway").GetSampleCount())
	require.EqualValues(t, 1, timingHistogram(t, families, "djob_schedule_materialization_lateness_seconds", "generic").GetSampleCount())
	require.Zero(t, timingHistogram(t, families, "djob_schedule_materialization_lateness_seconds", "generic").GetSampleSum())
	require.Zero(t, timingHistogram(t, families, "djob_schedule_materialization_lateness_seconds", "gateway").GetSampleCount())
	for _, labels := range [][]string{
		{"profile", "generic", "component", "queue_age", "result", "future"},
		{"profile", "generic", "component", "queue_age", "result", "missing"},
		{"profile", "generic", "component", "scheduled_admission", "result", "future"},
		{"profile", "generic", "component", "materialization", "result", "future"},
		{"profile", "gateway", "component", "materialization", "result", "missing"},
	} {
		require.Equal(t, float64(1), metricValue(t, families, "djob_scheduling_timing_discarded_total", labels...))
	}
	require.Len(t, families["djob_scheduling_timing_discarded_total"].Metric, 28)
}

func TestDueWorkTimingMetricsPreserveCallbacksAndDiscardInvalidOrigins(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newSchedulingMetrics(reg)
	s := &topLevelServer{metrics: &runtimeMetrics{scheduling: m}}
	var released, woken []scheduler.DueWorkObservation
	s.cfg.Retries.OnReleased = func(o scheduler.DueWorkObservation) { released = append(released, o) }
	s.cfg.JobMaintenance.OnWoken = func(o scheduler.DueWorkObservation) { woken = append(woken, o) }
	s.instrumentScheduling()
	observations := []scheduler.DueWorkObservation{
		{Lateness: 2 * time.Second, TimeKnown: true},
		{Lateness: 0, TimeKnown: true},
		{Lateness: -time.Second, TimeKnown: true},
		{Lateness: 0, TimeKnown: false},
	}
	for _, o := range observations {
		s.cfg.Retries.OnReleased(o)
		s.cfg.JobMaintenance.OnWoken(o)
	}
	require.Equal(t, observations, released)
	require.Equal(t, observations, woken, "embedding callbacks also receive the original diagnostic presence flags")
	gathered, err := reg.Gather()
	require.NoError(t, err)
	families := make(map[string]*dto.MetricFamily)
	for _, family := range gathered {
		families[family.GetName()] = family
	}
	for _, item := range []struct{ name, profile, component, other string }{
		{"djob_retry_release_lateness_seconds", "gateway", "retry_release", "generic"},
		{"djob_timer_wake_lateness_seconds", "generic", "timer_wake", "gateway"},
	} {
		histogram := timingHistogram(t, families, item.name, item.profile)
		require.EqualValues(t, 2, histogram.GetSampleCount())
		require.Equal(t, float64(2), histogram.GetSampleSum())
		require.Zero(t, timingHistogram(t, families, item.name, item.other).GetSampleCount())
		for _, reason := range []string{"missing", "future"} {
			require.Equal(t, float64(1), metricValue(t, families, "djob_scheduling_timing_discarded_total", "profile", item.profile, "component", item.component, "result", reason))
		}
	}
}

func TestEventConsumptionMetricsPreserveCallbackAndRejectInvalidOrigins(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newSchedulingMetrics(reg)
	s := &topLevelServer{metrics: &runtimeMetrics{scheduling: m}}
	var seen []jobs.EventConsumptionObservation
	s.cfg.JobWorker.OnEventConsumed = func(o jobs.EventConsumptionObservation) { seen = append(seen, o) }
	s.instrumentScheduling()
	observations := []jobs.EventConsumptionObservation{
		{Delay: 2 * time.Second, TimeKnown: true},
		{TimeKnown: true},
		{Delay: -time.Second, TimeKnown: true},
		{},
	}
	for _, o := range observations {
		s.cfg.JobWorker.OnEventConsumed(o)
	}
	require.Equal(t, observations, seen)
	gathered, err := reg.Gather()
	require.NoError(t, err)
	families := make(map[string]*dto.MetricFamily)
	for _, family := range gathered {
		families[family.GetName()] = family
	}
	h := timingHistogram(t, families, "djob_event_consumption_delay_seconds", "generic")
	require.EqualValues(t, 2, h.GetSampleCount(), "retain a valid zero delay, omit missing and future origins")
	require.Equal(t, float64(2), h.GetSampleSum())
	require.Zero(t, timingHistogram(t, families, "djob_event_consumption_delay_seconds", "gateway").GetSampleCount())
	for _, reason := range []string{"missing", "future"} {
		require.Equal(t, float64(1), metricValue(t, families, "djob_scheduling_timing_discarded_total", "profile", "generic", "component", "event_consumption", "result", reason))
	}
}
