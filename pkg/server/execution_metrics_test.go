package server

import (
	"context"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/jobs"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func executionHistogram(t *testing.T, families map[string]*dto.MetricFamily, profile string) *dto.Histogram {
	t.Helper()
	family := families["djob_execution_duration_seconds"]
	require.NotNil(t, family)
	for _, metric := range family.Metric {
		if len(metric.Label) == 1 && metric.Label[0].GetName() == "profile" && metric.Label[0].GetValue() == profile {
			return metric.Histogram
		}
	}
	t.Fatalf("missing execution histogram for %s", profile)
	return nil
}

func TestExecutionMetricsSeparateConversationWaitAndResumeAndResetPerMember(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.JobDispatch.PollInterval = 5 * time.Millisecond
	observations := make(chan jobs.ExecutionObservation, 8)
	events := make(chan jobs.EventConsumptionObservation, 4)
	cfg.JobWorker.OnEventConsumed = func(o jobs.EventConsumptionObservation) { events <- o }
	cfg.JobWorker.OnExecution = func(o jobs.ExecutionObservation) {
		if o.Phase == jobs.ExecutionReturned {
			observations <- o
		}
	}
	cfg.JobRegistry = func(JobBackend) (jobs.Registry, error) {
		return jobs.Registry{Handlers: map[jobs.Version]jobs.Handler{{Name: "conversation", Version: "1"}: func(v jobs.View) (jobs.Decision, error) {
			if v.Event == nil {
				return jobs.Decision{Kind: db.JobWait, WaitKey: "reply"}, nil
			}
			return jobs.Decision{Kind: db.JobComplete, Status: db.JobSucceeded, Result: v.Event.Data}, nil
		}}}, nil
	}
	a := startAPI(t, cfg)
	other := apiTestConfig(t.TempDir())
	other.APIAuth = genericCredentials()
	b := startAPI(t, other)
	c := genericClient(t, a)
	tenantAPI(t, c, "alpha")
	definitionAPI(t, c, "alpha", "conversation", "conversation", "", nil)
	launchAPI(t, c, "alpha", "conversation", "run")
	waitReturn := func(want string) {
		t.Helper()
		select {
		case o := <-observations:
			require.Equal(t, want, o.Result)
		case <-time.After(3 * time.Second):
			t.Fatal("custom execution observer was not called")
		}
	}
	waitReturn("waiting")
	families := scrapeMetrics(t, a)
	require.Equal(t, float64(1), metricValue(t, families, "djob_execution_started_total", "profile", "generic"))
	require.Zero(t, metricValue(t, families, "djob_execution_in_flight", "profile", "generic"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_execution_returns_total", "profile", "generic", "result", "waiting"))
	require.Zero(t, metricValue(t, families, "djob_execution_returns_total", "profile", "generic", "result", "succeeded"))
	require.EqualValues(t, 1, executionHistogram(t, families, "generic").GetSampleCount())
	require.EqualValues(t, 1, timingHistogram(t, families, "djob_queue_age_at_first_admission_seconds", "generic").GetSampleCount())
	require.Zero(t, timingHistogram(t, families, "djob_schedule_lateness_at_first_admission_seconds", "generic").GetSampleCount(), "unscheduled work has no calendar lateness")
	require.Zero(t, timingHistogram(t, families, "djob_event_consumption_delay_seconds", "generic").GetSampleCount(), "entering a wait alone does not consume an event")
	mutateJob(t, c, "alpha/callback", &jp.MutationRequest{Command: genericMeta("reply"), Operation: &jp.MutationRequest_Signal{Signal: &jp.Signal{RunId: "run", EventId: "reply", Key: "reply", Data: []byte("private reply")}}})
	waitReturn("succeeded")
	select {
	case o := <-events:
		require.True(t, o.TimeKnown)
		require.GreaterOrEqual(t, o.Delay, time.Duration(0))
	case <-time.After(time.Second):
		t.Fatal("custom event-consumption observer was not preserved")
	}
	families = scrapeMetrics(t, a)
	require.Equal(t, float64(2), metricValue(t, families, "djob_execution_started_total", "profile", "generic"))
	require.Zero(t, metricValue(t, families, "djob_execution_in_flight", "profile", "generic"))
	require.Zero(t, metricValue(t, families, "djob_execution_cleanup_failures_total", "profile", "generic"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_execution_returns_total", "profile", "generic", "result", "succeeded"))
	require.EqualValues(t, 2, executionHistogram(t, families, "generic").GetSampleCount())
	require.EqualValues(t, 1, timingHistogram(t, families, "djob_queue_age_at_first_admission_seconds", "generic").GetSampleCount(), "a resume is not another first admission")
	require.Positive(t, executionHistogram(t, families, "generic").GetSampleSum())
	require.EqualValues(t, 1, timingHistogram(t, families, "djob_event_consumption_delay_seconds", "generic").GetSampleCount())
	require.Positive(t, timingHistogram(t, families, "djob_event_consumption_delay_seconds", "generic").GetSampleSum())
	require.Zero(t, timingHistogram(t, scrapeMetrics(t, b), "djob_event_consumption_delay_seconds", "generic").GetSampleCount())
	require.Zero(t, metricValue(t, scrapeMetrics(t, b), "djob_execution_started_total", "profile", "generic"))
	require.Zero(t, timingHistogram(t, scrapeMetrics(t, b), "djob_queue_age_at_first_admission_seconds", "generic").GetSampleCount())
	require.NoError(t, a.Close(context.Background()))
	restarted := startAPI(t, cfg)
	families = scrapeMetrics(t, restarted)
	require.Zero(t, metricValue(t, families, "djob_execution_started_total", "profile", "generic"))
	require.Zero(t, metricValue(t, families, "djob_execution_returns_total", "profile", "generic", "result", "succeeded"))
	require.Zero(t, executionHistogram(t, families, "generic").GetSampleCount())
	require.Zero(t, timingHistogram(t, families, "djob_event_consumption_delay_seconds", "generic").GetSampleCount(), "recovered event history must not reconstruct local samples")
	require.Zero(t, timingHistogram(t, families, "djob_queue_age_at_first_admission_seconds", "generic").GetSampleCount())
}

func TestExecutionMetricsBoundUnexpectedResultsAndRemainIndependent(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newExecutionMetrics(reg)
	m.start("generic")
	m.returned("generic", "private tenant error", time.Second, true)
	gathered, err := reg.Gather()
	require.NoError(t, err)
	families := make(map[string]*dto.MetricFamily)
	for _, family := range gathered {
		families[family.GetName()] = family
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				require.NotContains(t, label.GetValue(), "private")
			}
		}
	}
	require.Len(t, families["djob_execution_returns_total"].Metric, 2*len(executionResults))
	require.Equal(t, float64(1), metricValue(t, families, "djob_execution_returns_total", "profile", "generic", "result", "error"))
	require.Zero(t, metricValue(t, families, "djob_execution_in_flight", "profile", "generic"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_execution_cleanup_failures_total", "profile", "generic"))
	require.Zero(t, metricValue(t, families, "djob_execution_started_total", "profile", "gateway"))
	require.EqualValues(t, 1, executionHistogram(t, families, "generic").GetSampleCount())
}
