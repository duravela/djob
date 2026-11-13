package server

import (
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/jobs"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func actionHistogram(t *testing.T, families map[string]*dto.MetricFamily, name, label, value string) *dto.Histogram {
	t.Helper()
	family := families[name]
	require.NotNil(t, family)
	for _, m := range family.Metric {
		if len(m.Label) == 1 && m.Label[0].GetName() == label && m.Label[0].GetValue() == value {
			return m.Histogram
		}
	}
	t.Fatalf("missing %s{%s=%q}", name, label, value)
	return nil
}

func TestActionMetricsBoundLabelsSeparateStagesAndRemainMemberLocal(t *testing.T) {
	reg, other := prometheus.NewRegistry(), prometheus.NewRegistry()
	m := newActionMetrics(reg)
	newActionMetrics(other)
	m.observe(jobs.ActionObservation{Status: "private protocol outcome", DriverInvoked: true, DriverDuration: 2 * time.Second, RecordDuration: 3 * time.Second})
	m.observe(jobs.ActionObservation{Status: db.EffectFailed, DriverDuration: time.Hour, RecordDuration: time.Second, RecordConfirmed: true})
	gather := func(reg *prometheus.Registry) map[string]*dto.MetricFamily {
		t.Helper()
		values, err := reg.Gather()
		require.NoError(t, err)
		families := make(map[string]*dto.MetricFamily)
		for _, family := range values {
			families[family.GetName()] = family
			for _, metric := range family.Metric {
				for _, label := range metric.Label {
					require.Contains(t, []string{"state", "result"}, label.GetName())
					require.Contains(t, []string{"succeeded", "failed", "unknown", "confirmed", "unconfirmed"}, label.GetValue())
				}
			}
		}
		return families
	}
	families := gather(reg)
	require.Len(t, families, 3)
	require.Len(t, families["djob_job_action_results_total"].Metric, 6)
	require.Len(t, families["djob_job_action_driver_duration_seconds"].Metric, 3)
	require.Len(t, families["djob_job_action_record_duration_seconds"].Metric, 2)
	require.Equal(t, float64(1), metricValue(t, families, "djob_job_action_results_total", "state", "unknown", "result", "unconfirmed"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_job_action_results_total", "state", "failed", "result", "confirmed"))
	require.Equal(t, float64(2), actionHistogram(t, families, "djob_job_action_driver_duration_seconds", "state", "unknown").GetSampleSum())
	require.Zero(t, actionHistogram(t, families, "djob_job_action_driver_duration_seconds", "state", "failed").GetSampleCount(), "factory failures cannot be counted as driver invocations")
	require.Equal(t, float64(1), actionHistogram(t, families, "djob_job_action_record_duration_seconds", "result", "confirmed").GetSampleSum())
	require.Equal(t, float64(3), actionHistogram(t, families, "djob_job_action_record_duration_seconds", "result", "unconfirmed").GetSampleSum())
	fresh := gather(other)
	for _, family := range fresh {
		for _, metric := range family.Metric {
			require.Zero(t, metric.GetCounter().GetValue())
			require.Zero(t, metric.GetHistogram().GetSampleCount())
		}
	}
}
