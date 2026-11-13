package server

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func queueMetricFamilies(t *testing.T, m *queueMonitor) map[string]*dto.MetricFamily {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(newQueueMetricsCollector(m))
	families, err := reg.Gather()
	require.NoError(t, err)
	result := make(map[string]*dto.MetricFamily)
	for _, family := range families {
		result[family.GetName()] = family
	}
	return result
}

func TestQueueMonitorPreservesLastValidSampleAcrossBusyErrorsAndDeadlines(t *testing.T) {
	m := newQueueMonitor(QueueMonitorConfig{SampleInterval: MinQueueSampleInterval, ScanLimit: 2})
	stats := db.QueueStats{AppliedIndex: 7, ObservedAt: time.Now(), Gateway: db.ReadyQueueStats{Complete: true}, Generic: db.ReadyQueueStats{Observed: 2, Complete: true}}
	m.probe = func(context.Context, int) (db.QueueStats, error) { return stats, nil }
	m.sample(context.Background())
	before := m.observation()
	for _, err := range []error{db.ErrQueueStatsBusy, errors.New("private database detail")} {
		m.probe = func(context.Context, int) (db.QueueStats, error) { return db.QueueStats{}, err }
		m.sample(context.Background())
		after := m.observation()
		require.True(t, after.haveSample)
		require.False(t, after.lastOK)
		require.Equal(t, before.stats, after.stats)
		require.Equal(t, before.sampledAt, after.sampledAt)
	}
	m.probe = func(ctx context.Context, _ int) (db.QueueStats, error) {
		<-ctx.Done()
		return stats, nil // A backend ignoring its deadline cannot publish success.
	}
	m.sample(context.Background())
	require.Equal(t, before.sampledAt, m.observation().sampledAt)
	require.EqualValues(t, 1, m.observation().results[queueProbeBusy])
	require.EqualValues(t, 2, m.observation().results[queueProbeError])
	stats.AppliedIndex, stats.Generic.Complete = 8, false
	m.probe = func(context.Context, int) (db.QueueStats, error) { return stats, nil }
	m.sample(context.Background())
	after := m.observation()
	require.True(t, after.lastOK)
	require.EqualValues(t, 1, after.results[queueProbeLimited])
	require.Equal(t, uint64(8), after.stats.AppliedIndex)
	families := queueMetricFamilies(t, m)
	require.Zero(t, metricValue(t, families, "djob_queue_sample_complete", "profile", "generic"))
	require.Equal(t, float64(2), metricValue(t, families, "djob_queue_ready_runs_observed", "profile", "generic"))
	require.Len(t, families["djob_queue_ready_runs"].Metric, 1, "an incomplete profile must not retain its prior exact gauge")
	require.Equal(t, float64(1), metricValue(t, families, "djob_queue_probe_success"), "a valid limited sample is distinct from a failed probe")
}

func TestQueueMetricsDistinguishUnknownEmptyLimitedAndInvalidOrigins(t *testing.T) {
	m := newQueueMonitor(QueueMonitorConfig{SampleInterval: MinQueueSampleInterval, ScanLimit: 2})
	families := queueMetricFamilies(t, m)
	require.Nil(t, families["djob_queue_ready_runs"])
	require.Nil(t, families["djob_queue_stats_applied_index"])
	require.True(t, math.IsInf(metricValue(t, families, "djob_queue_sample_age_seconds"), 1))
	now := time.Now()
	stats := db.QueueStats{ObservedAt: now, Gateway: db.ReadyQueueStats{Complete: true}, Generic: db.ReadyQueueStats{Complete: true}}
	m.probe = func(context.Context, int) (db.QueueStats, error) { return stats, nil }
	m.sample(context.Background())
	families = queueMetricFamilies(t, m)
	for _, profile := range []string{"gateway", "generic"} {
		require.Zero(t, metricValue(t, families, "djob_queue_ready_runs", "profile", profile))
		require.Zero(t, metricValue(t, families, "djob_queue_oldest_ready_age_seconds", "profile", profile))
		require.Zero(t, metricValue(t, families, "djob_queue_oldest_ready_created_age_seconds", "profile", profile))
	}
	stats.Generic = db.ReadyQueueStats{Observed: 2, Complete: true, OldestCreatedAt: now.Add(-time.Minute), MissingCreatedAt: 1}
	m.sample(context.Background())
	families = queueMetricFamilies(t, m)
	require.Equal(t, float64(2), metricValue(t, families, "djob_queue_ready_runs", "profile", "generic"))
	require.Equal(t, float64(60), metricValue(t, families, "djob_queue_oldest_observed_created_age_seconds", "profile", "generic"))
	require.Len(t, families["djob_queue_oldest_ready_created_age_seconds"].Metric, 1, "a missing origin makes the exact oldest age unknown")
	stats.Generic = db.ReadyQueueStats{Observed: 1, Complete: false, OldestCreatedAt: now.Add(time.Hour), FutureCreatedAt: 1}
	m.sample(context.Background())
	families = queueMetricFamilies(t, m)
	require.Len(t, families["djob_queue_ready_runs"].Metric, 1)
	require.Len(t, families["djob_queue_oldest_observed_created_age_seconds"].Metric, 1, "a future origin is not clamped into a zero-age queued run")
	require.Equal(t, float64(1), metricValue(t, families, "djob_queue_future_creation_times_observed", "profile", "generic"))
	stats.Generic = db.ReadyQueueStats{Observed: 1, Complete: true, OldestReadySince: now.Add(-time.Minute)}
	m.sample(context.Background())
	require.Equal(t, float64(60), metricValue(t, queueMetricFamilies(t, m), "djob_queue_oldest_ready_age_seconds", "profile", "generic"))
	stats.Generic.MissingReadySince = 1
	m.sample(context.Background())
	require.Len(t, queueMetricFamilies(t, m)["djob_queue_oldest_ready_age_seconds"].Metric, 1)
	stats.Generic.MissingReadySince, stats.Generic.FutureReadySince = 0, 1
	m.sample(context.Background())
	require.Len(t, queueMetricFamilies(t, m)["djob_queue_oldest_ready_age_seconds"].Metric, 1)
}

func TestQueueMonitorNeverOverlapsOrDetachesAnUncooperativeProbe(t *testing.T) {
	m := newQueueMonitor(QueueMonitorConfig{SampleInterval: MinQueueSampleInterval, ScanLimit: 2})
	started, cancelled, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Uint32
	m.probe = func(ctx context.Context, _ int) (db.QueueStats, error) {
		calls.Add(1)
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-release
		return db.QueueStats{}, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	var once sync.Once
	go func() { defer close(finished); m.run(ctx) }()
	t.Cleanup(func() {
		cancel()
		once.Do(func() { close(release) })
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("queue probe did not join")
		}
	})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("probe did not start")
	}
	cancel()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("probe did not receive cancellation")
	}
	select {
	case <-finished:
		t.Fatal("sampler detached its active probe")
	default:
	}
	families := queueMetricFamilies(t, m)
	require.Equal(t, float64(1), metricValue(t, families, "djob_queue_monitor_running"), "scraping remains available while the probe is stuck")
	require.EqualValues(t, 1, calls.Load())
	once.Do(func() { close(release) })
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("sampler did not join")
	}
	require.False(t, m.observation().running)
	require.False(t, m.observation().haveSample)
}
