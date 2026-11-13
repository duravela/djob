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

func completeStateInventory(index uint64) db.StateInventory {
	s := db.StateInventory{AppliedIndex: index}
	for k := range s.Kinds {
		s.Kinds[k].Complete = true
	}
	return s
}

func stateMetricFamilies(t *testing.T, m *stateMonitor) map[string]*dto.MetricFamily {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(newStateMetricsCollector(m))
	families, err := reg.Gather()
	require.NoError(t, err)
	result := make(map[string]*dto.MetricFamily)
	for _, family := range families {
		result[family.GetName()] = family
	}
	return result
}

func TestStateMetricsDistinguishUnknownEmptyAndLimitedInventory(t *testing.T) {
	m := newStateMonitor(StateMonitorConfig{SampleInterval: MinStateSampleInterval, ScanLimit: 2})
	families := stateMetricFamilies(t, m)
	require.Nil(t, families["djob_state_inventory"])
	require.Nil(t, families["djob_state_inventory_observed"])
	require.Nil(t, families["djob_state_stats_applied_index"])
	require.True(t, math.IsInf(metricValue(t, families, "djob_state_sample_age_seconds"), 1))
	stats := completeStateInventory(7)
	m.probe = func(context.Context, int) (db.StateInventory, error) { return stats, nil }
	m.sample(context.Background())
	families = stateMetricFamilies(t, m)
	for kind := db.StateInventoryKind(0); kind < db.StateInventoryKindCount; kind++ {
		for _, state := range kind.States() {
			require.Zero(t, metricValue(t, families, "djob_state_inventory", "kind", kind.String(), "state", state.String()))
		}
	}
	stats.Kinds[db.InventoryGenericRuns] = db.StateInventorySample{Observed: 2}
	stats.Kinds[db.InventoryGenericRuns].Counts[db.InventoryUnknown] = 1
	stats.Kinds[db.InventoryGenericRuns].Counts[db.InventoryWaiting] = 1
	m.sample(context.Background())
	families = stateMetricFamilies(t, m)
	require.Equal(t, float64(1), metricValue(t, families, "djob_state_probe_success"))
	require.Zero(t, metricValue(t, families, "djob_state_sample_complete", "kind", "generic_runs"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_state_inventory_observed", "kind", "generic_runs", "state", "unknown"))
	for _, metric := range families["djob_state_inventory"].Metric {
		for _, label := range metric.Label {
			require.False(t, label.GetName() == "kind" && label.GetValue() == "generic_runs", "a limited sample must remove prior exact counts for every state")
		}
	}
	// Arrays in observations own their data; callers cannot mutate cached counts.
	copy := m.observation()
	copy.stats.Kinds[db.InventoryGenericRuns].Counts[db.InventoryUnknown] = 99
	require.EqualValues(t, 1, m.observation().stats.Kinds[db.InventoryGenericRuns].Counts[db.InventoryUnknown])
	stats = completeStateInventory(8)
	m.sample(context.Background())
	require.Equal(t, float64(0), metricValue(t, stateMetricFamilies(t, m), "djob_state_inventory", "kind", "generic_runs", "state", "unknown"))
}

func TestStateMonitorKeepsLastValidInventoryAcrossBusyErrorsAndDeadlines(t *testing.T) {
	m := newStateMonitor(StateMonitorConfig{SampleInterval: MinStateSampleInterval, ScanLimit: 2})
	stats := completeStateInventory(7)
	stats.Kinds[db.InventoryChannels].Counts[db.InventoryBlocked] = 1
	stats.Kinds[db.InventoryChannels].Observed = 1
	m.probe = func(context.Context, int) (db.StateInventory, error) { return stats, nil }
	m.sample(context.Background())
	before := m.observation()
	for _, err := range []error{db.ErrStateInventoryBusy, errors.New("private database details")} {
		m.probe = func(context.Context, int) (db.StateInventory, error) { return db.StateInventory{}, err }
		m.sample(context.Background())
		after := m.observation()
		require.False(t, after.lastOK)
		require.Equal(t, before.stats, after.stats)
		require.Equal(t, before.sampledAt, after.sampledAt)
	}
	m.probe = func(ctx context.Context, _ int) (db.StateInventory, error) {
		<-ctx.Done()
		return completeStateInventory(99), nil
	}
	m.sample(context.Background())
	require.Equal(t, before.stats, m.observation().stats, "a late success cannot replace the last valid sample")
	require.Equal(t, before.sampledAt, m.observation().sampledAt)
	require.EqualValues(t, 1, m.observation().results[stateProbeBusy])
	require.EqualValues(t, 2, m.observation().results[stateProbeError])
	families := stateMetricFamilies(t, m)
	require.Equal(t, float64(1), metricValue(t, families, "djob_state_inventory", "kind", "channels", "state", "blocked"))
	require.Zero(t, metricValue(t, families, "djob_state_probe_success"))
}

func TestStateMonitorJoinsAnUncooperativeProbeWithoutOverlappingOrBlockingScrapes(t *testing.T) {
	m := newStateMonitor(StateMonitorConfig{SampleInterval: MinStateSampleInterval, ScanLimit: 2})
	started, cancelled, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Uint32
	m.probe = func(ctx context.Context, _ int) (db.StateInventory, error) {
		calls.Add(1)
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-release
		return db.StateInventory{}, ctx.Err()
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
			t.Error("state probe did not join")
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
	require.Equal(t, float64(1), metricValue(t, stateMetricFamilies(t, m), "djob_state_monitor_running"))
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
