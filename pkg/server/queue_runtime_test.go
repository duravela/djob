package server

import (
	"bytes"
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/duravela/djob/pkg/util/stop"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
)

func TestQueueMetricsStayUnknownUntilBackgroundProbeAndScrapesNeverProbe(t *testing.T) {
	cfg := apiTestConfig(filepath.Join(t.TempDir(), "unopened"))
	cfg.APIAuth = genericCredentials()
	startup, err := NewServer(cfg, stop.NewStopper())
	require.NoError(t, err)
	s := startup.(*Server)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	var probes atomic.Uint32
	s.queue.probe = func(context.Context, int) (db.QueueStats, error) { probes.Add(1); return db.QueueStats{}, nil }
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	r.Header.Set("Authorization", "Bearer "+genericToken("ops/alice"))
	w := httptest.NewRecorder()
	s.metricsHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(bytes.NewReader(w.Body.Bytes()))
	require.NoError(t, err)
	require.Zero(t, probes.Load())
	require.Nil(t, families["djob_queue_ready_runs"])
	require.Nil(t, families["djob_queue_ready_runs_observed"])
	require.True(t, math.IsInf(metricValue(t, families, "djob_queue_sample_age_seconds"), 1))
	require.Zero(t, metricValue(t, families, "djob_queue_probe_success"))
	require.Equal(t, DefaultQueueSampleInterval.Seconds(), metricValue(t, families, "djob_queue_sample_interval_seconds"))
	_, err = os.Stat(cfg.Raft.DataDir)
	require.True(t, os.IsNotExist(err))
}

func waitQueueInventory(t *testing.T, s *Server, index uint64, count uint64, complete bool) queueObservation {
	t.Helper()
	require.Eventually(t, func() bool {
		o := s.queue.observation()
		return o.haveSample && o.stats.AppliedIndex >= index && o.stats.Gateway.Observed == count && o.stats.Generic.Observed == count &&
			o.stats.Gateway.Complete == complete && o.stats.Generic.Complete == complete
	}, 5*time.Second, 10*time.Millisecond)
	return s.queue.observation()
}

func TestQueueMetricsIncludeUndispatchedWorkExposeCapsAndRecoverDurableInventory(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.QueueMonitor = QueueMonitorConfig{SampleInterval: MinQueueSampleInterval, ScanLimit: 2}
	s := startAPI(t, cfg) // Both execution profiles are deliberately disabled.
	legacy, generic := apiClient(t, s), genericClient(t, s)
	_, err := legacy.ConfigureChannel(genericContext("writer"), &pb.ConfigureChannelRequest{Command: meta("queue-channel"), Id: "private-gateway", GatewayId: "private-gateway", Protocol: "fake", Endpoint: "private-endpoint:502"})
	require.NoError(t, err)
	tenantAPI(t, generic, "alpha")
	definitionAPI(t, generic, "alpha", "work", "unregistered-handler", "", []byte("private input"))
	for _, id := range []string{"first", "second", "third"} {
		_, err = legacy.CreateRun(genericContext("writer"), &pb.CreateRunRequest{Command: meta("queue/" + id), Id: id, ChannelId: "private-gateway", Executor: "unregistered-executor", Payload: []byte("private payload")})
		require.NoError(t, err)
		launchAPI(t, generic, "alpha", "work", id)
	}
	index := s.application.Store.LastAppliedIndex()
	waitQueueInventory(t, s, index, 2, false)
	families := scrapeMetrics(t, s)
	require.Nil(t, families["djob_queue_ready_runs"], "a limited prefix is not an exact backlog count")
	require.Nil(t, families["djob_queue_oldest_ready_created_age_seconds"])
	for _, profile := range []string{"gateway", "generic"} {
		require.Zero(t, metricValue(t, families, "djob_dispatch_enabled", "profile", profile))
		require.Equal(t, float64(2), metricValue(t, families, "djob_queue_ready_runs_observed", "profile", profile))
		require.Zero(t, metricValue(t, families, "djob_queue_sample_complete", "profile", profile))
		require.Zero(t, metricValue(t, families, "djob_execution_started_total", "profile", profile))
	}
	_, err = legacy.CancelRun(genericContext("writer"), &pb.CancelRunRequest{Command: meta("queue-cancel"), Id: "third"})
	require.NoError(t, err)
	mutateJob(t, generic, "alpha/writer", &jp.MutationRequest{Command: genericMeta("queue-cancel"), Operation: &jp.MutationRequest_Cancel{Cancel: &jp.Cancel{RunId: "third"}}})
	index = s.application.Store.LastAppliedIndex()
	waitQueueInventory(t, s, index, 2, true)
	for range 3 {
		families = scrapeMetrics(t, s)
		for _, profile := range []string{"gateway", "generic"} {
			require.Equal(t, float64(2), metricValue(t, families, "djob_queue_ready_runs", "profile", profile))
			require.Equal(t, float64(1), metricValue(t, families, "djob_queue_sample_complete", "profile", profile))
			require.GreaterOrEqual(t, metricValue(t, families, "djob_queue_oldest_ready_created_age_seconds", "profile", profile), float64(0))
		}
	}
	require.Equal(t, index, s.application.Store.LastAppliedIndex(), "inventory sampling and scrapes append no commands or receipts")
	data := authenticatedHTTP(t, http.DefaultClient, "http://"+s.HTTPAddr(), http.MethodGet, "/metrics", genericToken("ops/alice"), nil, nil, 200, nil)
	for _, private := range []string{"private-gateway", "private-endpoint", "private input", "private payload", "unregistered-handler", "unregistered-executor", cfg.Raft.DataDir} {
		require.NotContains(t, string(data), private)
	}
	require.NoError(t, s.Close(context.Background()))
	require.False(t, s.queue.observation().running)
	restarted := startAPI(t, cfg)
	waitQueueInventory(t, restarted, index, 2, true)
	otherCfg := apiTestConfig(t.TempDir())
	otherCfg.APIAuth, otherCfg.QueueMonitor = genericCredentials(), cfg.QueueMonitor
	other := startAPI(t, otherCfg)
	waitQueueInventory(t, other, 0, 0, true)
	require.Equal(t, float64(2), metricValue(t, scrapeMetrics(t, restarted), "djob_queue_ready_runs", "profile", "generic"))
	require.Zero(t, metricValue(t, scrapeMetrics(t, other), "djob_queue_ready_runs", "profile", "generic"))
}

func TestQueueMonitorConfigurationIsRejectedBeforeStorageOpens(t *testing.T) {
	for _, config := range []QueueMonitorConfig{
		{SampleInterval: -time.Second, ScanLimit: 1}, {SampleInterval: MinQueueSampleInterval - 1, ScanLimit: 1},
		{SampleInterval: MaxQueueSampleInterval + 1, ScanLimit: 1}, {SampleInterval: time.Second, ScanLimit: -1},
		{SampleInterval: time.Second, ScanLimit: db.MaxQueueSampleRows + 1},
	} {
		cfg := apiTestConfig(filepath.Join(t.TempDir(), "unopened"))
		cfg.QueueMonitor = config
		stopper := stop.NewStopper()
		_, err := NewServer(cfg, stopper)
		require.Error(t, err)
		stopper.Stop(context.Background())
		_, err = os.Stat(cfg.Raft.DataDir)
		require.True(t, os.IsNotExist(err))
	}
}
