package server

import (
	"bytes"
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/duravela/djob/pkg/util/stop"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
)

func requireStorageSamples(t *testing.T, s *Server) storageObservation {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, sample := range s.storage.observation().components {
			if !sample.haveSample {
				return false
			}
		}
		return true
	}, 3*time.Second, 10*time.Millisecond)
	return s.storage.observation()
}

func TestStorageMetricsAreUnknownUntilFirstBackgroundSuccess(t *testing.T) {
	cfg := apiTestConfig(filepath.Join(t.TempDir(), "unopened"))
	cfg.APIAuth = genericCredentials()
	startup, err := NewServer(cfg, stop.NewStopper())
	require.NoError(t, err)
	s := startup.(*Server)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	var probes atomic.Uint64
	for component := range s.storage.probes {
		s.storage.probes[component] = func() (storageValue, error) { probes.Add(1); return storageValue{}, nil }
	}
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	r.Header.Set("Authorization", "Bearer "+genericToken("ops/alice"))
	w := httptest.NewRecorder()
	s.metricsHTTP(w, r)
	require.Equal(t, 200, w.Code)
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(bytes.NewReader(w.Body.Bytes()))
	require.NoError(t, err)
	require.Zero(t, probes.Load(), "a scrape must not initiate filesystem or database probes")
	require.Nil(t, families["djob_storage_volume_available_bytes"], "unknown capacity must not look like a full disk")
	require.Nil(t, families["djob_store_file_bytes"])
	for _, component := range storageComponentNames {
		require.Zero(t, metricValue(t, families, "djob_storage_probe_success", "component", component))
		require.True(t, math.IsInf(metricValue(t, families, "djob_storage_sample_age_seconds", "component", component), 1))
	}
	require.Equal(t, DefaultStorageSampleInterval.Seconds(), metricValue(t, families, "djob_storage_sample_interval_seconds"))
	_, err = os.Stat(cfg.Raft.DataDir)
	require.True(t, os.IsNotExist(err))
}

func TestStorageMetricsTrackFileMetadataAndDurableRecoveryWithoutWriting(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.StorageSampleInterval = MinStorageSampleInterval
	s := startAPI(t, cfg)
	initial := requireStorageSamples(t, s)
	beforeBytes := initial.components[storageStore].value.fileSize
	c := apiClient(t, s)
	_, err := c.ConfigureChannel(genericContext("writer"), &pb.ConfigureChannelRequest{Command: meta("storage-channel"), Id: "gateway", GatewayId: "gateway", Protocol: "fake", Endpoint: "private-endpoint:502"})
	require.NoError(t, err)
	for _, id := range []string{"first", "second", "third"} {
		_, err = c.CreateRun(genericContext("writer"), &pb.CreateRunRequest{Command: meta("storage-run/" + id), Id: id, ChannelId: "gateway", Executor: "fake", Payload: bytes.Repeat([]byte("private-payload"), 4096)})
		require.NoError(t, err)
	}
	code, _, _ := probeReadiness(t, s)
	require.Equal(t, 200, code)
	index := s.application.Store.LastAppliedIndex()
	file, err := os.Stat(filepath.Join(cfg.Raft.DataDir, "store/jobs.snap"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		observed := s.storage.observation()
		return observed.components[storageStore].value.fileSize == file.Size() && observed.components[storageDatabase].value.database.AppliedIndex >= index
	}, 3*time.Second, 10*time.Millisecond)
	for range 5 {
		families := scrapeMetrics(t, s)
		require.Equal(t, float64(file.Size()), metricValue(t, families, "djob_store_file_bytes"))
		require.GreaterOrEqual(t, metricValue(t, families, "djob_store_stats_applied_index"), float64(index))
		require.Positive(t, metricValue(t, families, "djob_store_page_size_bytes"))
		for _, component := range []string{"store", "raft_wal", "raft_snapshots"} {
			require.Positive(t, metricValue(t, families, "djob_storage_volume_total_bytes", "component", component))
			require.GreaterOrEqual(t, metricValue(t, families, "djob_storage_volume_free_bytes", "component", component), metricValue(t, families, "djob_storage_volume_available_bytes", "component", component))
		}
	}
	require.Equal(t, index, s.application.Store.LastAppliedIndex(), "storage monitoring and scraping create no log entries or receipts")
	data := authenticatedHTTP(t, http.DefaultClient, "http://"+s.HTTPAddr(), http.MethodGet, "/metrics", genericToken("ops/alice"), nil, nil, 200, nil)
	require.NotContains(t, string(data), cfg.Raft.DataDir)
	require.NotContains(t, string(data), "private-payload")
	require.NoError(t, s.Close(context.Background()))
	require.False(t, s.storage.observation().running)
	restarted := startAPI(t, cfg)
	observed := requireStorageSamples(t, restarted)
	require.GreaterOrEqual(t, observed.components[storageDatabase].value.database.AppliedIndex, index)
	require.GreaterOrEqual(t, observed.components[storageStore].value.fileSize, beforeBytes)
	page, err := apiClient(t, restarted).ListRuns(genericContext("reader"), &pb.ListRunsRequest{})
	require.NoError(t, err)
	require.Len(t, page.Runs, 3)
}

func TestStorageMetricsStayResponsiveDuringSlowIOAndShutdownJoinsTheProbe(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.StorageSampleInterval = MinStorageSampleInterval
	startup, err := NewServer(cfg, stop.NewStopper())
	require.NoError(t, err)
	s := startup.(*Server)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var calls atomic.Uint64
	s.storage.probes[storageStore] = func() (storageValue, error) {
		if calls.Add(1) > 1 {
			close(entered)
			<-release
		}
		return storageValue{space: filesystemSpace{1000, 400, 300}, fileSize: 250}, nil
	}
	require.NoError(t, s.PreStart(context.Background()))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("second storage probe did not start")
	}
	client := &http.Client{Timeout: time.Second}
	for range 3 {
		data := authenticatedHTTP(t, client, "http://"+s.HTTPAddr(), http.MethodGet, "/metrics", genericToken("ops/alice"), nil, nil, 200, nil)
		var parser expfmt.TextParser
		families, err := parser.TextToMetricFamilies(bytes.NewReader(data))
		require.NoError(t, err)
		require.Equal(t, float64(250), metricValue(t, families, "djob_store_file_bytes"))
		require.GreaterOrEqual(t, metricValue(t, families, "djob_storage_sample_age_seconds", "component", "store"), MinStorageSampleInterval.Seconds())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = s.Close(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	select {
	case <-s.Done():
		t.Fatal("server detached a blocked filesystem probe")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, s.Close(ctx))
	require.False(t, s.storage.observation().running)
	require.EqualValues(t, 2, calls.Load(), "one blocked sampler must not create retries or detached goroutines")
}

func TestStorageSamplingContinuesWithoutQuorum(t *testing.T) {
	members := startReadinessServers(t, func(c *Config) { c.StorageSampleInterval = MinStorageSampleInterval })
	var leader *Server
	for _, s := range members {
		if s.application.Raft.IsLeader() {
			leader = s
		}
	}
	require.NotNil(t, leader)
	before := requireStorageSamples(t, leader).components[storageStore].results[storageOK]
	for _, s := range members {
		if s != leader {
			require.NoError(t, s.Close(context.Background()))
		}
	}
	code, _, _ := probeReadiness(t, leader)
	require.Equal(t, 503, code)
	require.Eventually(t, func() bool {
		return leader.storage.observation().components[storageStore].results[storageOK] > before
	}, time.Second, 10*time.Millisecond)
	families := scrapeMetrics(t, leader)
	require.Equal(t, float64(1), metricValue(t, families, "djob_storage_monitor_running"))
	require.Positive(t, metricValue(t, families, "djob_storage_volume_total_bytes", "component", "raft_wal"))
}

func TestStorageSamplingBoundsAreValidatedBeforeOpeningData(t *testing.T) {
	for _, interval := range []time.Duration{-1, MinStorageSampleInterval - 1, MaxStorageSampleInterval + 1} {
		cfg := apiTestConfig(filepath.Join(t.TempDir(), "unopened"))
		cfg.StorageSampleInterval = interval
		s, err := Start(context.Background(), cfg)
		require.Error(t, err)
		require.Nil(t, s)
		_, err = os.Stat(cfg.Raft.DataDir)
		require.True(t, os.IsNotExist(err))
	}
	require.NoError(t, ValidateStorageSampleInterval(MinStorageSampleInterval))
	require.NoError(t, ValidateStorageSampleInterval(MaxStorageSampleInterval))
	require.Error(t, ValidateStorageSampleInterval(0))
}
