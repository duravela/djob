package server

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/duravela/djob/pkg/node"
	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
)

func scrapeMetrics(t *testing.T, s *Server) map[string]*dto.MetricFamily {
	t.Helper()
	response := authenticatedHTTP(t, http.DefaultClient, "http://"+s.HTTPAddr(), http.MethodGet, "/metrics", genericToken("ops/alice"), nil, nil, 200, nil)
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(strings.NewReader(string(response)))
	require.NoError(t, err)
	return families
}

func metricValue(t *testing.T, families map[string]*dto.MetricFamily, name string, labels ...string) float64 {
	t.Helper()
	family := families[name]
	require.NotNil(t, family, "missing %s", name)
	for _, m := range family.Metric {
		if len(m.Label)*2 != len(labels) {
			continue
		}
		match := true
		for i := 0; i < len(labels); i += 2 {
			found := false
			for _, label := range m.Label {
				if label.GetName() == labels[i] && label.GetValue() == labels[i+1] {
					found = true
				}
			}
			match = match && found
		}
		if match {
			if m.Counter != nil {
				return m.Counter.GetValue()
			}
			return m.Gauge.GetValue()
		}
	}
	t.Fatalf("missing %s with labels %v", name, labels)
	return 0
}

func TestMetricsRequireUnscopedOperatorAndUseAPITLS(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	tlsConfig, roots := apiTestCertificate(t)
	cfg.APITLS = tlsConfig
	s := startAPI(t, cfg)
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	endpoint := "https://" + s.HTTPAddr()
	token := genericToken("ops/alice")
	for _, tc := range []struct {
		token string
		code  int
	}{
		{"", 401}, {strings.Repeat("z", 40), 401},
		{genericToken("reader"), 403}, {genericToken("writer"), 403},
		{genericToken("alpha/operator"), 403}, {genericToken("alpha/callback"), 403}, {genericToken("alpha/producer"), 403},
		{token, 200},
	} {
		authenticatedHTTP(t, client, endpoint, http.MethodGet, "/metrics", tc.token, nil, nil, tc.code, nil)
	}
	for _, header := range []http.Header{
		{"Grpc-Metadata-Authorization": {"Bearer " + token}, "X-Forwarded-User": {"ops/alice"}},
		{"Authorization": {"Bearer " + token, "Bearer " + token}},
	} {
		authenticatedHTTP(t, client, endpoint, http.MethodGet, "/metrics", "", nil, nil, 401, header)
	}
	for _, tc := range []struct {
		method, path string
		code         int
	}{{http.MethodPost, "/metrics", 405}, {http.MethodHead, "/metrics", 405}, {http.MethodGet, "/metrics?", 400}, {http.MethodGet, "/metrics?tenant_id=alpha", 400}} {
		authenticatedHTTP(t, client, endpoint, tc.method, tc.path, token, nil, nil, tc.code, nil)
	}
	// Anonymous ordinary APIs must not make global operational metrics public.
	anonymous := &Server{scheduler: &schedulerServer{}}
	w := httptest.NewRecorder()
	anonymous.metricsHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, 401, w.Code)
	require.Equal(t, "Bearer", w.Header().Get("WWW-Authenticate"))
}

func TestMetricsAreMemberLocalReadOnlyAndResetOnRestart(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	a := startAPI(t, cfg)
	otherCfg := apiTestConfig(t.TempDir())
	otherCfg.APIAuth = genericCredentials()
	b := startAPI(t, otherCfg)
	client := apiClient(t, a)
	_, err := client.ConfigureChannel(genericContext("writer"), &pb.ConfigureChannelRequest{
		Command: meta("metrics-config"), Id: "secret-gateway", GatewayId: "secret-gateway", Protocol: "fake", Endpoint: "private-endpoint:502",
	})
	require.NoError(t, err)
	code, _, _ := probeReadiness(t, a)
	require.Equal(t, 200, code)
	index := a.application.Store.LastAppliedIndex()
	for range 3 {
		families := scrapeMetrics(t, a)
		require.Equal(t, float64(1), metricValue(t, families, "jobqueue_raft_proposals_total"))
		require.Equal(t, float64(1), metricValue(t, families, "djob_raft_leader"))
		require.Equal(t, float64(index), metricValue(t, families, "djob_raft_applied_index"))
		require.Zero(t, metricValue(t, families, "djob_raft_apply_lag_entries"))
		require.Equal(t, float64(1), metricValue(t, families, "djob_readiness_requests_total", "result", "ready"))
		require.EqualValues(t, 1, families["djob_readiness_duration_seconds"].Metric[0].Histogram.GetSampleCount())
		require.Zero(t, metricValue(t, scrapeMetrics(t, b), "jobqueue_raft_proposals_total"))
		require.Zero(t, metricValue(t, scrapeMetrics(t, b), "djob_readiness_requests_total", "result", "ready"))
		for name, family := range families {
			for _, metric := range family.Metric {
				for _, label := range metric.Label {
					require.Contains(t, []string{"result", "worker", "profile", "direction", "component", "kind", "state", "code"}, label.GetName(), name)
				}
			}
		}
	}
	require.Equal(t, index, a.application.Store.LastAppliedIndex(), "scraping creates no Raft entries or receipts")
	response, err := http.NewRequest(http.MethodGet, "http://"+a.HTTPAddr()+"/metrics", nil)
	require.NoError(t, err)
	response.Header.Set("Authorization", "Bearer "+genericToken("ops/alice"))
	result, err := http.DefaultClient.Do(response)
	require.NoError(t, err)
	data, err := io.ReadAll(result.Body)
	result.Body.Close()
	require.NoError(t, err)
	require.Equal(t, "no-store", result.Header.Get("Cache-Control"))
	require.Contains(t, result.Header.Get("Content-Type"), "version=0.0.4")
	for _, secret := range []string{"secret-gateway", "private-endpoint", genericToken("ops/alice"), "ops/alice", "go_goroutines"} {
		require.NotContains(t, string(data), secret)
	}
	require.NoError(t, a.Close(context.Background()))
	restarted := startAPI(t, cfg)
	families := scrapeMetrics(t, restarted)
	require.Zero(t, metricValue(t, families, "jobqueue_raft_proposals_total"))
	require.Zero(t, metricValue(t, families, "djob_readiness_requests_total", "result", "ready"))
	require.GreaterOrEqual(t, metricValue(t, families, "djob_raft_commit_index"), float64(index))
}

func TestMetricsRemainAvailableWhenQuorumIsLostAndProbesSaturate(t *testing.T) {
	members := startReadinessServers(t)
	var leader *Server
	for _, s := range members {
		if s.application.Raft.IsLeader() {
			leader = s
		}
	}
	require.NotNil(t, leader)
	for _, s := range members {
		if s != leader {
			require.NoError(t, s.Close(context.Background()))
		}
	}
	code, _, _ := probeReadiness(t, leader)
	require.Equal(t, 503, code)
	var wg sync.WaitGroup
	for range node.MaxPendingReadBarriers - 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			_, _ = leader.application.Raft.ReadBarrier(ctx)
		}()
	}
	wg.Wait()
	code, _, _ = probeReadiness(t, leader)
	require.Equal(t, 503, code)
	families := scrapeMetrics(t, leader)
	require.Equal(t, float64(1), metricValue(t, families, "djob_raft_leader"), "local role must not be presented as quorum")
	require.Equal(t, float64(node.MaxPendingReadBarriers), metricValue(t, families, "djob_raft_read_barriers_pending"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_readiness_requests_total", "result", "quorum_unavailable"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_readiness_requests_total", "result", "busy"))
	// A saturated scrape budget rejects immediately, independently of readiness.
	for range maxConcurrentMetricScrapes {
		leader.metrics.scrapes <- struct{}{}
	}
	authenticatedHTTP(t, http.DefaultClient, "http://"+leader.HTTPAddr(), http.MethodGet, "/metrics", genericToken("ops/alice"), nil, nil, 503, nil)
	for range maxConcurrentMetricScrapes {
		<-leader.metrics.scrapes
	}
	leader.cancelStartup()
	require.Equal(t, float64(1), metricValue(t, scrapeMetrics(t, leader), "djob_server_stopping"))
}

func TestMetricsIncludeGatewayCleanupAndPreserveErrorCallback(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	gateway := fake.New(fake.Options{})
	cfg.TransportFactory = gateway.Factory
	var errorsSeen atomic.Uint64
	var executionsStarted, executionsReturned atomic.Uint64
	cfg.Dispatcher.OnError = func(error) { errorsSeen.Add(1) }
	cfg.Dispatcher.Executor.OnExecution = func(o executor.ExecutionObservation) {
		if o.Phase == executor.ExecutionStarted {
			executionsStarted.Add(1)
		} else if o.Phase == executor.ExecutionReturned {
			executionsReturned.Add(1)
		}
	}
	s := startAPI(t, cfg, gateway)
	c := apiClient(t, s)
	_, err := c.ConfigureChannel(genericContext("writer"), &pb.ConfigureChannelRequest{Command: meta("metrics-gateway"), Id: "gateway", GatewayId: "gateway", Protocol: "fake", Endpoint: "private-endpoint:502"})
	require.NoError(t, err)
	_, err = c.CreateRun(genericContext("writer"), &pb.CreateRunRequest{Command: meta("metrics-run"), Id: "run", ChannelId: "gateway", Executor: "fake"})
	require.NoError(t, err)
	call := apiCall(t, gateway)
	call.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded}}
	apiPhase(t, call.RecoveryStarted)
	families := scrapeMetrics(t, s)
	require.Equal(t, float64(1), metricValue(t, families, "djob_dispatch_enabled", "profile", "gateway"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_dispatch_active", "profile", "gateway"), "cleanup still occupies the gateway")
	require.Equal(t, uint64(1), executionsStarted.Load())
	require.Zero(t, executionsReturned.Load())
	require.Equal(t, float64(1), metricValue(t, families, "djob_execution_started_total", "profile", "gateway"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_execution_in_flight", "profile", "gateway"))
	require.Zero(t, metricValue(t, families, "djob_execution_returns_total", "profile", "gateway", "result", "succeeded"))
	call.Recover <- errors.New("sensitive gateway recovery detail")
	apiPhase(t, call.CloseStarted)
	call.Close <- nil
	require.Eventually(t, func() bool { return errorsSeen.Load() > 0 && s.dispatcher.Pending() == 0 }, 3*time.Second, time.Millisecond)
	families = scrapeMetrics(t, s)
	require.Zero(t, metricValue(t, families, "djob_dispatch_active", "profile", "gateway"))
	require.Equal(t, uint64(1), executionsReturned.Load())
	require.Zero(t, metricValue(t, families, "djob_execution_in_flight", "profile", "gateway"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_execution_returns_total", "profile", "gateway", "result", "succeeded"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_execution_cleanup_failures_total", "profile", "gateway"))
	require.GreaterOrEqual(t, metricValue(t, families, "djob_background_errors_total", "worker", "gateway_dispatch"), float64(1))
	channel, err := s.application.Store.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, db.ChannelBlocked, channel.State)
	data := authenticatedHTTP(t, http.DefaultClient, "http://"+s.HTTPAddr(), http.MethodGet, "/metrics", genericToken("ops/alice"), nil, nil, 200, nil)
	require.NotContains(t, string(data), "sensitive gateway recovery detail")
}
