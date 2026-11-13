package server

import (
	"context"
	"crypto/tls"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node"
	"github.com/duravela/djob/sdk/go"
	"github.com/duravela/djob/sdk/go/handlers"
	"github.com/duravela/djob/sdk/go/worker"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestWorkerQuorumGraceRecoveryAndBoundedProbeShutdown(t *testing.T) {
	cfg := RemoteWorkerConfig{QuorumProbeInterval: 100 * time.Millisecond, QuorumProbeTimeout: 100 * time.Millisecond, QuorumGrace: 700 * time.Millisecond}
	require.NoError(t, cfg.defaults())
	h := &remoteWorkers{cfg: cfg, sessions: make(map[string]*workerSession)}
	registry := prometheus.NewRegistry()
	h.quorumMetrics = newWorkerQuorumMetrics(h, registry)
	var phase atomic.Int32
	failed, blocked := make(chan struct{}, 1), make(chan struct{}, 1)
	h.probe = func(ctx context.Context) error {
		switch phase.Load() {
		case 1:
			select {
			case failed <- struct{}{}:
			default:
			}
			return node.ErrReadBarrierChanged
		case 2:
			select {
			case blocked <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		default:
			return nil
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); h.run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("quorum probe did not stop")
		}
	})
	require.Eventually(t, func() bool { h.mu.Lock(); defer h.mu.Unlock(); return !h.lastConfirmed.IsZero() }, time.Second, time.Millisecond)
	phase.Store(1)
	select {
	case <-failed:
	case <-time.After(time.Second):
		t.Fatal("probe did not fail")
	}
	require.True(t, h.acceptingWorkers(), "a short election must not relocate sessions")
	require.Eventually(t, func() bool { return !h.acceptingWorkers() }, 2*time.Second, time.Millisecond)
	phase.Store(0)
	require.Eventually(t, h.acceptingWorkers, time.Second, time.Millisecond, "quorum recovery reopens registration")
	phase.Store(2)
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("probe did not block")
	}
	// Scraping reads cached, bounded-cardinality state and never waits on Raft.
	families, err := registry.Gather()
	require.NoError(t, err)
	metrics := make(map[string]*dto.MetricFamily)
	for _, family := range families {
		metrics[family.GetName()] = family
	}
	require.Equal(t, float64(1), metricValue(t, metrics, "djob_worker_controller_available"))
	require.Positive(t, metricValue(t, metrics, "djob_worker_quorum_probes_total", "result", "confirmed"))
	require.Positive(t, metricValue(t, metrics, "djob_worker_quorum_probes_total", "result", "failed"))
	require.Positive(t, metricValue(t, metrics, "djob_worker_quorum_last_success_timestamp_seconds"))
	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				require.Equal(t, "result", label.GetName())
			}
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel the active quorum probe")
	}
}

func TestRemoteWorkerLeavesReachableControllerWithoutQuorum(t *testing.T) {
	apiTLS, roots := apiTestCertificate(t)
	members := startReadinessServers(t, func(cfg *Config) {
		cfg.APITLS = apiTLS
		cfg.APIAuth.Credentials = append(cfg.APIAuth.Credentials, APICredential{Subject: "alpha/worker", TenantID: "alpha", Role: APIWorker, Token: workerToken("alpha")})
		cfg.RemoteWorkers = RemoteWorkerConfig{Enabled: true, Heartbeat: 100 * time.Millisecond, PollWait: 20 * time.Millisecond, QuorumProbeInterval: 100 * time.Millisecond, QuorumProbeTimeout: 100 * time.Millisecond, QuorumGrace: 500 * time.Millisecond}
	})
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(transport.CloseIdleConnections)
	c := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	var leader *Server
	for _, s := range members {
		require.Eventually(t, func() bool {
			s.remoteWorkers.mu.Lock()
			defer s.remoteWorkers.mu.Unlock()
			return !s.remoteWorkers.lastConfirmed.IsZero()
		}, 3*time.Second, time.Millisecond)
		if s.application.Raft.IsLeader() {
			leader = s
		}
	}
	require.NotNil(t, leader)
	_, done := runRemoteWorker(t, leader, c, "alpha", handlers.Builtins())
	require.Eventually(t, func() bool { return len(leader.remoteWorkers.snapshot()) == 1 }, time.Second, time.Millisecond)
	for _, s := range members {
		if s != leader {
			require.NoError(t, s.Close(context.Background()))
		}
	}
	select {
	case err := <-done:
		require.ErrorIs(t, err, worker.ErrSessionLost)
	case <-time.After(3 * time.Second):
		t.Fatal("worker stayed pinned to a reachable controller without quorum")
	}
	require.Empty(t, leader.remoteWorkers.snapshot())
	workerPost(t, c, leader, workerToken("alpha"), wire.Path+"register", wire.Register{Protocol: wire.Version, WorkerID: "new-session", TenantID: "alpha", Handlers: []djob.Version{{Name: "sha256", Version: "1"}}}, nil, 503)
	authenticatedHTTP(t, c, "https://"+leader.HTTPAddr(), http.MethodGet, "/healthz", "", nil, nil, 200, nil)
}

func TestRemoteRelocationJoinsDriverBeforeEndingSession(t *testing.T) {
	s, c := workerAPI(t, func(cfg *Config) { cfg.RemoteWorkers.ReplyGrace = 2 * time.Second })
	remotePrepare(t, s)
	d := &cancelRemoteDriver{started: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}), stopped: make(chan struct{})}
	registry := handlers.Builtins()
	registry.Drivers[djob.Version{Name: "device", Version: "1"}] = func(djob.JobRun) (djob.Driver, error) { return d, nil }
	_, done := runRemoteWorker(t, s, c, "alpha", registry)
	t.Cleanup(func() {
		select {
		case <-d.release:
		default:
			close(d.release)
		}
	})
	remoteLaunch(t, s, "alpha", "invoke", "relocating")
	select {
	case <-d.started:
	case <-time.After(3 * time.Second):
		t.Fatal("remote I/O did not start")
	}
	sessions := s.remoteWorkers.snapshot()
	require.Len(t, sessions, 1)
	session := sessions[0]
	session.relocate()
	select {
	case <-d.canceled:
	case <-time.After(time.Second):
		t.Fatal("relocation did not cancel active I/O")
	}
	require.False(t, session.finished())
	require.Len(t, s.remoteWorkers.snapshot(), 1, "RPCs remain available until I/O and cleanup return")
	resource, err := s.application.Store.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, db.ResourceOwned, resource.State)
	require.Zero(t, d.quiesced.Load())
	workerPost(t, c, s, workerToken("alpha"), wire.Path+session.id+"/drain", struct{}{}, nil, 204)
	close(d.release)
	select {
	case err := <-done:
		require.ErrorIs(t, err, worker.ErrSessionLost)
	case <-time.After(3 * time.Second):
		t.Fatal("relocation did not end the joined session")
	}
	require.False(t, d.premature.Load())
	require.EqualValues(t, 1, d.quiesced.Load())
	require.EqualValues(t, 1, d.closed.Load())
	remoteState(t, s, "alpha", "relocating", db.JobUnknown)
	resource, err = s.application.Store.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, db.ResourceAvailable, resource.State, "confirmed cleanup releases the bus even though the business outcome is unknown")
}
