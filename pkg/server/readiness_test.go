package server

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/node"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/stretchr/testify/require"
)

func probeReadiness(t *testing.T, s *Server) (int, readinessResponse, http.Header) {
	t.Helper()
	response, err := http.Get("http://" + s.HTTPAddr() + "/readyz")
	require.NoError(t, err)
	defer response.Body.Close()
	var body readinessResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	return response.StatusCode, body, response.Header
}
func TestReadinessIsPublicFreshAndDoesNotWriteProbeState(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	s := startAPI(t, cfg)
	code, initial, _ := probeReadiness(t, s)
	require.Equal(t, http.StatusOK, code)
	require.True(t, initial.Ready)
	before := s.application.Store.LastAppliedIndex()
	for range 5 {
		code, body, header := probeReadiness(t, s)
		require.Equal(t, http.StatusOK, code)
		require.True(t, body.Ready)
		require.Equal(t, s.nodeID, body.NodeID)
		require.GreaterOrEqual(t, body.ConfirmedIndex, before)
		require.GreaterOrEqual(t, body.AppliedIndex, body.ConfirmedIndex)
		require.Equal(t, "no-store", header.Get("Cache-Control"))
	}
	require.Equal(t, before, s.application.Store.LastAppliedIndex())
	for _, tc := range []struct {
		method, path string
		status       int
	}{{http.MethodPost, "/readyz", http.StatusMethodNotAllowed}, {http.MethodGet, "/readyz?timeout=1h", http.StatusBadRequest}, {http.MethodGet, "/readyz?", http.StatusBadRequest}} {
		req, err := http.NewRequest(tc.method, "http://"+s.HTTPAddr()+tc.path, nil)
		require.NoError(t, err)
		response, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, response.Body)
		require.NoError(t, err)
		response.Body.Close()
		require.Equal(t, tc.status, response.StatusCode)
	}
}

func startReadinessServers(t *testing.T, options ...func(*Config)) []*Server {
	t.Helper()
	peers := make([]string, 3)
	for i := range peers {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		peers[i] = "http://" + l.Addr().String()
		require.NoError(t, l.Close())
	}
	type answer struct {
		s   *Server
		err error
	}
	answers := make(chan answer, len(peers))
	for i := range peers {
		cfg := apiTestConfig(t.TempDir())
		id := uint64(i + 1)
		cfg.NodeID = &id
		cfg.Raft.Peers = peers
		cfg.APIAuth = genericCredentials()
		// These fixtures exercise API/quorum semantics. Accelerated 100ms
		// elections can churn during race-instrumented CI startup; retain the
		// production heartbeat interval while keeping probe deadlines short.
		cfg.Raft.RaftEventLoopHeartbeatInterval = 100 * time.Millisecond
		cfg.ReadinessTimeout = 100 * time.Millisecond
		for _, option := range options {
			option(&cfg)
		}
		go func() { s, err := Start(context.Background(), cfg); answers <- answer{s, err} }()
	}
	members := make([]*Server, 0, len(peers))
	for range peers {
		a := <-answers
		require.NoError(t, a.err)
		members = append(members, a.s)
		t.Cleanup(func() { require.NoError(t, a.s.Close(context.Background())) })
	}
	// Seeing one initial leader does not establish that every member has
	// finished campaigning. Confirm quorum reads in one common observed term.
	require.Eventually(t, func() bool {
		var term uint64
		leaders := 0
		for _, member := range members {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err := member.application.Raft.ReadBarrier(ctx)
			cancel()
			if err != nil {
				return false
			}
			observed := member.application.Raft.LocalStatus()
			if term != 0 && observed.Term != term {
				return false
			}
			term = observed.Term
			if observed.Leader {
				leaders++
			}
		}
		return term != 0 && leaders == 1
	}, 10*time.Second, 10*time.Millisecond, "fixture members did not establish fresh quorum in one term")
	return members
}
func TestReadinessRejectsStaleLeadershipAndBoundsOutstandingProbes(t *testing.T) {
	members := startReadinessServers(t)
	var leader *Server
	for _, s := range members {
		code, body, _ := probeReadiness(t, s)
		require.Equal(t, http.StatusOK, code)
		require.True(t, body.Ready)
		if s.application.Raft.IsLeader() {
			leader = s
		}
	}
	require.NotNil(t, leader)
	c := genericClient(t, leader)
	tenantAPI(t, c, "alpha")
	for _, s := range members {
		if s != leader {
			require.NoError(t, s.Close(context.Background()))
		}
	}
	require.True(t, leader.application.Raft.IsLeader())
	code, body, _ := probeReadiness(t, leader)
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.False(t, body.Ready)
	require.True(t, body.Leader)
	require.Equal(t, "quorum_unavailable", body.Reason)
	health, err := http.Get("http://" + leader.HTTPAddr() + "/healthz")
	require.NoError(t, err)
	health.Body.Close()
	require.Equal(t, http.StatusOK, health.StatusCode)
	// The first timed-out accepted probe keeps one slot inside Raft.
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
	code, body, _ = probeReadiness(t, leader)
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.Equal(t, "busy", body.Reason)
	require.NotNil(t, getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_TENANT}).GetTenant(), "local inspection still works during a quorum outage")
	// Draining supersedes a saturated probe queue immediately.
	leader.cancelStartup()
	w := httptest.NewRecorder()
	leader.readiness(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Contains(t, w.Body.String(), "stopping")
}

func TestReadinessCancelsAnActiveProbeDuringShutdown(t *testing.T) {
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
	leader.cfg.ReadinessTimeout = 5 * time.Second
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		leader.readiness(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		done <- w
	}()
	time.Sleep(20 * time.Millisecond)
	leader.cancelStartup()
	select {
	case w := <-done:
		require.Equal(t, http.StatusServiceUnavailable, w.Code)
		require.Contains(t, w.Body.String(), "stopping")
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel an active readiness probe")
	}
}

func TestReadinessConfigurationAndStartingState(t *testing.T) {
	for _, timeout := range []time.Duration{-time.Second, MaxReadinessTimeout + time.Nanosecond} {
		cfg := apiTestConfig(filepath.Join(t.TempDir(), "unopened"))
		cfg.ReadinessTimeout = timeout
		s, err := Start(context.Background(), cfg)
		require.Error(t, err)
		require.Nil(t, s)
		_, err = os.Stat(cfg.Raft.DataDir)
		require.True(t, os.IsNotExist(err))
	}
	var s Server
	w := httptest.NewRecorder()
	s.readiness(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Contains(t, w.Body.String(), "starting")
}
