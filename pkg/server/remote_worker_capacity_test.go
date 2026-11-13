package server

import (
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/sdk/go"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
)

// Discovery can be waiting for application/storage even after its caller stops.
// Hold it independently of the session so a disconnect cannot pretend it joined.
type blockedWorkerDiscovery struct {
	workerBackend
	entered, release chan struct{}
	once             sync.Once
}

func (b *blockedWorkerDiscovery) ListDispatchJobs(after []byte, limit int, excluded []string) (db.JobRecordPage, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return b.workerBackend.ListDispatchJobs(after, limit, excluded)
}

func remoteCapacityMetrics(t *testing.T, s *Server, c *http.Client) map[string]*dto.MetricFamily {
	t.Helper()
	data := authenticatedHTTP(t, c, "https://"+s.HTTPAddr(), http.MethodGet, "/metrics", genericToken("ops/alice"), nil, nil, 200, nil)
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(strings.NewReader(string(data)))
	require.NoError(t, err)
	return families
}

func TestRemoteWorkerCapacityRemainsReservedUntilControllerJoins(t *testing.T) {
	for _, test := range []struct {
		name     string
		max      int
		wantBeta int
	}{
		{"member limit", 1, http.StatusTooManyRequests},
		{"tenant limit", 2, http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, c := workerAPI(t, func(cfg *Config) {
				cfg.RemoteWorkers.MaxSessions, cfg.RemoteWorkers.MaxPerTenant = test.max, 1
			})
			backend := &blockedWorkerDiscovery{workerBackend: s.remoteWorkers.backend, entered: make(chan struct{}), release: make(chan struct{})}
			s.remoteWorkers.backend = backend
			t.Cleanup(func() {
				select {
				case <-backend.release:
				default:
					close(backend.release)
				}
			})
			registration := wire.Register{Protocol: wire.Version, WorkerID: "capacity", TenantID: "alpha", Handlers: []djob.Version{{Name: "sha256", Version: "1"}}}
			var alpha wire.Registered
			workerPost(t, c, s, workerToken("alpha"), wire.Path+"register", registration, &alpha, 200)
			sessions := s.remoteWorkers.snapshot()
			require.Len(t, sessions, 1)
			session := sessions[0]
			workerPost(t, c, s, workerToken("alpha"), wire.Path+alpha.SessionID+"/poll", struct{}{}, nil, 204)
			select {
			case <-backend.entered:
			case <-time.After(time.Second):
				t.Fatal("controller discovery did not start")
			}
			workerPost(t, c, s, workerToken("alpha"), wire.Path+alpha.SessionID+"/disconnect", struct{}{}, nil, 204)
			require.Empty(t, s.remoteWorkers.snapshot(), "a disconnected worker loses access to its old session")
			require.False(t, session.finished(), "disconnect must not pretend controller work has returned")
			workerPost(t, c, s, workerToken("alpha"), wire.Path+alpha.SessionID+"/heartbeat", wire.Heartbeat{}, nil, 404)
			for range 4 {
				workerPost(t, c, s, workerToken("alpha"), wire.Path+"register", registration, nil, 429)
			}
			registration.TenantID = "beta"
			var beta wire.Registered
			var betaReply any
			if test.wantBeta == 200 {
				betaReply = &beta
			}
			workerPost(t, c, s, workerToken("beta"), wire.Path+"register", registration, betaReply, test.wantBeta)
			live := float64(0)
			if test.wantBeta == 200 {
				live = 1
			}
			metrics := remoteCapacityMetrics(t, s, c)
			require.Equal(t, live, metricValue(t, metrics, "djob_worker_sessions"))
			require.Equal(t, live+1, metricValue(t, metrics, "djob_worker_session_slots"))
			require.Equal(t, float64(1), metricValue(t, metrics, "djob_worker_sessions_closing"))
			require.Equal(t, float64(test.max), metricValue(t, metrics, "djob_worker_session_slot_limit"))
			require.Equal(t, float64(1), metricValue(t, metrics, "djob_worker_tenant_session_slot_limit"))
			require.Equal(t, live, metricValue(t, metrics, "djob_worker_sessions_by_state", "state", "registered"))
			require.Zero(t, metricValue(t, metrics, "djob_worker_sessions_by_state", "state", "connected"), "registration alone does not establish active polling")
			close(backend.release)
			select {
			case <-session.done:
			case <-time.After(2 * time.Second):
				t.Fatal("controller execution did not join")
			}
			metrics = remoteCapacityMetrics(t, s, c)
			require.Equal(t, live, metricValue(t, metrics, "djob_worker_session_slots"))
			require.Zero(t, metricValue(t, metrics, "djob_worker_sessions_closing"))
			registration.TenantID = "alpha"
			workerPost(t, c, s, workerToken("alpha"), wire.Path+"register", registration, &alpha, 200)
			workerPost(t, c, s, workerToken("alpha"), wire.Path+alpha.SessionID+"/disconnect", struct{}{}, nil, 204)
			if test.wantBeta == 200 {
				workerPost(t, c, s, workerToken("beta"), wire.Path+beta.SessionID+"/disconnect", struct{}{}, nil, 204)
			}
			metrics = remoteCapacityMetrics(t, s, c)
			require.Zero(t, metricValue(t, metrics, "djob_worker_session_slots"), "sessions disconnected before first poll release their slot too")
			require.Zero(t, metricValue(t, metrics, "djob_worker_sessions_closing"))
			for _, state := range []string{"connected", "registered", "draining", "relocating", "stale", "unavailable"} {
				require.Zero(t, metricValue(t, metrics, "djob_worker_sessions_by_state", "state", state), "disconnected sessions must not retain a live state")
			}
		})
	}
}

func TestRemoteWorkerConcurrentStartAndStopReleasesCapacityOnce(t *testing.T) {
	s, c := workerAPI(t, func(cfg *Config) { cfg.RemoteWorkers.MaxSessions, cfg.RemoteWorkers.MaxPerTenant = 1, 1 })
	registration := wire.Register{Protocol: wire.Version, WorkerID: "concurrent", TenantID: "alpha", Handlers: []djob.Version{{Name: "sha256", Version: "1"}}}
	for range 32 {
		workerPost(t, c, s, workerToken("alpha"), wire.Path+"register", registration, nil, 200)
		sessions := s.remoteWorkers.snapshot()
		require.Len(t, sessions, 1)
		session := sessions[0]
		start := make(chan struct{})
		var callers sync.WaitGroup
		for i := range 16 {
			callers.Add(1)
			go func() {
				defer callers.Done()
				<-start
				if i%2 == 0 {
					session.start()
				} else {
					session.stop()
				}
			}()
		}
		stopped := make(chan struct{})
		go func() { callers.Wait(); close(stopped) }()
		close(start)
		for {
			s.remoteWorkers.mu.Lock()
			slots, live := s.remoteWorkers.slots, len(s.remoteWorkers.sessions)
			tenantSlots := s.remoteWorkers.tenantSlots["alpha"]
			s.remoteWorkers.mu.Unlock()
			require.GreaterOrEqual(t, slots, live, "a live route must retain its slot during concurrent stop")
			require.Equal(t, slots, tenantSlots)
			select {
			case <-stopped:
				goto joined
			default:
				runtime.Gosched()
			}
		}
	joined:
		select {
		case <-session.done:
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent stop did not join its dispatcher")
		}
		metrics := remoteCapacityMetrics(t, s, c)
		require.Zero(t, metricValue(t, metrics, "djob_worker_sessions"))
		require.Zero(t, metricValue(t, metrics, "djob_worker_session_slots"))
		require.Zero(t, metricValue(t, metrics, "djob_worker_sessions_closing"))
	}
}
