package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/jobs"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	old "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/duravela/djob/sdk/go"
	"github.com/duravela/djob/sdk/go/handlers"
	"github.com/duravela/djob/sdk/go/worker"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func workerToken(tenant string) string { return strings.Repeat(tenant+"-worker-", 4) }

func workerAPI(t *testing.T, edit ...func(*Config)) (*Server, *http.Client) {
	t.Helper()
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	for _, tenant := range []string{"alpha", "beta"} {
		cfg.APIAuth.Credentials = append(cfg.APIAuth.Credentials, APICredential{Subject: tenant + "/worker", TenantID: tenant, Role: APIWorker, Token: workerToken(tenant)})
	}
	// Rotation tokens share an audit subject, but cannot control each other's sessions.
	cfg.APIAuth.Credentials = append(cfg.APIAuth.Credentials, APICredential{Subject: "alpha/worker", TenantID: "alpha", Role: APIWorker, Token: strings.Repeat("rotated-", 5)})
	tlsConfig, roots := apiTestCertificate(t)
	cfg.APITLS = tlsConfig
	cfg.RemoteWorkers = RemoteWorkerConfig{Enabled: true, Heartbeat: 100 * time.Millisecond, PollWait: 20 * time.Millisecond, IdleTimeout: 3 * time.Second, ReplyGrace: time.Second}
	cfg.JobDispatch.PollInterval = 5 * time.Millisecond
	cfg.JobWorker = jobs.Config{LeaseDuration: 3 * time.Second, SafetyMargin: 500 * time.Millisecond, RenewInterval: 200 * time.Millisecond, CommitTimeout: 100 * time.Millisecond, OperationTimeout: 2 * time.Second, RecoveryTimeout: time.Second, PollInterval: 5 * time.Millisecond}
	for _, f := range edit {
		f(&cfg)
	}
	s := startAPI(t, cfg)
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(transport.CloseIdleConnections)
	return s, &http.Client{Transport: transport, Timeout: 3 * time.Second}
}

func workerPost(t *testing.T, c *http.Client, s *Server, token, path string, value any, reply any, want int) {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	r, err := http.NewRequest(http.MethodPost, "https://"+s.HTTPAddr()+path, bytes.NewReader(data))
	require.NoError(t, err)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	response, err := c.Do(r)
	require.NoError(t, err)
	defer response.Body.Close()
	data, err = io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, want, response.StatusCode, string(data))
	if reply != nil {
		require.NoError(t, json.Unmarshal(data, reply))
	}
}

func TestRemoteWorkerCredentialsAreTenantAndSessionScoped(t *testing.T) {
	s, c := workerAPI(t, func(cfg *Config) { cfg.RemoteWorkers.MaxPerTenant = 1 })
	registration := wire.Register{Protocol: wire.Version, WorkerID: "worker-1", TenantID: "alpha", Handlers: []djob.Version{{Name: "sha256", Version: "1"}}}
	for _, token := range []string{genericToken("ops/alice"), genericToken("alpha/writer")} {
		workerPost(t, c, s, token, wire.Path+"register", registration, nil, 403)
	}
	workerPost(t, c, s, "", wire.Path+"register", registration, nil, 401)
	workerPost(t, c, s, workerToken("beta"), wire.Path+"register", registration, nil, 403)
	var registered wire.Registered
	workerPost(t, c, s, workerToken("alpha"), wire.Path+"register", registration, &registered, 200)
	require.True(t, wire.ValidNonce(registered.SessionID))
	workerPost(t, c, s, workerToken("alpha"), wire.Path+"register", registration, nil, 429)
	for _, token := range []string{workerToken("beta"), strings.Repeat("rotated-", 5)} {
		workerPost(t, c, s, token, wire.Path+registered.SessionID+"/heartbeat", wire.Heartbeat{}, nil, 404)
	}
	workerPost(t, c, s, workerToken("alpha"), wire.Path+registered.SessionID+"/result", wire.Result{ID: strings.Repeat("b", 64)}, nil, 409)
	registration.Protocol = 2
	workerPost(t, c, s, workerToken("alpha"), wire.Path+"register", registration, nil, 426)
	workerPost(t, c, s, workerToken("alpha"), wire.Path+registered.SessionID+"/disconnect", struct{}{}, nil, 204)
	workerPost(t, c, s, workerToken("alpha"), wire.Path+registered.SessionID+"/heartbeat", wire.Heartbeat{}, nil, 404)
	// Worker credentials grant no ordinary read, mutation, callback or legacy access.
	for _, service := range []string{"jobs", "legacy"} {
		if service == "jobs" {
			for _, method := range jp.Jobs_ServiceDesc.Methods {
				_, err := method.Handler(s.jobs, metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+workerToken("alpha"))), func(any) error { return nil }, nil)
				require.Equal(t, codes.PermissionDenied, status.Code(err), method.MethodName)
			}
		} else {
			for _, method := range old.Scheduler_ServiceDesc.Methods {
				_, err := method.Handler(s.scheduler, metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+workerToken("alpha"))), func(any) error { return nil }, nil)
				require.Equal(t, codes.PermissionDenied, status.Code(err), method.MethodName)
			}
		}
	}
}

func TestRemoteWorkersRequireAuthenticationAndTLSBeforeStorage(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*Config)
	}{
		{"anonymous", func(c *Config) { c.APIAuth = nil }},
		{"plaintext", func(c *Config) { c.APITLS = nil }},
		{"invalid timing", func(c *Config) { c.RemoteWorkers.Heartbeat = time.Hour }},
		{"unbounded quorum probe", func(c *Config) { c.RemoteWorkers.QuorumProbeTimeout = time.Minute }},
		{"quorum probe storm", func(c *Config) { c.RemoteWorkers.QuorumProbeInterval = time.Millisecond }},
		{"no election grace", func(c *Config) { c.RemoteWorkers.QuorumGrace = time.Second }},
		{"unscoped credential", func(c *Config) {
			c.APIAuth.Credentials = []APICredential{{Subject: "worker", Role: APIWorker, Token: workerToken("alpha")}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := apiTestConfig(filepath.Join(t.TempDir(), "unopened"))
			cfg.APIAuth = genericCredentials()
			cfg.APITLS, _ = apiTestCertificate(t)
			cfg.RemoteWorkers.Enabled = true
			test.edit(&cfg)
			s, err := Start(context.Background(), cfg)
			require.Error(t, err)
			require.Nil(t, s)
			_, err = os.Stat(cfg.Raft.DataDir)
			require.True(t, os.IsNotExist(err))
		})
	}
}

func TestRemoteResultsAreBoundToOneDeliveredCommand(t *testing.T) {
	s, c := workerAPI(t)
	var registration wire.Registered
	workerPost(t, c, s, workerToken("alpha"), wire.Path+"register", wire.Register{Protocol: wire.Version, WorkerID: "outcomes", TenantID: "alpha", Handlers: []djob.Version{{Name: "test", Version: "1"}}}, &registration, 200)
	var session *workerSession
	for _, candidate := range s.remoteWorkers.snapshot() {
		if candidate.id == registration.SessionID {
			session = candidate
		}
	}
	require.NotNil(t, session)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan wire.Result, 1)
	go func() {
		result, _ := session.call(ctx, wire.Command{Kind: wire.Evaluate, View: &djob.View{Run: djob.JobRun{TenantID: "alpha"}}})
		done <- result
	}()
	var command wire.Command
	workerPost(t, c, s, workerToken("alpha"), wire.Path+session.id+"/poll", struct{}{}, &command, 200)
	decision := &djob.Decision{Kind: djob.JobComplete, Status: djob.JobSucceeded}
	workerPost(t, c, s, workerToken("alpha"), wire.Path+session.id+"/result", wire.Result{ID: strings.Repeat("f", 64), Decision: decision}, nil, 409)
	workerPost(t, c, s, workerToken("alpha"), wire.Path+session.id+"/result", wire.Result{ID: command.ID, Decision: decision}, nil, 204)
	workerPost(t, c, s, workerToken("alpha"), wire.Path+session.id+"/result", wire.Result{ID: command.ID, Decision: decision}, nil, 409)
	select {
	case result := <-done:
		require.Equal(t, command.ID, result.ID)
		require.Equal(t, decision, result.Decision)
	case <-ctx.Done():
		t.Fatal("matching result was not delivered")
	}
	// A decision cannot be replaced with an effect report, even with the right ID.
	go func() {
		_, _ = session.call(ctx, wire.Command{Kind: wire.Evaluate, View: &djob.View{Run: djob.JobRun{TenantID: "alpha"}}})
	}()
	workerPost(t, c, s, workerToken("alpha"), wire.Path+session.id+"/poll", struct{}{}, &command, 200)
	workerPost(t, c, s, workerToken("alpha"), wire.Path+session.id+"/result", wire.Result{ID: command.ID, Effect: &djob.Effect{Status: djob.EffectSucceeded}}, nil, 400)
	workerPost(t, c, s, workerToken("alpha"), wire.Path+session.id+"/heartbeat", wire.Heartbeat{}, nil, 404)
}

type remoteDevice struct{ calls, factories, quiesced, closed atomic.Int32 }
type remoteDriver struct {
	device *remoteDevice
	count  int
}

func (d *remoteDriver) Execute(ctx context.Context, a djob.Action) djob.Effect {
	if ctx.Err() != nil {
		return djob.Effect{Status: djob.EffectFailed, Error: "expired"}
	}
	d.count++
	d.device.calls.Add(1)
	return djob.Effect{Status: djob.EffectSucceeded, Data: []byte(fmt.Sprint(d.count))}
}
func (d *remoteDriver) Quiesce(context.Context) error { d.device.quiesced.Add(1); return nil }
func (d *remoteDriver) Close(context.Context) error   { d.device.closed.Add(1); return nil }

func remoteRegistry(d *remoteDevice) djob.Registry {
	r := handlers.Builtins()
	r.Handlers[djob.Version{Name: "sequence", Version: "1"}] = func(v djob.View) (djob.Decision, error) {
		switch string(v.Run.State) {
		case "":
			return djob.Decision{Kind: djob.JobDoAction, State: []byte("first"), Operation: "measure"}, nil
		case "first":
			return djob.Decision{Kind: djob.JobWait, State: []byte("waiting"), WaitKey: "continue"}, nil
		case "waiting":
			return djob.Decision{Kind: djob.JobDoAction, State: []byte("second"), Operation: "measure"}, nil
		default:
			return djob.Decision{Kind: djob.JobComplete, Status: djob.JobSucceeded, Result: v.Run.PendingAction.Result}, nil
		}
	}
	r.Drivers[djob.Version{Name: "device", Version: "1"}] = func(djob.JobRun) (djob.Driver, error) { d.factories.Add(1); return &remoteDriver{device: d}, nil }
	return r
}

func runRemoteWorker(t *testing.T, s *Server, c *http.Client, tenant string, r djob.Registry, edit ...func(*worker.Config)) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := worker.Config{ServerURL: "https://" + s.HTTPAddr(), WorkerID: tenant + "-device", TenantID: tenant, Token: workerToken(tenant), Registry: r, Client: c, DrainTimeout: time.Second}
	for _, f := range edit {
		f(&cfg)
	}
	done := make(chan error, 1)
	go func() {
		defer close(done)
		done <- worker.Run(ctx, cfg)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(4 * time.Second):
			t.Error("worker did not stop")
		}
	})
	return cancel, done
}

func remoteSeed(t *testing.T, s *Server, commands ...db.JobCommand) {
	t.Helper()
	for _, command := range commands {
		id, err := workerNonce()
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		result, err := s.application.Store.ProposeStateCommand(ctx, db.StateCommand{RequestID: "seed/" + id, At: time.Now().UTC(), Job: &command})
		cancel()
		require.NoError(t, err)
		require.NoError(t, result.Err())
	}
}

func remotePrepare(t *testing.T, s *Server) {
	remoteSeed(t, s, db.JobCommand{PutResource: &db.PutJobResource{Spec: db.ResourceSpec{ID: "bus", Duplex: "half", Capacity: 1}}})
	for _, tenant := range []string{"alpha", "beta"} {
		remoteSeed(t, s,
			db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: tenant}}},
			db.JobCommand{Grant: &db.PutResourceGrant{Grant: db.ResourceGrant{TenantID: tenant, ResourceID: "bus", Allowed: true}}},
			db.JobCommand{PutBinding: &db.PutJobBinding{Binding: db.JobBinding{TenantID: tenant, ID: "device", Driver: "device", DriverVersion: "1", Resources: []string{"bus"}}}},
			db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: tenant, ID: "sequence", Runtime: db.StepRuntime, Handler: "sequence", HandlerVersion: "1", BindingID: "device"}}},
			db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: tenant, ID: "invoke", Runtime: db.StepRuntime, Handler: "invoke", HandlerVersion: "1", BindingID: "device"}}},
			db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: tenant, ID: "compute", Runtime: db.StepRuntime, Handler: "sha256", HandlerVersion: "1", Input: []byte("independent")}}},
		)
	}
}

func remoteLaunch(t *testing.T, s *Server, tenant, definition, id string) {
	remoteSeed(t, s, db.JobCommand{Launch: &db.LaunchJob{TenantID: tenant, DefinitionID: definition, DefinitionRevision: 1, RunID: id}})
}

func remoteState(t *testing.T, s *Server, tenant, id string, status db.JobStatus) *db.JobRun {
	t.Helper()
	var run *db.JobRun
	require.EventuallyWithT(t, func(check *assert.CollectT) {
		v, err := s.application.Store.GetJobView(tenant, id)
		require.NoError(check, err)
		run = v.Run
		require.NotNil(check, run)
		require.Equal(check, status, run.Status, "%s/%s: %+v", tenant, id, run)
	}, 5*time.Second, 5*time.Millisecond, "%s/%s becomes %s", tenant, id, status)
	return run
}

func TestRemoteConversationRetainsItsDriverAndHalfDuplexReservationThroughDrain(t *testing.T) {
	s, c := workerAPI(t, func(cfg *Config) {
		// This scenario verifies successful drain, not subsecond disk latency.
		// Expiry and lost-acknowledgement tests retain the short fixture leases.
		cfg.JobWorker.LeaseDuration = 10 * time.Second
		cfg.JobWorker.SafetyMargin = 2 * time.Second
		cfg.JobWorker.RenewInterval = 500 * time.Millisecond
		cfg.JobWorker.CommitTimeout = time.Second
		cfg.RemoteWorkers.IdleTimeout = 5 * time.Second
	})
	require.Nil(t, s.jobDispatcher)
	remotePrepare(t, s)
	alpha, beta := &remoteDevice{}, &remoteDevice{}
	cancel, done := runRemoteWorker(t, s, c, "alpha", remoteRegistry(alpha), func(cfg *worker.Config) {
		// This success case must allow an operation (up to two seconds) and
		// its committed completion to finish within the drain grace.
		cfg.DrainTimeout = 8 * time.Second
	})
	runRemoteWorker(t, s, c, "beta", remoteRegistry(beta))
	remoteLaunch(t, s, "alpha", "sequence", "conversation")
	waiting := remoteState(t, s, "alpha", "conversation", db.JobWaiting)
	require.Contains(t, waiting.OwnerID, "remote-worker/1/alpha-device/")
	remoteLaunch(t, s, "beta", "invoke", "contender")
	remoteLaunch(t, s, "beta", "compute", "independent")
	remoteState(t, s, "beta", "independent", db.JobSucceeded)
	require.Zero(t, beta.calls.Load(), "shared half-duplex bus must remain reserved while waiting")
	cancel() // Stop admission, retain the live driver until the event finishes this conversation.
	remoteSeed(t, s, db.JobCommand{Signal: &db.SignalJob{TenantID: "alpha", RunID: "conversation", EventID: "continue", Key: "continue"}})
	finished := remoteState(t, s, "alpha", "conversation", db.JobSucceeded)
	require.Equal(t, []byte("2"), finished.Result)
	remoteState(t, s, "beta", "contender", db.JobSucceeded)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("drain did not complete")
	}
	require.EqualValues(t, 1, alpha.factories.Load())
	require.EqualValues(t, 2, alpha.calls.Load())
	require.EqualValues(t, 1, alpha.closed.Load())
}

func TestRemoteConnectionLossBlocksSharedResourceAndNeverResumesTheConversation(t *testing.T) {
	s, c := workerAPI(t)
	remotePrepare(t, s)
	alpha, beta := &remoteDevice{}, &remoteDevice{}
	_, done := runRemoteWorker(t, s, c, "alpha", remoteRegistry(alpha))
	runRemoteWorker(t, s, c, "beta", remoteRegistry(beta))
	remoteLaunch(t, s, "alpha", "sequence", "lost")
	remoteState(t, s, "alpha", "lost", db.JobWaiting)
	for _, session := range s.remoteWorkers.snapshot() {
		if session.tenant == "alpha" {
			session.stop()
		}
	}
	remoteState(t, s, "alpha", "lost", db.JobUnknown)
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("lost worker did not stop")
	}
	resource, err := s.application.Store.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, db.ResourceBlocked, resource.State)
	runRemoteWorker(t, s, c, "alpha", remoteRegistry(alpha))
	remoteLaunch(t, s, "beta", "invoke", "blocked-contender")
	remoteLaunch(t, s, "beta", "compute", "unrelated")
	remoteState(t, s, "beta", "unrelated", db.JobSucceeded)
	require.Never(t, func() bool { return beta.calls.Load() > 0 || alpha.calls.Load() > 1 }, 200*time.Millisecond, 10*time.Millisecond)
	action, err := s.application.Store.GetJobAction("alpha", "lost", 1)
	require.NoError(t, err)
	require.Equal(t, db.EffectSucceeded, action.Status, "confirmed action history survives connection loss")
}

type cancelRemoteDriver struct {
	started, canceled, release, stopped chan struct{}
	quiesced, closed                    atomic.Int32
	premature                           atomic.Bool
	recoveryErr                         error
}

func (d *cancelRemoteDriver) Execute(ctx context.Context, _ djob.Action) djob.Effect {
	close(d.started)
	<-ctx.Done()
	close(d.canceled)
	<-d.release // Model a driver still joining its canceled I/O.
	close(d.stopped)
	return djob.Effect{Status: djob.EffectUnknown, Error: "canceled after I/O began"}
}
func (d *cancelRemoteDriver) Quiesce(context.Context) error {
	select {
	case <-d.stopped:
	default:
		d.premature.Store(true)
	}
	d.quiesced.Add(1)
	return d.recoveryErr
}
func (d *cancelRemoteDriver) Close(context.Context) error { d.closed.Add(1); return nil }

func TestRemoteCancellationWaitsForDriverIOBeforeCleanup(t *testing.T) {
	for _, test := range []struct {
		name        string
		recoveryErr error
		want        db.ResourceState
	}{
		{"quiescence confirmed", nil, db.ResourceAvailable},
		{"quiescence unproven", errors.New("device idle state unproven"), db.ResourceBlocked},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, c := workerAPI(t, func(cfg *Config) { cfg.RemoteWorkers.ReplyGrace = 2 * time.Second })
			remotePrepare(t, s)
			d := &cancelRemoteDriver{started: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}), stopped: make(chan struct{})}

			d.recoveryErr = test.recoveryErr
			registry := handlers.Builtins()
			registry.Drivers[djob.Version{Name: "device", Version: "1"}] = func(djob.JobRun) (djob.Driver, error) { return d, nil }
			runRemoteWorker(t, s, c, "alpha", registry)
			t.Cleanup(func() {
				select {
				case <-d.release:
				default:
					close(d.release)
				}
			})
			remoteLaunch(t, s, "alpha", "invoke", "cancel")
			select {
			case <-d.started:
			case <-time.After(3 * time.Second):
				t.Fatal("remote I/O did not start")
			}
			remoteSeed(t, s, db.JobCommand{Cancel: &db.CancelJob{TenantID: "alpha", RunID: "cancel", Reason: "operator canceled"}})
			select {
			case <-d.canceled:
			case <-time.After(3 * time.Second):
				t.Fatal("durable cancellation did not reach the driver")
			}
			resource, err := s.application.Store.GetJobResource("bus")
			require.NoError(t, err)
			require.Equal(t, db.ResourceOwned, resource.State, "cancellation cannot release a bus while I/O is still returning")
			require.Zero(t, d.quiesced.Load())
			close(d.release)
			remoteState(t, s, "alpha", "cancel", db.JobUnknown)
			action, err := s.application.Store.GetJobAction("alpha", "cancel", 1)
			require.NoError(t, err)
			require.Equal(t, db.EffectUnknown, action.Status)
			require.False(t, d.premature.Load())
			require.EqualValues(t, 1, d.quiesced.Load())
			require.EqualValues(t, 1, d.closed.Load())
			resource, err = s.application.Store.GetJobResource("bus")
			require.NoError(t, err)
			require.Equal(t, test.want, resource.State, "release requires explicit quiescence, independently of the unknown business outcome")
		})
	}
}

func TestRemoteEvaluationDisconnectDoesNotFailPureRun(t *testing.T) {
	s, c := workerAPI(t)
	remotePrepare(t, s)
	registration := wire.Register{Protocol: wire.Version, WorkerID: "interrupted-evaluation", TenantID: "alpha", Handlers: []djob.Version{{Name: "sha256", Version: "1"}}}
	var registered wire.Registered
	workerPost(t, c, s, workerToken("alpha"), wire.Path+"register", registration, &registered, 200)
	remoteLaunch(t, s, "alpha", "compute", "interrupted")
	sessions := s.remoteWorkers.snapshot()
	require.Len(t, sessions, 1)
	var command *wire.Command
	require.Eventually(t, func() bool {
		var fault *workerFault
		command, fault = sessions[0].nextCommand(context.Background())
		require.Nil(t, fault)
		return command != nil && command.Kind == wire.Evaluate
	}, time.Second, 5*time.Millisecond)
	require.NotNil(t, command.View)
	require.Equal(t, "interrupted", command.View.Run.ID)
	sessions[0].stop()
	select {
	case <-sessions[0].done:
	case <-time.After(3 * time.Second):
		t.Fatal("interrupted dispatcher did not join")
	}
	remoteState(t, s, "alpha", "interrupted", db.JobReady)
	runRemoteWorker(t, s, c, "alpha", handlers.Builtins())
	remoteState(t, s, "alpha", "interrupted", db.JobSucceeded)
}
