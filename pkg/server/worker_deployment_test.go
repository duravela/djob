package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/duravela/djob/pkg/workercontrol"
	"github.com/duravela/djob/sdk/go/worker"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type failingWorkerRuntime struct{}

func (failingWorkerRuntime) Reconcile(context.Context, db.WorkerDeployment) (workercontrol.Observation, error) {
	return workercontrol.Observation{}, errors.New("private-runtime-marker")
}

func TestManagedWorkerRuntimeFailuresExposeRetryAndIncrementLocalMetrics(t *testing.T) {
	s, client := workerAPI(t, func(cfg *Config) { cfg.WorkerRuntime = failingWorkerRuntime{} })
	remotePrepare(t, s)
	enrollmentAPI(t, s, client, "company-bus", "enroll", 0, "djobw1."+strings.Repeat("b", 64))
	managedDeploymentAPI(t, s, client, "put", 0, 1)
	var reply struct {
		Runtime workercontrol.Observation `json:"runtime"`
	}
	require.Eventually(t, func() bool {
		workerPost(t, client, s, genericToken("ops/alice"), "/api/jobs/v1/worker-deployments/status", jp.WorkerDeploymentQuery{TenantID: "alpha", ID: "line-one"}, &reply, 200)
		return reply.Runtime.RetryAfter != nil
	}, 8*time.Second, 20*time.Millisecond)
	require.Equal(t, "error", reply.Runtime.State)
	require.Equal(t, "unavailable", reply.Runtime.Error)
	require.True(t, reply.Runtime.RetryAfter.After(reply.Runtime.ObservedAt))
	families, err := s.metrics.registry.Gather()
	require.NoError(t, err)
	values := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		values[family.GetName()] = family
	}
	require.GreaterOrEqual(t, metricValue(t, values, "djob_background_errors_total", "worker", "worker_deployments"), float64(1))
}

func managedDeploymentAPI(t *testing.T, s *Server, client *http.Client, action string, revision uint64, replicas uint32) jp.WorkerDeploymentAcknowledgement {
	t.Helper()
	nonce, err := workerNonce()
	require.NoError(t, err)
	q := jp.WorkerDeploymentMutation{Command: &jp.WorkerManagementCommand{RequestID: "deployment/" + nonce, At: time.Now().UTC()}, TenantID: "alpha", ID: "line-one", WorkerID: "company-bus", Action: action, ExpectedRevision: revision, Replicas: replicas, Reason: "company deployment"}
	if action == "put" {
		q.Replicas = 0
		q.Spec = &jp.WorkerDeploymentSpec{Pool: "default", Image: "registry.example/acme/worker@sha256:" + strings.Repeat("a", 64), ConfigSecret: "company-bus", Replicas: replicas}
	}
	var ack, duplicate jp.WorkerDeploymentAcknowledgement
	workerPost(t, client, s, genericToken("ops/alice"), "/api/jobs/v1/worker-deployments/mutate", q, &ack, 200)
	workerPost(t, client, s, genericToken("ops/alice"), "/api/jobs/v1/worker-deployments/mutate", q, &duplicate, 200)
	require.Equal(t, ack, duplicate)
	require.Equal(t, revision+1, ack.Deployment.Revision)
	return ack
}

func TestManagedWorkerAPIEnforcesTenantRolesAndGeneration(t *testing.T) {
	s, c := workerAPI(t)
	remotePrepare(t, s)
	token := "djobw1." + strings.Repeat("d", 64)
	enrollmentAPI(t, s, c, "company-bus", "enroll", 0, token)
	managedDeploymentAPI(t, s, c, "put", 0, 1)
	for _, tenant := range []string{"alpha", "beta"} {
		want := 200
		if tenant == "beta" {
			want = 403
		}
		workerPost(t, c, s, genericToken("alpha/reader"), "/api/jobs/v1/worker-deployments/list", jp.WorkerDeploymentQuery{TenantID: tenant}, nil, want)
	}
	for _, actor := range []string{"alpha/reader", "alpha/writer", "alpha/operator", "alpha/callback", "alpha/producer"} {
		workerPost(t, c, s, genericToken(actor), "/api/jobs/v1/worker-deployments/mutate", jp.WorkerDeploymentMutation{TenantID: "alpha"}, nil, 403)
	}
	registration := func(generation uint64, code codes.Code) {
		stream := workerGRPCStream(t, s, c, token)
		f := grpcRegistration()
		f.GetRegister().WorkerId = "company-bus"
		if generation > 0 {
			f.GetRegister().DeploymentId = "line-one"
			f.GetRegister().DeploymentGeneration = generation
		}
		_ = stream.Send(f)
		reply, err := stream.Recv()
		require.Equal(t, code, status.Code(err))
		if code == codes.OK {
			require.EqualValues(t, generation, reply.GetRegistered().DeploymentGeneration)
			require.Equal(t, "line-one", reply.GetRegistered().DeploymentId)
		}
		_ = stream.CloseSend()
	}
	registration(0, codes.PermissionDenied)
	registration(1, codes.OK)
	managedDeploymentAPI(t, s, c, "restart", 1, 0)
	registration(1, codes.PermissionDenied)
	registration(2, codes.OK)
	managedDeploymentAPI(t, s, c, "scale", 2, 0)
	registration(2, codes.PermissionDenied)
	registration(3, codes.PermissionDenied)
	managedDeploymentAPI(t, s, c, "scale", 3, 1)
	registration(3, codes.OK)
}

func TestManagedWorkerStopDrainsHalfDuplexConversationWithoutReleasingEarly(t *testing.T) {
	s, c := workerAPI(t, func(cfg *Config) {
		cfg.JobWorker.LeaseDuration = 10 * time.Second
		cfg.JobWorker.SafetyMargin = 2 * time.Second
		cfg.JobWorker.RenewInterval = 500 * time.Millisecond
		cfg.JobWorker.CommitTimeout = time.Second
		cfg.RemoteWorkers.IdleTimeout = 5 * time.Second
	})
	remotePrepare(t, s)
	token := "djobw1." + strings.Repeat("c", 64)
	enrollmentAPI(t, s, c, "company-bus", "enroll", 0, token)
	managedDeploymentAPI(t, s, c, "put", 0, 1)
	alpha, beta := &remoteDevice{}, &remoteDevice{}
	_, done := runRemoteWorker(t, s, c, "alpha", remoteRegistry(alpha), func(cfg *worker.Config) {
		cfg.WorkerID, cfg.Token = "company-bus", token
		cfg.DeploymentID, cfg.DeploymentGeneration = "line-one", 1
		cfg.DrainTimeout = 8 * time.Second
	})
	runRemoteWorker(t, s, c, "beta", remoteRegistry(beta))
	remoteLaunch(t, s, "alpha", "sequence", "managed-conversation")
	remoteState(t, s, "alpha", "managed-conversation", db.JobWaiting)
	managedDeploymentAPI(t, s, c, "scale", 1, 0)
	var stopped struct {
		Sessions jp.WorkerDeploymentSessions `json:"worker_sessions"`
	}
	workerPost(t, c, s, genericToken("alpha/reader"), "/api/jobs/v1/worker-deployments/status", jp.WorkerDeploymentQuery{TenantID: "alpha", ID: "line-one"}, &stopped, 200)
	require.EqualValues(t, 2, stopped.Sessions.Generation)
	require.Zero(t, stopped.Sessions.Connected)
	require.EqualValues(t, 1, stopped.Sessions.OtherGenerations, "the old generation may still hold its half-duplex conversation")
	remoteLaunch(t, s, "beta", "invoke", "managed-contender")
	remoteLaunch(t, s, "alpha", "compute", "must-stay-queued")
	resource, err := s.application.Store.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, db.ResourceOwned, resource.State)
	require.Zero(t, beta.calls.Load())
	remoteSeed(t, s, db.JobCommand{Signal: &db.SignalJob{TenantID: "alpha", RunID: "managed-conversation", EventID: "continue", Key: "continue"}})
	finished := remoteState(t, s, "alpha", "managed-conversation", db.JobSucceeded)
	require.Equal(t, []byte("2"), finished.Result)
	remoteState(t, s, "beta", "managed-contender", db.JobSucceeded)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("stopped deployment worker did not complete cleanup")
	}
	queued, err := s.application.Store.GetJobRun("alpha", "must-stay-queued")
	require.NoError(t, err)
	require.Equal(t, db.JobReady, queued.Status)
	require.EqualValues(t, 1, alpha.factories.Load())
	require.EqualValues(t, 2, alpha.calls.Load())
	require.EqualValues(t, 1, alpha.closed.Load())
}

func TestWorkerDeploymentRecoversPreProtectionReceiptWithoutApplyingUnsafeFallback(t *testing.T) {
	s, client := workerAPI(t)
	remotePrepare(t, s)
	enrollmentAPI(t, s, client, "company-bus", "enroll", 0, "djobw1."+strings.Repeat("e", 64))
	at := time.Now().UTC()
	spec := jp.WorkerDeploymentSpec{Pool: "default", Image: "registry.example/acme/worker@sha256:" + strings.Repeat("a", 64), ConfigSecret: "company-bus", Replicas: 1}
	payload := db.WorkerDeploymentCommand{TenantID: "alpha", ID: "line-one", WorkerID: "company-bus", Action: "put", Actor: "ops/alice", Reason: "historical request", Spec: ptrDeploymentSpec(deploymentSpec(spec))}
	command := db.StateCommand{RequestID: "worker-deploy/" + jobRequestID("alpha", "historical-put"), At: at, WorkerDeployment: &payload}
	result, err := s.application.Store.ProposeStateCommand(context.Background(), command)
	require.NoError(t, err)
	require.NoError(t, result.Err())
	request := jp.WorkerDeploymentMutation{Command: &jp.WorkerManagementCommand{RequestID: "historical-put", At: at}, TenantID: "alpha", ID: "line-one", WorkerID: "company-bus", Action: "put", Reason: "historical request", Spec: &spec}
	var ack jp.WorkerDeploymentAcknowledgement
	workerPost(t, client, s, genericToken("ops/alice"), "/api/jobs/v1/worker-deployments/mutate", request, &ack, 200)
	require.EqualValues(t, 1, ack.Deployment.Revision)
	request.Reason = "different request must never apply"
	workerPost(t, client, s, genericToken("ops/alice"), "/api/jobs/v1/worker-deployments/mutate", request, nil, 409)
	current, err := s.application.Store.GetWorkerDeployment("alpha", "line-one")
	require.NoError(t, err)
	require.EqualValues(t, 1, current.Revision)
}
func ptrDeploymentSpec(spec db.WorkerDeploymentSpec) *db.WorkerDeploymentSpec { return &spec }
