package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/duravela/djob/sdk/go"
	"github.com/duravela/djob/sdk/go/handlers"
	"github.com/duravela/djob/sdk/go/worker"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
	"github.com/stretchr/testify/require"
)

func enrollmentAPI(t *testing.T, s *Server, c *http.Client, id, action string, revision uint64, token string) jp.WorkerEnrollmentAcknowledgement {
	t.Helper()
	idNonce, err := workerNonce()
	require.NoError(t, err)
	q := jp.WorkerEnrollmentMutation{Command: &jp.WorkerManagementCommand{RequestID: "worker-enrollment/" + idNonce, At: time.Now().UTC()}, TenantID: "alpha", WorkerID: id, Action: action, ExpectedRevision: revision, Reason: "company deployment"}
	if action == "enroll" {
		q.Subject = "company/measurement"
	}
	if token != "" {
		hash := sha256.Sum256([]byte(token))
		q.CredentialHash = hex.EncodeToString(hash[:])
	}
	var ack jp.WorkerEnrollmentAcknowledgement
	workerPost(t, c, s, genericToken("ops/alice"), "/api/jobs/v1/worker-enrollments/mutate", q, &ack, 200)
	require.Equal(t, revision+1, ack.Enrollment.Revision)
	var duplicate jp.WorkerEnrollmentAcknowledgement
	workerPost(t, c, s, genericToken("ops/alice"), "/api/jobs/v1/worker-enrollments/mutate", q, &duplicate, 200)
	require.Equal(t, ack, duplicate)
	data, err := json.Marshal(ack)
	require.NoError(t, err)
	require.NotContains(t, string(data), "credential_hash")
	if token != "" {
		require.NotContains(t, string(data), token)
	}
	return ack
}

func TestWorkerEnrollmentAuthenticatesSDKWorkersAndEnforcesLifecycle(t *testing.T) {
	s, client := workerAPI(t)
	remotePrepare(t, s)
	token := "djobw1." + strings.Repeat("a", 64)
	enrollmentAPI(t, s, client, "company-one", "enroll", 0, token)
	q := jp.WorkerEnrollmentMutation{Command: &jp.WorkerManagementCommand{RequestID: "forbidden", At: time.Now().UTC()}, TenantID: "alpha", WorkerID: "forbidden", Action: "enroll", Subject: "forbidden", Reason: "test"}
	for _, actor := range []string{"alpha/reader", "alpha/writer", "alpha/operator", "alpha/callback", "alpha/producer"} {
		workerPost(t, client, s, genericToken(actor), "/api/jobs/v1/worker-enrollments/mutate", q, nil, 403)
	}
	for _, tenant := range []string{"alpha", "beta"} {
		want := 200
		if tenant == "beta" {
			want = 403
		}
		workerPost(t, client, s, genericToken("alpha/reader"), "/api/jobs/v1/worker-enrollments/list", jp.WorkerEnrollmentQuery{TenantID: tenant}, nil, want)
	}
	registration := wire.Register{Protocol: wire.Version, WorkerID: "some-other-id", TenantID: "alpha", Handlers: []djob.Version{{Name: "sha256", Version: "1"}}}
	workerPost(t, client, s, token, wire.Path+"register", registration, nil, 403)
	registration.WorkerID = "company-one"
	workerPost(t, client, s, workerToken("alpha"), wire.Path+"register", registration, nil, 403)
	workerPost(t, client, s, token, "/api/jobs/v1/worker-enrollments/list", jp.WorkerEnrollmentQuery{TenantID: "alpha"}, nil, 401)
	_, done := runRemoteWorker(t, s, client, "alpha", handlers.Builtins(), func(cfg *worker.Config) { cfg.WorkerID, cfg.Token = "company-one", token })
	remoteLaunch(t, s, "alpha", "compute", "before-drain")
	remoteState(t, s, "alpha", "before-drain", db.JobSucceeded)
	enrollmentAPI(t, s, client, "company-one", "drain", 1, "")
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("enrolled worker did not drain")
	}
	workerPost(t, client, s, token, wire.Path+"register", registration, nil, 403)
	enrollmentAPI(t, s, client, "company-one", "resume", 2, "")
	rotated := "djobw1." + strings.Repeat("b", 64)
	enrollmentAPI(t, s, client, "company-one", "rotate", 3, rotated)
	workerPost(t, client, s, token, wire.Path+"register", registration, nil, 401)
	_, done = runRemoteWorker(t, s, client, "alpha", handlers.Builtins(), func(cfg *worker.Config) { cfg.WorkerID, cfg.Token = "company-one", rotated })
	remoteLaunch(t, s, "alpha", "compute", "after-rotation")
	remoteState(t, s, "alpha", "after-rotation", db.JobSucceeded)
	enrollmentAPI(t, s, client, "company-one", "revoke", 4, "")
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("revoked worker did not finish its session")
	}
	workerPost(t, client, s, rotated, wire.Path+"register", registration, nil, 401)
}

func TestWorkerEnrollmentRevocationDrainsHalfDuplexConversationWithoutReleasingEarly(t *testing.T) {
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
	alpha, beta := &remoteDevice{}, &remoteDevice{}
	_, done := runRemoteWorker(t, s, c, "alpha", remoteRegistry(alpha), func(cfg *worker.Config) {
		cfg.WorkerID, cfg.Token = "company-bus", token
		cfg.DrainTimeout = 8 * time.Second
	})
	runRemoteWorker(t, s, c, "beta", remoteRegistry(beta))
	remoteLaunch(t, s, "alpha", "sequence", "enrolled-conversation")
	remoteState(t, s, "alpha", "enrolled-conversation", db.JobWaiting)
	enrollmentAPI(t, s, c, "company-bus", "revoke", 1, "")
	remoteLaunch(t, s, "beta", "invoke", "enrolled-contender")
	remoteLaunch(t, s, "alpha", "compute", "must-stay-queued")
	resource, err := s.application.Store.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, db.ResourceOwned, resource.State)
	require.Zero(t, beta.calls.Load())
	remoteSeed(t, s, db.JobCommand{Signal: &db.SignalJob{TenantID: "alpha", RunID: "enrolled-conversation", EventID: "continue", Key: "continue"}})
	finished := remoteState(t, s, "alpha", "enrolled-conversation", db.JobSucceeded)
	require.Equal(t, []byte("2"), finished.Result)
	remoteState(t, s, "beta", "enrolled-contender", db.JobSucceeded)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("revoked worker did not complete cleanup")
	}
	queued, err := s.application.Store.GetJobRun("alpha", "must-stay-queued")
	require.NoError(t, err)
	require.Equal(t, db.JobReady, queued.Status)
	require.EqualValues(t, 1, alpha.factories.Load())
	require.EqualValues(t, 2, alpha.calls.Load())
	require.EqualValues(t, 1, alpha.closed.Load())
}
