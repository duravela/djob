package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/stretchr/testify/require"
)

func TestDeploymentStatusSeparatesWorkerAccessFromDesiredReplicas(t *testing.T) {
	s, client := workerAPI(t)
	remotePrepare(t, s)
	token := "djobw1." + strings.Repeat("a", 64)
	enrollmentAPI(t, s, client, "company-bus", "enroll", 0, token)
	managedDeploymentAPI(t, s, client, "put", 0, 1)
	for i, state := range []string{"enabled", "draining", "enabled", "revoked"} {
		if i > 0 {
			action := []string{"", "drain", "resume", "revoke"}[i]
			enrollmentAPI(t, s, client, "company-bus", action, uint64(i), "")
		}
		var raw json.RawMessage
		workerPost(t, client, s, genericToken("alpha/reader"), "/api/jobs/v1/worker-deployments/status", jp.WorkerDeploymentQuery{TenantID: "alpha", ID: "line-one"}, &raw, 200)
		var reply struct {
			Access jp.WorkerDeploymentAccess `json:"worker_access"`
		}
		require.NoError(t, json.Unmarshal(raw, &reply))
		require.True(t, reply.Access.Observed)
		require.Equal(t, "company-bus", reply.Access.WorkerID)
		require.Equal(t, state, reply.Access.State)
		require.EqualValues(t, i+1, reply.Access.Revision)
		require.WithinDuration(t, time.Now(), reply.Access.ObservedAt, time.Second)
		hash := sha256.Sum256([]byte(token))
		require.NotContains(t, string(raw), token)
		require.NotContains(t, string(raw), hex.EncodeToString(hash[:]))
		require.NotContains(t, string(raw), "credential_hash")
		deployment, err := s.application.Store.GetWorkerDeployment("alpha", "line-one")
		require.NoError(t, err)
		require.EqualValues(t, 1, deployment.Revision)
		require.EqualValues(t, 1, deployment.Spec.Replicas, "enrollment policy must not silently rewrite desired process state")
	}
	workerPost(t, client, s, genericToken("beta/reader"), "/api/jobs/v1/worker-deployments/status", jp.WorkerDeploymentQuery{TenantID: "alpha", ID: "line-one"}, nil, 403)
}

type workerAccessBackend struct {
	JobsBackend
	value *db.WorkerEnrollment
	err   error
}

func (b workerAccessBackend) GetWorkerEnrollment(string, string) (*db.WorkerEnrollment, error) {
	return b.value, b.err
}

func TestDeploymentAccessUnavailableOrMismatchedRecordsStayUnknown(t *testing.T) {
	deployment := db.WorkerDeployment{TenantID: "alpha", WorkerID: "company-bus"}
	for _, backend := range []JobsBackend{
		nil,
		workerAccessBackend{err: errors.New("private-marker")},
		workerAccessBackend{},
		workerAccessBackend{value: &db.WorkerEnrollment{TenantID: "beta", WorkerID: "company-bus", Revision: 1, State: "enabled"}},
		workerAccessBackend{value: &db.WorkerEnrollment{TenantID: "alpha", WorkerID: "other-worker", Revision: 1, State: "enabled"}},
		workerAccessBackend{value: &db.WorkerEnrollment{TenantID: "alpha", WorkerID: "company-bus", State: "enabled"}},
		workerAccessBackend{value: &db.WorkerEnrollment{TenantID: "alpha", WorkerID: "company-bus", Revision: 1, State: "private-marker"}},
	} {
		service := jobsServer{backend: backend}
		out := service.workerDeploymentAccess(deployment)
		require.False(t, out.Observed)
		require.Zero(t, out.Revision)
		require.Empty(t, out.State)
		require.Equal(t, deployment.WorkerID, out.WorkerID)
		data, err := json.Marshal(out)
		require.NoError(t, err)
		require.NotContains(t, string(data), "private-marker")
	}
}
