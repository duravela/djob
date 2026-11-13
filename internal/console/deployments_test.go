package console

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/stretchr/testify/require"
)

func TestConsoleDeploymentKeepsExactIntentAcrossUncertainCommit(t *testing.T) {
	var submitted [][]byte
	b := newBrowserAs(t, Identity{Subject: "platform", Role: "operator"}, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/jobs/v1/worker-deployments/mutate", r.URL.Path)
		data, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		submitted = append(submitted, data)
		if len(submitted) == 1 {
			problem(w, 503, "receipt unavailable")
			return
		}
		var q jp.WorkerDeploymentMutation
		require.NoError(t, json.Unmarshal(data, &q))
		writeJSON(w, 200, jp.WorkerDeploymentAcknowledgement{RequestID: q.Command.RequestID, Deployment: jp.WorkerDeployment{TenantID: q.TenantID, ID: q.ID, WorkerID: q.WorkerID, Revision: 1, Generation: 1, State: "active", Spec: *q.Spec}})
	})
	q := jp.WorkerDeploymentMutation{TenantID: "alpha", ID: "line-one", WorkerID: "company-worker", Action: "put", Reason: "start company code", Spec: &jp.WorkerDeploymentSpec{Image: "registry.example/acme/worker@sha256:" + strings.Repeat("a", 64), ConfigSecret: "company-worker", Replicas: 2}}
	response := b.call(http.MethodPost, "mutations/worker-deployment", q)
	require.Equal(t, 200, response.Code, response.Body.String())
	var pending pendingMutation
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &pending))
	require.False(t, pending.WorkerConfigAvailable)
	require.Equal(t, "uncertain", b.commit(pending.RequestID).State)
	require.Equal(t, 409, b.call(http.MethodDelete, "mutations/current", map[string]string{"request_id": pending.RequestID}).Code)
	require.Equal(t, "confirmed", b.commit(pending.RequestID).State)
	require.Len(t, submitted, 2)
	require.Equal(t, submitted[0], submitted[1])
}

func TestConsoleDeploymentRejectsScopedManagementAndForeignQueries(t *testing.T) {
	b := newBrowser(t, func(http.ResponseWriter, *http.Request) { t.Error("unauthorized deployment reached a controller") })
	require.Equal(t, 403, b.call(http.MethodPost, "mutations/worker-deployment", map[string]string{"tenant_id": "alpha"}).Code)
	for _, method := range []string{"get", "list", "status"} {
		require.Equal(t, 403, b.call(http.MethodPost, "worker-deployments/"+method, map[string]string{"tenant_id": "beta", "id": "line-one"}).Code)
	}
}
