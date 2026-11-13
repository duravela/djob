package jobs

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func TestSuppliedHTTPDriverDoesNotClaimPhysicalRecoverySupport(t *testing.T) {
	b := newBackend(t)
	setupAction(t, b, true)
	var calls atomic.Int32
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(http.StatusAccepted) }))
	defer remote.Close()
	config, err := json.Marshal(HTTPBinding{Operations: []HTTPOperation{{Name: "do", Method: "POST", URL: remote.URL}}})
	require.NoError(t, err)
	state(t, b, db.JobCommand{PutBinding: &db.PutJobBinding{ExpectedRevision: 1, Binding: db.JobBinding{TenantID: "t", ID: "binding", Driver: "http", DriverVersion: "1", Config: config}}})
	// Launch a fresh snapshot of the updated driver; the existing run retains
	// its original binding, as required by immutable launch specifications.
	launch(t, b, "t", "job", "http")
	w := worker(t, b, Config{}, Registry{Handlers: map[Version]Handler{{"job", "1"}: oneAction}, Drivers: map[Version]DriverFactory{{"http", "1"}: HTTPFactory(remote.Client())}})
	r, err := w.Execute(context.Background(), "t", "http")
	require.NoError(t, err)
	require.Equal(t, db.JobFailed, r.Run.Status)
	require.Zero(t, calls.Load())
	resource, err := b.GetJobResource("resource")
	require.NoError(t, err)
	require.Equal(t, db.ResourceAvailable, resource.State)
	action, err := b.GetJobAction("t", "http", 1)
	require.NoError(t, err)
	require.Equal(t, db.EffectFailed, action.Status)
}
