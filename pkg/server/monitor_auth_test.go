package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	legacy "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func monitorCredentials() *APIAuthConfig {
	return &APIAuthConfig{Credentials: []APICredential{{Subject: "prometheus", Role: APIMonitor, Token: strings.Repeat("m", 40)}}}
}

func TestMonitoringCredentialsCannotAccessJobServices(t *testing.T) {
	cfg := monitorCredentials()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+cfg.Credentials[0].Token))
	old, err := NewSchedulerServiceWithAuth(nil, *cfg)
	require.NoError(t, err)
	jobs, err := NewJobsService(nil, *cfg)
	require.NoError(t, err)
	for _, method := range legacy.Scheduler_ServiceDesc.Methods {
		t.Run("legacy/"+method.MethodName, func(t *testing.T) {
			_, err := method.Handler(old, ctx, func(any) error { return nil }, nil)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
		})
	}
	for _, method := range jp.Jobs_ServiceDesc.Methods {
		t.Run("jobs/"+method.MethodName, func(t *testing.T) {
			_, err := method.Handler(jobs, ctx, func(any) error { return nil }, nil)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
		})
	}
}

func TestMonitoringCredentialConfigurationIsUnscoped(t *testing.T) {
	for _, scope := range []string{"none", "tenant", "source"} {
		t.Run(scope, func(t *testing.T) {
			cfg := monitorCredentials()
			if scope == "tenant" {
				cfg.Credentials[0].TenantID = "alpha"
			}
			if scope == "source" {
				cfg.Credentials[0].SourceID = "meters"
			}
			_, err := newAPIAuthorizer(cfg)
			if scope == "none" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.NotContains(t, err.Error(), cfg.Credentials[0].Token)
			}
		})
	}
}

func TestMonitoringRoleOnlyAllowsMetrics(t *testing.T) {
	s, c := workerAPI(t, func(cfg *Config) {
		cfg.APIAuth.Credentials = append(cfg.APIAuth.Credentials, monitorCredentials().Credentials...)
	})
	token := monitorCredentials().Credentials[0].Token
	data := authenticatedHTTP(t, c, "https://"+s.HTTPAddr(), http.MethodGet, "/metrics", token, nil, nil, 200, nil)
	require.Contains(t, string(data), "djob_raft_running 1")
	require.Contains(t, string(data), "djob_runtime_goroutines ")
	require.Contains(t, string(data), "djob_runtime_heap_objects_bytes ")
	require.Contains(t, string(data), "djob_runtime_gc_live_heap_bytes ")
	require.Contains(t, string(data), "djob_runtime_gc_heap_goal_bytes ")
	require.Contains(t, string(data), "djob_runtime_gc_cycles_total ")
	for _, path := range []string{"register", "not-a-session/heartbeat", "not-a-session/disconnect"} {
		workerPost(t, c, s, token, "/api/workers/v1/"+path, struct{}{}, nil, 403)
	}
	for _, method := range []string{"get", "list", "mutate"} {
		workerPost(t, c, s, token, "/api/jobs/v1/worker-enrollments/"+method, struct{}{}, nil, 403)
	}
	for _, request := range []struct {
		path  string
		input proto.Message
	}{
		{"/api/v1/channels/bus/expire", &legacy.ExpireChannelRequest{}},
		{"/api/v1/channels/bus/resolve", &legacy.ResolveChannelRequest{}},
		{"/api/v1/runs/run/resolve", &legacy.ResolveRunRequest{}},
		{"/api/jobs/v1/get", &jp.GetRequest{}},
		{"/api/jobs/v1/list", &jp.ListRequest{}},
		{"/api/jobs/v1/mutate", &jp.MutationRequest{}},
	} {
		authenticatedHTTP(t, c, "https://"+s.HTTPAddr(), http.MethodPost, request.path, token, request.input, nil, 403, nil)
	}
	for _, header := range []http.Header{
		{"Authorization": {"Bearer " + token, "Bearer " + token}},
		{"Grpc-Metadata-Authorization": {"Bearer " + token}},
	} {
		authenticatedHTTP(t, c, "https://"+s.HTTPAddr(), http.MethodGet, "/metrics", "", nil, nil, 401, header)
	}
	// An anonymous API configuration never grants the restricted monitoring permission.
	anonymous := &Server{scheduler: &schedulerServer{}}
	w := httptest.NewRecorder()
	anonymous.metricsHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, 401, w.Code)
}
