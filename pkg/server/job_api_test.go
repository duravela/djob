package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/jobs"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	old "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func genericCredentials() *APIAuthConfig {
	cfg := apiCredentials()
	for _, tenant := range []string{"alpha", "beta"} {
		for _, role := range []APIRole{APIReader, APIWriter, APIOperator, APICallback, APIProducer} {
			c := APICredential{Subject: tenant + "/" + string(role), TenantID: tenant, Role: role, Token: strings.Repeat(tenant+"-"+string(role)+"-", 4)}
			if role == APIProducer {
				c.SourceID = "meters"
			}
			cfg.Credentials = append(cfg.Credentials, c)
		}
	}
	return cfg
}
func genericToken(subject string) string {
	for _, c := range genericCredentials().Credentials {
		if c.Subject == subject {
			return c.Token
		}
	}
	panic("unknown test credential")
}
func genericContext(subject string) context.Context { return apiAuthContext(genericToken(subject)) }
func genericClient(t *testing.T, s *Server) jp.JobsClient {
	t.Helper()
	conn, err := grpc.NewClient(s.RPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return jp.NewJobsClient(conn)
}
func genericMeta(id string) *jp.Command { return &jp.Command{RequestId: id, At: timestamppb.Now()} }
func mutateJob(t *testing.T, c jp.JobsClient, actor string, q *jp.MutationRequest) *jp.MutationResponse {
	t.Helper()
	r, err := c.Mutate(genericContext(actor), q)
	require.NoError(t, err)
	return r
}
func tenantAPI(t *testing.T, c jp.JobsClient, tenant string) {
	t.Helper()
	mutateJob(t, c, "ops/alice", &jp.MutationRequest{Command: genericMeta("tenant"), TenantId: tenant, Operation: &jp.MutationRequest_PutTenant{PutTenant: &jp.PutTenant{MaxRunning: 2}}})
}
func definitionAPI(t *testing.T, c jp.JobsClient, tenant, id, handler, binding string, input []byte) *jp.MutationRequest {
	t.Helper()
	q := &jp.MutationRequest{Command: genericMeta("definition/" + id), TenantId: tenant, Operation: &jp.MutationRequest_PutDefinition{PutDefinition: &jp.PutDefinition{Definition: &jp.Definition{Id: id, Runtime: db.StepRuntime, Handler: handler, HandlerVersion: "1", BindingId: binding, Input: input}}}}
	mutateJob(t, c, tenant+"/writer", q)
	return q
}
func launchAPI(t *testing.T, c jp.JobsClient, tenant, definition, run string) *jp.MutationRequest {
	t.Helper()
	q := &jp.MutationRequest{Command: genericMeta("launch/" + run), TenantId: tenant, Operation: &jp.MutationRequest_Launch{Launch: &jp.Launch{DefinitionId: definition, DefinitionRevision: 1, RunId: run}}}
	mutateJob(t, c, tenant+"/writer", q)
	return q
}
func getGeneric(t *testing.T, c jp.JobsClient, actor string, ref *jp.Reference) *jp.Record {
	t.Helper()
	r, err := c.Get(genericContext(actor), &jp.GetRequest{Reference: ref})
	require.NoError(t, err)
	return r
}

func TestGenericMethodsAuthenticateAndLegacyMethodsRejectScopedCredentials(t *testing.T) {
	svc, err := NewJobsService(nil, *genericCredentials())
	require.NoError(t, err)
	for _, method := range jp.Jobs_ServiceDesc.Methods {
		_, err := method.Handler(svc, context.Background(), func(any) error { return nil }, nil)
		require.Equal(t, codes.Unauthenticated, status.Code(err), method.MethodName)
		_, err = method.Handler(&jobsServer{}, context.Background(), func(any) error { return nil }, nil)
		require.Equal(t, codes.Unauthenticated, status.Code(err))
	}
	legacy, err := NewSchedulerServiceWithAuth(nil, *genericCredentials())
	require.NoError(t, err)
	for _, role := range []APIRole{APIReader, APIWriter, APIOperator, APICallback, APIProducer} {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+genericToken("alpha/"+string(role))))
		for _, method := range old.Scheduler_ServiceDesc.Methods {
			_, err := method.Handler(legacy, ctx, func(any) error { return nil }, nil)
			require.Equal(t, codes.PermissionDenied, status.Code(err), method.MethodName)
		}
	}
}

func TestGenericAPIScopesMutationsReadsRequestsAndCallbacks(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	for _, tenant := range []string{"alpha", "beta"} {
		tenantAPI(t, c, tenant)
		definitionAPI(t, c, tenant, "hash", "sha256", "", []byte(tenant+"-private"))
		launchAPI(t, c, tenant, "hash", "r")
	}
	for _, actor := range []string{"reader", "writer"} {
		_, err := c.Get(genericContext(actor), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_RUN, TenantId: "alpha", Id: "r"}})
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	}
	for _, tenant := range []string{"alpha", "beta"} {
		r := getGeneric(t, c, tenant+"/reader", &jp.Reference{Kind: jp.Kind_RUN, Id: "r"})
		require.Equal(t, tenant, r.Reference.TenantId)
		require.Equal(t, []byte(tenant+"-private"), r.GetRun().Definition.Input)
		other := "beta"
		if tenant == "beta" {
			other = "alpha"
		}
		_, err := c.Get(genericContext(tenant+"/operator"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_RUN, TenantId: other, Id: "r"}})
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		_, err = c.List(genericContext(tenant+"/reader"), &jp.ListRequest{Kind: jp.Kind_TENANT})
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	}
	q := &jp.MutationRequest{Command: genericMeta("second-def"), Operation: &jp.MutationRequest_PutDefinition{PutDefinition: &jp.PutDefinition{Definition: &jp.Definition{Id: "other", Runtime: db.StepRuntime, Handler: "sha256", HandlerVersion: "1", Input: []byte("secret")}}}}
	first := mutateJob(t, c, "alpha/writer", q)
	retry := mutateJob(t, c, "alpha/operator", q)
	require.True(t, proto.Equal(first, retry))
	changed := proto.Clone(q).(*jp.MutationRequest)
	changed.GetPutDefinition().Definition.Input = []byte("changed")
	_, err := c.Mutate(genericContext("alpha/writer"), changed)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	for _, actor := range []string{"alpha/reader", "alpha/callback"} {
		_, err := c.Mutate(genericContext(actor), q)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	}
	for _, op := range []any{
		&jp.MutationRequest_PutTenant{PutTenant: &jp.PutTenant{}},
		&jp.MutationRequest_PutBinding{PutBinding: &jp.PutBinding{Binding: &jp.Binding{Id: "b", Driver: "http", DriverVersion: "1"}}},
		&jp.MutationRequest_PutGrant{PutGrant: &jp.PutGrant{ResourceId: "bus", Allowed: true}},
		&jp.MutationRequest_PutResource{PutResource: &jp.PutResource{Spec: &jp.ResourceSpec{Id: "bus", Duplex: "half", Capacity: 1}}},
	} {
		request := &jp.MutationRequest{Command: genericMeta("denied")}
		switch v := op.(type) {
		case *jp.MutationRequest_PutTenant:
			request.Operation = v
		case *jp.MutationRequest_PutBinding:
			request.Operation = v
		case *jp.MutationRequest_PutGrant:
			request.Operation = v
		case *jp.MutationRequest_PutResource:
			request.Operation = v
		}
		_, err := c.Mutate(genericContext("alpha/operator"), request)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	}
	page, err := c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_DEFINITION, PageSize: 1})
	require.NoError(t, err)
	require.NotEmpty(t, page.NextPageToken)
	_, err = c.List(genericContext("beta/reader"), &jp.ListRequest{Kind: jp.Kind_DEFINITION, PageSize: 1, PageToken: page.NextPageToken})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_BINDING, PageSize: 1, PageToken: page.NextPageToken})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	signal := &jp.MutationRequest{Command: genericMeta("callback"), Operation: &jp.MutationRequest_Signal{Signal: &jp.Signal{RunId: "r", EventId: "e", Key: "reply", Data: []byte("response")}}}
	ack := mutateJob(t, c, "alpha/callback", signal)
	require.Equal(t, jp.Kind_EVENT, ack.Reference.Kind)
	data, err := protojson.Marshal(ack)
	require.NoError(t, err)
	require.NotContains(t, string(data), "private")
	require.NotContains(t, string(data), "input")
	_, err = c.Get(genericContext("alpha/callback"), &jp.GetRequest{Reference: ack.Reference})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	signal.TenantId = "beta"
	_, err = c.Mutate(genericContext("alpha/callback"), signal)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_RESOURCE, Id: "bus"}})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

func genericHTTP(t *testing.T, s *Server, path, token string, input proto.Message, headers map[string]string) (int, []byte) {
	t.Helper()
	data, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(input)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, "http://"+s.HTTPAddr()+path, bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, body
}

func TestGenericHTTPUsesActualCredentialAndSameMutationReceipts(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	q := definitionAPI(t, c, "alpha", "hash", "sha256", "", []byte("secret"))
	for _, path := range []string{"/api/jobs/v1/mutate", "/djob.jobs.v1.Jobs/Mutate"} {
		code, data := genericHTTP(t, s, path, genericToken("alpha/writer"), q, nil)
		require.Equal(t, 200, code, string(data))
		var ack jp.MutationResponse
		require.NoError(t, protojson.Unmarshal(data, &ack))
		require.EqualValues(t, 1, ack.Revision)
		code, _ = genericHTTP(t, s, path, "", q, map[string]string{"Grpc-Metadata-Authorization": "Bearer " + genericToken("ops/alice")})
		require.Equal(t, 401, code)
		forged := proto.Clone(q).(*jp.MutationRequest)
		forged.TenantId = "beta"
		code, _ = genericHTTP(t, s, path, genericToken("alpha/writer"), forged, map[string]string{"Grpc-Metadata-Tenant-Id": "beta", "X-Tenant-Id": "beta"})
		require.Equal(t, 403, code)
	}
	code, _ := genericHTTP(t, s, "/api/jobs/v1/get", genericToken("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_DEFINITION, Id: "hash"}}, nil)
	require.Equal(t, 200, code)
}

func TestPublicGenericDispatchComputesAndResumesHTTPAfterRestart(t *testing.T) {
	var calls atomic.Int32
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("receipt"))
	}))
	defer remote.Close()
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.JobDispatch.PollInterval = time.Millisecond * 5
	actions := make(chan jobs.ActionObservation, 4)
	cfg.JobWorker.OnAction = func(o jobs.ActionObservation) { actions <- o }
	cfg.JobRegistry = func(JobBackend) (jobs.Registry, error) {
		r := jobs.Builtins()
		r.Drivers[jobs.Version{Name: "http", Version: "1"}] = jobs.HTTPFactory(remote.Client())
		return r, nil
	}
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	definitionAPI(t, c, "alpha", "hash", "sha256", "", []byte("input"))
	launchAPI(t, c, "alpha", "hash", "compute")
	require.Eventually(t, func() bool {
		r, err := c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_RUN, Id: "compute"}})
		return err == nil && r.GetRun().Status == "succeeded"
	}, 3*time.Second, time.Millisecond*5)
	r := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_RUN, Id: "compute"})
	sum := sha256.Sum256([]byte("input"))
	require.Equal(t, sum[:], r.GetRun().Result)
	require.Zero(t, actionHistogram(t, scrapeMetrics(t, s), "djob_job_action_record_duration_seconds", "result", "confirmed").GetSampleCount(), "pure compute has no driver action")
	config, err := json.Marshal(jobs.HTTPBinding{Operations: []jobs.HTTPOperation{{Name: "request", Method: "POST", URL: remote.URL}}})
	require.NoError(t, err)
	mutateJob(t, c, "ops/alice", &jp.MutationRequest{Command: genericMeta("binding"), TenantId: "alpha", Operation: &jp.MutationRequest_PutBinding{PutBinding: &jp.PutBinding{Binding: &jp.Binding{Id: "http", Driver: "http", DriverVersion: "1", Config: config}}}})
	input, err := json.Marshal(jobs.RequestReplyInput{CorrelationKey: "report-42", Payload: []byte("request"), Timeout: time.Minute})
	require.NoError(t, err)
	definitionAPI(t, c, "alpha", "report", "request-reply", "http", input)
	launchAPI(t, c, "alpha", "report", "conversation")
	require.Eventually(t, func() bool {
		v, err := s.application.Store.GetJobView("alpha", "conversation")
		return err == nil && v.Run.Status == db.JobWaiting && v.Run.OwnerID == ""
	}, 3*time.Second, time.Millisecond*5)
	select {
	case o := <-actions:
		require.True(t, o.DriverInvoked)
		require.True(t, o.RecordConfirmed)
		require.Equal(t, db.EffectSucceeded, o.Status)
	case <-time.After(time.Second):
		t.Fatal("the server did not preserve the supplied action observer")
	}
	families := scrapeMetrics(t, s)
	require.Equal(t, float64(1), metricValue(t, families, "djob_job_action_results_total", "state", "succeeded", "result", "confirmed"))
	require.Positive(t, actionHistogram(t, families, "djob_job_action_driver_duration_seconds", "state", "succeeded").GetSampleSum())
	require.Positive(t, actionHistogram(t, families, "djob_job_action_record_duration_seconds", "result", "confirmed").GetSampleSum())
	require.NoError(t, s.Close(context.Background()))
	s = startAPI(t, cfg)
	c = genericClient(t, s)
	signal := &jp.MutationRequest{Command: genericMeta("reply"), Operation: &jp.MutationRequest_Signal{Signal: &jp.Signal{RunId: "conversation", EventId: "callback", Key: "report-42", Data: []byte("result")}}}
	code, data := genericHTTP(t, s, "/api/jobs/v1/mutate", genericToken("alpha/callback"), signal, nil)
	require.Equal(t, 200, code, string(data))
	require.Eventually(t, func() bool {
		v, err := s.application.Store.GetJobView("alpha", "conversation")
		return err == nil && v.Run.Status == db.JobSucceeded
	}, 3*time.Second, time.Millisecond*5)
	r = getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_RUN, Id: "conversation"})
	require.Equal(t, []byte("result"), r.GetRun().Result)
	require.EqualValues(t, 2, r.GetRun().AttemptCount)
	require.EqualValues(t, 1, r.GetRun().ActionCount)
	require.EqualValues(t, 1, calls.Load())
	families = scrapeMetrics(t, s)
	require.Zero(t, metricValue(t, families, "djob_job_action_results_total", "state", "succeeded", "result", "confirmed"), "restart and a response-only resume must not reconstruct or repeat action samples")
	require.Zero(t, actionHistogram(t, families, "djob_job_action_driver_duration_seconds", "state", "succeeded").GetSampleCount())
	// The same runtime wakes a durable wait timer without resending its request.
	input, err = json.Marshal(jobs.RequestReplyInput{CorrelationKey: "unanswered", Timeout: 20 * time.Millisecond})
	require.NoError(t, err)
	definitionAPI(t, c, "alpha", "timeout", "request-reply", "http", input)
	launchAPI(t, c, "alpha", "timeout", "timeout")
	require.Eventually(t, func() bool {
		v, err := s.application.Store.GetJobView("alpha", "timeout")
		return err == nil && v.Run.Status == db.JobFailed
	}, 3*time.Second, 5*time.Millisecond)
	require.EqualValues(t, 2, calls.Load())
}

type wrongTenantJobBackend struct{ JobsBackend }

func (*wrongTenantJobBackend) GetJobRecord(db.JobRecordQuery) (db.JobRecord, error) {
	return db.JobRecord{Run: &db.JobRun{TenantID: "beta", ID: "r", Result: []byte("private-beta-result")}}, nil
}
func (b *wrongTenantJobBackend) ListJobRecords(db.JobRecordQuery, []byte, int) (db.JobRecordPage, error) {
	r, _ := b.GetJobRecord(db.JobRecordQuery{})
	return db.JobRecordPage{Entries: []db.JobRecordEntry{{Record: r, Cursor: []byte("cursor")}}}, nil
}
func TestGenericServiceRejectsBackendRecordsOutsideAuthorizedScope(t *testing.T) {
	svc, err := NewJobsService(&wrongTenantJobBackend{}, *genericCredentials())
	require.NoError(t, err)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+genericToken("alpha/reader")))
	_, err = svc.Get(ctx, &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_RUN, Id: "r"}})
	require.Equal(t, codes.Internal, status.Code(err))
	require.NotContains(t, err.Error(), "private-beta-result")
	_, err = svc.List(ctx, &jp.ListRequest{Kind: jp.Kind_RUN})
	require.Equal(t, codes.Internal, status.Code(err))
	require.NotContains(t, err.Error(), "private-beta-result")
}
