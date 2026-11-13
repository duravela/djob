package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	consoleapp "github.com/duravela/djob/internal/console"
	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestGenericRecoverySeparatesPlatformResourcesFromTenantOutcomes(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.JobMaintenance.PollInterval = time.Hour // This test explicitly expires an owner using its observed logical lease.
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	tenantAPI(t, c, "beta")
	base := time.Now().UTC().Add(-time.Minute)
	requests := []*jp.MutationRequest{
		{Operation: &jp.MutationRequest_PutResource{PutResource: &jp.PutResource{Spec: &jp.ResourceSpec{Id: "bus", Duplex: "half", Capacity: 1}}}},
		{TenantId: "alpha", Operation: &jp.MutationRequest_PutGrant{PutGrant: &jp.PutGrant{ResourceId: "bus", Allowed: true}}},
		{TenantId: "alpha", Operation: &jp.MutationRequest_PutBinding{PutBinding: &jp.PutBinding{Binding: &jp.Binding{Id: "driver", Driver: "test", DriverVersion: "1", Resources: []string{"bus"}}}}},
		{TenantId: "alpha", Operation: &jp.MutationRequest_PutDefinition{PutDefinition: &jp.PutDefinition{Definition: &jp.Definition{Id: "job", Runtime: db.StepRuntime, Handler: "test", HandlerVersion: "1", BindingId: "driver"}}}},
		{TenantId: "alpha", Operation: &jp.MutationRequest_Launch{Launch: &jp.Launch{DefinitionId: "job", DefinitionRevision: 1, RunId: "r"}}},
	}
	for i, q := range requests {
		q.Command = &jp.Command{RequestId: fmt.Sprintf("seed/%d", i), At: timestamppb.New(base.Add(time.Duration(i) * time.Millisecond))}
		mutateJob(t, c, "ops/alice", q)
	}
	owner := db.JobOwner{TenantID: "alpha", RunID: "r", AttemptID: "old-attempt", OwnerID: "crashed-worker"}
	lease := base.Add(time.Second)
	for i, command := range []db.StateCommand{
		{At: base.Add(10 * time.Millisecond), Job: &db.JobCommand{Claim: &db.ClaimJob{JobOwner: owner, LeaseUntil: lease}}},
		{At: base.Add(11 * time.Millisecond), Job: &db.JobCommand{Advance: &db.AdvanceJob{JobOwner: owner, Kind: db.JobDoAction, Operation: "write"}}},
		{At: lease, Job: &db.JobCommand{Expire: &db.ExpireJob{JobOwner: owner, LeaseUntil: lease}}},
	} {
		command.RequestID = fmt.Sprintf("crash/%d", i)
		_, err := s.application.Store.ProposeStateCommand(context.Background(), command)
		require.NoError(t, err)
	}
	resource := &jp.MutationRequest{Command: genericMeta("resolve-resource"), Operation: &jp.MutationRequest_ResolveResource{ResolveResource: &jp.ResolveResource{ResourceId: "bus", ExpectedEpoch: 1, Reason: "old worker excluded and target idle", Quiescent: true}}}
	_, err := c.Mutate(genericContext("alpha/operator"), resource)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	blocked := getGeneric(t, c, "ops/alice", &jp.Reference{Kind: jp.Kind_RESOURCE, Id: "bus"})
	require.Equal(t, "blocked", blocked.GetResource().State)
	// Exercise the console's retained review and exact acknowledgement against
	// a genuinely blocked resource, not a mocked receipt or a lagging UI sample.
	const origin = "http://127.0.0.1:8899"
	app, err := consoleapp.New(consoleapp.Config{Origin: origin, Controllers: []string{"http://" + s.HTTPAddr()}})
	require.NoError(t, err)
	defer app.Close()
	var cookie *http.Cookie
	var csrf string
	call := func(path string, body any) *httptest.ResponseRecorder {
		data, err := json.Marshal(body)
		require.NoError(t, err)
		r := httptest.NewRequest(http.MethodPost, origin+"/api/console/v1/"+path, bytes.NewReader(data))
		r.Header.Set("Origin", origin)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Djob-CSRF", csrf)
		if cookie != nil {
			r.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		app.ServeHTTP(w, r)
		require.Equal(t, 200, w.Code, w.Body.String())
		return w
	}
	login := call("session", map[string]string{"token": genericToken("ops/alice")})
	cookie = login.Result().Cookies()[0]
	var session struct {
		CSRF string `json:"csrf"`
	}
	require.NoError(t, json.Unmarshal(login.Body.Bytes(), &session))
	csrf = session.CSRF
	resource.Command = nil
	raw, err := protojson.Marshal(resource)
	require.NoError(t, err)
	prepared := call("mutations", json.RawMessage(raw))
	var pending struct {
		RequestID string `json:"request_id"`
		State     string `json:"state"`
	}
	require.NoError(t, json.Unmarshal(prepared.Body.Bytes(), &pending))
	resourceResolutionID := pending.RequestID
	require.Equal(t, "prepared", pending.State)
	committed := call("mutations/commit", map[string]string{"request_id": resourceResolutionID})
	require.NoError(t, json.Unmarshal(committed.Body.Bytes(), &pending))
	require.Equal(t, "confirmed", pending.State, committed.Body.String())
	auditResponse := call("query/get", map[string]any{"reference": map[string]string{"kind": "RESOLUTION", "id": resourceResolutionID}})
	var resourceAudit jp.Record
	require.NoError(t, protojson.Unmarshal(auditResponse.Body.Bytes(), &resourceAudit))
	require.Equal(t, resourceResolutionID, resourceAudit.Reference.Id)
	require.Empty(t, resourceAudit.Reference.TenantId)
	require.Equal(t, "bus", resourceAudit.GetResolution().ResourceId)
	require.Equal(t, "ops/alice", resourceAudit.GetResolution().Actor)
	released := getGeneric(t, c, "ops/alice", &jp.Reference{Kind: jp.Kind_RESOURCE, Id: "bus"})
	require.Equal(t, "available", released.GetResource().State)
	require.EqualValues(t, 1, released.GetResource().Epoch)
	run := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_RUN, Id: "r"})
	require.Equal(t, "unknown", run.GetRun().Status)
	resolve := &jp.MutationRequest{Command: genericMeta("resolve-job"), Operation: &jp.MutationRequest_ResolveRun{ResolveRun: &jp.ResolveRun{RunId: "r", ExpectedStep: run.GetRun().Step, Status: "failed", Reason: "write independently reconciled"}}}
	ctx := metadata.AppendToOutgoingContext(genericContext("alpha/operator"), "actor", "forged-admin")
	_, err = c.Mutate(ctx, resolve)
	require.NoError(t, err)
	audit := getGeneric(t, c, "alpha/operator", &jp.Reference{Kind: jp.Kind_RESOLUTION, Id: "resolve-job"})
	require.Equal(t, "alpha/operator", audit.GetResolution().Actor)
	require.Equal(t, "resolve-job", audit.GetResolution().RequestId)
	audit = getGeneric(t, c, "ops/alice", &jp.Reference{Kind: jp.Kind_RESOLUTION, Id: resourceResolutionID})
	require.Equal(t, "ops/alice", audit.GetResolution().Actor)
	require.Equal(t, "alpha", audit.GetResolution().TenantId)
	_, err = c.Get(genericContext("beta/operator"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_RESOLUTION, Id: "resolve-job"}})
	require.Equal(t, codes.NotFound, status.Code(err))
	action := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_ACTION, RunId: "r", Sequence: 1})
	require.Equal(t, "unknown", action.GetAction().Status)
}

func TestGenericReadBudgetsPreservePaginationAndRunInspection(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.Raft.SnapshotCount = 1000
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	var sequence int
	seed := func(job db.JobCommand) {
		sequence++
		_, err := s.application.Store.ProposeStateCommand(context.Background(), db.StateCommand{RequestID: fmt.Sprintf("large/%d", sequence), At: time.Now().UTC(), Job: &job})
		require.NoError(t, err)
	}
	blob := make([]byte, 1<<20)
	seed(db.JobCommand{PutBinding: &db.PutJobBinding{Binding: db.JobBinding{TenantID: "alpha", ID: "driver", Driver: "test", DriverVersion: "1"}}})
	seed(db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "alpha", ID: "large", Runtime: db.StepRuntime, Handler: "test", HandlerVersion: "1", BindingID: "driver", Input: blob}}})
	seed(db.JobCommand{Launch: &db.LaunchJob{TenantID: "alpha", DefinitionID: "large", DefinitionRevision: 1, RunID: "r"}})
	owner := db.JobOwner{TenantID: "alpha", RunID: "r", AttemptID: "attempt", OwnerID: "worker"}
	seed(db.JobCommand{Claim: &db.ClaimJob{JobOwner: owner, LeaseUntil: time.Now().Add(time.Minute)}})
	for i := range 2 {
		seed(db.JobCommand{Advance: &db.AdvanceJob{JobOwner: owner, ExpectedStep: uint64(i), Kind: db.JobDoAction, Operation: "large", Input: blob}})
		seed(db.JobCommand{CompleteAction: &db.CompleteJobAction{JobOwner: owner, Sequence: uint64(i + 1), Status: db.EffectSucceeded, Result: blob}})
	}
	page, err := c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_ACTION, RunId: "r", PageSize: 32})
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	require.NotEmpty(t, page.NextPageToken)
	require.EqualValues(t, 1, page.Records[0].GetAction().Sequence)
	next, err := c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_ACTION, RunId: "r", PageSize: 32, PageToken: page.NextPageToken})
	require.NoError(t, err)
	require.Len(t, next.Records, 1)
	require.Empty(t, next.NextPageToken)
	require.EqualValues(t, 2, next.Records[0].GetAction().Sequence)
	seed(db.JobCommand{Advance: &db.AdvanceJob{JobOwner: owner, ExpectedStep: 2, Kind: db.JobComplete, Status: db.JobSucceeded, State: blob, Result: blob, Quiescent: true}})
	ref := &jp.Reference{Kind: jp.Kind_RUN, Id: "r"}
	_, err = c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: ref})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	r, err := c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: ref, Summary: true})
	require.NoError(t, err)
	require.True(t, r.GetRun().Summary)
	require.Equal(t, "succeeded", r.GetRun().Status)
	require.Empty(t, r.GetRun().Definition.Input)
	require.Empty(t, r.GetRun().Result)
}
