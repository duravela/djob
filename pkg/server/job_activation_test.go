package server

import (
	"crypto/sha256"
	"net/http"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/jobs"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func eventTriggerAPI(id, definition string, maximum uint32) *jp.MutationRequest {
	return &jp.MutationRequest{Command: genericMeta("trigger/" + id), Operation: &jp.MutationRequest_PutTrigger{PutTrigger: &jp.PutTrigger{Definition: &jp.TriggerDefinition{Id: id, DefinitionId: definition, DefinitionRevision: 1, MaxPending: maximum, Event: &jp.EventTriggerSpec{Source: "meters"}}}}}
}
func activationAPI(trigger, id string) *jp.MutationRequest {
	return &jp.MutationRequest{Command: genericMeta("activate/" + id), Operation: &jp.MutationRequest_Activate{Activate: &jp.Activate{TriggerId: trigger, Source: "meters", EventId: id, OccurredAt: timestamppb.New(time.Now().UTC().Add(-time.Hour)), Data: []byte("payload")}}}
}

func TestJobActivationAPIExecutesEventInputAndRestrictsProducerCredentials(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.JobRegistry = func(JobBackend) (jobs.Registry, error) { return jobs.Builtins(), nil }
	cfg.JobDispatch.PollInterval = 5 * time.Millisecond
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	definitionAPI(t, c, "alpha", "event-hash", "sha256-event", "", []byte("static configuration"))
	mutateJob(t, c, "alpha/writer", eventTriggerAPI("ingest", "event-hash", 2))
	q := activationAPI("ingest", "e1")
	ack := mutateJob(t, c, "alpha/producer", q)
	require.Equal(t, "accepted", ack.Status)
	require.Equal(t, uint64(1), ack.Revision)
	require.Equal(t, db.EventJobRunID("alpha", "ingest", "meters", "e1"), ack.RunId)
	require.Equal(t, jp.Kind_ACTIVATION, ack.Reference.Kind)
	require.Equal(t, "alpha", ack.Reference.TenantId)
	require.Equal(t, "ingest", ack.Reference.TriggerId)
	require.Equal(t, "meters", ack.Reference.Source)
	duplicate := mutateJob(t, c, "alpha/producer", q)
	require.True(t, proto.Equal(ack, duplicate))
	require.Eventually(t, func() bool {
		r, err := c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_RUN, Id: ack.RunId}})
		return err == nil && r.GetRun().Status == "succeeded"
	}, 4*time.Second, 5*time.Millisecond)
	run := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_RUN, Id: ack.RunId}).GetRun()
	want := sha256.Sum256([]byte("payload"))
	require.Equal(t, want[:], run.Result)
	require.Equal(t, []byte("static configuration"), run.Definition.Input)
	require.Equal(t, []byte("payload"), run.Activation.Data)
	require.Nil(t, run.ScheduledAt)
	require.True(t, q.GetActivate().OccurredAt.AsTime().Equal(run.Activation.OccurredAt.AsTime()))
	activation := getGeneric(t, c, "alpha/reader", ack.Reference).GetActivation()
	require.Equal(t, []byte("payload"), activation.Data)
	require.Equal(t, ack.RunId, activation.RunId)
	page, err := c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_ACTIVATION, TriggerId: "ingest"})
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	require.True(t, page.Records[0].GetActivation().Summary)
	require.Empty(t, page.Records[0].GetActivation().Data)
	mutateJob(t, c, "alpha/writer", &jp.MutationRequest{Command: genericMeta("pause"), Operation: &jp.MutationRequest_PauseTrigger{PauseTrigger: &jp.PauseTrigger{Id: "ingest", ExpectedRevision: 1, Paused: true}}})
	redelivery := proto.Clone(q).(*jp.MutationRequest)
	redelivery.Command = genericMeta("redelivery")
	require.Equal(t, ack.RunId, mutateJob(t, c, "alpha/producer", redelivery).RunId)
	changed := proto.Clone(q).(*jp.MutationRequest)
	changed.Command = genericMeta("conflicting-data")
	changed.GetActivate().Data = []byte("different")
	_, err = c.Mutate(genericContext("alpha/producer"), changed)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	wrongSource := proto.Clone(q).(*jp.MutationRequest)
	wrongSource.Command = genericMeta("wrong-source")
	wrongSource.GetActivate().Source = "other-source"
	_, err = c.Mutate(genericContext("alpha/producer"), wrongSource)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	for _, actor := range []string{"alpha/callback", "alpha/reader", "beta/producer"} {
		denied := proto.Clone(q).(*jp.MutationRequest)
		denied.Command = genericMeta("denied/" + actor)
		denied.TenantId = "alpha"
		_, err = c.Mutate(genericContext(actor), denied)
		require.Equal(t, codes.PermissionDenied, status.Code(err), actor)
	}
	for _, mutation := range []*jp.MutationRequest{
		eventTriggerAPI("forbidden", "event-hash", 1),
		{Command: genericMeta("forbidden-launch"), Operation: &jp.MutationRequest_Launch{Launch: &jp.Launch{DefinitionId: "event-hash", DefinitionRevision: 1, RunId: "forbidden"}}},
		{Command: genericMeta("forbidden-signal"), Operation: &jp.MutationRequest_Signal{Signal: &jp.Signal{RunId: ack.RunId, EventId: "forbidden", Key: "callback"}}},
	} {
		_, err := c.Mutate(genericContext("alpha/producer"), mutation)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	}
	_, err = c.Get(genericContext("alpha/producer"), &jp.GetRequest{Reference: ack.Reference})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = c.List(genericContext("alpha/producer"), &jp.ListRequest{Kind: jp.Kind_ACTIVATION, TriggerId: "ingest"})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = c.Get(genericContext("beta/reader"), &jp.GetRequest{Reference: ack.Reference})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestJobActivationHTTPAdmissionRequiresNewEnvelopeAfterBackpressure(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	definitionAPI(t, c, "alpha", "hash", "sha256", "", nil)
	mutateJob(t, c, "alpha/writer", eventTriggerAPI("ingest", "hash", 1))
	first := activationAPI("ingest", "e1")
	code, body := genericHTTP(t, s, "/api/jobs/v1/mutate", genericToken("alpha/producer"), first, nil)
	require.Equal(t, http.StatusOK, code, string(body))
	var ack jp.MutationResponse
	require.NoError(t, protojson.Unmarshal(body, &ack))
	require.NotEmpty(t, ack.RunId)
	require.NotContains(t, string(body), "payload")
	second := activationAPI("ingest", "e2")
	code, _ = genericHTTP(t, s, "/djob.jobs.v1.Jobs/Mutate", genericToken("alpha/producer"), second, nil)
	require.Equal(t, http.StatusTooManyRequests, code)
	mutateJob(t, c, "alpha/writer", &jp.MutationRequest{Command: genericMeta("cancel-e1"), Operation: &jp.MutationRequest_Cancel{Cancel: &jp.Cancel{RunId: ack.RunId}}})
	code, _ = genericHTTP(t, s, "/api/jobs/v1/mutate", genericToken("alpha/producer"), second, nil)
	require.Equal(t, http.StatusTooManyRequests, code)
	second.Command = genericMeta("e2-new-envelope")
	code, body = genericHTTP(t, s, "/djob.jobs.v1.Jobs/Mutate", genericToken("alpha/producer"), second, nil)
	require.Equal(t, http.StatusOK, code, string(body))
	code, _ = genericHTTP(t, s, "/api/jobs/v1/mutate", genericToken("alpha/callback"), activationAPI("ingest", "e3"), map[string]string{"Grpc-Metadata-Source-Id": "meters", "Grpc-Metadata-Role": "producer"})
	require.Equal(t, http.StatusForbidden, code)
	mutateJob(t, c, "alpha/writer", &jp.MutationRequest{Command: genericMeta("pause"), Operation: &jp.MutationRequest_PauseTrigger{PauseTrigger: &jp.PauseTrigger{Id: "ingest", ExpectedRevision: 1, Paused: true}}})
	_, err := c.Mutate(genericContext("alpha/producer"), activationAPI("ingest", "e3"))
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestJobActivationInspectionPaginationScopesAndInputValidation(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	definitionAPI(t, c, "alpha", "hash", "sha256", "", nil)
	for _, id := range []string{"one", "two"} {
		mutateJob(t, c, "alpha/writer", eventTriggerAPI(id, "hash", 3))
		for _, event := range []string{"a", "b"} {
			q := activationAPI(id, event)
			q.Command = genericMeta(id + "/" + event)
			mutateJob(t, c, "alpha/producer", q)
		}
	}
	page, err := c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_ACTIVATION, TriggerId: "one", Source: "meters", PageSize: 1})
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	require.NotEmpty(t, page.NextPageToken)
	_, err = c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_ACTIVATION, TriggerId: "two", Source: "meters", PageSize: 1, PageToken: page.NextPageToken})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_ACTIVATION, TriggerId: "one", Source: "other", PageSize: 1, PageToken: page.NextPageToken})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	next, err := c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_ACTIVATION, TriggerId: "one", Source: "meters", PageSize: 1, PageToken: page.NextPageToken})
	require.NoError(t, err)
	require.Len(t, next.Records, 1)
	require.NotEqual(t, page.Records[0].Reference.Id, next.Records[0].Reference.Id)
	_, err = c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_ACTIVATION})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_RUN, Id: "anything", TriggerId: "one"}})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	invalid := activationAPI("one", "invalid")
	invalid.GetActivate().OccurredAt = nil
	_, err = c.Mutate(genericContext("alpha/producer"), invalid)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	invalid = activationAPI("one", "too-large")
	invalid.GetActivate().Data = make([]byte, (64<<10)+1)
	_, err = c.Mutate(genericContext("alpha/producer"), invalid)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	both := eventTriggerAPI("mixed", "hash", 1)
	both.GetPutTrigger().Definition.Timing = &jp.Timing{Kind: "once", StartAt: timestamppb.Now()}
	_, err = c.Mutate(genericContext("alpha/writer"), both)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestProducerCredentialRequiresExplicitTenantAndSourceScope(t *testing.T) {
	c := APICredential{Subject: "producer", Role: APIProducer, Token: genericToken("alpha/producer"), TenantID: "alpha", SourceID: "meters"}
	for _, change := range []func(*APICredential){func(c *APICredential) { c.TenantID = "" }, func(c *APICredential) { c.SourceID = "" }, func(c *APICredential) { c.Role = APIWriter }} {
		invalid := c
		change(&invalid)
		_, err := newAPIAuthorizer(&APIAuthConfig{Credentials: []APICredential{invalid}})
		require.Error(t, err)
		require.NotContains(t, err.Error(), invalid.Token)
	}
}
