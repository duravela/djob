package server

import (
	"fmt"
	"net/http"
	"testing"

	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func limitsAPI(tenant string, expected uint64, limits *jp.Limits) *jp.MutationRequest {
	return &jp.MutationRequest{Command: genericMeta(fmt.Sprintf("limits/%d", expected)), TenantId: tenant, Operation: &jp.MutationRequest_PutLimits{PutLimits: &jp.PutLimits{ExpectedRevision: expected, Limits: limits}}}
}

func TestJobLimitsAPIControlsAllRunAdmissionAndReportsTenantUsage(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	tenantAPI(t, c, "beta")
	definitionAPI(t, c, "alpha", "hash", "sha256", "", nil)
	initial := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_LIMITS}).GetLimits()
	require.Zero(t, initial.Revision)
	require.Zero(t, getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_USAGE}).GetUsage().StoredRuns)
	q := limitsAPI("alpha", 0, &jp.Limits{MaxPendingRuns: 1, MaxStoredRuns: 2, MaxInboxEvents: 1, MaxInboxBytes: 2})
	ack := mutateJob(t, c, "ops/alice", q)
	require.Equal(t, jp.Kind_LIMITS, ack.Reference.Kind)
	require.Equal(t, uint64(1), ack.Revision)
	require.True(t, proto.Equal(ack, mutateJob(t, c, "ops/alice", q)))
	for _, actor := range []string{"alpha/reader", "alpha/writer", "alpha/operator", "alpha/callback", "alpha/producer", "writer"} {
		denied := proto.Clone(q).(*jp.MutationRequest)
		denied.Command = genericMeta("denied/" + actor)
		_, err := c.Mutate(genericContext(actor), denied)
		require.Equal(t, codes.PermissionDenied, status.Code(err), actor)
	}
	launchAPI(t, c, "alpha", "hash", "one")
	denied := &jp.MutationRequest{Command: genericMeta("launch-two-rejected"), Operation: &jp.MutationRequest_Launch{Launch: &jp.Launch{DefinitionId: "hash", DefinitionRevision: 1, RunId: "two"}}}
	_, err := c.Mutate(genericContext("alpha/writer"), denied)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	usage := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_USAGE, Id: "alpha"}).GetUsage()
	require.Equal(t, uint64(1), usage.PendingRuns)
	require.Equal(t, uint64(1), usage.StoredRuns)
	signal := &jp.MutationRequest{Command: genericMeta("signal"), Operation: &jp.MutationRequest_Signal{Signal: &jp.Signal{RunId: "one", EventId: "accepted", Key: "reply", Data: []byte("ab")}}}
	mutateJob(t, c, "alpha/callback", signal)
	mutateJob(t, c, "alpha/callback", signal)
	extra := proto.Clone(signal).(*jp.MutationRequest)
	extra.Command = genericMeta("inbox-full")
	extra.GetSignal().EventId = "extra"
	_, err = c.Mutate(genericContext("alpha/callback"), extra)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	usage = getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_USAGE}).GetUsage()
	require.Equal(t, uint64(1), usage.InboxEvents)
	require.Equal(t, uint64(2), usage.InboxBytes)
	mutateJob(t, c, "alpha/writer", &jp.MutationRequest{Command: genericMeta("cancel-one"), Operation: &jp.MutationRequest_Cancel{Cancel: &jp.Cancel{RunId: "one"}}})
	launchAPI(t, c, "alpha", "hash", "two")
	mutateJob(t, c, "alpha/writer", &jp.MutationRequest{Command: genericMeta("cancel-two"), Operation: &jp.MutationRequest_Cancel{Cancel: &jp.Cancel{RunId: "two"}}})
	denied.Command = genericMeta("retained-limit")
	denied.GetLaunch().RunId = "three"
	_, err = c.Mutate(genericContext("alpha/writer"), denied)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	usage = getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_USAGE}).GetUsage()
	require.Zero(t, usage.PendingRuns)
	require.Equal(t, uint64(2), usage.StoredRuns)
	mutateJob(t, c, "ops/alice", limitsAPI("alpha", 1, &jp.Limits{MaxStoredRuns: 3}))
	launchAPI(t, c, "alpha", "hash", "three")
	stale := limitsAPI("alpha", 0, &jp.Limits{MaxStoredRuns: 100})
	stale.Command = genericMeta("stale-limits")
	_, err = c.Mutate(genericContext("ops/alice"), stale)
	require.Equal(t, codes.Aborted, status.Code(err))
	_, err = c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_USAGE, TenantId: "beta"}})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Zero(t, getGeneric(t, c, "beta/reader", &jp.Reference{Kind: jp.Kind_USAGE}).GetUsage().StoredRuns)
	for _, actor := range []string{"alpha/callback", "alpha/producer"} {
		_, err = c.Get(genericContext(actor), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_USAGE}})
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	}
	_, err = c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_USAGE})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestJobLimitsHTTPRejectsActivationAtomicallyAndDefaultsAreInspectable(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	definitionAPI(t, c, "alpha", "hash", "sha256", "", nil)
	mutateJob(t, c, "alpha/writer", eventTriggerAPI("ingest", "hash", 2))
	policy := limitsAPI("alpha", 0, &jp.Limits{MaxActivationBytes: 1, MaxActivations: 2})
	code, body := genericHTTP(t, s, "/api/jobs/v1/mutate", genericToken("ops/alice"), policy, nil)
	require.Equal(t, http.StatusOK, code, string(body))
	event := activationAPI("ingest", "event")
	code, _ = genericHTTP(t, s, "/djob.jobs.v1.Jobs/Mutate", genericToken("alpha/producer"), event, nil)
	require.Equal(t, http.StatusTooManyRequests, code)
	code, body = genericHTTP(t, s, "/api/jobs/v1/get", genericToken("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_USAGE}}, nil)
	require.Equal(t, http.StatusOK, code, string(body))
	var record jp.Record
	require.NoError(t, protojson.Unmarshal(body, &record))
	require.Zero(t, record.GetUsage().StoredRuns)
	require.Zero(t, record.GetUsage().Activations)
	_, err := c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_ACTIVATION, TriggerId: "ingest", Source: "meters", Id: "event"}})
	require.Equal(t, codes.NotFound, status.Code(err))
	mutateJob(t, c, "ops/alice", limitsAPI("alpha", 1, &jp.Limits{MaxActivationBytes: 100, MaxActivations: 2}))
	code, _ = genericHTTP(t, s, "/api/jobs/v1/mutate", genericToken("alpha/producer"), event, nil)
	require.Equal(t, http.StatusTooManyRequests, code)
	event.Command = genericMeta("event-new-envelope")
	ack := mutateJob(t, c, "alpha/producer", event)
	require.Equal(t, "accepted", ack.Status)
	usage := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_USAGE}).GetUsage()
	require.Equal(t, uint64(1), usage.StoredRuns)
	require.Equal(t, uint64(1), usage.Activations)
	require.Equal(t, uint64(len(event.GetActivate().Data)), usage.ActivationBytes)
}
