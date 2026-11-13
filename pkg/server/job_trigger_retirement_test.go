package server

import (
	"net/http"
	"testing"
	"time"

	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func retirementAPI(id string, revision uint64) *jp.MutationRequest {
	return &jp.MutationRequest{Command: genericMeta("retire/" + id), Operation: &jp.MutationRequest_RetireTrigger{RetireTrigger: &jp.RetireTrigger{Id: id, ExpectedRevision: revision, Reason: "obsolete activation configuration"}}}
}

func TestJobTriggerRetirementAPIAuthorizesAuditAndPreservesAcceptedRuns(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	definitionAPI(t, c, "alpha", "work", "sha256", "", []byte("input"))
	created := eventTriggerAPI("ingest", "work", 2)
	mutateJob(t, c, "alpha/writer", created)
	event := activationAPI("ingest", "reading")
	accepted := mutateJob(t, c, "alpha/producer", event)
	q := retirementAPI("ingest", 1)
	for _, actor := range []string{"alpha/reader", "alpha/callback", "alpha/producer", "beta/writer"} {
		denied := proto.Clone(q).(*jp.MutationRequest)
		denied.Command = genericMeta("denied/" + actor)
		denied.TenantId = "alpha"
		_, err := c.Mutate(genericContext(actor), denied)
		require.Equal(t, codes.PermissionDenied, status.Code(err), actor)
	}
	stale := retirementAPI("ingest", 2)
	stale.Command = genericMeta("stale-retirement")
	_, err := c.Mutate(genericContext("alpha/writer"), stale)
	require.Equal(t, codes.Aborted, status.Code(err))
	invalid := retirementAPI("ingest", 1)
	invalid.Command = genericMeta("invalid-retirement")
	invalid.GetRetireTrigger().Reason = ""
	_, err = c.Mutate(genericContext("alpha/writer"), invalid)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// Exercise the HTTP JSON mapping as well as gRPC receipt replay.
	code, body := genericHTTP(t, s, "/api/jobs/v1/mutate", genericToken("alpha/writer"), q, nil)
	require.Equal(t, http.StatusOK, code, string(body))
	var ack jp.MutationResponse
	require.NoError(t, protojson.Unmarshal(body, &ack))
	require.Equal(t, "retired", ack.Status)
	require.Equal(t, uint64(1), ack.Revision)
	require.Equal(t, jp.Kind_TRIGGER_RETIREMENT, ack.Reference.Kind)
	require.Equal(t, "alpha", ack.Reference.TenantId)
	require.True(t, proto.Equal(&ack, mutateJob(t, c, "alpha/writer", q)))
	retired := getGeneric(t, c, "alpha/reader", ack.Reference).GetTriggerRetirement()
	require.Equal(t, "alpha/writer", retired.Actor)
	require.Equal(t, q.GetRetireTrigger().Reason, retired.Reason)
	require.Equal(t, uint64(1), retired.Revision)
	require.True(t, q.Command.At.AsTime().Equal(retired.RetiredAt.AsTime()))
	page, err := c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_TRIGGER_RETIREMENT})
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	require.True(t, proto.Equal(retired, page.Records[0].GetTriggerRetirement()))
	history, err := c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_ACTIVATION, TriggerId: "ingest"})
	require.NoError(t, err)
	require.Len(t, history.Records, 1)
	require.Equal(t, accepted.RunId, history.Records[0].GetActivation().RunId)
	_, err = c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_TRIGGER, Id: "ingest"}})
	require.Equal(t, codes.NotFound, status.Code(err))
	_, err = c.Get(genericContext("beta/reader"), &jp.GetRequest{Reference: ack.Reference})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = c.List(genericContext("beta/reader"), &jp.ListRequest{Kind: jp.Kind_TRIGGER_RETIREMENT, TenantId: "alpha"})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	usage := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_CONFIGURATION_USAGE}).GetConfigurationUsage()
	require.Zero(t, usage.Triggers)
	require.Equal(t, uint64(len("input")), usage.PayloadBytes)
	redelivery := proto.Clone(event).(*jp.MutationRequest)
	redelivery.Command = genericMeta("redelivery")
	require.Equal(t, accepted.RunId, mutateJob(t, c, "alpha/producer", redelivery).RunId)
	_, err = c.Mutate(genericContext("alpha/producer"), activationAPI("ingest", "new"))
	require.Equal(t, codes.NotFound, status.Code(err))
	recreated := proto.Clone(created).(*jp.MutationRequest)
	recreated.Command = genericMeta("recreate")
	_, err = c.Mutate(genericContext("alpha/writer"), recreated)
	require.Equal(t, codes.NotFound, status.Code(err))
	run := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_RUN, Id: accepted.RunId}).GetRun()
	require.Equal(t, "ready", run.Status)
	require.Equal(t, []byte("input"), run.Definition.Input)
	// Cancellation remains available after retirement and frees pending usage.
	mutateJob(t, c, "alpha/writer", &jp.MutationRequest{Command: genericMeta("cancel"), Operation: &jp.MutationRequest_Cancel{Cancel: &jp.Cancel{RunId: run.Id, Reason: "no longer required"}}})
	mutateJob(t, c, "alpha/operator", eventTriggerAPI("replacement", "work", 1))
	mutateJob(t, c, "alpha/operator", retirementAPI("replacement", 1))
}

func TestJobTriggerRetirementStopsRecurringControllerAfterPendingSlotIsReleased(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.JobTriggers.PollInterval = 5 * time.Millisecond
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	definitionAPI(t, c, "alpha", "work", "sha256", "", nil)
	q := triggerAPI("poll", "work", time.Now().UTC())
	q.GetPutTrigger().Definition.Timing.Kind = "interval"
	q.GetPutTrigger().Definition.Timing.IntervalNs = int64(10 * time.Millisecond)
	q.GetPutTrigger().Definition.MaxPending = 1
	mutateJob(t, c, "alpha/writer", q)
	var runID string
	require.Eventually(t, func() bool {
		p, err := c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_RUN})
		if err != nil || len(p.Records) != 1 {
			return false
		}
		runID = p.Records[0].GetRun().Id
		return true
	}, 3*time.Second, 5*time.Millisecond)
	mutateJob(t, c, "alpha/writer", retirementAPI("poll", 1))
	mutateJob(t, c, "alpha/writer", &jp.MutationRequest{Command: genericMeta("cancel"), Operation: &jp.MutationRequest_Cancel{Cancel: &jp.Cancel{RunId: runID, Reason: "release pending slot"}}})
	// Runs are retained, so one read after several controller sweeps detects
	// any additional launch without leaving an assertion goroutine in flight.
	time.Sleep(80 * time.Millisecond)
	p, err := c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_RUN})
	require.NoError(t, err)
	require.Len(t, p.Records, 1)
	require.Equal(t, "cancelled", p.Records[0].GetRun().Status)
}
