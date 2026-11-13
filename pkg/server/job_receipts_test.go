package server

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func compactReceiptsAPI(id, run string, step uint64, batch uint32) *jp.MutationRequest {
	return &jp.MutationRequest{Command: genericMeta(id), Operation: &jp.MutationRequest_CompactReceipts{CompactReceipts: &jp.CompactReceipts{RunId: run, ExpectedStep: step, BatchSize: batch, Reason: "remove copies from historical receipts"}}}
}

func TestJobReceiptCompactionAPIPreservesPublicAcknowledgementsAndScope(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.JobRetention.PollInterval = time.Hour
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	tenantAPI(t, c, "beta")
	definitionAPI(t, c, "alpha", "work", "sha256-event", "", []byte("private application input"))
	mutateJob(t, c, "alpha/writer", eventTriggerAPI("ingest", "work", 2))
	type retry struct {
		actor string
		q     *jp.MutationRequest
		ack   *jp.MutationResponse
	}
	var retries []retry
	event := activationAPI("ingest", "reading")
	accepted := mutateJob(t, c, "alpha/producer", event)
	retries = append(retries, retry{"alpha/producer", event, accepted})
	for i := range 5 {
		q := &jp.MutationRequest{Command: genericMeta(fmt.Sprintf("inbox/%d", i)), Operation: &jp.MutationRequest_Signal{Signal: &jp.Signal{RunId: accepted.RunId, EventId: fmt.Sprintf("reply-%d", i), Key: "reply", Data: []byte("private callback payload")}}}
		retries = append(retries, retry{"alpha/callback", q, mutateJob(t, c, "alpha/callback", q)})
	}
	cancel := &jp.MutationRequest{Command: genericMeta("cancel-original"), Operation: &jp.MutationRequest_Cancel{Cancel: &jp.Cancel{RunId: accepted.RunId, Reason: "collection finished"}}}
	retries = append(retries, retry{"alpha/writer", cancel, mutateJob(t, c, "alpha/writer", cancel)})
	run := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_RUN, Id: accepted.RunId}).GetRun()
	_, err := c.Mutate(genericContext("alpha/operator"), compactReceiptsAPI("too-early", run.Id, run.Step, 1))
	require.Equal(t, codes.NotFound, status.Code(err))
	mutateJob(t, c, "alpha/operator", purgeAPI("purge-complete", run.Id, run.Step, db.MaxJobPurgeBatch))
	ref := &jp.Reference{Kind: jp.Kind_RECEIPT_COMPACTION, TenantId: "alpha", Id: run.Id}
	_, err = c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: ref})
	require.Equal(t, codes.NotFound, status.Code(err), "purging alone does not opt into receipt compaction")
	q := compactReceiptsAPI("compact-original", run.Id, run.Step, 1)
	for _, actor := range []string{"alpha/reader", "alpha/writer", "alpha/callback", "alpha/producer", "beta/operator"} {
		denied := proto.Clone(q).(*jp.MutationRequest)
		denied.TenantId = "alpha"
		_, err := c.Mutate(genericContext(actor), denied)
		require.Equal(t, codes.PermissionDenied, status.Code(err), actor)
	}
	code, body := genericHTTP(t, s, "/api/jobs/v1/mutate", genericToken("alpha/operator"), q, map[string]string{"Grpc-Metadata-Actor": "forged"})
	require.Equal(t, http.StatusOK, code, string(body))
	var ack jp.MutationResponse
	require.NoError(t, protojson.Unmarshal(body, &ack))
	require.Equal(t, jp.Kind_RECEIPT_COMPACTION, ack.Reference.Kind)
	require.Equal(t, "compacting", ack.Status)
	progress := getGeneric(t, c, "alpha/reader", ref).GetReceiptCompaction()
	require.False(t, progress.Complete)
	require.Equal(t, uint64(1), progress.ReceiptsCompacted)
	require.Equal(t, "alpha/operator", progress.Actor)
	for i := 0; i < 20 && !progress.Complete; i++ {
		mutateJob(t, c, "alpha/operator", compactReceiptsAPI(fmt.Sprintf("continue/%d", i), run.Id, run.Step, 1))
		progress = getGeneric(t, c, "alpha/reader", ref).GetReceiptCompaction()
	}
	require.True(t, progress.Complete)
	require.Equal(t, uint64(len(retries)), progress.ReceiptsCompacted)
	require.Positive(t, progress.PayloadBytesRemoved)
	for _, retry := range retries {
		again := mutateJob(t, c, retry.actor, retry.q)
		require.True(t, proto.Equal(retry.ack, again), retry.actor)
		code, body := genericHTTP(t, s, "/api/jobs/v1/mutate", genericToken(retry.actor), retry.q, nil)
		require.Equal(t, http.StatusOK, code, string(body))
		var httpAck jp.MutationResponse
		require.NoError(t, protojson.Unmarshal(body, &httpAck))
		require.True(t, proto.Equal(retry.ack, &httpAck))
	}
	again := mutateJob(t, c, "alpha/operator", q)
	require.True(t, proto.Equal(&ack, again), "the original batch acknowledgement remains stable after completion")
	changed := proto.Clone(event).(*jp.MutationRequest)
	changed.GetActivate().Data = []byte("conflict")
	_, err = c.Mutate(genericContext("alpha/producer"), changed)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	changed.Command = genericMeta("new-command-same-event-conflict")
	_, err = c.Mutate(genericContext("alpha/producer"), changed)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	late := proto.Clone(event).(*jp.MutationRequest)
	late.Command = genericMeta("late-redelivery")
	lateAck := mutateJob(t, c, "alpha/producer", late)
	require.Equal(t, run.Id, lateAck.RunId)
	require.True(t, proto.Equal(lateAck, mutateJob(t, c, "alpha/producer", late)))
	_, err = c.Mutate(genericContext("alpha/writer"), &jp.MutationRequest{Command: genericMeta("forbidden-reuse"), Operation: &jp.MutationRequest_Launch{Launch: &jp.Launch{DefinitionId: "work", DefinitionRevision: 1, RunId: run.Id}}})
	require.Equal(t, codes.NotFound, status.Code(err))
	_, err = c.Get(genericContext("beta/reader"), &jp.GetRequest{Reference: ref})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	page, err := c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_RECEIPT_COMPACTION})
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	usage := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_USAGE}).GetUsage()
	require.Zero(t, usage.StoredRuns)
	require.Zero(t, usage.InboxEvents)
	require.Zero(t, usage.Activations)
}
