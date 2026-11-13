package server

import (
	"fmt"
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

func compactConfigurationReceiptsAPI(id string, step uint64, batch uint32) *jp.MutationRequest {
	return &jp.MutationRequest{Command: genericMeta(fmt.Sprintf("configuration-sweep/%s/%d", id, step)), Operation: &jp.MutationRequest_CompactConfigurationReceipts{CompactConfigurationReceipts: &jp.CompactConfigurationReceipts{CompactionId: id, ExpectedStep: step, BatchSize: batch, Reason: "compact historical configuration copies"}}}
}
func TestConfigurationReceiptCompactionAPIPreservesEveryPublicConfigurationAcknowledgement(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.JobRetention.PollInterval = time.Hour
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	type retry struct {
		actor string
		q     *jp.MutationRequest
		ack   *jp.MutationResponse
	}
	var retries []retry
	save := func(actor string, q *jp.MutationRequest) *jp.MutationResponse {
		ack := mutateJob(t, c, actor, q)
		retries = append(retries, retry{actor, proto.Clone(q).(*jp.MutationRequest), proto.Clone(ack).(*jp.MutationResponse)})
		return ack
	}
	save("ops/alice", &jp.MutationRequest{Command: genericMeta("tenant"), TenantId: "alpha", Operation: &jp.MutationRequest_PutTenant{PutTenant: &jp.PutTenant{MaxRunning: 2}}})
	tenantAPI(t, c, "beta")
	mutateJob(t, c, "ops/alice", &jp.MutationRequest{Command: genericMeta("resource"), Operation: &jp.MutationRequest_PutResource{PutResource: &jp.PutResource{Spec: &jp.ResourceSpec{Id: "slot", Duplex: "half", Capacity: 1}}}})
	save("ops/alice", &jp.MutationRequest{Command: genericMeta("grant"), TenantId: "alpha", Operation: &jp.MutationRequest_PutGrant{PutGrant: &jp.PutGrant{ResourceId: "slot", Allowed: true}}})
	save("alpha/writer", &jp.MutationRequest{Command: genericMeta("group"), Operation: &jp.MutationRequest_PutGroup{PutGroup: &jp.PutGroup{Group: &jp.Group{Id: "group", Runtime: "steps/v1"}}}})
	save("ops/alice", &jp.MutationRequest{Command: genericMeta("binding"), TenantId: "alpha", Operation: &jp.MutationRequest_PutBinding{PutBinding: &jp.PutBinding{Binding: &jp.Binding{Id: "driver", Driver: "test", DriverVersion: "1", Config: []byte("binding payload")}}}})
	definition := &jp.MutationRequest{Command: genericMeta("definition"), Operation: &jp.MutationRequest_PutDefinition{PutDefinition: &jp.PutDefinition{Definition: &jp.Definition{Id: "work", GroupId: "group", Runtime: "steps/v1", Handler: "sha256", HandlerVersion: "1", BindingId: "driver", Input: []byte("application payload")}}}}
	save("alpha/writer", definition)
	save("alpha/writer", eventTriggerAPI("ingest", "work", 2))
	save("alpha/writer", &jp.MutationRequest{Command: genericMeta("pause"), Operation: &jp.MutationRequest_PauseTrigger{PauseTrigger: &jp.PauseTrigger{Id: "ingest", ExpectedRevision: 1, Paused: true}}})
	save("ops/alice", limitsAPI("alpha", 0, &jp.Limits{MaxStoredRuns: 8}))
	save("ops/alice", configurationLimitsAPI("alpha", 0, &jp.ConfigurationLimits{MaxDefinitions: 2}))
	save("ops/alice", &jp.MutationRequest{Command: genericMeta("retention"), TenantId: "alpha", Operation: &jp.MutationRequest_PutRetention{PutRetention: &jp.PutRetention{Policy: &jp.Retention{}}}})
	first := mutateJob(t, c, "alpha/operator", compactConfigurationReceiptsAPI("audit", 0, 1))
	require.Equal(t, "compacting", first.Status)
	require.Equal(t, uint64(1), first.Step)
	for _, actor := range []string{"alpha/reader", "alpha/writer", "alpha/callback", "alpha/producer", "writer"} {
		_, err := c.Mutate(genericContext(actor), compactConfigurationReceiptsAPI("forbidden", 0, 1))
		require.Equal(t, codes.PermissionDenied, status.Code(err), actor)
	}
	before := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_CONFIGURATION_USAGE}).GetConfigurationUsage()
	last := mutateJob(t, c, "alpha/operator", compactConfigurationReceiptsAPI("audit", first.Step, 128))
	require.Equal(t, "complete", last.Status)
	for _, r := range retries {
		require.True(t, proto.Equal(r.ack, mutateJob(t, c, r.actor, r.q)), r.q.Command.RequestId)
	}
	changed := proto.Clone(definition).(*jp.MutationRequest)
	changed.GetPutDefinition().Definition.Input = []byte("changed")
	_, err := c.Mutate(genericContext("alpha/writer"), changed)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	live := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_DEFINITION, Id: "work"}).GetDefinition()
	require.Equal(t, "application payload", string(live.Input))
	after := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_CONFIGURATION_USAGE}).GetConfigurationUsage()
	require.True(t, proto.Equal(before, after))
	progress := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_CONFIGURATION_RECEIPT_COMPACTION, Id: "audit"}).GetConfigurationReceiptCompaction()
	require.True(t, progress.Complete)
	require.Equal(t, uint64(len(retries)), progress.ReceiptsCompacted)
	require.Equal(t, "alpha/operator", progress.Actor)
	require.Positive(t, progress.PayloadBytesRemoved)
	_, err = c.Get(genericContext("beta/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_CONFIGURATION_RECEIPT_COMPACTION, TenantId: "alpha", Id: "audit"}})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	page, err := c.List(genericContext("beta/reader"), &jp.ListRequest{Kind: jp.Kind_CONFIGURATION_RECEIPT_COMPACTION})
	require.NoError(t, err)
	require.Empty(t, page.Records)
	for _, actor := range []string{"alpha/callback", "alpha/producer"} {
		_, err = c.Get(genericContext(actor), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_CONFIGURATION_RECEIPT_COMPACTION, Id: "audit"}})
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	}
}

func TestConfigurationReceiptCompactionHTTPIsBoundedAuditedAndRequiresExplicitScope(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.JobRetention.PollInterval = time.Hour
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	definitionAPI(t, c, "alpha", "hash", "sha256", "", []byte("input"))
	q := compactConfigurationReceiptsAPI("http", 0, 1)
	code, body := genericHTTP(t, s, "/api/jobs/v1/mutate", genericToken("alpha/operator"), q, nil)
	require.Equal(t, http.StatusOK, code, string(body))
	var ack jp.MutationResponse
	require.NoError(t, protojson.Unmarshal(body, &ack))
	require.Equal(t, "compacting", ack.Status)
	code, body = genericHTTP(t, s, "/api/jobs/v1/get", genericToken("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_CONFIGURATION_RECEIPT_COMPACTION, Id: "http"}}, nil)
	require.Equal(t, http.StatusOK, code, string(body))
	var row jp.Record
	require.NoError(t, protojson.Unmarshal(body, &row))
	require.Equal(t, uint64(1), row.GetConfigurationReceiptCompaction().ReceiptsCompacted)
	_, err := c.Mutate(genericContext("ops/alice"), compactConfigurationReceiptsAPI("missing-tenant", 0, 1))
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = c.Mutate(genericContext("alpha/operator"), compactConfigurationReceiptsAPI("bad-batch", 0, 129))
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	stale := compactConfigurationReceiptsAPI("http", 0, 1)
	stale.Command = genericMeta("stale-configuration-sweep")
	_, err = c.Mutate(genericContext("alpha/operator"), stale)
	require.Equal(t, codes.Aborted, status.Code(err))
	bad := compactConfigurationReceiptsAPI("bad-reason", 0, 1)
	bad.GetCompactConfigurationReceipts().Reason = ""
	_, err = c.Mutate(genericContext("alpha/operator"), bad)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
