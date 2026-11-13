package server

import (
	"crypto/sha256"
	"fmt"
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
)

func purgeAPI(id, run string, step uint64, batch uint32) *jp.MutationRequest {
	meta := genericMeta(id)
	return &jp.MutationRequest{Command: meta, Operation: &jp.MutationRequest_Purge{Purge: &jp.Purge{RunId: run, ExpectedStep: step, FinishedBefore: meta.At, BatchSize: batch, Reason: "completed application history"}}}
}
func retentionAPI(tenant, id string, revision uint64, enabled bool, keep time.Duration) *jp.MutationRequest {
	return &jp.MutationRequest{Command: genericMeta(id), TenantId: tenant, Operation: &jp.MutationRequest_PutRetention{PutRetention: &jp.PutRetention{ExpectedRevision: revision, Policy: &jp.Retention{Enabled: enabled, KeepForNs: int64(keep)}}}}
}

func TestJobPurgeAPIProtectsTenantScopeAndRetainsActivationDeduplication(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.JobRetention.PollInterval = time.Hour
	cfg.JobDispatch.PollInterval = 5 * time.Millisecond
	cfg.JobRegistry = func(JobBackend) (jobs.Registry, error) {
		r := jobs.Builtins()
		r.Handlers[jobs.Version{Name: "wait", Version: "1"}] = func(v jobs.View) (jobs.Decision, error) {
			if len(v.Run.State) == 0 {
				return jobs.Decision{Kind: db.JobWait, State: []byte("waiting"), WaitKey: "finish"}, nil
			}
			return jobs.Decision{Kind: db.JobComplete, Status: db.JobSucceeded, Result: v.Event.Data}, nil
		}
		return r, nil
	}
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	definitionAPI(t, c, "alpha", "work", "wait", "", nil)
	mutateJob(t, c, "alpha/writer", eventTriggerAPI("ingest", "work", 2))
	event := activationAPI("ingest", "reading")
	accepted := mutateJob(t, c, "alpha/producer", event)
	require.Eventually(t, func() bool {
		r, err := c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_RUN, Id: accepted.RunId}})
		return err == nil && r.GetRun().Status == "waiting"
	}, 3*time.Second, 5*time.Millisecond)
	for i := 0; i < 3; i++ {
		mutateJob(t, c, "alpha/callback", &jp.MutationRequest{Command: genericMeta(fmt.Sprintf("extra/%d", i)), Operation: &jp.MutationRequest_Signal{Signal: &jp.Signal{RunId: accepted.RunId, EventId: fmt.Sprintf("extra-%d", i), Key: "extra", Data: []byte("payload")}}})
	}
	mutateJob(t, c, "alpha/callback", &jp.MutationRequest{Command: genericMeta("finish"), Operation: &jp.MutationRequest_Signal{Signal: &jp.Signal{RunId: accepted.RunId, EventId: "finish", Key: "finish", Data: []byte("result")}}})
	require.Eventually(t, func() bool {
		r, err := c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_RUN, Id: accepted.RunId}})
		return err == nil && r.GetRun().Status == "succeeded"
	}, 3*time.Second, 5*time.Millisecond)
	run := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_RUN, Id: accepted.RunId}).GetRun()
	q := purgeAPI("purge", run.Id, run.Step, 1)
	for _, actor := range []string{"alpha/reader", "alpha/writer", "alpha/callback", "alpha/producer", "beta/operator"} {
		denied := proto.Clone(q).(*jp.MutationRequest)
		denied.TenantId = "alpha"
		_, err := c.Mutate(genericContext(actor), denied)
		require.Equal(t, codes.PermissionDenied, status.Code(err), actor)
	}
	code, body := genericHTTP(t, s, "/api/jobs/v1/mutate", genericToken("alpha/operator"), q, map[string]string{"Grpc-Metadata-Actor": "spoofed"})
	require.Equal(t, http.StatusOK, code, string(body))
	var ack jp.MutationResponse
	require.NoError(t, protojson.Unmarshal(body, &ack))
	require.Equal(t, jp.Kind_DELETION, ack.Reference.Kind)
	deletion := getGeneric(t, c, "alpha/reader", ack.Reference).GetDeletion()
	require.False(t, deletion.CleanupComplete)
	require.Equal(t, "alpha/operator", deletion.Actor)
	_, err := c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_RUN, Id: run.Id}})
	require.Equal(t, codes.NotFound, status.Code(err))
	receipt := getGeneric(t, c, "alpha/reader", accepted.Reference).GetActivation()
	require.True(t, receipt.DataDeleted)
	require.Empty(t, receipt.Data)
	digest := sha256.Sum256(event.GetActivate().Data)
	require.Equal(t, digest[:], receipt.DataSha256)
	redelivery := proto.Clone(event).(*jp.MutationRequest)
	redelivery.Command = genericMeta("redelivery")
	require.Equal(t, run.Id, mutateJob(t, c, "alpha/producer", redelivery).RunId)
	for i := 0; i < 20 && !deletion.CleanupComplete; i++ {
		mutateJob(t, c, "alpha/operator", purgeAPI(fmt.Sprintf("cleanup/%d", i), run.Id, run.Step, 1))
		deletion = getGeneric(t, c, "alpha/reader", ack.Reference).GetDeletion()
	}
	require.True(t, deletion.CleanupComplete)
	usage := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_USAGE}).GetUsage()
	require.Zero(t, usage.StoredRuns)
	require.Zero(t, usage.InboxEvents)
	require.Zero(t, usage.Activations)
	require.Zero(t, usage.ActivationBytes)
	_, err = c.Mutate(genericContext("alpha/writer"), &jp.MutationRequest{Command: genericMeta("reuse"), Operation: &jp.MutationRequest_Launch{Launch: &jp.Launch{DefinitionId: "work", DefinitionRevision: 1, RunId: run.Id}}})
	require.Equal(t, codes.NotFound, status.Code(err))
	page, err := c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_DELETION})
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	_, err = c.Get(genericContext("beta/reader"), &jp.GetRequest{Reference: ack.Reference})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestJobRetentionAPIAutomaticCleanupIsOptInAndFreesStoredCapacity(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.JobDispatch.PollInterval = 5 * time.Millisecond
	cfg.JobRetention.PollInterval = 5 * time.Millisecond
	cfg.JobRetention.BatchSize = 1
	cfg.JobRetention.DeleteBatchSize = 1
	cfg.JobRegistry = func(JobBackend) (jobs.Registry, error) { return jobs.Builtins(), nil }
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	definitionAPI(t, c, "alpha", "hash", "sha256", "", nil)
	mutateJob(t, c, "ops/alice", limitsAPI("alpha", 0, &jp.Limits{MaxStoredRuns: 1}))
	launchAPI(t, c, "alpha", "hash", "one")
	require.Eventually(t, func() bool {
		r, err := c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_RUN, Id: "one"}})
		return err == nil && r.GetRun().Status == "succeeded"
	}, 3*time.Second, 5*time.Millisecond)
	policy := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_RETENTION}).GetRetention()
	require.False(t, policy.Enabled)
	time.Sleep(70 * time.Millisecond)
	getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_RUN, Id: "one"})
	q := retentionAPI("alpha", "retention", 0, true, 50*time.Millisecond)
	_, err := c.Mutate(genericContext("alpha/operator"), q)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	ack := mutateJob(t, c, "ops/alice", q)
	require.Equal(t, jp.Kind_RETENTION, ack.Reference.Kind)
	require.Eventually(t, func() bool {
		r, err := c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_DELETION, Id: "one"}})
		return err == nil && r.GetDeletion().CleanupComplete
	}, 3*time.Second, 5*time.Millisecond)
	d := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_DELETION, Id: "one"}).GetDeletion()
	require.Equal(t, "djob/retention", d.Actor)
	require.Equal(t, uint64(1), d.PolicyRevision)
	mutateJob(t, c, "ops/alice", retentionAPI("alpha", "disable-retention", 1, false, 0))
	launchAPI(t, c, "alpha", "hash", "two")
	time.Sleep(90 * time.Millisecond)
	getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_RUN, Id: "two"})
	_, err = c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_RETENTION})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
