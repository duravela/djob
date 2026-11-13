package server

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/jobs"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func triggerAPI(id, definition string, start time.Time) *jp.MutationRequest {
	return &jp.MutationRequest{Command: genericMeta("trigger/" + id), Operation: &jp.MutationRequest_PutTrigger{PutTrigger: &jp.PutTrigger{Definition: &jp.TriggerDefinition{Id: id, DefinitionId: definition, DefinitionRevision: 1, Timing: &jp.Timing{Kind: "once", StartAt: timestamppb.New(start)}}}}}
}

func TestGenericTriggerAPIAutomaticallyExecutesPinnedDefinition(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.JobRegistry = func(JobBackend) (jobs.Registry, error) { return jobs.Builtins(), nil }
	cfg.JobTriggers.PollInterval = 5 * time.Millisecond
	cfg.JobDispatch.PollInterval = 5 * time.Millisecond
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	definitionAPI(t, c, "alpha", "hash", "sha256", "", []byte("original"))
	start := time.Now().UTC().Add(300 * time.Millisecond)
	q := triggerAPI("once", "hash", start)
	ack := mutateJob(t, c, "alpha/writer", q)
	require.Equal(t, jp.Kind_TRIGGER, ack.Reference.Kind)
	require.Equal(t, uint64(1), ack.Revision)
	duplicate := mutateJob(t, c, "alpha/writer", q)
	require.True(t, proto.Equal(ack, duplicate))
	// Updating the source definition does not change an existing trigger's template.
	mutateJob(t, c, "alpha/writer", &jp.MutationRequest{Command: genericMeta("definition-v2"), Operation: &jp.MutationRequest_PutDefinition{PutDefinition: &jp.PutDefinition{ExpectedRevision: 1, Definition: &jp.Definition{Id: "hash", Runtime: db.StepRuntime, Handler: "sha256", HandlerVersion: "1", Input: []byte("replacement")}}}})
	id := db.JobOccurrenceRunID("alpha", "once", 1, start)
	require.Eventually(t, func() bool {
		r, err := c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_RUN, Id: id}})
		return err == nil && r.GetRun().Status == "succeeded"
	}, 4*time.Second, 5*time.Millisecond)
	run := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_RUN, Id: id}).GetRun()
	sum := sha256.Sum256([]byte("original"))
	require.Equal(t, sum[:], run.Result)
	require.Equal(t, "once", run.TriggerId)
	require.Equal(t, uint64(1), run.TriggerRevision)
	require.True(t, start.Equal(run.ScheduledAt.AsTime()))
	trigger := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_TRIGGER, Id: "once"}).GetTrigger()
	require.True(t, trigger.Completed)
	require.Equal(t, []byte("original"), trigger.JobDefinition.Input)
	page, err := c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_TRIGGER, PageSize: 1})
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	require.True(t, page.Records[0].GetTrigger().Summary)
	require.Empty(t, page.Records[0].GetTrigger().JobDefinition.Input)
	summary, err := c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_TRIGGER, Id: "once"}, Summary: true})
	require.NoError(t, err)
	require.True(t, summary.GetTrigger().Summary)
	require.Empty(t, summary.GetTrigger().JobDefinition.Input)
	for _, actor := range []string{"alpha/callback", "alpha/reader", "beta/writer"} {
		denied := proto.Clone(q).(*jp.MutationRequest)
		denied.Command = genericMeta("denied/" + actor)
		denied.TenantId = "alpha"
		_, err := c.Mutate(genericContext(actor), denied)
		require.Equal(t, codes.PermissionDenied, status.Code(err), actor)
	}
	_, err = c.Get(genericContext("beta/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_TRIGGER, TenantId: "alpha", Id: "once"}})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestGenericRecurringTriggerPausesAndQueuesWithinLimitWithoutWorkers(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.JobTriggers.PollInterval = 5 * time.Millisecond
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	definitionAPI(t, c, "alpha", "hash", "sha256", "", []byte("input"))
	q := triggerAPI("recurring", "hash", time.Now().UTC())
	d := q.GetPutTrigger().Definition
	d.Timing.Kind = "interval"
	d.Timing.IntervalNs = int64(20 * time.Millisecond)
	d.MaxPending = 1
	mutateJob(t, c, "alpha/writer", q)
	list := func() *jp.ListResponse {
		p, err := c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: jp.Kind_RUN})
		require.NoError(t, err)
		return p
	}
	require.Eventually(t, func() bool { return len(list().Records) == 1 }, 3*time.Second, 5*time.Millisecond)
	pause := func(id string, expected uint64, paused bool) *jp.MutationResponse {
		return mutateJob(t, c, "alpha/writer", &jp.MutationRequest{Command: genericMeta(id), Operation: &jp.MutationRequest_PauseTrigger{PauseTrigger: &jp.PauseTrigger{Id: "recurring", ExpectedRevision: expected, Paused: paused}}})
	}
	require.Equal(t, uint64(2), pause("pause", 1, true).Revision)
	first := list().Records[0].GetRun()
	require.Equal(t, "ready", first.Status)
	mutateJob(t, c, "alpha/writer", &jp.MutationRequest{Command: genericMeta("cancel"), Operation: &jp.MutationRequest_Cancel{Cancel: &jp.Cancel{RunId: first.Id, Reason: "cancel while paused"}}})
	// A paused trigger does not create a replacement when capacity is released.
	time.Sleep(60 * time.Millisecond)
	require.Len(t, list().Records, 1)
	require.Equal(t, uint64(3), pause("resume", 2, false).Revision)
	require.Eventually(t, func() bool { return len(list().Records) == 2 }, 3*time.Second, 5*time.Millisecond)
	// Further due times are deferred without creating more runs.
	time.Sleep(60 * time.Millisecond)
	page := list()
	require.Len(t, page.Records, 2)
	statuses := map[string]int{}
	for _, record := range page.Records {
		statuses[record.GetRun().Status]++
	}
	require.Equal(t, map[string]int{"cancelled": 1, "ready": 1}, statuses)
	stale := &jp.MutationRequest{Command: genericMeta("stale-pause"), Operation: &jp.MutationRequest_PauseTrigger{PauseTrigger: &jp.PauseTrigger{Id: "recurring", ExpectedRevision: 1, Paused: true}}}
	_, err := c.Mutate(genericContext("alpha/writer"), stale)
	require.Equal(t, codes.Aborted, status.Code(err))
}

func TestGenericTriggerAPIRejectsInvalidCalendarsBeforeProposing(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	definitionAPI(t, c, "alpha", "hash", "sha256", "", nil)
	for _, spec := range []*jp.Timing{
		{Kind: "cron", StartAt: timestamppb.Now(), Cron: "* * * * *", TimeZone: "Local"},
		{Kind: "cron", StartAt: timestamppb.Now(), Cron: "61 * * * *", TimeZone: "UTC"},
		{Kind: "interval", StartAt: timestamppb.Now(), IntervalNs: -1},
		{Kind: "once"},
	} {
		q := triggerAPI("invalid", "hash", time.Now().UTC())
		q.GetPutTrigger().Definition.Timing = spec
		_, err := c.Mutate(genericContext("alpha/writer"), q)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
}
