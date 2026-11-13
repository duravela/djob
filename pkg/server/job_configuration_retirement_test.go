package server

import (
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func configurationRetirementAPI(kind jp.Kind, id string, revision uint64) *jp.MutationRequest {
	return &jp.MutationRequest{Command: genericMeta("retire/" + kind.String() + "/" + id), Operation: &jp.MutationRequest_RetireConfiguration{RetireConfiguration: &jp.RetireConfiguration{Kind: kind, Id: id, ExpectedRevision: revision, Reason: "obsolete configuration"}}}
}

func TestJobConfigurationRetirementAPIScopesDependenciesAndRetryAudit(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	tenantAPI(t, c, "beta")
	group := &jp.MutationRequest{Command: genericMeta("group"), Operation: &jp.MutationRequest_PutGroup{PutGroup: &jp.PutGroup{Group: &jp.Group{Id: "same", Runtime: db.StepRuntime, MaxRunning: 2}}}}
	mutateJob(t, c, "alpha/writer", group)
	binding := &jp.MutationRequest{Command: genericMeta("binding"), TenantId: "alpha", Operation: &jp.MutationRequest_PutBinding{PutBinding: &jp.PutBinding{Binding: &jp.Binding{Id: "same", Driver: "test", DriverVersion: "1", Config: []byte("xy")}}}}
	mutateJob(t, c, "ops/alice", binding)
	definition := &jp.MutationRequest{Command: genericMeta("definition"), Operation: &jp.MutationRequest_PutDefinition{PutDefinition: &jp.PutDefinition{Definition: &jp.Definition{Id: "same", GroupId: "same", BindingId: "same", Handler: "test", HandlerVersion: "1", Input: []byte("abc")}}}}
	mutateJob(t, c, "alpha/writer", definition)
	mutateJob(t, c, "alpha/writer", eventTriggerAPI("ingest", "same", 3))
	accepted := mutateJob(t, c, "alpha/producer", activationAPI("ingest", "first"))
	for _, kind := range []jp.Kind{jp.Kind_GROUP, jp.Kind_BINDING} {
		q := configurationRetirementAPI(kind, "same", 1)
		q.Command = genericMeta("in-use/" + kind.String())
		q.TenantId = "alpha"
		_, err := c.Mutate(genericContext("ops/alice"), q)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	}
	for _, tc := range []struct {
		kind, audit jp.Kind
		actor       string
	}{{jp.Kind_DEFINITION, jp.Kind_DEFINITION_RETIREMENT, "alpha/writer"}, {jp.Kind_BINDING, jp.Kind_BINDING_RETIREMENT, "ops/alice"}, {jp.Kind_GROUP, jp.Kind_GROUP_RETIREMENT, "alpha/operator"}} {
		q := configurationRetirementAPI(tc.kind, "same", 1)
		q.TenantId = "alpha"
		for _, actor := range []string{"alpha/reader", "alpha/callback", "alpha/producer", "beta/writer", "beta/operator"} {
			denied := proto.Clone(q).(*jp.MutationRequest)
			denied.Command = genericMeta("denied/" + tc.kind.String() + "/" + actor)
			_, err := c.Mutate(genericContext(actor), denied)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
		}
		if tc.kind == jp.Kind_BINDING {
			for _, actor := range []string{"alpha/writer", "alpha/operator"} {
				_, err := c.Mutate(genericContext(actor), q)
				require.Equal(t, codes.PermissionDenied, status.Code(err))
			}
		}
		stale := proto.Clone(q).(*jp.MutationRequest)
		stale.Command = genericMeta("stale/" + tc.kind.String())
		stale.GetRetireConfiguration().ExpectedRevision = 2
		_, err := c.Mutate(genericContext(tc.actor), stale)
		require.Equal(t, codes.Aborted, status.Code(err))
		code, body := genericHTTP(t, s, "/api/jobs/v1/mutate", genericToken(tc.actor), q, nil)
		require.Equal(t, http.StatusOK, code, string(body))
		var ack jp.MutationResponse
		require.NoError(t, protojson.Unmarshal(body, &ack))
		require.Equal(t, tc.audit, ack.Reference.Kind)
		require.Equal(t, "alpha", ack.Reference.TenantId)
		require.Equal(t, "retired", ack.Status)
		require.Equal(t, uint64(1), ack.Revision)
		require.True(t, proto.Equal(&ack, mutateJob(t, c, tc.actor, q)))
		audit := getGeneric(t, c, "alpha/reader", ack.Reference).GetConfigurationRetirement()
		require.Equal(t, tc.kind, audit.Kind)
		require.Equal(t, tc.actor, audit.Actor)
		require.Equal(t, q.GetRetireConfiguration().Reason, audit.Reason)
		require.True(t, q.Command.At.AsTime().Equal(audit.RetiredAt.AsTime()))
		if tc.kind == jp.Kind_GROUP {
			require.Equal(t, uint64(2), audit.GroupMaxRunning)
		}
		page, err := c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: tc.audit, PageSize: 1})
		require.NoError(t, err)
		require.Len(t, page.Records, 1)
		require.True(t, proto.Equal(audit, page.Records[0].GetConfigurationRetirement()))
		_, err = c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: tc.kind, Id: "same"}})
		require.Equal(t, codes.NotFound, status.Code(err))
		_, err = c.Get(genericContext("beta/reader"), &jp.GetRequest{Reference: ack.Reference})
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		conflict := proto.Clone(q).(*jp.MutationRequest)
		conflict.GetRetireConfiguration().Reason = "different reason"
		_, err = c.Mutate(genericContext(tc.actor), conflict)
		require.Equal(t, codes.AlreadyExists, status.Code(err))
	}
	usage := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_CONFIGURATION_USAGE}).GetConfigurationUsage()
	require.Zero(t, usage.Groups)
	require.Zero(t, usage.Definitions)
	require.Zero(t, usage.Bindings)
	require.Equal(t, uint64(1), usage.Triggers)
	require.Equal(t, uint64(5), usage.PayloadBytes)
	run := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_RUN, Id: accepted.RunId}).GetRun()
	require.Equal(t, "ready", run.Status)
	require.Equal(t, []byte("abc"), run.Definition.Input)
	require.Equal(t, []byte("xy"), run.Binding.Config)
	mutateJob(t, c, "alpha/producer", activationAPI("ingest", "second"))
	for _, q := range []*jp.MutationRequest{group, definition, binding} {
		recreate := proto.Clone(q).(*jp.MutationRequest)
		recreate.Command = genericMeta("recreate/" + q.Command.RequestId)
		recreate.TenantId = "alpha"
		_, err := c.Mutate(genericContext("ops/alice"), recreate)
		require.Equal(t, codes.NotFound, status.Code(err))
	}
	for _, kind := range []jp.Kind{jp.Kind_TRIGGER, jp.Kind_RESOURCE, jp.Kind_TENANT, jp.Kind_GROUP_RETIREMENT} {
		_, err := c.Mutate(genericContext("alpha/writer"), configurationRetirementAPI(kind, "same", 1))
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
}

func TestJobConfigurationRetirementSerializesWithConcurrentDefinitionCreation(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	for i := range 5 {
		id := fmt.Sprintf("group-%d", i)
		mutateJob(t, c, "alpha/writer", &jp.MutationRequest{Command: genericMeta("put/" + id), Operation: &jp.MutationRequest_PutGroup{PutGroup: &jp.PutGroup{Group: &jp.Group{Id: id, Runtime: db.StepRuntime}}}})
		commands := []*jp.MutationRequest{
			configurationRetirementAPI(jp.Kind_GROUP, id, 1),
			{Command: genericMeta("definition/" + id), Operation: &jp.MutationRequest_PutDefinition{PutDefinition: &jp.PutDefinition{Definition: &jp.Definition{Id: id, GroupId: id, Handler: "test", HandlerVersion: "1"}}}},
		}
		var errs [2]error
		var wg sync.WaitGroup
		for j := range commands {
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				_, errs[j] = c.Mutate(genericContext("alpha/writer"), commands[j])
			}(j)
		}
		wg.Wait()
		if errs[0] == nil {
			require.Equal(t, codes.NotFound, status.Code(errs[1]))
			_, err := c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_DEFINITION, Id: id}})
			require.Equal(t, codes.NotFound, status.Code(err))
		} else {
			require.Equal(t, codes.FailedPrecondition, status.Code(errs[0]))
			require.NoError(t, errs[1])
			getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_GROUP, Id: id})
		}
	}
}
