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

func configurationLimitsAPI(tenant string, expected uint64, limits *jp.ConfigurationLimits) *jp.MutationRequest {
	return &jp.MutationRequest{Command: genericMeta(fmt.Sprintf("configuration-limits/%d", expected)), TenantId: tenant, Operation: &jp.MutationRequest_PutConfigurationLimits{PutConfigurationLimits: &jp.PutConfigurationLimits{ExpectedRevision: expected, Limits: limits}}}
}

func TestJobConfigurationLimitsAPIAuthorizationDefaultsAndRevision(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	tenantAPI(t, c, "beta")
	defaults := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_CONFIGURATION_LIMITS}).GetConfigurationLimits()
	require.NotNil(t, defaults)
	require.Zero(t, defaults.Revision)
	usage := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_CONFIGURATION_USAGE}).GetConfigurationUsage()
	require.NotNil(t, usage)
	require.Zero(t, usage.Definitions)
	q := configurationLimitsAPI("alpha", 0, &jp.ConfigurationLimits{MaxGroups: 1, MaxDefinitions: 1, MaxBindings: 1, MaxTriggers: 1, MaxPayloadBytes: 32})
	ack := mutateJob(t, c, "ops/alice", q)
	require.Equal(t, jp.Kind_CONFIGURATION_LIMITS, ack.Reference.Kind)
	require.Equal(t, uint64(1), ack.Revision)
	require.True(t, proto.Equal(ack, mutateJob(t, c, "ops/alice", q)))
	for _, actor := range []string{"alpha/reader", "alpha/writer", "alpha/operator", "alpha/callback", "alpha/producer", "writer"} {
		_, err := c.Mutate(genericContext(actor), q)
		require.Equal(t, codes.PermissionDenied, status.Code(err), actor)
	}
	for _, kind := range []jp.Kind{jp.Kind_CONFIGURATION_LIMITS, jp.Kind_CONFIGURATION_USAGE} {
		for _, actor := range []string{"alpha/callback", "alpha/producer"} {
			_, err := c.Get(genericContext(actor), &jp.GetRequest{Reference: &jp.Reference{Kind: kind}})
			require.Equal(t, codes.PermissionDenied, status.Code(err))
		}
		_, err := c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: kind, TenantId: "beta"}})
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		_, err = c.Get(genericContext("ops/alice"), &jp.GetRequest{Reference: &jp.Reference{Kind: kind}})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		_, err = c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: kind, Id: "unrelated"}})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		_, err = c.List(genericContext("alpha/reader"), &jp.ListRequest{Kind: kind})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	definitionAPI(t, c, "alpha", "first", "sha256", "", nil)
	denied := &jp.MutationRequest{Command: genericMeta("configuration-full"), Operation: &jp.MutationRequest_PutDefinition{PutDefinition: &jp.PutDefinition{Definition: &jp.Definition{Id: "second", Runtime: "steps/v1", Handler: "sha256", HandlerVersion: "1"}}}}
	_, err := c.Mutate(genericContext("alpha/writer"), denied)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	definitionAPI(t, c, "beta", "first", "sha256", "", nil)
	actual := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_CONFIGURATION_USAGE, Id: "alpha"}).GetConfigurationUsage()
	require.Equal(t, uint64(1), actual.Definitions)
	stale := configurationLimitsAPI("alpha", 0, &jp.ConfigurationLimits{MaxDefinitions: 2})
	stale.Command = genericMeta("stale-configuration-policy")
	_, err = c.Mutate(genericContext("ops/alice"), stale)
	require.Equal(t, codes.Aborted, status.Code(err))
	mutateJob(t, c, "ops/alice", configurationLimitsAPI("alpha", 1, &jp.ConfigurationLimits{MaxDefinitions: 2}))
	_, err = c.Mutate(genericContext("alpha/writer"), denied)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	denied.Command = genericMeta("configuration-fresh-envelope")
	mutateJob(t, c, "alpha/writer", denied)
	malformed := configurationLimitsAPI("alpha", 2, nil)
	_, err = c.Mutate(genericContext("ops/alice"), malformed)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestJobConfigurationLimitsHTTPCountsPinnedPayloadsAndPermitsPausedTriggers(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	s := startAPI(t, cfg)
	c := genericClient(t, s)
	tenantAPI(t, c, "alpha")
	mutateJob(t, c, "ops/alice", &jp.MutationRequest{Command: genericMeta("binding"), TenantId: "alpha", Operation: &jp.MutationRequest_PutBinding{PutBinding: &jp.PutBinding{Binding: &jp.Binding{Id: "driver", Driver: "test", DriverVersion: "1", Config: []byte("xy")}}}})
	definitionAPI(t, c, "alpha", "hash", "sha256-event", "driver", []byte("abcd"))
	mutateJob(t, c, "ops/alice", configurationLimitsAPI("alpha", 0, &jp.ConfigurationLimits{MaxPayloadBytes: 11}))
	trigger := eventTriggerAPI("ingest", "hash", 2)
	code, _ := genericHTTP(t, s, "/api/jobs/v1/mutate", genericToken("alpha/writer"), trigger, nil)
	require.Equal(t, http.StatusTooManyRequests, code)
	usage := getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_CONFIGURATION_USAGE}).GetConfigurationUsage()
	require.Equal(t, uint64(6), usage.PayloadBytes)
	require.Zero(t, usage.Triggers)
	mutateJob(t, c, "ops/alice", configurationLimitsAPI("alpha", 1, &jp.ConfigurationLimits{MaxPayloadBytes: 12}))
	trigger.Command = genericMeta("trigger-with-capacity")
	mutateJob(t, c, "alpha/writer", trigger)
	code, body := genericHTTP(t, s, "/api/jobs/v1/get", genericToken("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_CONFIGURATION_USAGE}}, nil)
	require.Equal(t, http.StatusOK, code, string(body))
	var record jp.Record
	require.NoError(t, protojson.Unmarshal(body, &record))
	require.Equal(t, uint64(12), record.GetConfigurationUsage().PayloadBytes)
	mutateJob(t, c, "ops/alice", configurationLimitsAPI("alpha", 2, &jp.ConfigurationLimits{MaxPayloadBytes: 1}))
	mutateJob(t, c, "alpha/writer", &jp.MutationRequest{Command: genericMeta("pause"), Operation: &jp.MutationRequest_PauseTrigger{PauseTrigger: &jp.PauseTrigger{Id: "ingest", ExpectedRevision: 1, Paused: true}}})
	require.Equal(t, uint64(12), getGeneric(t, c, "alpha/reader", &jp.Reference{Kind: jp.Kind_CONFIGURATION_USAGE}).GetConfigurationUsage().PayloadBytes)
}
