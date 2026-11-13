package server

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor/modbus"
	"github.com/duravela/djob/pkg/executor/modbusrtutcp"
	"github.com/duravela/djob/pkg/jobs"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	old "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/stretchr/testify/require"
)

func TestRTUConversationReservesWholeGatewayAcrossWaitAndTenantHandover(t *testing.T) {
	gateway := startModbusSimulatorFraming(t, "127.0.0.1:0", true)
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.TransportFactory = modbusrtutcp.Factory
	cfg.Dispatcher.SharedIdleTimeout = 20 * time.Millisecond
	cfg.JobDispatch.PollInterval = 5 * time.Millisecond
	cfg.JobDispatch.MaxConcurrent, cfg.JobDispatch.MaxPerTenant = 4, 2
	cfg.JobRegistry = func(backend JobBackend) (jobs.Registry, error) {
		r := jobs.Builtins()
		r.Drivers[jobs.Version{Name: modbusrtutcp.Protocol, Version: "1"}] = jobs.GatewayFactory(backend, modbusrtutcp.Factory,
			func(binding db.JobBinding, action jobs.Action) (string, []byte, error) {
				var target struct {
					ChannelID string `json:"channel_id"`
					UnitID    int    `json:"unit_id"`
				}
				if err := json.Unmarshal(binding.Config, &target); err != nil {
					return "", nil, err
				}
				if (action.Operation != "request" && action.Operation != "execute") || len(action.Input) != 0 {
					return "", nil, fmt.Errorf("unsupported semantic operation")
				}
				return modbus.ExecutorName, []byte(fmt.Sprintf(`{"version":1,"unit_id":%d,"function":"read_holding_registers","address":0,"quantity":1}`, target.UnitID)), nil
			}, time.Second)
		return r, nil
	}
	s := startAPI(t, cfg)
	c, legacy := genericClient(t, s), apiClient(t, s)
	operator := genericContext("ops/alice")
	_, err := legacy.ConfigureChannel(operator, &old.ConfigureChannelRequest{Command: meta("gateway"), Id: "gateway", GatewayId: "gateway", Protocol: modbusrtutcp.Protocol, Endpoint: gateway.endpoint(10 * time.Millisecond)})
	require.NoError(t, err)
	mutateJob(t, c, "ops/alice", &jp.MutationRequest{Command: genericMeta("resource"), Operation: &jp.MutationRequest_PutResource{PutResource: &jp.PutResource{Spec: &jp.ResourceSpec{Id: "gateway", ChannelId: "gateway", Duplex: "half", Capacity: 1}}}})
	for i, tenant := range []string{"alpha", "beta"} {
		tenantAPI(t, c, tenant)
		mutateJob(t, c, "ops/alice", &jp.MutationRequest{Command: genericMeta("grant"), TenantId: tenant, Operation: &jp.MutationRequest_PutGrant{PutGrant: &jp.PutGrant{ResourceId: "gateway", Allowed: true}}})
		mutateJob(t, c, "ops/alice", &jp.MutationRequest{Command: genericMeta("binding"), TenantId: tenant, Operation: &jp.MutationRequest_PutBinding{PutBinding: &jp.PutBinding{Binding: &jp.Binding{Id: "device", Driver: modbusrtutcp.Protocol, DriverVersion: "1", Config: []byte(fmt.Sprintf(`{"channel_id":"gateway","unit_id":%d}`, 3+4*i)), Resources: []string{"gateway"}}}}})
	}
	definitionAPI(t, c, "alpha", "conversation", "request-reply", "device", []byte(`{"correlation_key":"continue"}`))
	launchAPI(t, c, "alpha", "conversation", "alpha-run")
	first := gateway.next(t)
	require.Equal(t, byte(3), first.unit())
	first.reply(t, []byte{3, 2, 0, 42})
	require.Eventually(t, func() bool {
		r, err := s.application.Store.GetJobView("alpha", "alpha-run")
		return err == nil && r.Run.Status == db.JobWaiting
	}, 4*time.Second, time.Millisecond)
	resource, err := s.application.Store.GetJobResource("gateway")
	require.NoError(t, err)
	require.Equal(t, db.ResourceOwned, resource.State)
	require.Equal(t, "alpha-run", resource.RunID)
	definitionAPI(t, c, "beta", "invoke", "invoke", "device", nil)
	launchAPI(t, c, "beta", "invoke", "beta-run")
	_, err = legacy.CreateRun(operator, modbusRun("legacy", "gateway", 9, false))
	require.NoError(t, err)
	definitionAPI(t, c, "beta", "compute", "sha256", "", []byte("independent"))
	launchAPI(t, c, "beta", "compute", "compute")
	require.Eventually(t, func() bool {
		r, err := s.application.Store.GetJobView("beta", "compute")
		return err == nil && r.Run.Status == db.JobSucceeded
	}, 4*time.Second, time.Millisecond)
	select {
	case <-gateway.calls:
		t.Fatal("a tenant or legacy run bypassed the waiting conversation's gateway reservation")
	default:
	}
	mutateJob(t, c, "alpha/callback", &jp.MutationRequest{Command: genericMeta("continue"), Operation: &jp.MutationRequest_Signal{Signal: &jp.Signal{RunId: "alpha-run", EventId: "continue", Key: "continue", Data: []byte("application reply")}}})
	seen := make(map[byte]bool)
	for range 2 {
		call := gateway.next(t)
		seen[call.unit()] = true
		if call.unit() == 9 {
			call.reply(t, []byte{3, 4, 0, 1, 0, 2})
		} else {
			call.reply(t, []byte{3, 2, 0, 42})
		}
	}
	require.True(t, seen[7], "the other tenant must eventually acquire the same physical bus")
	require.True(t, seen[9], "the legacy scheduler must eventually acquire the same physical bus")
	require.Eventually(t, func() bool {
		a, errA := s.application.Store.GetJobView("alpha", "alpha-run")
		b, errB := s.application.Store.GetJobView("beta", "beta-run")
		old, errOld := s.application.Store.GetRun("legacy")
		return errA == nil && errB == nil && errOld == nil && a.Run.Status == db.JobSucceeded && b.Run.Status == db.JobSucceeded && old.State == db.RunSucceeded
	}, 4*time.Second, time.Millisecond)
	run, err := s.application.Store.GetJobView("alpha", "alpha-run")
	require.NoError(t, err)
	require.Equal(t, []byte("application reply"), run.Run.Result)
	require.Equal(t, []byte(`{"correlation_key":"continue"}`), run.Run.Definition.Input, "framing and unit IDs stay in the binding")
}
