package e2e_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

type gatewayControl struct {
	h         *harness
	url       string
	container *testcontainers.DockerContainer
}

func (h *harness) gateway(rtu bool) (string, gatewayControl) {
	h.t.Helper()
	container, alias := h.bootstrap, "bootstrap-fixture"
	if rtu {
		alias = "rtu-gateway"
		var err error
		container, err = testcontainers.Run(h.ctx, h.image, testcontainers.WithEntrypoint("/gateway"), testcontainers.WithCmd("--rtu"), testcontainers.WithExposedPorts("8080/tcp"), network.WithNetwork([]string{alias}, h.network), testcontainers.WithWaitStrategy(wait.ForListeningPort("8080/tcp").SkipInternalCheck()))
		h.own(container, alias)
		if err != nil {
			h.t.Fatal(err)
		}
	}
	url, err := container.PortEndpoint(h.ctx, "8080/tcp", "http")
	if err != nil {
		h.t.Fatal(err)
	}
	endpoint := "tcp://" + alias + ":1502?turnaround=5ms"
	if rtu {
		endpoint += "&baud=9600"
	}
	return endpoint, gatewayControl{h: h, url: url, container: container}
}

func (g gatewayControl) call(method, path string) object {
	g.h.t.Helper()
	req, err := http.NewRequestWithContext(g.h.ctx, method, g.url+path, nil)
	if err != nil {
		g.h.t.Fatal(err)
	}
	response, err := g.h.client.Do(req)
	if err != nil {
		g.h.t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 && response.StatusCode != 204 {
		g.h.t.Fatalf("gateway control status %d", response.StatusCode)
	}
	var result object
	if response.StatusCode == 200 {
		if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result); err != nil {
			g.h.t.Fatal(err)
		}
	}
	return result
}

func (h *harness) configure(bus, protocol, endpoint, driver, operation string) {
	h.t.Helper()
	h.request(0, "PUT", "/api/v1/channels/"+bus, "platform", object{"command": h.meta(), "gateway_id": bus, "protocol": protocol, "endpoint": endpoint}, 200)
	h.mutate(0, "platform", "", "put_resource", object{"spec": object{"id": bus, "channel_id": bus, "duplex": "half", "capacity": 1}})
	for i, tenant := range []string{"alpha", "beta"} {
		h.mutate(i, "platform", tenant, "put_grant", object{"resource_id": bus, "allowed": true})
		op := object{"version": 1, "unit_id": 3 + 4*i, "function": "read_holding_registers", "address": 20, "quantity": 1}
		config := object{"channel_id": bus, "operations": []object{{"name": operation, "operation": op}, {"name": "request", "operation": op}}}
		if h.remote && driver == "acme.meter" {
			config["protocol"] = protocol
		}
		h.mutate(i, "platform", tenant, "put_binding", object{"binding": object{"id": bus, "driver": driver, "driver_version": "1", "resources": []string{bus}, "config": encode(config)}})
	}
}

func (h *harness) channel(i int, bus, state string) {
	h.t.Helper()
	poll(h.t, bus+" channel state "+state, func() (bool, error) {
		code, result, err := h.call(i, "GET", "/api/v1/channels/"+bus, "platform", nil)
		if err != nil || code != 200 {
			return false, fmt.Errorf("channel read: status %d, error %v", code, err)
		}
		channel, _ := result["channel"].(map[string]any)
		if channel["state"] == state {
			return true, nil
		}
		return false, fmt.Errorf("observed %v", channel["state"])
	})
}

func (h *harness) leader() int {
	h.t.Helper()
	leader := -1
	poll(h.t, "one elected leader", func() (bool, error) {
		count := 0
		for i := range h.members {
			status, result, err := h.call(i, "GET", "/healthz", "", nil)
			if err == nil && status == 200 && result["leader"] == true {
				count++
				leader = i
			}
		}
		return count == 1, nil
	})
	return leader
}

func TestContainerClusterGatewayRecovery(t *testing.T) {
	h := newHarness(t, false)
	h.request(0, "POST", "/api/jobs/v1/get", "", object{"reference": object{"kind": "RUN", "id": "private"}}, 401)
	for i, tenant := range []string{"alpha", "beta"} {
		h.mutate(i, "platform", tenant, "put_tenant", object{"max_running": "4"})
	}
	h.request(0, "POST", "/api/jobs/v1/get", "alpha-reader", object{"reference": object{"kind": "RUN", "tenant_id": "beta", "id": "private"}}, 403)
	var gateways []gatewayControl
	for _, rtu := range []bool{false, true} {
		bus, protocol := "tcp-bus", "modbus-tcp"
		if rtu {
			bus, protocol = "rtu-bus", "modbus-rtu-tcp"
		}
		endpoint, gateway := h.gateway(rtu)
		gateways = append(gateways, gateway)
		h.configure(bus, protocol, endpoint, protocol, "execute")
		h.definition("alpha", bus+"-conversation", "request-reply", bus, object{"correlation_key": "reply", "timeout_ns": 120000000000})
		h.launch(0, "alpha", bus+"-conversation", bus+"-session")
		h.run(0, "alpha", bus+"-session", "waiting")
		for i, tenant := range []string{"alpha", "beta"} {
			h.definition(tenant, bus, "invoke", bus, nil)
			h.launch(i+1, tenant, bus, bus+"-read")
			h.run(0, tenant, bus+"-read", "ready")
		}
		// The canonical gateway also excludes the legacy execution profile.
		h.request(2, "PUT", "/api/v1/runs/"+bus+"-legacy", "platform", object{"command": h.meta(), "channel_id": bus, "executor": "modbus", "payload": encode(object{"version": 1, "unit_id": 9, "function": "read_holding_registers", "address": 20, "quantity": 1})}, 200)
		time.Sleep(150 * time.Millisecond)
		if stats := gateway.call("GET", "/stats"); stats["requests"] != float64(1) {
			t.Fatalf("a contender bypassed the held gateway: %v", stats)
		}
		h.mutate(1, "alpha-callback", "", "signal", object{"run_id": bus + "-session", "event_id": "reply", "key": "reply"})
		h.run(0, "alpha", bus+"-session", "succeeded")
		for _, tenant := range []string{"alpha", "beta"} {
			h.run(0, tenant, bus+"-read", "succeeded")
		}
		h.channel(0, bus, "available")
		poll(t, "all serialized gateway reads", func() (bool, error) {
			stats := gateway.call("GET", "/stats")
			return stats["requests"] == float64(4) && stats["active"] == float64(0), nil
		})
		if stats := gateway.call("GET", "/stats"); stats["maximum"] != float64(1) {
			t.Fatalf("overlapping gateway I/O: %v", stats)
		}
		gateway.call("POST", "/corrupt-next")
		h.launch(1, "alpha", bus, bus+"-uncertain")
		h.run(0, "alpha", bus+"-uncertain", "unknown")
		h.channel(0, bus, "blocked")
		h.launch(2, "beta", bus, bus+"-blocked")
		h.run(0, "beta", bus+"-blocked", "ready")
	}
	// Acknowledgement identity and ordinary work survive an actual SIGKILL of
	// the elected leader. No uncertain gateway is unblocked during this test.
	original := object{"command": h.meta(), "put_definition": object{"definition": object{"id": "hash", "runtime": "steps/v1", "handler": "sha256", "handler_version": "1", "input": encode("durable")}}}
	expected := h.request(0, "POST", "/api/jobs/v1/mutate", "alpha-writer", original, 200)
	leader := h.leader()
	zero := time.Duration(0)
	if err := h.members[leader].Stop(h.ctx, &zero); err != nil {
		t.Fatal(err)
	}
	survivor := (leader + 1) % 3
	h.ready(survivor)
	if retry := h.request(survivor, "POST", "/api/jobs/v1/mutate", "alpha-writer", original, 200); !reflect.DeepEqual(retry, expected) {
		t.Fatal("leader loss changed the exact retry acknowledgement")
	}
	if err := h.members[leader].Start(h.ctx); err != nil {
		t.Fatal(err)
	}
	h.refreshURL(leader)
	h.ready(leader)
	for _, member := range h.members {
		if err := member.Stop(h.ctx, &zero); err != nil {
			t.Fatal(err)
		}
	}
	h.startAll()
	for i := range h.members {
		h.ready(i)
		for _, bus := range []string{"tcp-bus", "rtu-bus"} {
			h.channel(i, bus, "blocked")
			h.run(i, "alpha", bus+"-uncertain", "unknown")
			h.run(i, "beta", bus+"-blocked", "ready")
		}
	}
	h.launch(0, "alpha", "hash", "after-restart")
	h.run(0, "alpha", "after-restart", "succeeded")
	time.Sleep(200 * time.Millisecond)
	for _, gateway := range gateways {
		if stats := gateway.call("GET", "/stats"); stats["requests"] != float64(5) || stats["maximum"] != float64(1) {
			t.Fatalf("blocked resource admitted more gateway I/O after restart: %v", stats)
		}
	}
}

func TestCompanyNodeExtension(t *testing.T) {
	h := newHarness(t, true)
	for _, tenant := range []string{"alpha", "beta"} {
		h.mutate(0, "platform", tenant, "put_tenant", object{"max_running": "2"})
	}
	endpoint, gateway := h.gateway(false)
	h.configure("meter", "modbus-tcp", endpoint, "acme.meter", "measure")
	for i, tc := range []struct {
		maximum int
		status  string
	}{{100, "succeeded"}, {10, "failed"}} {
		id := fmt.Sprintf("company-%d", i)
		h.definition("alpha", id, "acme.check-measurement", "meter", object{"maximum": tc.maximum})
		h.launch(i, "alpha", id, id)
		run := h.run(0, "alpha", id, tc.status)
		data, err := base64.StdEncoding.DecodeString(run["result"].(string))
		if err != nil || !bytes.Equal(data, []byte(`{"value":42}`)) {
			t.Fatalf("company result: %s, %v", data, err)
		}
		h.channel(0, "meter", "available")
	}
	if stats := gateway.call("GET", "/stats"); stats["requests"] != float64(2) || stats["maximum"] != float64(1) {
		t.Fatalf("custom worker duplicated or overlapped an action: %v", stats)
	}
}
