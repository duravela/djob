package e2e_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	dockernetwork "github.com/moby/moby/api/types/network"
	dockerclient "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

func (h *harness) startWorker(tenant, id, binary string, member int, args ...string) *testcontainers.DockerContainer {
	return h.startWorkerControllers(tenant, id, binary, []int{member}, args...)
}

func (h *harness) startWorkerControllers(tenant, id, binary string, members []int, args ...string) *testcontainers.DockerContainer {
	h.t.Helper()
	settings := object{"server_url": fmt.Sprintf("https://member-%d:8080", members[0]+1), "worker_id": id, "tenant_id": tenant, "token": h.tokens[tenant+"-worker"], "ca_file": "/api-ca.pem"}
	var fallbacks []string
	for _, member := range members[1:] {
		fallbacks = append(fallbacks, fmt.Sprintf("https://member-%d:8080", member+1))
	}
	if len(fallbacks) > 0 {
		settings["fallback_urls"] = fallbacks
	}
	return h.startWorkerConfig(id, binary, settings, args...)
}

func (h *harness) startWorkerConfig(id, binary string, settings object, args ...string) *testcontainers.DockerContainer {
	h.t.Helper()
	config, _ := json.Marshal(settings)
	files := []testcontainers.ContainerFile{
		{Reader: bytes.NewReader(config), ContainerFilePath: "/worker.json", FileMode: 0600},
		{Reader: bytes.NewReader(h.apiCA), ContainerFilePath: "/api-ca.pem", FileMode: 0600},
		{Reader: bytes.NewReader(h.apiCA), ContainerFilePath: "/ca.pem", FileMode: 0600},
	}
	for path, data := range h.mqttFiles {
		files = append(files, testcontainers.ContainerFile{Reader: bytes.NewReader(data), ContainerFilePath: path, FileMode: 0600})
	}
	args = append([]string{"--worker-config", "/worker.json"}, args...)
	w, err := testcontainers.Run(h.ctx, h.image, testcontainers.WithEntrypoint(binary), testcontainers.WithCmd(args...), testcontainers.WithFiles(files...), network.WithNetwork([]string{id}, h.network))
	h.own(w, id)
	if err != nil {
		h.t.Fatal(err)
	}
	return w
}

func TestWorkerReconnectsAcrossControllerLossWithoutReplayingGatewayIO(t *testing.T) {
	testWorkerControllerFailover(t, false)
}

func TestWorkerRelocatesFromReachableControllerWithoutQuorum(t *testing.T) {
	testWorkerControllerFailover(t, true)
}

func testWorkerControllerFailover(t *testing.T, partition bool) {
	h := newHarness(t, false, func(h *harness) {
		h.remote = true
		if partition {
			h.raftNetwork = newRaftNetwork(h)
		}
	})
	for _, tenant := range []string{"alpha", "beta"} {
		h.mutate(0, "platform", tenant, "put_tenant", object{"max_running": "4"})
	}
	oldEndpoint, oldGateway := h.gateway(false)
	newEndpoint, newGateway := h.gateway(true)
	h.configure("old-bus", "modbus-tcp", oldEndpoint, "acme.meter", "measure")
	h.configure("other-bus", "modbus-rtu-tcp", newEndpoint, "acme.meter", "measure")
	h.definition("alpha", "old", "acme.measure-twice", "old-bus", object{"maximum": 100})
	h.definition("alpha", "other", "acme.measure-twice", "other-bus", object{"maximum": 100})
	h.definition("alpha", "compute", "sha256", "", object{"independent": true})
	h.definition("beta", "old", "acme.check-measurement", "old-bus", object{"maximum": 100})
	attached := h.leader()
	survivor, other := (attached+1)%3, (attached+2)%3
	worker := h.startWorkerControllers("alpha", "reconnecting-alpha", "/company-worker", []int{attached, survivor, other}, "--reconnect-min-backoff", "100ms", "--reconnect-max-backoff", "1s")
	var idle *testcontainers.DockerContainer
	if partition {
		idle = h.startWorkerControllers("alpha", "idle-alpha", "/djob-worker", []int{attached, survivor, other}, "--reconnect-min-backoff", "100ms", "--reconnect-max-backoff", "1s")
		poll(t, "both active and idle workers attach", func() (bool, error) { value, err := h.metric(attached, "djob_worker_sessions"); return value == 2, err })
		poll(t, "both workers begin polling", func() (bool, error) {
			value, err := h.metric(attached, `djob_worker_sessions_by_state{state="connected"}`)
			return value == 2, err
		})
		for name, want := range map[string]float64{"djob_worker_session_slot_limit": 16, "djob_worker_tenant_session_slot_limit": 4} {
			value, err := h.metric(attached, name)
			if err != nil || value != want {
				t.Fatalf("configured worker capacity %s = %v, want %v: %v", name, value, want, err)
			}
		}
	}
	h.startWorker("beta", "contending-beta", "/company-worker", survivor)
	h.launch(attached, "alpha", "old", "old-conversation")
	waiting := h.run(attached, "alpha", "old-conversation", "waiting")
	oldOwner, _ := waiting["owner_id"].(string)
	if !strings.Contains(oldOwner, fmt.Sprintf("remote-worker/%d/reconnecting-alpha/", attached+1)) {
		t.Fatalf("worker did not attach to the configured member: %v", waiting)
	}
	h.launch(survivor, "beta", "old", "blocked-contender")
	h.run(survivor, "beta", "blocked-contender", "ready")
	var restore func()
	if partition {
		// Only Raft traffic is removed. The API, worker heartbeats, gateway
		// connections and all processes remain alive on their original network.
		restore = h.partitionRaft(attached)
		h.request(attached, "GET", "/healthz", "", nil, 200)
		h.request(attached, "GET", "/readyz", "", nil, 503)
		poll(t, "isolated controller joins worker sessions and releases their capacity", func() (bool, error) {
			names := []string{"djob_worker_sessions", "djob_worker_session_slots", "djob_worker_sessions_closing"}
			for _, state := range []string{"connected", "registered", "draining", "relocating", "stale", "unavailable"} {
				names = append(names, `djob_worker_sessions_by_state{state="`+state+`"}`)
			}
			for _, name := range names {
				value, err := h.metric(attached, name)
				if err != nil || value != 0 {
					return false, fmt.Errorf("%s = %v: %v", name, value, err)
				}
			}
			return true, nil
		})
	} else {
		// No external supervisor restarts the worker. The SDK opens a fresh session.
		zero := time.Duration(0)
		if err := h.members[attached].Stop(h.ctx, &zero); err != nil {
			t.Fatal(err)
		}
	}
	h.ready(survivor)
	h.launch(survivor, "alpha", "other", "new-conversation")
	fresh := h.run(survivor, "alpha", "new-conversation", "waiting")
	newOwner, _ := fresh["owner_id"].(string)
	if newOwner == oldOwner || strings.Contains(newOwner, fmt.Sprintf("remote-worker/%d/", attached+1)) || !strings.Contains(newOwner, "/reconnecting-alpha/") {
		t.Fatalf("old allocation was resumed: old=%q new=%q", oldOwner, newOwner)
	}
	if partition {
		// The company worker is occupied by the new conversation, so independent
		// computation also proves the previously idle worker relocated and runs.
		h.launch(survivor, "alpha", "compute", "idle-after-failover")
		completed := h.run(survivor, "alpha", "idle-after-failover", "succeeded")
		history := h.request(survivor, "POST", "/api/jobs/v1/get", "alpha-reader", object{"reference": object{"kind": "ATTEMPT", "run_id": "idle-after-failover", "id": completed["last_attempt_id"]}}, 200)
		attempt, _ := history["attempt"].(map[string]any)
		owner, _ := attempt["owner_id"].(string)
		if !strings.Contains(owner, "/idle-alpha/") || strings.Contains(owner, fmt.Sprintf("remote-worker/%d/", attached+1)) {
			t.Fatalf("idle worker did not execute through a healthy controller: %v", history)
		}
	}
	h.run(survivor, "alpha", "old-conversation", "unknown")
	h.channel(survivor, "old-bus", "blocked")
	action := h.request(survivor, "POST", "/api/jobs/v1/get", "alpha-reader", object{"reference": object{"kind": "ACTION", "run_id": "old-conversation", "sequence": "1"}}, 200)
	if effect, _ := action["action"].(map[string]any); effect["status"] != "succeeded" {
		t.Fatalf("confirmed effect history changed during reconnect: %v", action)
	}
	h.mutate(survivor, "alpha-callback", "", "signal", object{"run_id": "new-conversation", "event_id": "continue", "key": "continue"})
	h.run(survivor, "alpha", "new-conversation", "succeeded")
	h.channel(survivor, "other-bus", "available")
	h.launch(survivor, "alpha", "compute", "after-failover")
	h.run(survivor, "alpha", "after-failover", "succeeded")
	info, err := worker.Inspect(h.ctx)
	if err != nil || !info.State.Running {
		t.Fatalf("worker process did not survive controller loss: %v", err)
	}
	if partition {
		info, err := idle.Inspect(h.ctx)
		if err != nil || !info.State.Running {
			t.Fatalf("idle worker did not survive quorum loss: %v", err)
		}
		h.request(attached, "GET", "/healthz", "", nil, 200)
		restore()
	} else {
		if err := h.members[attached].Start(h.ctx); err != nil {
			t.Fatal(err)
		}
		h.refreshURL(attached)
	}
	h.ready(attached)
	if partition {
		// Recovery must reopen worker admission on the same running controller.
		h.startWorker("alpha", "healed-alpha", "/djob-worker", attached)
		poll(t, "healed controller accepts a fresh worker slot", func() (bool, error) {
			value, err := h.metric(attached, "djob_worker_session_slots")
			if err != nil || value != 1 {
				return false, err
			}
			value, err = h.metric(attached, `djob_worker_sessions_by_state{state="connected"}`)
			return value == 1, err
		})
	}
	for i := range h.members {
		h.channel(i, "old-bus", "blocked")
		h.run(i, "beta", "blocked-contender", "ready")
	}
	if stats := oldGateway.call("GET", "/stats"); stats["requests"] != float64(1) || stats["maximum"] != float64(1) {
		t.Fatalf("reconnection replayed a physical operation or bypassed the bus block: %v", stats)
	}
	if stats := newGateway.call("GET", "/stats"); stats["requests"] != float64(2) || stats["maximum"] != float64(1) {
		t.Fatalf("fresh conversation duplicated or overlapped I/O: %v", stats)
	}
}

func (h *harness) metric(member int, name string) (float64, error) {
	req, err := http.NewRequestWithContext(h.ctx, http.MethodGet, h.urls[member]+"/metrics", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+h.tokens["monitor"])
	response, err := h.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return 0, err
	}
	if response.StatusCode != 200 {
		return 0, fmt.Errorf("metrics status %d", response.StatusCode)
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == name {
			return strconv.ParseFloat(fields[1], 64)
		}
	}
	return 0, fmt.Errorf("missing metric %s", name)
}

func (h *harness) partitionRaft(member int) func() {
	h.t.Helper()
	client, err := testcontainers.NewDockerClientWithOpts(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = client.Close() })
	container := h.members[member]
	info, err := container.Inspect(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	endpoint := info.NetworkSettings.Networks[h.raftNetwork.Name]
	if endpoint == nil {
		h.t.Fatal("member is not attached to the separate Raft network")
	}
	if _, err := client.NetworkDisconnect(h.ctx, h.raftNetwork.ID, dockerclient.NetworkDisconnectOptions{Container: container.GetContainerID(), Force: true}); err != nil {
		h.t.Fatal(err)
	}
	h.refreshURL(member)
	var once sync.Once
	restore := func() {
		once.Do(func() {
			// Keep peer DNS and the address of existing Raft transports stable.
			settings := &dockernetwork.EndpointSettings{Aliases: endpoint.Aliases, IPAMConfig: &dockernetwork.EndpointIPAMConfig{IPv4Address: endpoint.IPAddress}}
			if _, err := client.NetworkConnect(h.ctx, h.raftNetwork.ID, dockerclient.NetworkConnectOptions{Container: container.GetContainerID(), EndpointConfig: settings}); err != nil {
				h.t.Errorf("restore Raft network: %v", err)
			}
			h.refreshURL(member)
		})
	}
	h.t.Cleanup(restore)
	return restore
}

func TestSeparateCompanyWorkersAndGatewayLoss(t *testing.T) {
	h := newHarness(t, false, func(h *harness) { h.remote = true })
	for _, tenant := range []string{"alpha", "beta"} {
		h.mutate(0, "platform", tenant, "put_tenant", object{"max_running": "4"})
	}
	alpha := h.startWorker("alpha", "company-alpha-1", "/company-worker", 0)
	beta := h.startWorker("beta", "company-beta-1", "/company-worker", 1)
	gateways := make(map[string]gatewayControl)
	for _, rtu := range []bool{false, true} {
		bus, protocol := "tcp-bus", "modbus-tcp"
		if rtu {
			bus, protocol = "rtu-bus", "modbus-rtu-tcp"
		}
		endpoint, gateway := h.gateway(rtu)
		gateways[bus] = gateway
		h.configure(bus, protocol, endpoint, "acme.meter", "measure")
		h.definition("alpha", bus, "acme.measure-twice", bus, object{"maximum": 100})
		h.definition("beta", bus, "acme.check-measurement", bus, object{"maximum": 100})
		h.launch(0, "alpha", bus, bus+"-conversation")
		waiting := h.run(0, "alpha", bus+"-conversation", "waiting")
		if owner, _ := waiting["owner_id"].(string); !bytes.Contains([]byte(owner), []byte("remote-worker/1/company-alpha-1/")) {
			t.Fatalf("missing worker identity in claim: %v", waiting)
		}
		h.launch(1, "beta", bus, bus+"-contender")
		h.run(0, "beta", bus+"-contender", "ready")
		time.Sleep(150 * time.Millisecond)
		if stats := gateway.call("GET", "/stats"); stats["requests"] != float64(1) {
			t.Fatalf("another tenant used a conversation's reserved bus: %v", stats)
		}
		h.mutate(2, "alpha-callback", "", "signal", object{"run_id": bus + "-conversation", "event_id": "continue", "key": "continue"})
		result := h.run(0, "alpha", bus+"-conversation", "succeeded")
		data, err := base64.StdEncoding.DecodeString(result["result"].(string))
		if err != nil || string(data) != `{"value":42}` {
			t.Fatalf("company result: %s, %v", data, err)
		}
		h.run(0, "beta", bus+"-contender", "succeeded")
		h.channel(0, bus, "available")
		if stats := gateway.call("GET", "/stats"); stats["requests"] != float64(3) || stats["maximum"] != float64(1) {
			t.Fatalf("duplicated or overlapping gateway exchanges: %v", stats)
		}
	}
	// A confirmed first exchange does not reconstruct a lost physical session.
	h.launch(0, "alpha", "rtu-bus", "lost-wait")
	h.run(0, "alpha", "lost-wait", "waiting")
	zero := time.Duration(0)
	if err := alpha.Stop(h.ctx, &zero); err != nil {
		t.Fatal(err)
	}
	h.run(0, "alpha", "lost-wait", "unknown")
	h.channel(0, "rtu-bus", "blocked")
	alpha = h.startWorker("alpha", "company-alpha-2", "/company-worker", 2)
	h.launch(1, "beta", "rtu-bus", "blocked-rtu")
	h.run(0, "beta", "blocked-rtu", "ready")
	// A killed worker during I/O also cannot turn an unconfirmed effect into a
	// replay. The independent fixture confirms the request reached the wire.
	gateways["tcp-bus"].call("POST", "/hold-next")
	h.launch(0, "alpha", "tcp-bus", "lost-io")
	poll(t, "remote worker begins gateway I/O", func() (bool, error) {
		stats := gateways["tcp-bus"].call("GET", "/stats")
		return stats["requests"] == float64(4) && stats["active"] == float64(1), nil
	})
	if err := alpha.Stop(h.ctx, &zero); err != nil {
		t.Fatal(err)
	}
	h.run(0, "alpha", "lost-io", "unknown")
	h.channel(0, "tcp-bus", "blocked")
	h.launch(1, "beta", "tcp-bus", "blocked-tcp")
	h.run(0, "beta", "blocked-tcp", "ready")
	// Persist both blocks through an actual full controller restart. Workers
	// establish fresh sessions; no old session or external effect is resumed.
	if err := beta.Stop(h.ctx, &zero); err != nil {
		t.Fatal(err)
	}
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
		}
	}
	h.startWorker("alpha", "company-alpha-3", "/company-worker", 0)
	h.startWorker("beta", "company-beta-2", "/company-worker", 1)
	h.definition("alpha", "compute", "sha256", "", object{"unrelated": true})
	h.launch(0, "alpha", "compute", "after-restart")
	h.run(0, "alpha", "after-restart", "succeeded")
	for bus, gateway := range gateways {
		if stats := gateway.call("GET", "/stats"); stats["requests"] != float64(4) || stats["maximum"] != float64(1) {
			t.Fatalf("%s: blocked work was replayed or overlapped: %v", bus, stats)
		}
		h.run(0, "beta", "blocked-"+map[string]string{"tcp-bus": "tcp", "rtu-bus": "rtu"}[bus], "ready")
	}
}
