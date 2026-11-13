package cli_test

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCLIClusterFaultRecovery(t *testing.T) {
	c := newCluster(t)
	t.Log("three real CLI processes: authenticated APIs and mutually authenticated Raft")
	for i := range c.members {
		c.metrics(i, "", 401)
		c.metrics(i, "alpha-reader", 403)
		metrics := c.metrics(i, "platform", 200)
		for _, name := range []string{"djob_admission_enabled", "djob_admission_monitor_running", `djob_admission_state{state="healthy"}`, "djob_admission_peer_clock_fresh"} {
			if metrics[name] != 1 {
				t.Fatalf("member %d missing healthy admission metric %s: %v", i+1, name, metrics[name])
			}
		}
	}
	certificate, err := tls.LoadX509KeyPair(c.tls[0].CertFile, c.tls[0].KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: c.ca.Roots, Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13}}
	defer transport.CloseIdleConnections()
	peerClient := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	response, err := peerClient.Get(c.peers[1] + "/raft/probing")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 200 || response.TLS.Version != tls.VersionTLS13 {
		t.Fatalf("peer TLS probe: status %d, version %d", response.StatusCode, response.TLS.Version)
	}
	untrusted := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: c.ca.Roots, MinVersion: tls.VersionTLS13}}
	defer untrusted.CloseIdleConnections()
	if response, err := (&http.Client{Transport: untrusted, Timeout: 2 * time.Second}).Get(c.peers[1] + "/raft/probing"); err == nil {
		_ = response.Body.Close()
		t.Fatal("Raft accepted a client without a member certificate")
	}
	for i, tenant := range []string{"alpha", "beta"} {
		c.mutate(i, "platform", tenant, "put_tenant", object{"max_running": "2"})
	}
	input := []byte("private application input")
	original := object{"command": c.meta(), "put_definition": object{"definition": object{"id": "hash", "runtime": "steps/v1", "handler": "sha256", "handler_version": "1", "input": encoded(input)}}}
	acknowledgement := c.request(1, "POST", "/api/jobs/v1/mutate", "alpha-writer", original, 200)
	var httpCalls atomic.Int32
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 4096))
		httpCalls.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer remote.Close()
	c.mutate(1, "platform", "alpha", "put_binding", object{"binding": object{"id": "http", "driver": "http", "driver_version": "1", "config": encoded(object{"operations": []object{{"name": "request", "method": "POST", "url": remote.URL + "/start"}}})}})
	c.definition(1, "alpha", "conversation", "request-reply", "http", object{"correlation_key": "reply", "payload": encoded([]byte("request")), "timeout_ns": 600000000000})
	c.launch(1, "alpha", "conversation", "conversation")
	c.awaitRun(1, "alpha", "conversation", "waiting")
	if httpCalls.Load() != 1 {
		t.Fatal("HTTP action was duplicated")
	}
	var recorded, driverSamples, recordSamples, unconfirmed float64
	for i := range c.members {
		m := c.metrics(i, "platform", 200)
		recorded += m[`djob_job_action_results_total{result="confirmed",state="succeeded"}`]
		driverSamples += m[`djob_job_action_driver_duration_seconds_count{state="succeeded"}`]
		recordSamples += m[`djob_job_action_record_duration_seconds_count{result="confirmed"}`]
		unconfirmed += m[`djob_job_action_record_duration_seconds_count{result="unconfirmed"}`]
	}
	if recorded != 1 || driverSamples != 1 || recordSamples != 1 || unconfirmed != 0 {
		t.Fatalf("HTTP action timing: confirmed=%g driver=%g record=%g unconfirmed=%g", recorded, driverSamples, recordSamples, unconfirmed)
	}
	// An explicit tenant override cannot escape the authenticated tenant scope.
	c.request(0, "POST", "/api/jobs/v1/get", "alpha-reader", object{"reference": object{"kind": "RUN", "tenant_id": "beta", "id": "other"}}, 403)

	t.Log("raw RTU conversations, another tenant and legacy dispatch share one gateway")
	rtu, tcp := newGateway(t, true), newGateway(t, false)
	c.channel(0, "rtu-gateway", "modbus-rtu-tcp", rtu.endpoint())
	c.channel(0, "tcp-gateway", "modbus-tcp", tcp.endpoint())
	for _, id := range []string{"rtu-gateway", "tcp-gateway"} {
		c.mutate(0, "platform", "", "put_resource", object{"spec": object{"id": id, "channel_id": id, "duplex": "half", "capacity": 1}})
	}
	read := object{"version": 1, "unit_id": 3, "function": "read_holding_registers", "address": 20, "quantity": 1}
	for i, tenant := range []string{"alpha", "beta"} {
		op := object{"version": 1, "unit_id": 3 + 4*i, "function": "read_holding_registers", "address": 20, "quantity": 1}
		c.mutate(0, "platform", tenant, "put_grant", object{"resource_id": "rtu-gateway", "allowed": true})
		c.mutate(0, "platform", tenant, "put_binding", object{"binding": object{"id": "device", "driver": "modbus-rtu-tcp", "driver_version": "1", "resources": []string{"rtu-gateway"}, "config": encoded(object{"channel_id": "rtu-gateway", "operations": []object{{"name": "request", "operation": op}, {"name": "execute", "operation": op}}})}})
	}
	c.mutate(0, "alpha-writer", "", "put_group", object{"group": object{"id": "device-sessions", "runtime": "steps/v1", "max_running": "1"}})
	c.mutate(0, "alpha-writer", "", "put_definition", object{"definition": object{"id": "device-conversation", "group_id": "device-sessions", "handler": "request-reply", "handler_version": "1", "binding_id": "device", "input": encoded(object{"correlation_key": "device-reply", "timeout_ns": 600000000000})}})
	c.mutate(0, "alpha-writer", "", "put_trigger", object{"definition": object{"id": "device-events", "definition_id": "device-conversation", "definition_revision": "1", "max_pending": 2, "event": object{"source": "meters"}}})
	deviceEvent := object{"trigger_id": "device-events", "source": "meters", "event_id": "reading", "occurred_at": time.Now().UTC().Format(time.RFC3339Nano)}
	deviceAccepted := c.mutate(0, "alpha-writer", "", "activate", deviceEvent)
	deviceRun := deviceAccepted["run_id"].(string)
	first := rtu.next(t)
	if first.unit() != 3 {
		t.Fatalf("first RTU unit: %d", first.unit())
	}
	c.definition(1, "beta", "device", "invoke", "device", nil)
	c.launch(1, "beta", "device", "measurement")
	write := object{"version": 1, "unit_id": 9, "function": "write_multiple_registers", "address": 1, "registers": []int{4660, 0}}
	c.createLegacy(2, "rtu-legacy", "rtu-gateway", write)
	first.respond(t, false)
	c.awaitRun(0, "alpha", deviceRun, "waiting")
	retire := object{"command": c.meta(), "retire_trigger": object{"id": "device-events", "expected_revision": "1", "reason": "retire while device session is waiting"}}
	retirement := c.request(1, "POST", "/api/jobs/v1/mutate", "alpha-writer", retire, 200)
	configurationAudits := []struct {
		kind, audit, id, actor string
		command, ack           object
	}{
		{kind: "DEFINITION", audit: "DEFINITION_RETIREMENT", id: "device-conversation", actor: "alpha-writer"},
		{kind: "BINDING", audit: "BINDING_RETIREMENT", id: "device", actor: "platform"},
		{kind: "GROUP", audit: "GROUP_RETIREMENT", id: "device-sessions", actor: "alpha-writer"},
	}
	for i := range configurationAudits {
		v := &configurationAudits[i]
		v.command = object{"command": c.meta(), "tenant_id": "alpha", "retire_configuration": object{"kind": v.kind, "id": v.id, "expected_revision": "1", "reason": "retire source configuration during a device wait"}}
		v.ack = c.request(1, "POST", "/api/jobs/v1/mutate", v.actor, v.command, 200)
	}
	checkRetirement := func(member int) {
		t.Helper()
		// Inspection is member-local; a follower may lag the acknowledged write.
		eventually(t, "retirement applies to member", func() (bool, error) {
			code, _, err := c.call(member, "POST", "/api/jobs/v1/get", "alpha-reader", object{"reference": object{"kind": "TRIGGER", "id": "device-events"}})
			return err == nil && code == 404, err
		})
		row := c.request(member, "POST", "/api/jobs/v1/get", "alpha-reader", object{"reference": object{"kind": "TRIGGER_RETIREMENT", "id": "device-events"}}, 200)["trigger_retirement"].(map[string]any)
		if row["actor"] != "alpha-writer" || row["revision"] != "1" {
			t.Fatalf("retirement audit changed: %v", row)
		}
		if got := c.request(member, "POST", "/api/jobs/v1/mutate", "alpha-writer", retire, 200); !reflect.DeepEqual(got, retirement) {
			t.Fatal("retirement retry acknowledgement changed")
		}
		if got := c.mutate(member, "alpha-writer", "", "activate", deviceEvent); got["run_id"] != deviceRun {
			t.Fatal("retired event trigger lost accepted event identity")
		}
		for _, v := range configurationAudits {
			eventually(t, "configuration retirement applies to member", func() (bool, error) {
				code, _, err := c.call(member, "POST", "/api/jobs/v1/get", "alpha-reader", object{"reference": object{"kind": v.kind, "id": v.id}})
				return err == nil && code == 404, err
			})
			audit := c.request(member, "POST", "/api/jobs/v1/get", "alpha-reader", object{"reference": object{"kind": v.audit, "id": v.id}}, 200)["configuration_retirement"].(map[string]any)
			if audit["actor"] != v.actor || audit["kind"] != v.kind {
				t.Fatalf("configuration retirement audit changed: %v", audit)
			}
			if got := c.request(member, "POST", "/api/jobs/v1/mutate", v.actor, v.command, 200); !reflect.DeepEqual(got, v.ack) {
				t.Fatal("configuration retirement retry acknowledgement changed")
			}
		}
	}
	checkRetirement(0)
	c.request(0, "POST", "/api/jobs/v1/mutate", "alpha-writer", object{"command": c.meta(), "activate": object{"trigger_id": "device-events", "source": "meters", "event_id": "new-reading", "occurred_at": time.Now().UTC().Format(time.RFC3339Nano)}}, 404)
	c.definition(1, "beta", "hash", "sha256", "", []byte("independent"))
	c.launch(1, "beta", "hash", "independent")
	c.awaitRun(1, "beta", "independent", "succeeded")
	select {
	case <-rtu.calls:
		t.Fatal("another unit bypassed a waiting conversation's gateway reservation")
	default:
	}
	c.mutate(0, "alpha-callback", "", "signal", object{"run_id": deviceRun, "event_id": "device-reply", "key": "device-reply", "data": encoded([]byte("application reply"))})
	seen := make(map[byte]bool)
	for range 2 {
		call := rtu.next(t)
		seen[call.unit()] = true
		call.respond(t, false)
	}
	if !seen[7] || !seen[9] {
		t.Fatalf("gateway handover omitted a contender: %v", seen)
	}
	c.awaitRun(0, "alpha", deviceRun, "succeeded")
	c.awaitRun(1, "beta", "measurement", "succeeded")
	c.awaitLegacy(2, "rtu-legacy", "succeeded")
	eventually(t, "one event consumption sample across live CLI members", func() (bool, error) {
		var samples float64
		for i := range c.members {
			m := c.metrics(i, "platform", 200)
			samples += m[`djob_event_consumption_delay_seconds_count{profile="generic"}`]
			if m[`djob_event_consumption_delay_seconds_count{profile="gateway"}`] != 0 {
				return false, fmt.Errorf("generic event timing leaked into the gateway profile")
			}
		}
		return samples == 1, nil
	})
	c.createLegacy(0, "tcp-read", "tcp-gateway", read)
	tcp.next(t).respond(t, false)
	c.awaitLegacy(0, "tcp-read", "succeeded")
	for _, channel := range []string{"rtu-gateway", "tcp-gateway"} {
		c.awaitChannel(0, channel, "available")
	}
	rtu.check(t, 3)
	tcp.check(t, 1)
	for i := range c.members {
		c.awaitStateInventory(i, false)
	}

	t.Log("bounded backlog observations include unsupported jobs on every member")
	c.definition(0, "alpha", "unsupported", "unregistered-handler", "", nil)
	for i := range 3 {
		c.launch(0, "alpha", "unsupported", fmt.Sprintf("backlog-%d", i))
	}
	for i := range c.members {
		c.awaitQueue(i, false)
	}
	for i := range 2 {
		c.mutate(0, "alpha-writer", "", "cancel", object{"run_id": fmt.Sprintf("backlog-%d", i)})
	}
	for i := range c.members {
		c.awaitQueue(i, true)
	}

	t.Log("leader SIGKILL preserves exact retry acknowledgements and a waiting conversation")
	leader := c.leader(-1)
	c.stop(leader, true)
	replacement := c.leader(leader)
	c.ready(replacement)
	checkRetirement(replacement)
	if got := c.request(replacement, "POST", "/api/jobs/v1/mutate", "alpha-writer", original, 200); !reflect.DeepEqual(got, acknowledgement) {
		t.Fatalf("retry acknowledgement changed: %v != %v", got, acknowledgement)
	}
	if c.run(replacement, "alpha", "conversation")["status"] != "waiting" || httpCalls.Load() != 1 {
		t.Fatal("conversation was lost or HTTP was replayed")
	}
	c.mutate(replacement, "alpha-callback", "", "signal", object{"run_id": "conversation", "event_id": "reply", "key": "reply", "data": encoded([]byte("result"))})
	result := c.awaitRun(replacement, "alpha", "conversation", "succeeded")
	if result["result"] != encoded([]byte("result")) || httpCalls.Load() != 1 {
		t.Fatal("conversation resumed with a duplicated or incorrect effect")
	}
	c.launch(replacement, "alpha", "hash", "after-failover")
	result = c.awaitRun(replacement, "alpha", "after-failover", "succeeded")
	sum := sha256.Sum256(input)
	if result["result"] != encoded(sum[:]) {
		t.Fatal("pinned computation input changed")
	}
	// Remove only this stopped test member's materialized store. Retained WAL
	// and later peer entries must rebuild it; no production path is touched.
	if err := os.RemoveAll(filepath.Join(c.dataDir(leader), "store")); err != nil {
		t.Fatal(err)
	}
	c.start(leader)
	c.waitStarted(leader)
	for i := range c.members {
		c.ready(i)
		c.awaitRun(i, "alpha", "after-failover", "succeeded")
		checkRetirement(i)
		if got := c.request(i, "POST", "/api/jobs/v1/mutate", "alpha-writer", original, 200); !reflect.DeepEqual(got, acknowledgement) {
			t.Fatal("rebuild changed an exact retry acknowledgement")
		}
		c.awaitQueue(i, true)
	}

	t.Log("quorum loss rejects readiness while cached diagnostics keep progressing")
	isolated := c.leader(-1)
	for i := range c.members {
		if i != isolated {
			c.stop(i, true)
		}
	}
	started := time.Now()
	unavailable := c.request(isolated, "GET", "/readyz", "", nil, 503)
	if unavailable["ready"] != false || unavailable["leader"] != true || unavailable["reason"] != "quorum_unavailable" || time.Since(started) > 2*time.Second {
		t.Fatalf("quorum readiness: %v", unavailable)
	}
	before := c.metrics(isolated, "platform", 200)
	keys := []string{"djob_clock_samples_total", `djob_queue_probe_results_total{result="ok"}`, `djob_storage_probe_results_total{component="store",result="ok"}`, `djob_state_probe_results_total{result="ok"}`}
	eventually(t, "local diagnostics progress without quorum", func() (bool, error) {
		after := c.metrics(isolated, "platform", 200)
		for _, key := range keys {
			if after[key] <= before[key] {
				return false, nil
			}
		}
		return after[`djob_queue_ready_runs{profile="generic"}`] == 1, nil
	})
	eventually(t, "cached admission reports expired majority clock agreement", func() (bool, error) {
		after := c.metrics(isolated, "platform", 200)
		return after[`djob_admission_state{state="clock_untrusted"}`] == 1 &&
			after["djob_admission_all_mutations_held"] == 1 &&
			after["djob_admission_peer_clock_fresh"] == 0 &&
			after[`djob_admission_peer_clock_checks_total{result="unavailable"}`] > 0, nil
	})
	for i := range c.members {
		if i != isolated {
			c.start(i)
		}
	}
	for i := range c.members {
		if i != isolated {
			c.waitStarted(i)
		}
		c.ready(i)
	}
	c.launch(isolated, "alpha", "hash", "after-quorum")
	c.awaitRun(isolated, "alpha", "after-quorum", "succeeded")

	t.Log("a corrupt RTU reply durably blocks all units across a whole-cluster restart")
	badWrite := object{"version": 1, "unit_id": 13, "function": "write_multiple_registers", "address": 1, "registers": []int{4660, 0}}
	c.createLegacy(0, "uncertain", "rtu-gateway", badWrite)
	call := rtu.next(t)
	if call.unit() != 13 {
		t.Fatalf("unexpected uncertain unit: %d", call.unit())
	}
	c.createLegacy(1, "blocked", "rtu-gateway", read)
	call.respond(t, true)
	unknown := c.awaitLegacy(0, "uncertain", "unknown")
	if unknown["attempt_count"] != "1" {
		t.Fatalf("uncertain operation attempts: %v", unknown)
	}
	for i := range c.members {
		c.awaitChannel(i, "rtu-gateway", "blocked")
		c.awaitStateInventory(i, true)
		c.ready(i)
	}
	for i := range c.members {
		c.stop(i, true)
	}
	for i := range c.members {
		c.start(i)
	}
	for i := range c.members {
		c.waitStarted(i)
		c.ready(i)
	}
	for i := range c.members {
		c.awaitChannel(i, "rtu-gateway", "blocked")
		c.awaitStateInventory(i, true)
		checkRetirement(i)
		c.awaitLegacy(i, "uncertain", "unknown")
		blocked := c.awaitLegacy(i, "blocked", "ready")
		if count, _ := blocked["attempt_count"].(string); count != "" && count != "0" {
			t.Fatalf("blocked work started: %v", blocked)
		}
	}
	c.launch(0, "alpha", "hash", "after-block")
	c.awaitRun(0, "alpha", "after-block", "succeeded")
	for i := range c.members {
		c.awaitQueue(i, true)
	}
	rtu.check(t, 4)
	tcp.check(t, 1)
	if httpCalls.Load() != 1 {
		t.Fatal("HTTP effect replayed across restart")
	}
	for i := range c.members {
		c.stop(i, false)
	}
	c.checkStorageReport(0)
	c.checkBackupVerification(0)
}

func (c *cluster) metrics(i int, actor string, expected int) map[string]float64 {
	c.t.Helper()
	started := time.Now()
	code, data, err := c.call(i, "GET", "/metrics", actor, nil)
	if err != nil || code != expected {
		c.t.Fatalf("metrics status %d: %v", code, err)
	}
	if expected != 200 {
		return nil
	}
	if time.Since(started) > 1500*time.Millisecond {
		c.t.Fatal("cached metrics scrape stalled")
	}
	for _, token := range c.tokens {
		if strings.Contains(string(data), token) {
			c.t.Fatal("metrics exposed a credential")
		}
	}
	for _, secret := range []string{c.root, "private application input", "alpha-writer", "http://127.0.0.1", "https://127.0.0.1"} {
		if strings.Contains(string(data), secret) {
			c.t.Fatalf("metrics exposed private context %q", secret)
		}
	}
	samples := make(map[string]float64)
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			c.t.Fatalf("malformed metric %q", line)
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			c.t.Fatal(err)
		}
		samples[fields[0]] = value
	}
	return samples
}

func (c *cluster) awaitQueue(i int, complete bool) {
	c.t.Helper()
	eventually(c.t, "bounded queue sample", func() (bool, error) {
		m := c.metrics(i, "platform", 200)
		wanted := float64(0)
		if complete {
			wanted = 1
		}
		if m["djob_queue_probe_success"] != 1 || m[`djob_queue_ready_runs_observed{profile="generic"}`] != 1 || m[`djob_queue_sample_complete{profile="generic"}`] != wanted {
			return false, nil
		}
		_, exact := m[`djob_queue_ready_runs{profile="generic"}`]
		if exact != complete {
			return false, fmt.Errorf("incomplete sample reported exact inventory")
		}
		return true, nil
	})
}

func (c *cluster) awaitStateInventory(i int, blocked bool) {
	c.t.Helper()
	eventually(c.t, "durable state inventory reflects gateway outcome", func() (bool, error) {
		m := c.metrics(i, "platform", 200)
		if m["djob_state_probe_success"] != 1 {
			return false, nil
		}
		for _, kind := range []string{"gateway_runs", "generic_runs", "channels", "resources"} {
			if m[`djob_state_sample_complete{kind="`+kind+`"}`] != 1 {
				return false, nil
			}
		}
		want := float64(0)
		if blocked {
			want = 1
		}
		return m[`djob_state_inventory{kind="channels",state="blocked"}`] == want &&
			m[`djob_state_inventory{kind="gateway_runs",state="unknown"}`] == want, nil
	})
}

func (c *cluster) channel(i int, id, protocol, endpoint string) {
	c.t.Helper()
	c.request(i, "PUT", "/api/v1/channels/"+id, "platform", object{"command": c.meta(), "gateway_id": id, "protocol": protocol, "endpoint": endpoint}, 200)
}

func (c *cluster) awaitChannel(i int, id, state string) {
	c.t.Helper()
	c.eventually(id+" channel becomes "+state, func() (bool, error) {
		code, data, err := c.call(i, "GET", "/api/v1/channels/"+id, "platform", nil)
		if err != nil || code != 200 {
			return false, fmt.Errorf("channel read: status %d, error %v", code, err)
		}
		var response struct{ Channel object }
		if err := json.Unmarshal(data, &response); err != nil {
			return false, err
		}
		if response.Channel["state"] != state {
			return false, fmt.Errorf("observed channel state %v", response.Channel["state"])
		}
		return true, nil
	})
}

func (c *cluster) createLegacy(i int, id, channel string, op object) {
	c.t.Helper()
	c.request(i, "PUT", "/api/v1/runs/"+id, "platform", object{"command": c.meta(), "channel_id": channel, "executor": "modbus", "payload": encoded(op)}, 200)
}

func (c *cluster) awaitLegacy(i int, id, state string) object {
	c.t.Helper()
	var result object
	c.eventually(id+" gateway run becomes "+state, func() (bool, error) {
		code, data, err := c.call(i, "GET", "/api/v1/runs/"+id, "platform", nil)
		if err != nil || code != 200 {
			return false, fmt.Errorf("gateway run read: status %d, error %v", code, err)
		}
		var response struct{ Run object }
		if err := json.Unmarshal(data, &response); err != nil {
			return false, err
		}
		result = response.Run
		return result["state"] == state, nil
	})
	return result
}
