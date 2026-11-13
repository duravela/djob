package e2e_test

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	dockernetwork "github.com/moby/moby/api/types/network"
	dockerclient "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

type soakJob struct {
	tenant, id, kind, outcome string
	submitted                 time.Time
}

func TestSimulatorSoak(t *testing.T) {
	cfg := soakConfiguration(t)
	h, simulator := newMQTTHarness(t, true, cfg.Duration+3*time.Minute, func(h *harness) {
		if cfg.GoMemoryMiB > 0 {
			h.controllerEnv = map[string]string{"GOMEMLIMIT": fmt.Sprint(cfg.GoMemoryMiB) + "MiB"}
		}
		if cfg.MaxDatabaseMiB > 0 {
			h.controllerArgs = []string{"--max-database-bytes", fmt.Sprint(int64(cfg.MaxDatabaseMiB) << 20)}
		}
		h.raftNetwork = newRaftNetwork(h)
	})
	tenants := []string{"alpha", "beta"}
	configurationReplays := make(map[string]soakReplay)
	for _, tenant := range tenants {
		body := object{"command": h.meta(), "tenant_id": tenant, "put_tenant": object{"max_running": "4"}}
		reply := h.request(0, "POST", "/api/jobs/v1/mutate", "platform", body, 200)
		configurationReplays[tenant] = soakReplay{request: body, reply: reply}
	}
	endpoint, gateway := h.gateway(true)
	h.configure("soak-bus", "modbus-rtu-tcp", endpoint, "acme.meter", "measure")
	components := map[string]*testcontainers.DockerContainer{"diorama": simulator.diorama}
	for i, c := range h.members {
		components[fmt.Sprintf("member-%d", i+1)] = c
	}
	var company [2]*testcontainers.DockerContainer
	revisions := map[string]int{"alpha": 1, "beta": 1}
	unknown := map[string]int{"alpha": 0, "beta": 0}
	for i, tenant := range tenants {
		h.definition(tenant, "compute", "sha256", "", object{"soak": true})
		h.definition(tenant, "conversation", "acme.measure-twice", "soak-bus", object{"maximum": 100})
		h.definition(tenant, "contender", "acme.check-measurement", "soak-bus", object{"maximum": 100})
		h.mqttBinding(tenant, "list", "lab", "request_reply", "gateway/gw-alpha-001/requests/devices/{request_id}", "gateway/gw-alpha-001/responses/devices/{request_id}")
		h.definition(tenant, "list", "invoke", "list", object{"operation": "list", "resource": "devices"})
		company[i] = h.startWorkerControllers(tenant, "soak-company-"+tenant, "/company-worker", []int{i * 2, 1, 2 - i*2})
		stock := h.startWorkerControllers(tenant, "soak-stock-"+tenant, "/djob-worker", []int{1 - i, 2, i}, "--mqtt-profiles", "/mqtt.json")
		components["company-"+tenant], components["stock-"+tenant] = company[i], stock
		h.mutate(0, tenant+"-writer", "", "put_trigger", object{"definition": object{
			"id": "calendar", "definition_id": "compute", "definition_revision": "1", "max_pending": 8,
			"timing": object{"kind": "interval", "start_at": time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano), "interval_ns": "1000000000", "missed_policy": "coalesce"},
		}})
	}
	report := newSoakReport(h, cfg, components)
	report.configurationReplays = configurationReplays
	isolate, heal := soakPartitioner(h)
	launch := func(tenant, definition, id, outcome string) soakJob {
		job := soakJob{tenant: tenant, id: id, kind: definition, outcome: outcome, submitted: time.Now()}
		body := object{"command": h.meta(), "launch": object{"definition_id": definition, "definition_revision": "1", "run_id": id}}
		reply := report.requestMutation(1, tenant+"-writer", body)
		if cfg.HistoryWindow > 0 && definition == "compute" && id == "compute-0-0" {
			report.replays[tenant] = soakReplay{id, body, reply}
		}
		return job
	}
	await := func(job soakJob) {
		run := h.run(1, job.tenant, job.id, job.outcome)
		report.observe(job.submitted, job.outcome == "unknown")
		if job.outcome != "succeeded" {
			return
		}
		switch job.kind {
		case "compute":
			sum := sha256.Sum256([]byte(`{"soak":true}`))
			if run["result"] != base64.StdEncoding.EncodeToString(sum[:]) {
				t.Fatalf("invalid compute result: %v", run)
			}
		case "list":
			data, err := base64.StdEncoding.DecodeString(fmt.Sprint(run["result"]))
			var result struct {
				Status string
				Items  []object
			}
			if err != nil || json.Unmarshal(data, &result) != nil || result.Status != "ok" || len(result.Items) != 1 || result.Items[0]["serialNumber"] != "10000001" {
				t.Fatalf("unexpected Diorama response: %s, %v", data, err)
			}
		}
	}
	pause := func(paused bool) {
		for _, tenant := range tenants {
			h.pauseSoakCalendar(report, tenant, revisions[tenant], paused)
			revisions[tenant]++
		}
		if !paused {
			return
		}
		// Unknown manual requests intentionally retain pending quota. Everything
		// else must finish, including driver cleanup, before injecting a fault.
		for _, tenant := range tenants {
			poll(t, "only expected uncertain runs retain pending quota", func() (bool, error) {
				code, result, err := h.call(0, "POST", "/api/jobs/v1/get", tenant+"-reader", object{"reference": object{"kind": "USAGE"}})
				if err != nil || code != 200 {
					return false, fmt.Errorf("usage status=%d error=%v", code, err)
				}
				usage, _ := result["usage"].(map[string]any)
				if fmt.Sprint(usage["pending_runs"]) == fmt.Sprint(unknown[tenant]) {
					return true, nil
				}
				return false, fmt.Errorf("pending quota: %v", usage)
			})
		}
	}
	faults := func(cycle int) {
		pause(true)
		simulator.control.call("POST", "/sim/gateways/gw-alpha-001/offline")
		poll(t, "Diorama gateway goes offline", func() (bool, error) {
			items, _ := simulator.control.call("GET", "/sim/gateways/gw-alpha-001")["items"].(map[string]any)
			return items["lifecycle"] == "offline" && items["mqttConnected"] == false, nil
		})
		var pending []soakJob
		for _, tenant := range tenants {
			pending = append(pending, launch(tenant, "list", fmt.Sprintf("offline-%d", cycle), "unknown"), launch(tenant, "compute", fmt.Sprintf("offline-compute-%d", cycle), "succeeded"))
		}
		for _, job := range pending {
			await(job)
			if job.outcome == "unknown" {
				unknown[job.tenant]++
			}
		}
		simulator.control.call("POST", "/sim/gateways/gw-alpha-001/online")
		poll(t, "Diorama gateway reconnects", func() (bool, error) {
			items, _ := simulator.control.call("GET", "/sim/gateways/gw-alpha-001")["items"].(map[string]any)
			return items["lifecycle"] == "online" && items["mqttConnected"] == true, nil
		})
		for _, tenant := range tenants {
			await(launch(tenant, "list", fmt.Sprintf("recovered-%d", cycle), "succeeded"))
		}
		worker := company[cycle%len(company)]
		if err := worker.Stop(h.ctx, nil); err != nil {
			t.Fatal(err)
		}
		info, err := worker.Inspect(h.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.State.ExitCode != 0 {
			t.Fatalf("worker did not drain successfully: exit=%d", info.State.ExitCode)
		}
		if err := worker.Start(h.ctx); err != nil {
			t.Fatal(err)
		}
		isolate()
		poll(t, "isolated controller releases worker slots", func() (bool, error) {
			values, err := h.monitorValues(0)
			return values["djob_worker_controller_available"] == 0 && values["djob_worker_session_slots"] == 0, err
		})
		h.request(0, "GET", "/readyz", "", nil, 503)
		for _, tenant := range tenants {
			await(launch(tenant, "compute", fmt.Sprintf("partition-compute-%d", cycle), "succeeded"))
		}
		heal()
		if t.Failed() {
			t.FailNow()
		}
		h.ready(0)
		for _, tenant := range tenants {
			id := fmt.Sprintf("offline-%d", cycle)
			h.run(1, tenant, id, "unknown")
			record := h.request(1, "POST", "/api/jobs/v1/get", tenant+"-reader", object{"reference": object{"kind": "ACTION", "run_id": id, "sequence": "1"}}, 200)
			action, _ := record["action"].(map[string]any)
			if action["status"] != "unknown" {
				t.Fatalf("reconnection changed an uncertain action: %v", record)
			}
		}
		report.FaultRounds++
		pause(false)
	}
	for _, tenant := range tenants {
		poll(t, "calendar materializes before qualification starts", func() (bool, error) {
			record := h.request(0, "POST", "/api/jobs/v1/get", tenant+"-reader", object{"reference": object{"kind": "TRIGGER", "id": "calendar"}}, 200)
			trigger, _ := record["trigger"].(map[string]any)
			return trigger["last_occurrence"] != nil && trigger["last_occurrence"] != "", nil
		})
	}
	report.sample()
	deadline, sampled := time.Now().Add(cfg.Duration), time.Now()
	swept := time.Now()
	for cycle := 0; cycle == 0 || time.Now().Before(deadline); cycle++ {
		cycleStarted := time.Now()
		owner, other := tenants[cycle%2], tenants[1-cycle%2]
		before := gateway.call("GET", "/stats")["requests"].(float64)
		conversation := launch(owner, "conversation", fmt.Sprintf("conversation-%d", cycle), "succeeded")
		h.run(1, owner, conversation.id, "waiting")
		contender := launch(other, "contender", fmt.Sprintf("contender-%d", cycle), "succeeded")
		h.run(1, other, contender.id, "ready")
		var pending []soakJob
		for i := range cfg.BatchSize {
			for _, tenant := range tenants {
				pending = append(pending, launch(tenant, "compute", fmt.Sprintf("compute-%d-%d", cycle, i), "succeeded"), launch(tenant, "list", fmt.Sprintf("list-%d-%d", cycle, i), "succeeded"))
			}
		}
		for _, job := range pending {
			await(job)
		}
		if stats := gateway.call("GET", "/stats"); stats["requests"] != before+1 || stats["maximum"] != float64(1) {
			t.Fatalf("waiting conversation lost exclusion: %v", stats)
		}
		report.mutate(1, owner+"-callback", "", "signal", object{"run_id": conversation.id, "event_id": "continue", "key": "continue"})
		await(conversation)
		await(contender)
		if stats := gateway.call("GET", "/stats"); stats["requests"] != before+3 || stats["maximum"] != float64(1) {
			t.Fatalf("bus replay or overlap: %v", stats)
		}
		report.WireRequests += 3
		report.Cycles++
		if cfg.FaultEvery > 0 && cycle%cfg.FaultEvery == 0 {
			faults(cycle)
		}
		if cfg.HistoryWindow > 0 && time.Since(swept) >= 30*time.Second {
			h.retireSoakHistory(report)
			swept = time.Now()
		}
		if time.Since(sampled) >= 5*time.Second {
			report.sample()
			sampled = time.Now()
		}
		until := cycleStarted.Add(cfg.CycleInterval)
		if until.After(deadline) {
			until = deadline
		}
		for time.Now().Before(until) {
			wait := min(5*time.Second, time.Until(until))
			timer := time.NewTimer(wait)
			select {
			case <-h.ctx.Done():
				timer.Stop()
				t.Fatal(h.ctx.Err())
			case <-timer.C:
			}
			if time.Since(sampled) >= 5*time.Second {
				report.sample()
				sampled = time.Now()
			}
		}
	}
	pause(true)
	if cfg.HistoryWindow > 0 {
		h.retireSoakHistory(report)
	}
	h.ready(1) // Catch this reader up to the committed pause and final outcomes.
	h.auditSoakHistory(report, unknown)
	if stats := gateway.call("GET", "/stats"); stats["requests"] != float64(report.WireRequests) || stats["maximum"] != float64(1) || stats["active"] != float64(0) {
		t.Fatalf("wire history differs from completed conversations: %v", stats)
	}
	h.channel(1, "soak-bus", "available")
	report.sample()
	report.Status = "passed"
}

func (h *harness) pauseSoakCalendar(report *soakReport, tenant string, revision int, paused bool) {
	h.t.Helper()
	for range 5 {
		record := h.request(0, "POST", "/api/jobs/v1/get", tenant+"-reader", object{"reference": object{"kind": "TRIGGER", "id": "calendar"}}, 200)
		trigger, _ := record["trigger"].(map[string]any)
		last, err := time.Parse(time.RFC3339Nano, fmt.Sprint(trigger["last_transition"]))
		if err != nil {
			h.t.Fatal(err)
		}
		at := time.Now().UTC()
		if last.After(at) {
			at = last
		}
		meta := h.meta()
		meta["at"] = at.Format(time.RFC3339Nano)
		code, result, err := report.callMutation(0, tenant+"-writer", object{"command": meta, "pause_trigger": object{"id": "calendar", "expected_revision": fmt.Sprint(revision), "paused": paused}})
		if err != nil {
			h.t.Fatalf("uncertain pause acknowledgement: %v", err)
		}
		if code == 200 {
			return
		}
		// Only an explicitly rejected stale timestamp permits a fresh request.
		// An uncertain acknowledgement must never create a different mutation.
		if code != 409 || !strings.HasPrefix(fmt.Sprint(result["message"]), "stale_time:") {
			h.t.Fatalf("pause status=%d response=%v", code, result)
		}
	}
	h.t.Fatal("calendar kept advancing before its pause could commit")
}

// One Docker client and one cleanup callback cover every round of a long run.
// Repeated use must not itself accumulate idle clients or cleanup registrations.
func soakPartitioner(h *harness) (isolate, heal func()) {
	client, err := testcontainers.NewDockerClientWithOpts(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = client.Close() })
	var endpoint *dockernetwork.EndpointSettings
	isolated := false
	heal = func() {
		if !isolated {
			return
		}
		settings := &dockernetwork.EndpointSettings{Aliases: endpoint.Aliases, IPAMConfig: &dockernetwork.EndpointIPAMConfig{IPv4Address: endpoint.IPAddress}}
		if _, err := client.NetworkConnect(h.ctx, h.raftNetwork.ID, dockerclient.NetworkConnectOptions{Container: h.members[0].GetContainerID(), EndpointConfig: settings}); err != nil {
			h.t.Error(err)
			return
		}
		isolated = false
		h.refreshURL(0)
	}
	h.t.Cleanup(heal)
	isolate = func() {
		info, err := h.members[0].Inspect(h.ctx)
		if err != nil {
			h.t.Fatal(err)
		}
		endpoint = info.NetworkSettings.Networks[h.raftNetwork.Name]
		if endpoint == nil {
			h.t.Fatal("missing Raft endpoint")
		}
		if _, err := client.NetworkDisconnect(h.ctx, h.raftNetwork.ID, dockerclient.NetworkDisconnectOptions{Container: h.members[0].GetContainerID(), Force: true}); err != nil {
			h.t.Fatal(err)
		}
		isolated = true
		h.refreshURL(0)
	}
	return isolate, heal
}
