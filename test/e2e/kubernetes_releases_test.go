package e2e_test

import (
	"encoding/json"
	"fmt"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

func TestManagedWorkerReleasesOnKubernetes(t *testing.T) {
	runManagedWorkerReleases(t, false)
}

func TestManagedWorkerReleaseSoak(t *testing.T) {
	runManagedWorkerReleases(t, true)
}

func runManagedWorkerReleases(t *testing.T, soak bool) {
	duration := 30 * time.Second
	if soak {
		if raw, ok := os.LookupEnv("DJOB_RELEASE_SOAK_DURATION"); ok {
			var err error
			duration, err = time.ParseDuration(raw)
			if err != nil || duration < 10*time.Second || duration > 24*time.Hour {
				t.Fatal("DJOB_RELEASE_SOAK_DURATION must be between 10s and 24h")
			}
		}
	}
	var kube *kubernetesFixture
	h := newHarnessWithTimeout(t, false, duration+10*time.Minute, func(h *harness) {
		h.remote = true
		kube = startKubernetesFixture(h)
		kube.protectedConfigs = true
		var cfg object
		if err := json.Unmarshal(h.controllerFiles["/worker-kubernetes.json"], &cfg); err != nil {
			t.Fatal(err)
		}
		cfg["require_immutable_config"] = true
		cfg["protect_disruptions"] = true
		cfg["spread_across_nodes"] = true
		h.controllerFiles["/worker-kubernetes.json"], _ = json.Marshal(cfg)
		kube.apply(object{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": object{"name": "djob-worker-manager", "namespace": "djob-workers"}, "rules": []any{object{"apiGroups": []string{"apps"}, "resources": []string{"deployments"}, "verbs": []string{"get", "create", "update"}}, object{"apiGroups": []string{""}, "resources": []string{"secrets"}, "verbs": []string{"get"}}, object{"apiGroups": []string{"policy"}, "resources": []string{"poddisruptionbudgets"}, "verbs": []string{"get", "create", "update"}}}})
		policy, err := os.ReadFile("../../deploy/kubernetes/worker-release-config-policy.yaml")
		if err != nil {
			t.Fatal(err)
		}
		if err = kube.node.CopyToContainer(h.ctx, policy, "/tmp/release-policy.yaml", 0600); err != nil {
			t.Fatal(err)
		}
		kube.kubectl("apply", "-f", "/tmp/release-policy.yaml")
		kube.kubectl("label", "namespace", "djob-workers", "djob.dev/immutable-release-configs=true")
	})
	endpoint, gateway := h.gateway(true)
	kube.connectControllers(map[string]*testcontainers.DockerContainer{"bootstrap-fixture": h.bootstrap, "rtu-gateway": gateway.container})
	h.mutate(0, "platform", "alpha", "put_tenant", object{"max_running": "4"})
	h.mutate(0, "platform", "beta", "put_tenant", object{"max_running": "4"})
	h.configure("release-bus", "modbus-rtu-tcp", endpoint, "acme.meter", "measure")
	images := map[string]string{}
	change := func(id, action, revision string, extra object, want int) object {
		t.Helper()
		body := object{"command": h.meta(), "tenant_id": "alpha", "id": id, "worker_id": id, "action": action, "expected_revision": revision, "reason": "Worker release coexistence qualification"}
		for key, value := range extra {
			body[key] = value
		}
		return h.request(0, http.MethodPost, "/api/jobs/v1/worker-deployments/mutate", "platform", body, want)
	}
	definition := func(id, release, handler, binding string, input any) {
		h.mutate(0, "alpha-writer", "", "put_definition", object{"definition": object{"id": id, "runtime": "steps/v1", "handler": handler, "handler_version": "1", "binding_id": binding, "worker_release": release, "input": encode(input)}})
	}
	assertRelease := func(run object, id, release string) {
		t.Helper()
		if run["definition"].(map[string]any)["worker_release"] != release {
			t.Fatalf("run lost its release: %v", run)
		}
		attempt := h.request(0, http.MethodPost, "/api/jobs/v1/get", "alpha-reader", object{"reference": object{"kind": "ATTEMPT", "run_id": id, "id": run["last_attempt_id"]}}, 200)["attempt"].(map[string]any)
		if !strings.Contains(attempt["owner_id"].(string), "/"+release+"/") {
			t.Fatalf("wrong release claimed run: %v", attempt)
		}
	}
	for _, id := range []string{"release-v1", "release-v2"} {
		secret := kube.workerSecret("alpha", id, nil)
		images[id] = kube.workerImage(id, "company-worker")
		change(id, "put", "0", object{"spec": object{"image": images[id], "config_secret": secret, "replicas": 1}}, 200)
		change(id, "publish", "1", nil, 200)
	}
	if images["release-v1"] == images["release-v2"] {
		t.Fatal("fixture needs distinct image digests")
	}
	kube.waitReplicas(2)
	kube.waitSessions("alpha", "release-v1", "1", 1, 0)
	kube.waitSessions("alpha", "release-v2", "1", 1, 0)
	var budgets struct {
		Items []struct {
			Status struct {
				Allowed int `json:"disruptionsAllowed"`
			} `json:"status"`
			Spec struct {
				MaxUnavailable int `json:"maxUnavailable"`
			} `json:"spec"`
		} `json:"items"`
	}
	if err := json.Unmarshal(kube.kubectl("-n", "djob-workers", "get", "pdb", "-o", "json"), &budgets); err != nil {
		t.Fatal(err)
	}
	if len(budgets.Items) != 2 {
		t.Fatalf("expected two managed release budgets: %+v", budgets)
	}
	for _, budget := range budgets.Items {
		if budget.Spec.MaxUnavailable != 0 || budget.Status.Allowed != 0 {
			t.Fatal("release budget allowed voluntary disruption")
		}
	}

	h.mutate(0, "alpha-writer", "", "put_definition", object{"definition": object{"id": "weighted-compute", "runtime": "steps/v1", "handler": "sha256", "handler_version": "1", "worker_release_weights": object{"release-v1": 5000, "release-v2": 5000}, "input": encode(object{"ab": true})}})
	choices := map[string]int{}
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("weighted-%d", i)
		h.launch(0, "alpha", "weighted-compute", id)
		run := h.run(0, "alpha", id, "succeeded")
		release := run["definition"].(map[string]any)["worker_release"].(string)
		choices[release]++
		assertRelease(run, id, release)
	}
	if choices["release-v1"] == 0 || choices["release-v2"] == 0 {
		t.Fatalf("both weighted routes must execute: %v", choices)
	}
	if soak {
		runReleaseCycles(h, kube, duration, assertRelease)
	}

	definition("old-conversation", "release-v1", "acme.measure-twice", "release-bus", object{"maximum": 100})
	definition("new-compute", "release-v2", "sha256", "", object{"version": 2})
	h.launch(0, "alpha", "old-conversation", "old-run")
	h.run(0, "alpha", "old-run", "waiting")
	outstanding := func(release string, want int) {
		t.Helper()
		page := h.request(0, http.MethodPost, "/api/jobs/v1/list", "alpha-reader", object{"kind": "RUN", "worker_release": release, "page_size": 1}, 200)
		records, _ := page["records"].([]any)
		if len(records) != want {
			t.Fatalf("release %s outstanding runs: got %d, want %d", release, len(records), want)
		}
		for _, record := range records {
			run := record.(map[string]any)["run"].(map[string]any)
			if run["definition"].(map[string]any)["worker_release"] != release {
				t.Fatal("inspection returned another release's work")
			}
		}
	}
	outstanding("release-v1", 1)
	change("release-v1", "restart", "2", nil, 400)
	change("release-v1", "scale", "2", object{"replicas": 0}, 400)
	// The namespace policy must protect the name as well as immutable contents.
	poll(t, "release configuration deletion denied", func() (bool, error) {
		code, r, err := kube.node.Exec(h.ctx, []string{"/bin/kubectl", "-n", "djob-workers", "delete", "secret", "release-v1-config", "--dry-run=server"}, tcexec.Multiplexed())
		if err != nil {
			return false, err
		}
		raw, err := io.ReadAll(r)
		return code != 0 && strings.Contains(string(raw), "cannot be deleted"), err
	})
	code, r, err := kube.node.Exec(h.ctx, []string{"/bin/kubectl", "-n", "djob-workers", "label", "secret", "release-v1-config", "djob.dev/release-config-", "--dry-run=server"}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(r)
	if code == 0 || !strings.Contains(string(raw), "retain their protection label") {
		t.Fatalf("configuration label removal was not denied: %s", raw)
	}

	podsBefore := map[string]bool{}
	for _, pod := range kube.pods() {
		podsBefore[pod.(map[string]any)["metadata"].(map[string]any)["uid"].(string)] = true
	}
	ack := change("release-v1", "retiring", "2", nil, 200)["deployment"].(map[string]any)
	if ack["generation"] != "1" || ack["outstanding_runs"] != "1" {
		t.Fatalf("retirement changed execution generation or lost work: %v", ack)
	}
	kube.waitRuntimeRevision(0, "alpha", "release-v1", "available", "3")
	for _, pod := range kube.pods() {
		if !podsBefore[pod.(map[string]any)["metadata"].(map[string]any)["uid"].(string)] {
			t.Fatal("beginning retirement replaced a worker Pod")
		}
	}
	change("release-v1", "scale", "3", object{"replicas": 0}, 400)
	h.request(0, http.MethodPost, "/api/jobs/v1/mutate", "alpha-writer", object{"command": h.meta(), "launch": object{"definition_id": "old-conversation", "definition_revision": "1", "run_id": "refused-new-v1"}}, 400)
	h.launch(1, "alpha", "new-compute", "parallel-v2")
	assertRelease(h.run(0, "alpha", "parallel-v2", "succeeded"), "parallel-v2", "release-v2")
	h.mutate(2, "alpha-callback", "", "signal", object{"run_id": "old-run", "event_id": "continue", "key": "continue"})
	assertRelease(h.run(0, "alpha", "old-run", "succeeded"), "old-run", "release-v1")
	outstanding("release-v1", 0)
	change("release-v1", "retire", "3", nil, 200)
	kube.waitReplicas(1, images["release-v2"])
	h.launch(1, "alpha", "new-compute", "after-v1-retirement")
	assertRelease(h.run(0, "alpha", "after-v1-retirement", "succeeded"), "after-v1-retirement", "release-v2")
	t.Log("distinct pinned images coexist; v1 finishes its conversation before retirement; v2 continues serving")
}

// Repeatedly move the current definition forward and back while already accepted
// runs retain their original image. Resource and latency observations are bounded
// measurements of this fixture, not a universal capacity claim.
func runReleaseCycles(h *harness, kube *kubernetesFixture, duration time.Duration, assertRelease func(object, string, string)) {
	t := h.t
	report := object{"format": "djob-release-soak/v1", "started_at": time.Now().UTC(), "requested_duration_ns": duration, "passed": false}
	code, output, err := h.members[0].Exec(h.ctx, []string{"/djob", "version"}, tcexec.Multiplexed())
	if err != nil || code != 0 {
		t.Fatalf("release binary identity: %d %v", code, err)
	}
	var build object
	if err = json.NewDecoder(output).Decode(&build); err != nil {
		t.Fatal(err)
	}
	report["binary"] = build
	if raw, err := os.ReadFile(filepath.Join(os.Getenv("DJOB_E2E_BUILD_DIR"), "release-manifest.json")); err == nil {
		var manifest object
		if err = json.Unmarshal(raw, &manifest); err != nil {
			t.Fatal(err)
		}
		report["release_manifest"] = manifest
	}
	cycles, runs, controllerRestarts, workerRestarts := 0, 0, 0, 0
	var maxLatency, maxAdmissionDelay, maxWorkerRecovery time.Duration
	var maxDatabase, maxHeap float64
	started := time.Now()
	dir := os.Getenv("DJOB_TEST_ARTIFACT_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	t.Cleanup(func() {
		report["finished_at"], report["elapsed_ns"] = time.Now().UTC(), time.Since(started)
		report["cycles"], report["runs"], report["controller_restarts"], report["worker_restarts"] = cycles, runs, controllerRestarts, workerRestarts
		report["max_observed_completion_latency_ns"], report["max_sampled_database_bytes"], report["max_sampled_heap_bytes"] = maxLatency, maxDatabase, maxHeap
		report["max_durable_admission_delay_ns"], report["max_idle_worker_recovery_ns"] = maxAdmissionDelay, maxWorkerRecovery
		report["completed_runs_per_second"] = float64(runs) / time.Since(started).Seconds()
		report["batch_size"], report["tenant_max_running"] = 8, 4
		report["passed"] = !t.Failed()
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Error(err)
			return
		}
		raw, err := json.MarshalIndent(report, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(dir, "release-soak-report.json"), append(raw, '\n'), 0600)
		}
		if err != nil {
			t.Error(err)
		}
	})
	for cycles < 6 || time.Since(started) < duration {
		release := fmt.Sprintf("release-v%d", cycles%2+1)
		revision := cycles * 2
		h.mutate(0, "alpha-writer", "", "put_definition", object{"expected_revision": fmt.Sprint(revision), "definition": object{"id": "soak-current", "runtime": "steps/v1", "handler": "sha256", "handler_version": "1", "worker_release": release, "input": encode(object{"cycle": cycles})}})
		submitted := time.Now()
		ids := make([]string, 8)
		for i := range ids {
			ids[i] = fmt.Sprintf("release-soak-%d-%d", cycles, i)
			h.mutate(i%3, "alpha-writer", "", "launch", object{"definition_id": "soak-current", "definition_revision": fmt.Sprint(revision + 1), "run_id": ids[i]})
		}
		// Change the current definition before joining accepted work. Looking up
		// the latest definition during dispatch must not redirect pinned runs.
		otherRelease := fmt.Sprintf("release-v%d", (cycles+1)%2+1)
		h.mutate(1, "alpha-writer", "", "put_definition", object{"expected_revision": fmt.Sprint(revision + 1), "definition": object{"id": "soak-current", "runtime": "steps/v1", "handler": "sha256", "handler_version": "1", "worker_release": otherRelease, "input": encode(object{"next_cycle": cycles + 1})}})
		if cycles%3 == 0 {
			member := (cycles / 3) % 3
			h.stopMember(member)
			if err := h.members[member].Start(h.ctx); err != nil {
				t.Fatal(err)
			}
			h.refreshURL(member)
			h.ready(member)
			controllerRestarts++
		}
		for _, id := range ids {
			run := h.run(0, "alpha", id, "succeeded")
			assertRelease(run, id, release)
			attempt := h.request(0, http.MethodPost, "/api/jobs/v1/get", "alpha-reader", object{"reference": object{"kind": "ATTEMPT", "run_id": id, "id": run["last_attempt_id"]}}, 200)["attempt"].(map[string]any)
			created, createErr := time.Parse(time.RFC3339Nano, fmt.Sprint(run["created_at"]))
			claimed, claimErr := time.Parse(time.RFC3339Nano, fmt.Sprint(attempt["started_at"]))
			if createErr != nil || claimErr != nil || claimed.Before(created) {
				t.Fatal("missing or inconsistent durable admission timing")
			}
			maxAdmissionDelay = max(maxAdmissionDelay, claimed.Sub(created))
			maxLatency = max(maxLatency, time.Since(submitted))
			runs++
		}
		// Idle Pod loss is deliberate fault injection. It does not demonstrate
		// that deleting a busy worker or bypassing a PDB is safe.
		if cycles%5 == 0 {
			recoveryStarted := time.Now()
			pods := kube.pods()
			pod := pods[0].(map[string]any)["metadata"].(map[string]any)["name"].(string)
			kube.kubectl("-n", "djob-workers", "delete", "pod", pod, "--wait=true")
			kube.waitReplicas(2)
			kube.waitSessions("alpha", "release-v1", "1", 1, 0)
			kube.waitSessions("alpha", "release-v2", "1", 1, 0)
			maxWorkerRecovery = max(maxWorkerRecovery, time.Since(recoveryStarted))
			workerRestarts++
		}
		for member := range h.members {
			values, err := h.monitorValues(member)
			if err != nil {
				t.Fatal(err)
			}
			maxDatabase = max(maxDatabase, values["djob_store_file_bytes"])
			maxHeap = max(maxHeap, values["djob_runtime_heap_objects_bytes"])
		}
		cycles++
	}
	t.Logf("release soak: %d cycles, %d runs, %d controller and %d idle worker restarts; maximum observed completion %s", cycles, runs, controllerRestarts, workerRestarts, maxLatency)
}
