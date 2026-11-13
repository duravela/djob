package e2e_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestManagedWorkerCatalogOnKubernetes(t *testing.T) {
	var kube *kubernetesFixture
	h := newHarnessWithTimeout(t, false, 10*time.Minute, func(h *harness) { h.remote = true; kube = startKubernetesFixture(h) })
	kube.connectControllers(nil)
	h.mutate(0, "platform", "alpha", "put_tenant", object{"max_running": "4"})
	image := kube.workerImage("catalog", "company-worker")
	const count = 40 // More than one 32-record reconciliation page.
	for index := range count {
		id := fmt.Sprintf("catalog-%02d", index)
		secret := kube.workerSecret("alpha", id, nil)
		h.request(0, http.MethodPost, "/api/jobs/v1/worker-deployments/mutate", "platform", object{
			"command": h.meta(), "tenant_id": "alpha", "id": id, "worker_id": id, "action": "put", "expected_revision": "0", "reason": "multi-page catalog qualification",
			"spec": object{"pool": "default", "image": image, "config_secret": secret, "replicas": 0, "cpu_millis": 100, "memory_mib": 128, "drain_seconds": 45},
		}, 200)
	}
	deployments := func() int {
		var list struct {
			Items []json.RawMessage `json:"items"`
		}
		if err := json.Unmarshal(kube.kubectl("-n", "djob-workers", "get", "deployments", "-l", "app.kubernetes.io/managed-by=djob", "-o", "json"), &list); err != nil {
			t.Fatal(err)
		}
		return len(list.Items)
	}
	poll(t, "all catalog pages converge to Kubernetes", func() (bool, error) {
		observed := deployments()
		if observed != count {
			return false, fmt.Errorf("observed %d of %d deployments", observed, count)
		}
		return true, nil
	})
	const tailID = "catalog-39"
	kube.waitRuntime(0, "alpha", tailID, "stopped")
	h.definition("alpha", "compute", "sha256", "", object{"catalog": true})
	h.launch(0, "alpha", "compute", "queued-for-tail-deployment")
	h.run(0, "alpha", "queued-for-tail-deployment", "ready")
	change := func(revision string, replicas int) {
		h.request(0, http.MethodPost, "/api/jobs/v1/worker-deployments/mutate", "platform", object{
			"command": h.meta(), "tenant_id": "alpha", "id": tailID, "worker_id": tailID, "action": "scale", "expected_revision": revision, "replicas": replicas, "reason": "change after first catalog page",
		}, 200)
	}
	change("1", 1)
	kube.waitReplicas(1, image)
	kube.waitSessions("alpha", tailID, "1", 1, 0)
	h.run(0, "alpha", "queued-for-tail-deployment", "succeeded")
	change("2", 0)
	kube.waitReplicas(0)
	kube.waitRuntime(0, "alpha", tailID, "stopped")
	kube.waitSessions("alpha", tailID, "2", 0, 0)
	if observed := deployments(); observed != count {
		t.Fatalf("start/stop changed retained revision fences: got %d deployments, want %d", observed, count)
	}
}
