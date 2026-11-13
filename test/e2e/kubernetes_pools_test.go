package e2e_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestManagedWorkerPoolsOnKubernetes(t *testing.T) {
	var kube *kubernetesFixture
	h := newHarnessWithTimeout(t, false, 10*time.Minute, func(h *harness) {
		h.remote = true
		kube = startKubernetesFixture(h)
		kube.addPool("secondary", "djob-workers-secondary")
	})
	kube.connectControllers(nil)
	image := kube.workerImage("pools", "company-worker")
	member := 0
	change := func(tenant, id, worker, action, revision string, extra object) object {
		body := object{"command": h.meta(), "tenant_id": tenant, "id": id, "worker_id": worker, "action": action, "expected_revision": revision, "reason": "worker pool qualification"}
		for key, value := range extra {
			body[key] = value
		}
		return h.request(member, http.MethodPost, "/api/jobs/v1/worker-deployments/mutate", "platform", body, 200)
	}
	poolCases := []struct{ tenant, pool, namespace string }{
		{"alpha", "default", "djob-workers"},
		{"beta", "secondary", "djob-workers-secondary"},
	}
	for _, test := range poolCases {
		h.mutate(member, "platform", test.tenant, "put_tenant", object{"max_running": "4"})
		// Identically named Secrets contain different tenant/worker credentials.
		secret := kube.workerSecretInNamespace(test.namespace, test.tenant, "shared-worker", nil)
		h.definition(test.tenant, "compute", "sha256", "", object{"pool": test.pool})
		h.launch(member, test.tenant, "compute", "queued-before-pool")
		h.run(member, test.tenant, "queued-before-pool", "ready")
		change(test.tenant, "company", "shared-worker", "put", "0", object{"spec": object{
			"pool": test.pool, "image": image, "config_secret": secret, "replicas": 1, "cpu_millis": 100, "memory_mib": 128, "drain_seconds": 45,
		}})
		runtime := kube.waitRuntime(member, test.tenant, "company", "available")
		if runtime["namespace"] != test.namespace || runtime["pool"] != test.pool {
			t.Fatalf("worker routed to the wrong pool: %v", runtime)
		}
		kube.waitSessions(test.tenant, "company", "1", 1, 0)
		h.run(member, test.tenant, "queued-before-pool", "succeeded")
	}
	secret := kube.workerSecret("alpha", "unmapped-worker", nil)
	change("alpha", "unmapped", "unmapped-worker", "put", "0", object{"spec": object{
		"pool": "unconfigured", "image": image, "config_secret": secret, "replicas": 1, "cpu_millis": 100, "memory_mib": 128, "drain_seconds": 45,
	}})
	if runtime := kube.waitRuntime(member, "alpha", "unmapped", "error"); runtime["error"] != "pool_unavailable" || runtime["namespace"] != "" {
		t.Fatalf("unconfigured pool did not fail without a target namespace: %v", runtime)
	}
	// One pool loses its Deployment permissions. A newer generation needs an
	// API operation, while the other pool must continue reconciling and executing.
	kube.kubectl("-n", "djob-workers", "delete", "rolebinding", "djob-worker-manager")
	change("alpha", "company", "shared-worker", "restart", "1", nil)
	if runtime := kube.waitRuntimeRevision(member, "alpha", "company", "error", "2"); runtime["error"] != "forbidden" {
		t.Fatalf("pool permission failure was not reported: %v", runtime)
	}
	leader := h.leader()
	h.stopMember(leader)
	member = (leader + 1) % len(h.members)
	h.ready(member)
	change("beta", "company", "shared-worker", "scale", "1", object{"replicas": 0})
	kube.waitRuntimeRevision(member, "beta", "company", "stopped", "2")
	h.launch(member, "beta", "compute", "queued-during-pool-outage")
	h.run(member, "beta", "queued-during-pool-outage", "ready")
	change("beta", "company", "shared-worker", "scale", "2", object{"replicas": 1})
	kube.waitRuntimeRevision(member, "beta", "company", "available", "3")
	h.run(member, "beta", "queued-during-pool-outage", "succeeded")
	kube.apply(object{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": object{"name": "djob-worker-manager", "namespace": "djob-workers"}, "roleRef": object{"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "djob-worker-manager"}, "subjects": []any{object{"kind": "ServiceAccount", "name": "djob-worker-manager", "namespace": "djob-workers"}}})
	kube.waitRuntimeRevision(member, "alpha", "company", "available", "2")
	h.launch(member, "alpha", "compute", "after-pool-recovery")
	h.run(member, "alpha", "after-pool-recovery", "succeeded")
	if err := h.members[leader].Start(h.ctx); err != nil {
		t.Fatal(err)
	}
	h.refreshURL(leader)
	h.ready(leader)
	kube.connectControllers(nil)
	for _, test := range poolCases {
		kube.waitSessions(test.tenant, "company", "2", 1, 0)
		var deployments struct {
			Items []object `json:"items"`
		}
		if err := json.Unmarshal(kube.kubectl("-n", test.namespace, "get", "deployments", "-l", "app.kubernetes.io/managed-by=djob", "-o", "json"), &deployments); err != nil {
			t.Fatal(err)
		}
		if len(deployments.Items) != 1 {
			t.Fatalf("pool %s duplicated or misplaced workloads: %d", test.pool, len(deployments.Items))
		}
		metadata := deployments.Items[0]["metadata"].(map[string]any)
		annotations := metadata["annotations"].(map[string]any)
		if metadata["namespace"] != test.namespace || annotations["djob.io/tenant"] != test.tenant || annotations["djob.io/revision"] != map[string]string{"alpha": "2", "beta": "3"}[test.tenant] {
			t.Fatalf("pool %s lost its namespace, tenant or revision fence", test.pool)
		}
	}
}
