package e2e_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

func TestManagedMQTTWorkerOnKubernetes(t *testing.T) {
	var kube *kubernetesFixture
	h, fixture := newMQTTHarness(t, true, 10*time.Minute, func(h *harness) { kube = startKubernetesFixture(h) })
	kube.connectControllers(map[string]*testcontainers.DockerContainer{"mqtt-broker": fixture.broker})
	h.mutate(0, "platform", "alpha", "put_tenant", object{"max_running": "4"})
	profiles, _ := json.Marshal(object{"connections": object{"lab": object{"broker": "tls://mqtt-broker:8883", "ca_file": "mqtt-ca.pem", "tenants": []string{"alpha"}}}})
	secret := kube.workerSecret("alpha", "managed-mqtt", map[string][]byte{"mqtt.json": profiles, "mqtt-ca.pem": h.mqttFiles["/mqtt-ca.pem"]})
	image := kube.workerImage("mqtt", "djob-worker")
	h.mqttBinding("alpha", "list", "lab", "request_reply", "gateway/gw-alpha-001/requests/devices/{request_id}", "gateway/gw-alpha-001/responses/devices/{request_id}")
	h.definition("alpha", "list", "invoke", "list", object{"operation": "list", "resource": "devices"})
	h.launch(0, "alpha", "list", "waiting-for-worker")
	h.run(0, "alpha", "waiting-for-worker", "ready")
	// The manager can create Deployments but an independent namespace quota
	// rejects their Pods. The operator must see the rollout failure, not success
	// inferred from the desired replica count or an indefinitely pending state.
	kube.apply(object{"apiVersion": "v1", "kind": "ResourceQuota", "metadata": object{"name": "deny-worker-pods", "namespace": "djob-workers"}, "spec": object{"hard": object{"pods": "0"}}})
	kube.kubectl("-n", "djob-workers", "wait", "--for=jsonpath={.status.hard.pods}=0", "resourcequota/deny-worker-pods", "--timeout=30s")
	spec := object{"pool": "default", "image": image, "config_secret": secret, "replicas": 1, "cpu_millis": 100, "memory_mib": 128, "drain_seconds": 45,
		"args": []string{"--worker-config", "/var/run/djob/credentials/worker.json", "--mqtt-profiles", "/var/run/djob/credentials/mqtt.json"}}
	h.request(0, http.MethodPost, "/api/jobs/v1/worker-deployments/mutate", "platform", object{"command": h.meta(), "tenant_id": "alpha", "id": "mqtt", "worker_id": "managed-mqtt", "action": "put", "expected_revision": "0", "spec": spec, "reason": "managed MQTT qualification"}, 200)
	blocked := kube.waitRuntime(0, "alpha", "mqtt", "blocked")
	if blocked["error"] != "replica_failure" {
		t.Fatalf("missing safe rollout diagnostic: %v", blocked)
	}
	h.run(0, "alpha", "waiting-for-worker", "ready")
	kube.kubectl("-n", "djob-workers", "delete", "resourcequota", "deny-worker-pods", "--wait=true")
	kube.waitReplicas(1, image)
	kube.waitRuntime(0, "alpha", "mqtt", "available")
	kube.waitSessions("alpha", "mqtt", "1", 1, 0)
	assertResult := func(id string) {
		t.Helper()
		run := h.run(0, "alpha", id, "succeeded")
		data, err := base64.StdEncoding.DecodeString(run["result"].(string))
		var result struct {
			Status string   `json:"status"`
			Items  []object `json:"items"`
		}
		if err != nil || json.Unmarshal(data, &result) != nil || result.Status != "ok" || len(result.Items) != 1 || result.Items[0]["serialNumber"] != "10000001" {
			t.Fatalf("unexpected managed MQTT result: %s, %v", data, err)
		}
	}
	assertResult("waiting-for-worker")
	kube.crashWorker()
	h.launch(0, "alpha", "list", "after-container-restart")
	assertResult("after-container-restart")
	t.Log("managed MQTT worker recovers after quota denial and process loss using projected TLS profiles")
}
