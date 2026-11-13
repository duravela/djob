package e2e_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestManagedWorkerPlatformConfigurationOnKubernetes(t *testing.T) {
	var kube *kubernetesFixture
	h := newHarnessWithTimeout(t, false, 10*time.Minute, func(h *harness) { h.remote = true; kube = startKubernetesFixture(h) })
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		for _, value := range kube.pods() {
			pod := value.(map[string]any)
			status, _ := pod["status"].(map[string]any)
			t.Logf("worker Pod diagnostic: name=%v phase=%v reason=%v message=%v", pod["metadata"].(map[string]any)["name"], status["phase"], status["reason"], status["message"])
		}
	})
	kube.connectControllers(nil)
	h.mutate(0, "platform", "alpha", "put_tenant", object{"max_running": "4"})
	secret := kube.workerSecret("alpha", "company-policy", nil)
	image := kube.workerImage("platform-one", "company-worker")
	spec := object{"pool": "default", "image": image, "config_secret": secret, "replicas": 1, "cpu_millis": 100, "memory_mib": 128, "drain_seconds": 45}
	change := func(action, revision string, extra object) {
		body := object{"command": h.meta(), "tenant_id": "alpha", "id": "company", "worker_id": "company-policy", "action": action, "expected_revision": revision, "reason": "preserve company workload policy"}
		for key, value := range extra {
			body[key] = value
		}
		h.request(0, http.MethodPost, "/api/jobs/v1/worker-deployments/mutate", "platform", body, 200)
	}
	change("put", "0", object{"spec": spec})
	kube.waitReplicas(1, image)
	name := kube.waitRuntime(0, "alpha", "company", "available")["name"].(string)
	livePods := func(count int) []map[string]any {
		var current []map[string]any
		poll(t, "non-terminating worker Pods settle", func() (bool, error) {
			current = nil
			for _, value := range kube.pods() {
				pod := value.(map[string]any)
				status, _ := pod["status"].(map[string]any)
				if pod["metadata"].(map[string]any)["deletionTimestamp"] == nil && status["phase"] != "Failed" && status["phase"] != "Succeeded" {
					current = append(current, pod)
				}
			}
			if len(current) == count {
				return true, nil
			}
			return false, fmt.Errorf("observed %d non-terminating Pods, want %d", len(current), count)
		})
		return current
	}
	// Model platform policy additions without installing a mutating webhook.
	// The init container uses the existing fixture binary's successful help path.
	policy := object{"metadata": object{"labels": object{"company.example/tier": "gateways"}}, "spec": object{"template": object{
		"metadata": object{"annotations": object{"company.example/policy": "injected"}},
		"spec": object{
			"nodeSelector":   object{"kubernetes.io/os": "linux"},
			"initContainers": []any{object{"name": "company-init", "image": image, "command": []string{"/company-worker", "--help"}}},
			"containers": []any{object{"name": "worker", "env": []any{object{"name": "COMPANY_NODE", "valueFrom": object{"fieldRef": object{"fieldPath": "spec.nodeName"}}}}, "resources": object{"limits": object{"ephemeral-storage": "256Mi"}},
				"volumeMounts": []any{object{"name": "credentials", "mountPath": "/company/credentials", "readOnly": true}, object{"name": "company", "mountPath": "/company/cache"}}}},
			"volumes": []any{object{"name": "temporary", "emptyDir": object{"medium": "Memory"}}, object{"name": "company", "emptyDir": object{"sizeLimit": "1Mi"}}},
		},
	}}}
	data, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := kube.node.CopyToContainer(h.ctx, data, "/tmp/company-policy.json", 0600); err != nil {
		t.Fatal(err)
	}
	kube.kubectl("-n", "djob-workers", "patch", "deployment", name, "--type=strategic", "--patch-file=/tmp/company-policy.json")
	kube.kubectl("-n", "djob-workers", "rollout", "status", "deployment/"+name, "--timeout=120s")
	kube.waitReplicas(1, image)
	firstPod := livePods(1)[0]["metadata"].(map[string]any)["name"].(string)
	change("scale", "1", object{"replicas": 2})
	kube.waitReplicas(2, image)
	kube.waitRuntime(0, "alpha", "company", "available")
	kube.waitSessions("alpha", "company", "1", 2, 0)
	retained := false
	beforePause := livePods(2)
	for _, pod := range beforePause {
		retained = retained || pod["metadata"].(map[string]any)["name"] == firstPod
	}
	if !retained {
		t.Fatal("scaling rolled the existing Pod by rewriting platform configuration")
	}
	// Platform rollout holds must survive desired-template changes. djob must
	// explain the hold instead of treating old ready Pods as the new rollout.
	kube.kubectl("-n", "djob-workers", "rollout", "pause", "deployment/"+name)
	paused := kube.waitRuntime(0, "alpha", "company", "paused")
	if paused["rollout_paused"] != true {
		t.Fatal("runtime observation omitted the platform rollout hold")
	}
	previousImage := image
	image = kube.workerImage("platform-two", "company-worker")
	spec["image"], spec["replicas"] = image, 2
	change("put", "2", object{"spec": spec})
	paused = kube.waitRuntimeRevision(0, "alpha", "company", "paused", "3")
	if paused["revision"] != "3" || paused["generation"] != "2" || paused["rollout_paused"] != true {
		t.Fatal("paused observation did not describe the newly committed rollout")
	}
	var held object
	if err := json.Unmarshal(kube.kubectl("-n", "djob-workers", "get", "deployment", name, "-o", "json"), &held); err != nil {
		t.Fatal(err)
	}
	heldSpec := held["spec"].(map[string]any)
	if heldSpec["paused"] != true || heldSpec["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["image"] != image {
		t.Fatal("desired update lost the pause or failed to update its held template")
	}
	// Pausing prevents a new template rollout, but Kubernetes may redistribute
	// replicas among existing ReplicaSets. Exact Pod counts/UIDs are not frozen.
	for _, value := range kube.pods() {
		pod := value.(map[string]any)
		if pod["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["image"] != previousImage {
			t.Fatal("a paused template update started the new worker image before platform approval")
		}
	}
	kube.kubectl("-n", "djob-workers", "rollout", "resume", "deployment/"+name)
	kube.waitReplicas(2, image)
	if resumed := kube.waitRuntime(0, "alpha", "company", "available"); resumed["rollout_paused"] == true {
		t.Fatal("runtime retained a rollout hold after the platform resumed it")
	}
	kube.waitSessions("alpha", "company", "2", 2, 0)
	var deployment object
	if err := json.Unmarshal(kube.kubectl("-n", "djob-workers", "get", "deployment", name, "-o", "json"), &deployment); err != nil {
		t.Fatal(err)
	}
	if deployment["metadata"].(map[string]any)["labels"].(map[string]any)["company.example/tier"] != "gateways" {
		t.Fatal("image update removed platform metadata")
	}
	for _, value := range kube.pods() {
		pod := value.(map[string]any)
		if pod["metadata"].(map[string]any)["annotations"].(map[string]any)["company.example/policy"] != "injected" {
			t.Fatal("image update removed the company Pod policy")
		}
		current := pod["spec"].(map[string]any)
		if current["nodeSelector"].(map[string]any)["kubernetes.io/os"] != "linux" || len(current["initContainers"].([]any)) != 1 {
			t.Fatal("image update removed node placement or the company init container")
		}
		container := current["containers"].([]any)[0].(map[string]any)
		if container["resources"].(map[string]any)["limits"].(map[string]any)["ephemeral-storage"] != "256Mi" {
			t.Fatal("image update removed the platform storage limit")
		}
		env, companyMount, repeatedMount, memoryVolume, companyVolume := false, false, false, false, false
		for _, value := range container["env"].([]any) {
			env = env || value.(map[string]any)["name"] == "COMPANY_NODE"
		}
		for _, value := range container["volumeMounts"].([]any) {
			mount := value.(map[string]any)
			companyMount = companyMount || mount["mountPath"] == "/company/cache"
			repeatedMount = repeatedMount || mount["mountPath"] == "/company/credentials"
		}
		for _, value := range current["volumes"].([]any) {
			volume := value.(map[string]any)
			companyVolume = companyVolume || volume["name"] == "company"
			if volume["name"] == "temporary" {
				memoryVolume = volume["emptyDir"].(map[string]any)["medium"] == "Memory"
			}
		}
		if !env || !companyMount || !repeatedMount || !memoryVolume || !companyVolume {
			t.Fatal("image update removed company environment, mounts or volumes")
		}
	}
	h.definition("alpha", "compute", "sha256", "", object{"platform_policy": true})
	h.launch(0, "alpha", "compute", "company-policy-survived")
	h.run(0, "alpha", "company-policy-survived", "succeeded")

	// Enrollment policy remains distinct from the desired process count. The
	// controller status must expose a drain even while Pod observations lag.
	access := func(state, revision string) {
		poll(t, "controllers observe worker access "+state, func() (bool, error) {
			for member := range h.members {
				code, body, err := h.call(member, http.MethodPost, "/api/jobs/v1/worker-deployments/status", "platform", object{"tenant_id": "alpha", "id": "company"})
				if err != nil || code != 200 {
					return false, fmt.Errorf("controller %d access observation unavailable", member)
				}
				observed, _ := body["worker_access"].(map[string]any)
				if observed["observed"] != true || observed["worker_id"] != "company-policy" || observed["state"] != state || observed["revision"] != revision {
					return false, fmt.Errorf("controller %d worker access: %v", member, observed)
				}
			}
			return true, nil
		})
	}
	changeAccess := func(action, revision string) {
		h.request(0, http.MethodPost, "/api/jobs/v1/worker-enrollments/mutate", "platform", object{"command": h.meta(), "tenant_id": "alpha", "worker_id": "company-policy", "action": action, "expected_revision": revision, "reason": "managed worker policy observation"}, 200)
	}
	access("enabled", "1")
	changeAccess("drain", "1")
	access("draining", "2")
	kube.waitSessions("alpha", "company", "2", 0, 0)
	changeAccess("resume", "2")
	access("enabled", "3")
	kube.waitReplicas(2, image)
	kube.waitSessions("alpha", "company", "2", 2, 0)
	h.launch(0, "alpha", "compute", "company-access-resumed")
	h.run(0, "alpha", "company-access-resumed", "succeeded")

	// Hold deletion with a fixture-owned finalizer. Management must leave this
	// object alone even when a newer stop is committed while it terminates.
	const finalizer = "tests.djob.io/deletion-hold"
	oldUID := deployment["metadata"].(map[string]any)["uid"]
	patch := func(value object) {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		kube.kubectl("-n", "djob-workers", "patch", "deployment", name, "--type=strategic", "--patch", string(data))
	}
	patch(object{"metadata": object{"finalizers": []string{finalizer}}})
	kube.kubectl("-n", "djob-workers", "delete", "deployment", name, "--cascade=foreground", "--wait=false")
	kube.waitRuntime(0, "alpha", "company", "deleting")
	change("scale", "3", object{"replicas": 0})
	deleting := kube.waitRuntime(0, "alpha", "company", "deleting")
	if deleting["deletion_requested_at"] == nil || deleting["desired_replicas"] != float64(0) {
		t.Fatal("deletion observation omitted its timestamp or latest desired stop")
	}
	var terminating object
	if err := json.Unmarshal(kube.kubectl("-n", "djob-workers", "get", "deployment", name, "-o", "json"), &terminating); err != nil {
		t.Fatal(err)
	}
	if terminating["spec"].(map[string]any)["replicas"] != float64(2) || terminating["metadata"].(map[string]any)["annotations"].(map[string]any)["djob.io/revision"] != "3" {
		t.Fatal("manager updated the terminating object instead of waiting for finalization")
	}
	// Remove only the test's hold; leave Kubernetes's own finalizers to their owners.
	patch(object{"metadata": object{"$deleteFromPrimitiveList/finalizers": []string{finalizer}}})
	poll(t, "latest desired state recreates a zero-replica revision fence", func() (bool, error) {
		var list struct {
			Items []object `json:"items"`
		}
		if err := json.Unmarshal(kube.kubectl("-n", "djob-workers", "get", "deployments", "--field-selector=metadata.name="+name, "-o", "json"), &list); err != nil {
			return false, err
		}
		if len(list.Items) != 1 {
			return false, fmt.Errorf("observed %d deployments", len(list.Items))
		}
		value := list.Items[0]
		metadata := value["metadata"].(map[string]any)
		if metadata["uid"] == oldUID || metadata["deletionTimestamp"] != nil || value["spec"].(map[string]any)["replicas"] != float64(0) || metadata["annotations"].(map[string]any)["djob.io/revision"] != "4" {
			return false, fmt.Errorf("waiting for a replacement object at stopped revision 4")
		}
		return true, nil
	})
	kube.waitReplicas(0)
	kube.waitSessions("alpha", "company", "3", 0, 0)
	if stopped := kube.waitRuntime(0, "alpha", "company", "stopped"); stopped["deletion_requested_at"] != nil {
		t.Fatal("replacement workload retained the old object's deletion timestamp")
	}
	change("scale", "4", object{"replicas": 1})
	kube.waitReplicas(1, image)
	kube.waitSessions("alpha", "company", "3", 1, 0)
	h.launch(0, "alpha", "compute", "after-runtime-deletion")
	h.run(0, "alpha", "after-runtime-deletion", "succeeded")
}
