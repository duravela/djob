package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	dockercontainer "github.com/moby/moby/api/types/container"
	dockerclient "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

type kubernetesFixture struct {
	h                *harness
	node             *testcontainers.DockerContainer
	next             int
	protectedConfigs bool
}

func (k *kubernetesFixture) command(args ...string) []byte {
	k.h.t.Helper()
	code, reader, err := k.node.Exec(k.h.ctx, args, tcexec.Multiplexed())
	if err != nil {
		k.h.t.Fatalf("Kubernetes fixture command failed: %v", err)
	}
	data, err := io.ReadAll(io.LimitReader(reader, 1<<20))
	if err != nil || code != 0 {
		k.h.t.Fatalf("Kubernetes fixture command %s failed: exit %d: %s", args[0], code, data)
	}
	return data
}
func (k *kubernetesFixture) kubectl(args ...string) []byte {
	return k.command(append([]string{"/bin/kubectl"}, args...)...)
}
func (k *kubernetesFixture) apply(value any) {
	k.h.t.Helper()
	k.next++
	path := fmt.Sprintf("/tmp/fixture-%d.json", k.next)
	data, err := json.Marshal(value)
	if err != nil {
		k.h.t.Fatal(err)
	}
	if err := k.node.CopyToContainer(k.h.ctx, data, path, 0600); err != nil {
		k.h.t.Fatal(err)
	}
	k.kubectl("apply", "-f", path)
}

func startKubernetesFixture(h *harness) *kubernetesFixture {
	h.t.Helper()
	image, err := os.ReadFile("k3s-image")
	if err != nil {
		h.t.Fatal(err)
	}
	node, err := testcontainers.Run(h.ctx, strings.TrimSpace(string(image)),
		testcontainers.WithCmd("server", "--tls-san=kube-api", "--node-name=djob-managed-test", "--snapshotter=native", "--flannel-backend=host-gw", "--disable=traefik", "--disable=servicelb", "--disable=metrics-server", "--disable=local-storage", "--disable-network-policy"),
		testcontainers.WithHostConfigModifier(func(c *dockercontainer.HostConfig) {
			c.Privileged = true
			c.Tmpfs = map[string]string{"/run": "", "/var/run": ""}
			c.Memory = 2 << 30
		}),
		network.WithNetwork([]string{"kube-api"}, h.network),
		testcontainers.WithWaitStrategyAndDeadline(3*time.Minute, wait.ForExec([]string{"/bin/kubectl", "get", "--raw=/readyz"}).WithStartupTimeout(3*time.Minute).WithPollInterval(time.Second)))
	h.own(node, "kubernetes")
	if err != nil {
		h.t.Fatal(err)
	}
	k := &kubernetesFixture{h: h, node: node}
	code, bridgeReader, probeErr := node.Exec(h.ctx, []string{"sysctl", "-n", "net.bridge.bridge-nf-call-iptables"}, tcexec.Multiplexed())
	var bridge []byte
	if probeErr == nil {
		bridge, probeErr = io.ReadAll(io.LimitReader(bridgeReader, 128))
	}
	if probeErr != nil || code != 0 || strings.TrimSpace(string(bridge)) != "1" {
		h.t.Fatal("K3s fixture requires br_netfilter and net.bridge.bridge-nf-call-iptables=1 on the Docker Linux host; see test/e2e/README.md")
	}
	reader, err := node.CopyFileFromContainer(h.ctx, "/var/lib/rancher/k3s/server/tls/server-ca.crt")
	if err != nil {
		h.t.Fatal(err)
	}
	ca, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		h.t.Fatal(err)
	}
	token := k.poolToken("djob-workers")
	config, _ := json.Marshal(object{"server_url": "https://kube-api:6443", "cluster_id": "managed-e2e", "namespace": "djob-workers", "pool": "default", "ca_file": "/kube-ca.pem", "token_file": "/kube-token"})
	h.controllerFiles = map[string][]byte{"/worker-kubernetes.json": config, "/kube-ca.pem": ca, "/kube-token": token}
	h.controllerArgs = append(h.controllerArgs, "--worker-kubernetes-config", "/worker-kubernetes.json")
	return k
}

func (k *kubernetesFixture) poolToken(namespace string) []byte {
	k.h.t.Helper()
	k.apply(object{"apiVersion": "v1", "kind": "List", "items": []any{
		object{"apiVersion": "v1", "kind": "Namespace", "metadata": object{"name": namespace}},
		object{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": object{"name": "djob-worker-manager", "namespace": namespace}},
		object{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": object{"name": "djob-worker-manager", "namespace": namespace}, "rules": []any{object{"apiGroups": []string{"apps"}, "resources": []string{"deployments"}, "verbs": []string{"get", "create", "update"}}}},
		object{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": object{"name": "djob-worker-manager", "namespace": namespace}, "roleRef": object{"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "djob-worker-manager"}, "subjects": []any{object{"kind": "ServiceAccount", "name": "djob-worker-manager", "namespace": namespace}}},
	}})
	deadline, _ := k.h.ctx.Deadline()
	duration := max(30*time.Minute, time.Until(deadline)+5*time.Minute).Round(time.Minute)
	token := bytes.TrimSpace(k.kubectl("-n", namespace, "create", "token", "djob-worker-manager", "--duration="+duration.String()))
	parts := strings.Split(string(token), ".")
	var claims struct {
		Expires int64 `json:"exp"`
	}
	if len(parts) != 3 {
		k.h.t.Fatal("fixture token is not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || json.Unmarshal(payload, &claims) != nil || time.Unix(claims.Expires, 0).Before(deadline.Add(time.Minute)) {
		k.h.t.Fatal("Kubernetes fixture credential does not cover the test deadline")
	}
	return token
}

func (k *kubernetesFixture) addPool(pool, namespace string) {
	k.h.t.Helper()
	tokenPath, configPath := "/kube-token-"+pool, "/worker-kubernetes-"+pool+".json"
	config, err := json.Marshal(object{"server_url": "https://kube-api:6443", "cluster_id": "managed-e2e", "namespace": namespace, "pool": pool, "ca_file": "/kube-ca.pem", "token_file": tokenPath})
	if err != nil {
		k.h.t.Fatal(err)
	}
	k.h.controllerFiles[configPath], k.h.controllerFiles[tokenPath] = config, k.poolToken(namespace)
	k.h.controllerArgs = append(k.h.controllerArgs, "--worker-kubernetes-config", configPath)
}

func (k *kubernetesFixture) connectControllers(peers map[string]*testcontainers.DockerContainer) {
	var servers strings.Builder
	names := map[string]*testcontainers.DockerContainer{}
	for i, member := range k.h.members {
		names[fmt.Sprintf("member-%d", i+1)] = member
	}
	for name, peer := range peers {
		names[name] = peer
	}
	aliases := make([]string, 0, len(names))
	for name := range names {
		aliases = append(aliases, name)
	}
	sort.Strings(aliases)
	for _, name := range aliases {
		ip, err := names[name].ContainerIP(k.h.ctx)
		if err != nil {
			k.h.t.Fatal(err)
		}
		// Answer both address families locally for Docker-only controller names.
		// CoreDNS's ordinary fallthrough otherwise forwards missing AAAA records
		// to Docker's loopback resolver, which is not reachable from a DNS Pod.
		fmt.Fprintf(&servers, "%s:53 {\n  hosts {\n    %s %s\n  }\n}\n", name, ip, name)
	}
	// Use K3s's custom import instead of racing its startup reconciliation of
	// the built-in Corefile/NodeHosts ConfigMap. See docs.k3s.io/advanced.
	k.apply(object{"apiVersion": "v1", "kind": "ConfigMap", "metadata": object{"name": "coredns-custom", "namespace": "kube-system"}, "data": object{"djob.server": servers.String()}})
	k.kubectl("-n", "kube-system", "wait", "--for=create", "deployment/coredns", "--timeout=90s")
	// Restart after changing the projected ConfigMap. Waiting on the unchanged
	// Deployment alone does not wait for kubelet's ConfigMap propagation.
	k.kubectl("-n", "kube-system", "rollout", "restart", "deployment/coredns")
	k.kubectl("-n", "kube-system", "rollout", "status", "deployment/coredns", "--timeout=120s")
}

func (k *kubernetesFixture) workerImage(variant, binary string) string {
	k.h.t.Helper()
	dir := os.Getenv("DJOB_E2E_BUILD_DIR")
	if err := os.Chmod(filepath.Join(dir, binary), 0555); err != nil {
		k.h.t.Fatal(err)
	}
	dockerfile := "Dockerfile.worker-" + variant
	contents := "FROM scratch\nLABEL djob.test.variant=" + variant + "\nCOPY " + binary + " /" + binary + "\nENTRYPOINT [\"/" + binary + "\"]\n"
	if err := os.WriteFile(filepath.Join(dir, dockerfile), []byte(contents), 0600); err != nil {
		k.h.t.Fatal(err)
	}
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		k.h.t.Fatal(err)
	}
	defer provider.Close()
	repository := "djob.local/" + binary
	name, err := provider.BuildImage(k.h.ctx, &testcontainers.ContainerRequest{FromDockerfile: testcontainers.FromDockerfile{Context: dir, Dockerfile: dockerfile, Repo: repository, Tag: variant}})
	if err != nil {
		k.h.t.Fatal(err)
	}
	defer func() {
		_, _ = provider.Client().ImageRemove(context.Background(), name, dockerclient.ImageRemoveOptions{Force: true, PruneChildren: true})
	}()
	archive, err := provider.Client().ImageSave(k.h.ctx, []string{name})
	if err != nil {
		k.h.t.Fatal(err)
	}
	path := filepath.Join(k.h.t.TempDir(), "worker-image.tar")
	file, err := os.Create(path)
	if err != nil {
		k.h.t.Fatal(err)
	}
	_, copyErr := io.Copy(file, archive)
	_ = archive.Close()
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		k.h.t.Fatalf("worker image archive failed: %v %v", copyErr, closeErr)
	}
	if err := k.node.CopyFileToContainer(k.h.ctx, path, "/tmp/worker-image.tar", 0600); err != nil {
		k.h.t.Fatal(err)
	}
	k.command("/bin/ctr", "--namespace", "k8s.io", "images", "import", "/tmp/worker-image.tar")
	listing := string(k.command("/bin/ctr", "--namespace", "k8s.io", "images", "list"))
	digest := ""
	for _, line := range strings.Split(listing, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[0] == name {
			digest = fields[2]
		}
	}
	if !strings.HasPrefix(digest, "sha256:") || len(digest) != 71 {
		k.h.t.Fatal("imported worker image digest unavailable")
	}
	ref := repository + "@" + digest
	k.command("/bin/ctr", "--namespace", "k8s.io", "images", "tag", name, ref)
	return ref
}

func (k *kubernetesFixture) pods() []any {
	var value object
	if err := json.Unmarshal(k.kubectl("-n", "djob-workers", "get", "pods", "-o", "json"), &value); err != nil {
		k.h.t.Fatal(err)
	}
	items, _ := value["items"].([]any)
	return items
}
func (k *kubernetesFixture) waitReplicas(count int, image ...string) {
	k.h.t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		pods := k.pods()
		ready := 0
		for _, entry := range pods {
			pod := entry.(map[string]any)
			metadata := pod["metadata"].(map[string]any)
			if metadata["deletionTimestamp"] != nil {
				continue
			}
			if len(image) != 0 {
				spec := pod["spec"].(map[string]any)
				containers := spec["containers"].([]any)
				if containers[0].(map[string]any)["image"] != image[0] {
					continue
				}
			}
			status, _ := pod["status"].(map[string]any)
			conditions, _ := status["conditions"].([]any)
			for _, entry := range conditions {
				condition := entry.(map[string]any)
				if condition["type"] == "Ready" && condition["status"] == "True" {
					ready++
				}
			}
		}
		if len(pods) == count && ready == count {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	k.h.t.Fatalf("Kubernetes did not converge to %d workers: %s", count, k.kubectl("-n", "djob-workers", "get", "pods", "-o", "wide"))
}

func (k *kubernetesFixture) workerSecret(tenant, workerID string, files map[string][]byte) string {
	k.h.t.Helper()
	return k.workerSecretInNamespace("djob-workers", tenant, workerID, files)
}

func (k *kubernetesFixture) workerSecretInNamespace(namespace, tenant, workerID string, files map[string][]byte) string {
	k.h.t.Helper()
	var tokenBytes [32]byte
	if _, err := rand.Read(tokenBytes[:]); err != nil {
		k.h.t.Fatal(err)
	}
	token := "djobw1." + hex.EncodeToString(tokenBytes[:])
	hash := sha256.Sum256([]byte(token))
	k.h.request(0, http.MethodPost, "/api/jobs/v1/worker-enrollments/mutate", "platform", object{"command": k.h.meta(), "tenant_id": tenant, "worker_id": workerID, "action": "enroll", "expected_revision": "0", "subject": "company/managed", "reason": "Kubernetes fixture", "credential_hash": hex.EncodeToString(hash[:])}, 200)
	config, _ := json.Marshal(object{"server_url": "https://member-1:8080", "fallback_urls": []string{"https://member-2:8080", "https://member-3:8080"}, "worker_id": workerID, "tenant_id": tenant, "token": token, "ca_file": "api-ca.pem"})
	data := object{}
	for name, contents := range files {
		data[name] = base64.StdEncoding.EncodeToString(contents)
	}
	data["worker.json"], data["api-ca.pem"] = base64.StdEncoding.EncodeToString(config), base64.StdEncoding.EncodeToString(k.h.apiCA)
	secretName := workerID + "-config"
	secret := object{"apiVersion": "v1", "kind": "Secret", "metadata": object{"name": secretName, "namespace": namespace}, "type": "Opaque", "data": data}
	if k.protectedConfigs {
		secret["immutable"] = true
		secret["metadata"].(object)["labels"] = object{"djob.dev/release-config": "true"}
	}
	k.apply(secret)
	return secretName
}

func (k *kubernetesFixture) waitRuntime(member int, tenant, id, state string) object {
	k.h.t.Helper()
	return k.waitRuntimeRevision(member, tenant, id, state, "")
}

func (k *kubernetesFixture) waitRuntimeRevision(member int, tenant, id, state, revision string) object {
	k.h.t.Helper()
	var runtime object
	poll(k.h.t, "managed deployment becomes "+state, func() (bool, error) {
		// Runtime samples are local to the leader that reconciled, not replicated
		// state. Mirror the console's source discovery instead of assuming member 0.
		for offset := range k.h.members {
			index := (member + offset) % len(k.h.members)
			code, body, err := k.h.call(index, http.MethodPost, "/api/jobs/v1/worker-deployments/status", "platform", object{"tenant_id": tenant, "id": id})
			if err != nil || code != 200 {
				continue
			}
			runtime, _ = body["runtime"].(map[string]any)
			if runtime["revision"] == body["revision"] && runtime["state"] == state && (revision == "" || runtime["revision"] == revision) {
				return true, nil
			}
		}
		return false, fmt.Errorf("observed runtime state %v", runtime["state"])
	})
	return runtime
}

func (k *kubernetesFixture) waitSessions(tenant, id, generation string, connected, other int) {
	k.h.t.Helper()
	poll(k.h.t, "managed gRPC sessions match deployment generation "+generation, func() (bool, error) {
		active, old := 0, 0
		seen := map[string]bool{}
		for member := range k.h.members {
			code, body, err := k.h.call(member, http.MethodPost, "/api/jobs/v1/worker-deployments/status", "platform", object{"tenant_id": tenant, "id": id})
			if err != nil || code != 200 {
				return false, fmt.Errorf("controller %d session observation unavailable", member)
			}
			sample, _ := body["worker_sessions"].(map[string]any)
			controller, _ := sample["controller_id"].(string)
			current, okCurrent := sample["connected"].(float64)
			previous, okOther := sample["other_generations"].(float64)
			if sample["generation"] != generation || sample["enabled"] != true || sample["instance_id"] == "" || controller == "" || seen[controller] || !okCurrent || !okOther {
				return false, fmt.Errorf("controller %d session observation incomplete", member)
			}
			seen[controller] = true
			active, old = active+int(current), old+int(previous)
		}
		if active != connected || old != other {
			return false, fmt.Errorf("observed %d current and %d other-generation sessions, want %d and %d", active, old, connected, other)
		}
		return true, nil
	})
}

// Kill the actual process rather than only deleting its API record. Kubernetes
// must start a new container; its new gRPC session cannot reconstruct old I/O.
func (k *kubernetesFixture) crashWorker() {
	k.h.t.Helper()
	pods := k.pods()
	if len(pods) != 1 {
		k.h.t.Fatalf("expected one worker Pod before crash, got %d", len(pods))
	}
	status := pods[0].(map[string]any)["status"].(map[string]any)
	containers := status["containerStatuses"].([]any)
	container := containers[0].(map[string]any)
	id, _ := container["containerID"].(string)
	if !strings.HasPrefix(id, "containerd://") {
		k.h.t.Fatal("worker container identity unavailable")
	}
	previous := container["restartCount"].(float64)
	k.command("/bin/ctr", "--namespace", "k8s.io", "tasks", "kill", "--signal", "SIGKILL", strings.TrimPrefix(id, "containerd://"))
	poll(k.h.t, "Kubernetes restarts the crashed worker container", func() (bool, error) {
		pods := k.pods()
		if len(pods) != 1 {
			return false, nil
		}
		status := pods[0].(map[string]any)["status"].(map[string]any)
		containers, _ := status["containerStatuses"].([]any)
		if len(containers) != 1 {
			return false, nil
		}
		current := containers[0].(map[string]any)
		restarts, _ := current["restartCount"].(float64)
		return restarts > previous && current["containerID"] != id, nil
	})
	k.waitReplicas(1)
}

func TestManagedWorkersOnKubernetes(t *testing.T) {
	var kube *kubernetesFixture
	h := newHarnessWithTimeout(t, false, 10*time.Minute, func(h *harness) { h.remote = true; kube = startKubernetesFixture(h) })
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, logs, err := kube.node.Exec(ctx, []string{"/bin/kubectl", "-n", "djob-workers", "logs", "--all-containers=true", "-l", "app.kubernetes.io/managed-by=djob", "--prefix", "--tail=12"}, tcexec.Multiplexed())
		if err == nil {
			data, _ := io.ReadAll(io.LimitReader(logs, 8192))
			t.Logf("managed Pod diagnostics: %s", data)
		}
	})
	rtuEndpoint, rtuGateway := h.gateway(true)
	tcpEndpoint, tcpGateway := h.gateway(false)
	kube.connectControllers(map[string]*testcontainers.DockerContainer{"bootstrap-fixture": h.bootstrap, "rtu-gateway": rtuGateway.container})
	h.mutate(0, "platform", "alpha", "put_tenant", object{"max_running": "4"})
	h.mutate(0, "platform", "beta", "put_tenant", object{"max_running": "4"})
	secretName := kube.workerSecret("alpha", "managed-worker", nil)
	image := kube.workerImage("one", "company-worker")
	h.definition("alpha", "compute", "sha256", "", object{"managed": true})
	h.launch(0, "alpha", "compute", "queued-before-deployment")
	h.run(0, "alpha", "queued-before-deployment", "ready")
	spec := object{"pool": "default", "image": image, "config_secret": secretName, "replicas": 2, "cpu_millis": 100, "memory_mib": 128, "drain_seconds": 45}
	change := func(member int, action, revision string, extra object) object {
		body := object{"command": h.meta(), "tenant_id": "alpha", "id": "company", "worker_id": "managed-worker", "action": action, "expected_revision": revision, "reason": "Kubernetes lifecycle qualification"}
		for key, value := range extra {
			body[key] = value
		}
		return h.request(member, http.MethodPost, "/api/jobs/v1/worker-deployments/mutate", "platform", body, 200)
	}
	change(0, "put", "0", object{"spec": spec})
	kube.waitReplicas(2, image)
	kube.waitSessions("alpha", "company", "1", 2, 0)
	t.Log("managed worker Pods started with projected credentials")
	h.run(0, "alpha", "queued-before-deployment", "succeeded")
	firstPod := kube.pods()[0].(map[string]any)["metadata"].(map[string]any)["name"].(string)
	kube.kubectl("-n", "djob-workers", "delete", "pod", firstPod, "--wait=true", "--timeout=90s")
	kube.waitReplicas(2)
	for _, entry := range kube.pods() {
		if entry.(map[string]any)["metadata"].(map[string]any)["name"] == firstPod {
			t.Fatal("Kubernetes did not replace the lost instance")
		}
	}
	change(0, "scale", "1", object{"replicas": 0})
	kube.waitReplicas(0)
	kube.waitSessions("alpha", "company", "2", 0, 0)
	t.Log("stopped deployment has no remaining worker Pods")
	h.launch(0, "alpha", "compute", "queued-while-stopped")
	h.run(0, "alpha", "queued-while-stopped", "ready")
	change(0, "scale", "2", object{"replicas": 1})
	kube.waitReplicas(1)
	kube.waitSessions("alpha", "company", "2", 1, 0)
	h.run(0, "alpha", "queued-while-stopped", "succeeded")
	image = kube.workerImage("two", "company-worker")
	spec["image"], spec["replicas"] = image, 1
	ack := change(0, "put", "3", object{"spec": spec})
	if ack["deployment"].(map[string]any)["generation"] != "3" {
		t.Fatal("image rollout did not advance generation")
	}
	kube.waitReplicas(1, image)
	kube.waitRuntime(0, "alpha", "company", "available")
	kube.waitSessions("alpha", "company", "3", 1, 0)
	t.Log("replacement image is running")
	h.launch(0, "alpha", "compute", "after-image-rollout")
	h.run(0, "alpha", "after-image-rollout", "succeeded")
	// A replacement process must preserve both confirmed conversation history
	// and uncertain effects while other tenants contend for the same gateway.
	h.startWorker("beta", "company-beta", "/company-worker", 1)
	h.definition("beta", "compute", "sha256", "", object{"unrelated": true})
	for _, fault := range []struct {
		bus, protocol, endpoint string
		gateway                 gatewayControl
		waiting                 bool
	}{
		{"managed-wait", "modbus-rtu-tcp", rtuEndpoint, rtuGateway, true},
		{"managed-io", "modbus-tcp", tcpEndpoint, tcpGateway, false},
	} {
		h.configure(fault.bus, fault.protocol, fault.endpoint, "acme.meter", "measure")
		h.definition("alpha", fault.bus, "acme.measure-twice", fault.bus, object{"maximum": 100})
		h.definition("beta", fault.bus, "acme.check-measurement", fault.bus, object{"maximum": 100})
		if !fault.waiting {
			fault.gateway.call("POST", "/hold-next")
		}
		h.launch(0, "alpha", fault.bus, fault.bus+"-lost")
		if fault.waiting {
			h.run(0, "alpha", fault.bus+"-lost", "waiting")
		} else {
			poll(t, "managed worker begins gateway I/O", func() (bool, error) {
				stats := fault.gateway.call("GET", "/stats")
				return stats["requests"] == float64(1) && stats["active"] == float64(1), nil
			})
		}
		kube.crashWorker()
		h.run(0, "alpha", fault.bus+"-lost", "unknown")
		h.channel(0, fault.bus, "blocked")
		h.launch(1, "beta", fault.bus, fault.bus+"-contender")
		h.run(0, "beta", fault.bus+"-contender", "ready")
		for _, tenant := range []string{"alpha", "beta"} {
			h.launch(0, tenant, "compute", fault.bus+"-"+tenant+"-unrelated")
			h.run(0, tenant, fault.bus+"-"+tenant+"-unrelated", "succeeded")
		}
		if stats := fault.gateway.call("GET", "/stats"); stats["requests"] != float64(1) || stats["maximum"] != float64(1) {
			t.Fatalf("managed replacement replayed or overlapped gateway I/O: %v", stats)
		}
		h.run(0, "beta", fault.bus+"-contender", "ready")
	}
	t.Log("managed process crashes preserve uncertain outcomes and half-duplex blocks")
	leader := h.leader()
	if err := h.members[leader].Stop(h.ctx, nil); err != nil {
		t.Fatal(err)
	}
	other := (leader + 1) % 3
	h.ready(other)
	h.launch(other, "alpha", "compute", "after-manager-failover")
	h.run(other, "alpha", "after-manager-failover", "succeeded")
	h.channel(other, "managed-wait", "blocked")
	h.channel(other, "managed-io", "blocked")
	change(other, "retire", "4", nil)
	kube.waitReplicas(0)
	var deployments object
	if err := json.Unmarshal(kube.kubectl("-n", "djob-workers", "get", "deployments", "-o", "json"), &deployments); err != nil {
		t.Fatal(err)
	}
	if len(deployments["items"].([]any)) != 1 {
		t.Fatal("controller failover duplicated the Kubernetes deployment")
	}
}
