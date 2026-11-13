package workercontrol

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

type kubeFixture struct {
	mu                  sync.Mutex
	value               map[string]any
	creates, updates    int
	token               string
	namespace           string
	conflict, loseReply bool
	mutate              func(map[string]any)
	secret              map[string]any
	budget              map[string]any
}

func (f *kubeFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer "+f.token {
		w.WriteHeader(403)
		_, _ = w.Write([]byte("secret-marker"))
		return
	}
	if strings.HasPrefix(r.URL.Path, "/apis/policy/v1/") {
		if r.Method == http.MethodGet {
			if f.budget == nil {
				w.WriteHeader(404)
				return
			}
			json.NewEncoder(w).Encode(f.budget)
			return
		}
		var value map[string]any
		decoder := json.NewDecoder(r.Body)
		decoder.UseNumber()
		if decoder.Decode(&value) != nil {
			w.WriteHeader(400)
			return
		}
		f.budget = value
		object(value["metadata"])["resourceVersion"] = "budget-version"
		json.NewEncoder(w).Encode(value)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/") && strings.Contains(r.URL.Path, "/secrets/") {
		if f.secret == nil {
			w.WriteHeader(404)
			return
		}
		json.NewEncoder(w).Encode(f.secret)
		return
	}
	namespace := f.namespace
	if namespace == "" {
		namespace = "workers"
	}
	if !strings.HasPrefix(r.URL.Path, "/apis/apps/v1/namespaces/"+namespace+"/deployments") {
		w.WriteHeader(404)
		return
	}
	if r.Method == http.MethodGet {
		if f.value == nil {
			w.WriteHeader(404)
			return
		}
		_ = json.NewEncoder(w).Encode(f.value)
		return
	}
	data, _ := io.ReadAll(r.Body)
	value, err := decodeObject(data)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	if r.Method == http.MethodPost {
		if f.value != nil {
			w.WriteHeader(409)
			return
		}
		f.creates++
	} else {
		if f.conflict {
			f.conflict = false
			w.WriteHeader(409)
			return
		}
		if text(object(value["metadata"])["resourceVersion"]) != text(object(f.value["metadata"])["resourceVersion"]) {
			w.WriteHeader(409)
			return
		}
		f.updates++
	}
	if f.mutate != nil {
		f.mutate(value)
	}
	f.value = value
	metadata := object(value["metadata"])
	metadata["resourceVersion"] = strings.Repeat("v", f.creates+f.updates)
	metadata["generation"] = json.Number("1")
	value["status"] = map[string]any{"observedGeneration": json.Number("1"), "replicas": object(value["spec"])["replicas"], "readyReplicas": object(value["spec"])["replicas"], "availableReplicas": object(value["spec"])["replicas"], "updatedReplicas": object(value["spec"])["replicas"]}
	if f.loseReply {
		f.loseReply = false
		conn, _, _ := w.(http.Hijacker).Hijack()
		_ = conn.Close()
		return
	}
	if r.Method == http.MethodPost {
		w.WriteHeader(201)
	}
	_ = json.NewEncoder(w).Encode(value)
}

func kubeRuntime(t *testing.T) (*Kubernetes, *kubeFixture, string) {
	t.Helper()
	fixture := &kubeFixture{token: "service-account-one"}
	server := httptest.NewTLSServer(fixture)
	t.Cleanup(server.Close)
	dir := t.TempDir()
	ca, token := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600))
	require.NoError(t, os.WriteFile(token, []byte(fixture.token), 0600))
	k, err := NewKubernetes(KubernetesConfig{ServerURL: server.URL, ClusterID: "plant-east", Pool: "default", Namespace: "workers", CAFile: ca, TokenFile: token})
	require.NoError(t, err)
	t.Cleanup(k.Close)
	return k, fixture, token
}
func desiredWorker() db.WorkerDeployment {
	return db.WorkerDeployment{TenantID: "alpha", ID: "line-one", WorkerID: "worker-one", State: "active", Revision: 1, Generation: 1, Spec: db.WorkerDeploymentSpec{Pool: "default", Image: "registry.example/company/worker@sha256:" + strings.Repeat("a", 64), ConfigSecret: "worker-one-config", Replicas: 2, CPUMillis: 1000, MemoryMiB: 1024}.WithDefaults()}
}

func TestKubernetesReconciliationIsIdempotentAndRejectsStaleRevisions(t *testing.T) {
	k, f, _ := kubeRuntime(t)
	v := desiredWorker()
	first, err := k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	require.Equal(t, "available", first.State)
	require.Equal(t, 1, f.creates)
	_, err = k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	require.Zero(t, f.updates)
	pod := object(object(object(f.value["spec"])["template"])["spec"])
	require.Equal(t, false, pod["automountServiceAccountToken"])
	container := object(pod["containers"].([]any)[0])
	require.Equal(t, v.Spec.Image, container["image"])
	resources := object(object(container["resources"])["limits"])
	require.Equal(t, "1", resources["cpu"])
	require.Equal(t, "1Gi", resources["memory"])
	v.Revision, v.Generation = 2, 2
	v.Spec.Image = "registry.example/company/worker@sha256:" + strings.Repeat("b", 64)
	_, err = k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	require.Equal(t, 1, f.updates)
	stale, err := k.Reconcile(context.Background(), desiredWorker())
	require.NoError(t, err)
	require.Equal(t, "superseded", stale.State)
	require.Equal(t, 1, f.updates)
	object(object(f.value["metadata"])["annotations"])["djob.io/tenant"] = "beta"
	_, err = k.Reconcile(context.Background(), v)
	require.ErrorContains(t, err, "ownership_conflict")
	require.Equal(t, 1, f.updates)
}

func TestKubernetesConflictAndLostReplyAreRecoveredByObservation(t *testing.T) {
	k, f, _ := kubeRuntime(t)
	v := desiredWorker()
	_, err := k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	v.Revision, v.Generation = 2, 2
	f.conflict = true
	observation, err := k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	require.Equal(t, "conflict", observation.State)
	require.Zero(t, f.updates)
	f.loseReply = true
	_, err = k.Reconcile(context.Background(), v)
	require.ErrorContains(t, err, "unavailable")
	require.Equal(t, 1, f.updates)
	observation, err = k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	require.Equal(t, "available", observation.State)
	require.Equal(t, 1, f.updates)
}

func TestKubernetesServiceAccountTokenRotatesAndErrorsStayPrivate(t *testing.T) {
	k, f, token := kubeRuntime(t)
	v := desiredWorker()
	_, err := k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	f.token = "service-account-two"
	require.NoError(t, os.WriteFile(token, []byte(f.token), 0600))
	_, err = k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(token, []byte("old-token"), 0600))
	_, err = k.Reconcile(context.Background(), v)
	require.ErrorContains(t, err, "forbidden")
	require.NotContains(t, err.Error(), "secret-marker")
	require.NotContains(t, err.Error(), "old-token")
}

func TestKubernetesRepairsAppendedArgumentsWithoutRejectingAdmissionSidecars(t *testing.T) {
	k, f, _ := kubeRuntime(t)
	v := desiredWorker()
	_, err := k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	f.mu.Lock()
	pod := object(object(object(f.value["spec"])["template"])["spec"])
	containers := pod["containers"].([]any)
	container := object(containers[0])
	container["args"] = append(container["args"].([]any), "--reconnect=false")
	pod["containers"] = append(containers, map[string]any{"name": "injected-sidecar", "image": "injected.example/sidecar"})
	f.mu.Unlock()
	_, err = k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	require.Equal(t, 1, f.updates)
	pod = workloadPod(f.inspect(t)["spec"])
	require.Len(t, pod["containers"].([]any), 2, "repairing worker arguments must preserve the injected sidecar")
	_, err = k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	require.Equal(t, 1, f.updates, "an unrelated admission-injected container does not cause rollout churn")
}

func TestImmutableConfigurationRequiresProtectionAndFencesSecretReplacement(t *testing.T) {
	k, f, _ := kubeRuntime(t)
	k.cfg.RequireImmutableConfig = true
	v := desiredWorker()
	v.Published = true
	_, err := k.Reconcile(context.Background(), v)
	require.ErrorContains(t, err, "immutable_config_required")
	require.Zero(t, f.creates)
	f.secret = map[string]any{"immutable": false, "metadata": map[string]any{"uid": "original", "labels": map[string]any{"djob.dev/release-config": "true"}}}
	_, err = k.Reconcile(context.Background(), v)
	require.ErrorContains(t, err, "immutable_config_required")
	f.secret["immutable"] = true
	_, err = k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	require.Equal(t, 1, f.creates)
	require.Equal(t, "original", text(object(object(f.value["metadata"])["annotations"])["djob.io/config-secret-uid"]))
	f.secret["metadata"].(map[string]any)["uid"] = "replacement"
	_, err = k.Reconcile(context.Background(), v)
	require.ErrorContains(t, err, "immutable_config_replaced")
	require.Zero(t, f.updates)
	// Stopping remains possible even if the Secret disappears, and retains the UID fence.
	f.secret = nil
	v.Spec.Replicas = 0
	v.Revision++
	v.Generation++
	_, err = k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	require.Equal(t, "original", text(object(object(f.value["metadata"])["annotations"])["djob.io/config-secret-uid"]))
}

func TestDisruptionProtectionHasReleaseScopedSelectorAndSafePlacement(t *testing.T) {
	k, f, _ := kubeRuntime(t)
	k.cfg.ProtectDisruptions = true
	k.cfg.SpreadAcrossNodes = true
	f.mutate = func(value map[string]any) {
		pod := workloadPod(value["spec"])
		pod["topologySpreadConstraints"] = append(pod["topologySpreadConstraints"].([]any), map[string]any{"topologyKey": "topology.kubernetes.io/zone", "maxSkew": json.Number("1"), "whenUnsatisfiable": "ScheduleAnyway", "labelSelector": map[string]any{"matchLabels": map[string]any{"company": "worker"}}})
	}
	v := desiredWorker()
	_, err := k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	f.mu.Lock()
	require.NotNil(t, f.budget)
	spec := object(f.budget["spec"])
	require.Equal(t, json.Number("0"), spec["maxUnavailable"])
	require.Equal(t, "IfHealthyBudget", spec["unhealthyPodEvictionPolicy"])
	selector := object(object(spec["selector"])["matchLabels"])
	require.Equal(t, deploymentName(k.cfg.ClusterID, v), selector["djob.io/instance"])
	constraints := workloadPod(f.value["spec"])["topologySpreadConstraints"].([]any)
	require.Len(t, constraints, 2)
	require.Equal(t, "topology.kubernetes.io/zone", object(constraints[1])["topologyKey"])
	require.Equal(t, "kubernetes.io/hostname", object(constraints[0])["topologyKey"])
	require.Equal(t, "ScheduleAnyway", object(constraints[0])["whenUnsatisfiable"])
	f.mu.Unlock()
	_, err = k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	f.mu.Lock()
	object(f.budget["metadata"])["annotations"] = map[string]any{"djob.io/tenant": "foreign"}
	f.mu.Unlock()
	_, err = k.Reconcile(context.Background(), v)
	require.ErrorContains(t, err, "disruption_budget_conflict")
}
