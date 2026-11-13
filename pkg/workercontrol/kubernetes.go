// Package workercontrol reconciles desired worker processes outside Raft. It
// does not execute jobs or supply execution authority to a worker.
package workercontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/sdk/go/worker"
)

type KubernetesConfig struct {
	ProtectDisruptions     bool   `json:"protect_disruptions"`
	SpreadAcrossNodes      bool   `json:"spread_across_nodes"`
	RequireImmutableConfig bool   `json:"require_immutable_config"`
	ServerURL              string `json:"server_url"`
	ClusterID              string `json:"cluster_id"`
	Pool                   string `json:"pool"`
	Namespace              string `json:"namespace"`
	CAFile                 string `json:"ca_file"`
	TokenFile              string `json:"token_file"`
}

// LoadKubernetesConfig reads local operator configuration, not deployment state.
// TokenFile is read for every request so projected service-account tokens rotate.
func LoadKubernetesConfig(path string) (KubernetesConfig, error) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 || info.Size() > 16384 {
		return KubernetesConfig{}, fmt.Errorf("Kubernetes runtime configuration must be an operator-controlled regular file of at most 16 KiB")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return KubernetesConfig{}, fmt.Errorf("cannot read Kubernetes runtime configuration")
	}
	var cfg KubernetesConfig
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(&cfg) != nil || d.Decode(new(any)) != io.EOF {
		return cfg, fmt.Errorf("invalid Kubernetes runtime configuration")
	}
	for _, p := range []*string{&cfg.CAFile, &cfg.TokenFile} {
		if *p != "" && !filepath.IsAbs(*p) {
			*p = filepath.Join(filepath.Dir(path), *p)
		}
	}
	return cfg, nil
}

type Kubernetes struct {
	cfg    KubernetesConfig
	origin *url.URL
	client *http.Client
}

func runtimeName(s string) bool {
	if len(s) == 0 || len(s) > 63 || s[0] == '-' || s[len(s)-1] == '-' {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

func NewKubernetes(cfg KubernetesConfig) (*Kubernetes, error) {
	if cfg.Pool == "" {
		cfg.Pool = "default"
	}
	if !runtimeName(cfg.ClusterID) || !runtimeName(cfg.Namespace) || !runtimeName(cfg.Pool) {
		return nil, fmt.Errorf("Kubernetes runtime requires DNS-label cluster, namespace and pool identities")
	}
	if cfg.ServerURL == "" {
		host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
		if port == "" {
			port = "443"
		}
		cfg.ServerURL = "https://" + net.JoinHostPort(host, port)
		if cfg.CAFile == "" {
			cfg.CAFile = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
		}
		if cfg.TokenFile == "" {
			cfg.TokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
		}
	}
	u, err := url.Parse(cfg.ServerURL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Path != "" && u.Path != "/" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || cfg.TokenFile == "" {
		return nil, fmt.Errorf("Kubernetes runtime requires a verified HTTPS API origin and local token file")
	}
	u.Path = ""
	trust := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		roots := x509.NewCertPool()
		if err != nil || len(pem) > 1<<20 || !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("invalid Kubernetes server CA")
		}
		trust.RootCAs = roots
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = trust
	transport.MaxConnsPerHost = 4
	transport.MaxIdleConnsPerHost = 2
	transport.DisableCompression = true
	return &Kubernetes{cfg: cfg, origin: u, client: &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (k *Kubernetes) Close()       { k.client.CloseIdleConnections() }
func (k *Kubernetes) Pool() string { return k.cfg.Pool }

type runtimeError struct{ code string }

func (e *runtimeError) Error() string { return "Kubernetes worker runtime: " + e.code }

func runtimeFailure(code string) error { return &runtimeError{code: code} }

func (k *Kubernetes) request(ctx context.Context, method, name string, body any) (map[string]any, int, error) {
	file, err := os.Open(k.cfg.TokenFile)
	if err != nil {
		return nil, 0, runtimeFailure("credential_unavailable")
	}
	data, err := io.ReadAll(io.LimitReader(file, 16385))
	_ = file.Close()
	token := strings.TrimSpace(string(data))
	if err != nil || len(data) > 16384 || token == "" || strings.ContainsAny(token, "\r\n\t ") {
		return nil, 0, runtimeFailure("invalid_credential")
	}
	var encoded []byte
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil || len(encoded) > 1<<20 {
			return nil, 0, runtimeFailure("invalid_deployment")
		}
	}
	u := *k.origin
	u.Path = "/apis/apps/v1/namespaces/" + k.cfg.Namespace + "/deployments"
	if strings.HasPrefix(name, "budget:") {
		u.Path = "/apis/policy/v1/namespaces/" + k.cfg.Namespace + "/poddisruptionbudgets"
		if suffix := strings.TrimPrefix(name, "budget:"); suffix != "" {
			u.Path += "/" + suffix
		}
		name = ""
	}
	if strings.HasPrefix(name, "secret:") {
		if method != http.MethodGet {
			return nil, 0, runtimeFailure("invalid_request")
		}
		u.Path = "/api/v1/namespaces/" + k.cfg.Namespace + "/secrets/" + strings.TrimPrefix(name, "secret:")
		name = ""
	}
	if name != "" {
		u.Path += "/" + name
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(encoded))
	if err != nil {
		return nil, 0, runtimeFailure("invalid_request")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	response, err := k.client.Do(req)
	if err != nil {
		return nil, 0, runtimeFailure("unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode == 404 || response.StatusCode == 409 {
		return nil, response.StatusCode, nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code := "rejected"
		if response.StatusCode == 401 || response.StatusCode == 403 {
			code = "forbidden"
		}
		return nil, response.StatusCode, runtimeFailure(code)
	}
	data, err = io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return nil, response.StatusCode, runtimeFailure("invalid_response")
	}
	object, err := decodeObject(data)
	if err != nil {
		return nil, response.StatusCode, runtimeFailure("invalid_response")
	}
	return object, response.StatusCode, nil
}

func decodeObject(data []byte) (map[string]any, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var object map[string]any
	if d.Decode(&object) != nil || object == nil || d.Decode(new(any)) != io.EOF {
		return nil, fmt.Errorf("invalid object")
	}
	return object, nil
}
func object(v any) map[string]any { m, _ := v.(map[string]any); return m }
func text(v any) string           { s, _ := v.(string); return s }
func number(v any) uint64 {
	n, _ := v.(json.Number)
	out, _ := strconv.ParseUint(string(n), 10, 64)
	return out
}

func deploymentName(cluster string, v db.WorkerDeployment) string {
	sum := sha256.Sum256([]byte(cluster + "\x00" + v.TenantID + "\x00" + v.ID))
	return "djob-worker-" + hex.EncodeToString(sum[:16])
}

func (k *Kubernetes) desired(v db.WorkerDeployment) map[string]any {
	name := deploymentName(k.cfg.ClusterID, v)
	labels := map[string]any{"app.kubernetes.io/managed-by": "djob", "djob.io/instance": name, "djob.io/cluster": k.cfg.ClusterID}
	annotations := map[string]any{"djob.io/cluster": k.cfg.ClusterID, "djob.io/tenant": v.TenantID, "djob.io/deployment": v.ID, "djob.io/worker": v.WorkerID, "djob.io/revision": strconv.FormatUint(v.Revision, 10)}
	args := v.Spec.Args
	if len(args) == 0 {
		args = []string{"--worker-config", "/var/run/djob/credentials/worker.json"}
	}
	env := []any{
		map[string]any{"name": worker.DeploymentIDEnvironment, "value": v.ID},
		map[string]any{"name": worker.DeploymentGenerationEnvironment, "value": strconv.FormatUint(v.Generation, 10)},
		map[string]any{"name": worker.CredentialDirectoryEnvironment, "value": "/var/run/djob/credentials"},
	}
	cpu, memory := strconv.FormatUint(uint64(v.Spec.CPUMillis), 10)+"m", strconv.FormatUint(uint64(v.Spec.MemoryMiB), 10)+"Mi"
	if v.Spec.CPUMillis%1000 == 0 {
		cpu = strconv.FormatUint(uint64(v.Spec.CPUMillis/1000), 10)
	}
	if v.Spec.MemoryMiB%1024 == 0 {
		memory = strconv.FormatUint(uint64(v.Spec.MemoryMiB/1024), 10) + "Gi"
	}
	resources := map[string]any{"cpu": cpu, "memory": memory}
	container := map[string]any{"name": "worker", "image": v.Spec.Image, "imagePullPolicy": "IfNotPresent", "args": args, "env": env,
		"resources":       map[string]any{"requests": resources, "limits": resources},
		"securityContext": map[string]any{"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "capabilities": map[string]any{"drop": []string{"ALL"}}},
		"volumeMounts":    []any{map[string]any{"name": "credentials", "mountPath": "/var/run/djob/credentials", "readOnly": true}, map[string]any{"name": "temporary", "mountPath": "/tmp"}},
	}
	pod := map[string]any{"automountServiceAccountToken": false, "enableServiceLinks": false, "terminationGracePeriodSeconds": v.Spec.DrainSeconds,
		"securityContext": map[string]any{"runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532, "fsGroup": 65532, "fsGroupChangePolicy": "OnRootMismatch", "seccompProfile": map[string]any{"type": "RuntimeDefault"}},
		"containers":      []any{container}, "volumes": []any{map[string]any{"name": "credentials", "secret": map[string]any{"secretName": v.Spec.ConfigSecret, "defaultMode": 256}}, map[string]any{"name": "temporary", "emptyDir": map[string]any{"sizeLimit": "64Mi"}}},
	}
	if k.cfg.SpreadAcrossNodes {
		pod["topologySpreadConstraints"] = []any{map[string]any{"maxSkew": 1, "topologyKey": "kubernetes.io/hostname", "whenUnsatisfiable": "ScheduleAnyway", "labelSelector": map[string]any{"matchLabels": map[string]any{"djob.io/instance": name}}}}
	}
	if v.Spec.PullSecret != "" {
		pod["imagePullSecrets"] = []any{map[string]any{"name": v.Spec.PullSecret}}
	}
	result := map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": name, "namespace": k.cfg.Namespace, "labels": labels, "annotations": annotations},
		"spec": map[string]any{"replicas": v.Spec.Replicas, "revisionHistoryLimit": 2, "progressDeadlineSeconds": 180, "strategy": map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"maxSurge": 1, "maxUnavailable": 0}},
			"selector": map[string]any{"matchLabels": map[string]any{"djob.io/instance": name}}, "template": map[string]any{"metadata": map[string]any{"labels": labels}, "spec": pod}},
	}
	data, _ := json.Marshal(result)
	normalized, _ := decodeObject(data)
	return normalized
}

func (k *Kubernetes) Reconcile(ctx context.Context, v db.WorkerDeployment) (Observation, error) {
	out := Observation{Pool: k.cfg.Pool, Namespace: k.cfg.Namespace, Name: deploymentName(k.cfg.ClusterID, v), Revision: v.Revision, Generation: v.Generation, Desired: v.Spec.Replicas, ObservedAt: time.Now().UTC(), State: "pending"}
	if v.Spec.Pool != k.cfg.Pool {
		return out, runtimeFailure("pool_unavailable")
	}
	if err := v.Spec.Validate(); err != nil {
		return out, runtimeFailure("invalid_deployment")
	}
	desired := k.desired(v)
	configUID := ""
	if k.cfg.RequireImmutableConfig && v.Spec.Replicas > 0 {
		secret, code, err := k.request(ctx, http.MethodGet, "secret:"+v.Spec.ConfigSecret, nil)
		if err != nil {
			return out, err
		}
		if code != 200 || secret["immutable"] != true || text(object(object(secret["metadata"])["labels"])["djob.dev/release-config"]) != "true" {
			return out, runtimeFailure("immutable_config_required")
		}
		configUID = text(object(secret["metadata"])["uid"])
		if configUID == "" {
			return out, runtimeFailure("invalid_response")
		}
		metadata := object(desired["metadata"])
		annotations := object(metadata["annotations"])
		annotations["djob.io/config-secret-uid"] = configUID
		metadata["annotations"] = annotations
	}
	current, code, err := k.request(ctx, http.MethodGet, out.Name, nil)
	if err != nil {
		return out, err
	}
	if code == 404 {
		current, code, err = k.request(ctx, http.MethodPost, "", desired)
	} else {
		if k.cfg.RequireImmutableConfig {
			oldUID := text(object(object(current["metadata"])["annotations"])["djob.io/config-secret-uid"])
			if v.Published && configUID != "" && oldUID != "" && oldUID != configUID {
				return out, runtimeFailure("immutable_config_replaced")
			}
			if configUID == "" && oldUID != "" {
				metadata := object(desired["metadata"])
				annotations := object(metadata["annotations"])
				annotations["djob.io/config-secret-uid"] = oldUID
				metadata["annotations"] = annotations
			}
		}
		metadata := object(current["metadata"])
		annotations := object(metadata["annotations"])
		for _, key := range []string{"djob.io/cluster", "djob.io/tenant", "djob.io/deployment", "djob.io/worker"} {
			if text(annotations[key]) != text(object(object(desired["metadata"])["annotations"])[key]) {
				return out, runtimeFailure("ownership_conflict")
			}
		}
		revision, parseErr := strconv.ParseUint(text(annotations["djob.io/revision"]), 10, 64)
		if parseErr != nil || revision == 0 {
			return out, runtimeFailure("invalid_revision")
		}
		if revision > v.Revision {
			out.State = "superseded"
			return out, nil
		}
		deleting, deletionErr := deploymentDeletionTime(current)
		if deletionErr != nil {
			return out, deletionErr
		}
		if deleting != nil {
			// Deletion cannot be canceled by updating the Deployment. Wait for
			// finalization, then create from a fresh desired record on a later pass.
			if k.cfg.ProtectDisruptions && v.Spec.Replicas > 0 {
				if err := k.reconcileBudget(ctx, v, current); err != nil {
					return out, err
				}
			}
			return observeDeployment(out, current, v.State == "retired")
		}
		if !contains(metadata["annotations"], object(desired["metadata"])["annotations"]) || !containsDeploymentSpec(current["spec"], desired["spec"]) || !contains(metadata["labels"], object(desired["metadata"])["labels"]) || revision != v.Revision {
			if text(metadata["resourceVersion"]) == "" {
				return out, runtimeFailure("invalid_resource_version")
			}
			for _, field := range []string{"labels", "annotations"} {
				existing := object(metadata[field])
				if existing == nil {
					existing = map[string]any{}
				}
				for key, value := range object(object(desired["metadata"])[field]) {
					existing[key] = value
				}
				metadata[field] = existing
			}
			current["spec"] = mergeDeploymentSpec(current["spec"], desired["spec"])
			delete(current, "status")
			current, code, err = k.request(ctx, http.MethodPut, out.Name, current)
		}
	}
	if err != nil {
		return out, err
	}
	if code == 409 {
		out.State = "conflict"
		return out, nil
	}
	if code == 404 {
		return out, runtimeFailure("not_found")
	}
	if !containsDeploymentSpec(current["spec"], desired["spec"]) || !contains(current["metadata"], desired["metadata"]) {
		return out, runtimeFailure("configuration_conflict")
	}
	if k.cfg.ProtectDisruptions && v.Spec.Replicas > 0 {
		if err := k.reconcileBudget(ctx, v, current); err != nil {
			return out, err
		}
	}
	return observeDeployment(out, current, v.State == "retired")
}

// Deployment status describes the rollout, not worker registration or execution
// authority. Ready Pods may still belong to the previous ReplicaSet.
func observeDeployment(out Observation, current map[string]any, retired bool) (Observation, error) {
	generation := number(object(current["metadata"])["generation"])
	if generation == 0 {
		return out, runtimeFailure("invalid_response")
	}
	if value, present := object(current["spec"])["paused"]; present {
		var ok bool
		out.RolloutPaused, ok = value.(bool)
		if !ok {
			return out, runtimeFailure("invalid_response")
		}
	}
	var err error
	out.DeletionRequestedAt, err = deploymentDeletionTime(current)
	if err != nil {
		return out, err
	}
	status := object(current["status"])
	for field, target := range map[string]*uint32{"replicas": &out.Replicas, "readyReplicas": &out.Ready, "availableReplicas": &out.Available, "updatedReplicas": &out.Updated} {
		if value, present := status[field]; present {
			n, ok := value.(json.Number)
			count, err := strconv.ParseUint(string(n), 10, 32)
			if !ok || err != nil {
				return out, runtimeFailure("invalid_response")
			}
			*target = uint32(count)
		}
	}
	if value, present := status["terminatingReplicas"]; present {
		n, ok := value.(json.Number)
		count, err := strconv.ParseUint(string(n), 10, 32)
		if !ok || err != nil {
			return out, runtimeFailure("invalid_response")
		}
		count32 := uint32(count)
		out.Terminating = &count32
	}
	if out.DeletionRequestedAt != nil {
		out.State = "deleting"
		return out, nil
	}
	// A platform pause prevents template rollouts, including a newly committed
	// worker generation. Old ready Pods and progress conditions cannot explain
	// that hold. Scaling to zero and deletion still retain their own status.
	if out.Desired > 0 && out.RolloutPaused {
		out.State = "paused"
		return out, nil
	}
	if number(status["observedGeneration"]) >= generation {
		terminating := out.Terminating != nil && *out.Terminating > 0
		if out.Desired == 0 {
			if out.Replicas != 0 || terminating {
				out.State = "stopping"
				return out, nil
			}
			out.State = "stopped"
			if retired {
				out.State = "retired"
			}
		} else if out.Desired > 0 && out.Replicas == out.Desired && out.Updated == out.Desired && out.Ready >= out.Desired && out.Available >= out.Desired && !terminating {
			out.State = "available"
		} else if out.Desired > 0 {
			conditions, _ := status["conditions"].([]any)
			for _, entry := range conditions {
				condition := object(entry)
				if text(condition["type"]) == "Progressing" && text(condition["status"]) == "False" {
					out.State, out.Error = "blocked", "rollout_failed"
					if text(condition["reason"]) == "ProgressDeadlineExceeded" {
						out.State, out.Error = "stalled", "progress_deadline_exceeded"
					}
					break
				}
				if text(condition["type"]) == "ReplicaFailure" && text(condition["status"]) == "True" {
					out.State, out.Error = "blocked", "replica_failure"
				}
			}
		}
	}
	return out, nil
}

func deploymentDeletionTime(current map[string]any) (*time.Time, error) {
	value := object(current["metadata"])["deletionTimestamp"]
	if value == nil {
		return nil, nil
	}
	timestamp, ok := value.(string)
	if !ok || len(timestamp) == 0 || len(timestamp) > 64 {
		return nil, runtimeFailure("invalid_response")
	}
	at, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		return nil, runtimeFailure("invalid_response")
	}
	at = at.UTC()
	return &at, nil
}
