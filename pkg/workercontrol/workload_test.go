package workercontrol

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/duravela/djob/sdk/go/worker"
	"github.com/stretchr/testify/require"
)

func (f *kubeFixture) inspect(t *testing.T) map[string]any {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := json.Marshal(f.value)
	require.NoError(t, err)
	value, err := decodeObject(data)
	require.NoError(t, err)
	return value
}

func listEntry(t *testing.T, list any, key, value string) map[string]any {
	t.Helper()
	var found map[string]any
	for _, item := range list.([]any) {
		candidate := object(item)
		if text(candidate[key]) == value {
			require.Nil(t, found, "owned entry %s must not be duplicated", value)
			found = candidate
		}
	}
	require.NotNil(t, found, "missing entry %s", value)
	return found
}

func TestKubernetesUpdatesPreservePlatformConfigurationAndRepairOwnedSettings(t *testing.T) {
	k, f, _ := kubeRuntime(t)
	v := desiredWorker()
	v.Spec.PullSecret = "old-pull"
	_, err := k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	f.mu.Lock()
	spec := object(f.value["spec"])
	template := object(spec["template"])
	object(template["metadata"])["annotations"] = map[string]any{"company.example/policy": "injected"}
	object(object(f.value["metadata"])["labels"])["company.example/tier"] = "iot"
	pod := object(template["spec"])
	pod["nodeSelector"] = map[string]any{"company.example/zone": "gateways"}
	pod["initContainers"] = []any{map[string]any{"name": "company-init", "image": "company.example/init"}}
	pod["containers"] = append(pod["containers"].([]any), map[string]any{"name": "company-sidecar", "image": "company.example/sidecar"})
	container := object(pod["containers"].([]any)[0])
	container["env"] = append(container["env"].([]any), map[string]any{"name": "COMPANY_NODE", "valueFrom": map[string]any{"fieldRef": map[string]any{"fieldPath": "spec.nodeName"}}}, map[string]any{"name": worker.DeploymentGenerationEnvironment, "value": "shadowed-generation"})
	container["volumeMounts"] = append(container["volumeMounts"].([]any), map[string]any{"name": "credentials", "mountPath": "/company/credentials", "readOnly": true}, map[string]any{"name": "company", "mountPath": "/company/data"})
	object(object(container["resources"])["limits"])["ephemeral-storage"] = "8Mi"
	volumes := pod["volumes"].([]any)
	object(object(volumes[1])["emptyDir"])["medium"] = "Memory"
	pod["volumes"] = append(volumes, map[string]any{"name": "company", "emptyDir": map[string]any{}})
	f.mu.Unlock()
	v.Revision, v.Generation = 2, 2
	v.Spec.ConfigSecret, v.Spec.PullSecret, v.Spec.CPUMillis = "new-config", "new-pull", 2000
	v.Spec.Args = []string{"--worker-config", "/var/run/djob/credentials/worker.json", "--reconnect=true"}
	_, err = k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	value := f.inspect(t)
	spec, template = object(value["spec"]), object(object(value["spec"])["template"])
	pod = object(template["spec"])
	require.Equal(t, "injected", object(object(template["metadata"])["annotations"])["company.example/policy"])
	require.Equal(t, "iot", object(object(value["metadata"])["labels"])["company.example/tier"])
	require.Equal(t, "gateways", object(pod["nodeSelector"])["company.example/zone"])
	require.Len(t, pod["initContainers"], 1)
	require.Equal(t, "company.example/sidecar", listEntry(t, pod["containers"], "name", "company-sidecar")["image"])
	container = listEntry(t, pod["containers"], "name", "worker")
	require.Equal(t, []any{"--worker-config", "/var/run/djob/credentials/worker.json", "--reconnect=true"}, container["args"])
	require.Equal(t, "2", listEntry(t, container["env"], "name", worker.DeploymentGenerationEnvironment)["value"])
	require.Contains(t, listEntry(t, container["env"], "name", "COMPANY_NODE"), "valueFrom")
	require.Equal(t, "credentials", listEntry(t, container["volumeMounts"], "mountPath", "/company/credentials")["name"])
	require.Equal(t, "company", listEntry(t, container["volumeMounts"], "mountPath", "/company/data")["name"])
	require.Equal(t, "8Mi", object(object(container["resources"])["limits"])["ephemeral-storage"])
	require.Equal(t, "2", object(object(container["resources"])["limits"])["cpu"])
	require.Equal(t, "new-config", object(listEntry(t, pod["volumes"], "name", "credentials")["secret"])["secretName"])
	require.Equal(t, "Memory", object(listEntry(t, pod["volumes"], "name", "temporary")["emptyDir"])["medium"])
	require.Len(t, pod["imagePullSecrets"], 1)
	require.Equal(t, "new-pull", listEntry(t, pod["imagePullSecrets"], "name", "new-pull")["name"])
	v.Revision, v.Generation, v.Spec.PullSecret = 3, 3, ""
	_, err = k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	value = f.inspect(t)
	pod = object(object(object(value["spec"])["template"])["spec"])
	require.NotContains(t, pod, "imagePullSecrets", "clearing the managed pull Secret must remove its old reference")
	_, err = k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	require.Equal(t, 2, f.updates, "preserved additions must not cause perpetual updates")
}

func TestKubernetesRepairsDuplicateOwnedEnvironmentAndLabels(t *testing.T) {
	k, f, _ := kubeRuntime(t)
	v := desiredWorker()
	_, err := k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	f.mu.Lock()
	container := object(object(object(object(f.value["spec"])["template"])["spec"])["containers"].([]any)[0])
	container["env"] = append(container["env"].([]any), map[string]any{"name": worker.DeploymentGenerationEnvironment, "value": "shadowed-generation"})
	delete(object(object(f.value["metadata"])["labels"]), "app.kubernetes.io/managed-by")
	f.mu.Unlock()
	_, err = k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	value := f.inspect(t)
	container = object(object(object(object(value["spec"])["template"])["spec"])["containers"].([]any)[0])
	require.Equal(t, "1", listEntry(t, container["env"], "name", worker.DeploymentGenerationEnvironment)["value"])
	require.Equal(t, "djob", object(object(value["metadata"])["labels"])["app.kubernetes.io/managed-by"])
}

func TestKubernetesRepairsIncompatibleOwnedSources(t *testing.T) {
	k, f, _ := kubeRuntime(t)
	v := desiredWorker()
	_, err := k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	f.mu.Lock()
	pod := workloadPod(f.value["spec"])
	object(pod["securityContext"])["seccompProfile"] = map[string]any{"type": "Localhost", "localhostProfile": "company/profile.json"}
	container := object(pod["containers"].([]any)[0])
	env := object(container["env"].([]any)[0])
	delete(env, "value")
	env["valueFrom"] = map[string]any{"fieldRef": map[string]any{"fieldPath": "metadata.namespace"}}
	volumes := pod["volumes"].([]any)
	volumes[0] = map[string]any{"name": "credentials", "configMap": map[string]any{"name": "incorrect-config"}}
	f.mu.Unlock()
	_, err = k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	pod = workloadPod(f.inspect(t)["spec"])
	profile := object(object(pod["securityContext"])["seccompProfile"])
	require.Equal(t, "RuntimeDefault", profile["type"])
	require.NotContains(t, profile, "localhostProfile")
	container = listEntry(t, pod["containers"], "name", "worker")
	env = listEntry(t, container["env"], "name", worker.DeploymentIDEnvironment)
	require.Equal(t, v.ID, env["value"])
	require.NotContains(t, env, "valueFrom")
	volume := listEntry(t, pod["volumes"], "name", "credentials")
	require.NotContains(t, volume, "configMap")
	require.Equal(t, v.Spec.ConfigSecret, object(volume["secret"])["secretName"])
}

func TestKubernetesReportsAdmissionConflictsInsteadOfAvailable(t *testing.T) {
	for _, create := range []bool{true, false} {
		t.Run(map[bool]string{true: "create", false: "update"}[create], func(t *testing.T) {
			k, f, _ := kubeRuntime(t)
			v := desiredWorker()
			if !create {
				_, err := k.Reconcile(context.Background(), v)
				require.NoError(t, err)
				v.Revision, v.Generation = 2, 2
			}
			f.mu.Lock()
			f.mutate = func(value map[string]any) {
				container := object(workloadPod(value["spec"])["containers"].([]any)[0])
				object(object(container["resources"])["limits"])["cpu"] = "5m"
			}
			f.mu.Unlock()
			out, err := k.Reconcile(context.Background(), v)
			require.ErrorContains(t, err, "configuration_conflict")
			require.NotEqual(t, "available", out.State)
			f.mu.Lock()
			f.mutate = nil
			f.mu.Unlock()
			out, err = k.Reconcile(context.Background(), v)
			require.NoError(t, err)
			require.Equal(t, "available", out.State)
		})
	}
}
