package workercontrol

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKubernetesPausedRolloutsRemainVisibleWithoutHidingStopsOrDeletion(t *testing.T) {
	for _, test := range []struct {
		name, state       string
		desired, replicas uint32
		observed          string
		deleting, retired bool
	}{
		{"ready workers do not resume a rollout", "paused", 2, 2, "8", false, false},
		{"new template is not observed yet", "paused", 2, 2, "7", false, false},
		{"zero desired replicas still stop", "stopping", 0, 2, "8", false, false},
		{"zero replicas are stopped", "stopped", 0, 0, "8", false, false},
		{"retirement stays visible", "retired", 0, 0, "8", false, true},
		{"deletion takes precedence", "deleting", 2, 2, "8", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			current, err := decodeObject([]byte(`{"metadata":{"generation":8},"spec":{"paused":true},"status":{"observedGeneration":` + test.observed + `,"replicas":0}}`))
			require.NoError(t, err)
			if test.replicas > 0 {
				for _, field := range []string{"replicas", "updatedReplicas", "readyReplicas", "availableReplicas"} {
					object(current["status"])[field] = json.Number("2")
				}
				object(current["status"])["conditions"] = []any{map[string]any{"type": "Progressing", "status": "False", "reason": "ProgressDeadlineExceeded"}}
			}
			if test.deleting {
				object(current["metadata"])["deletionTimestamp"] = "2026-11-11T12:34:56Z"
			}
			out, err := observeDeployment(Observation{State: "pending", Desired: test.desired}, current, test.retired)
			require.NoError(t, err)
			require.Equal(t, test.state, out.State)
			require.Empty(t, out.Error, "paused rollouts must not inherit an earlier progress failure")
			data, err := json.Marshal(out)
			require.NoError(t, err)
			require.Contains(t, string(data), `"rollout_paused":true`)
		})
	}
}

func TestKubernetesPreservesPlatformPauseAcrossManagedUpdates(t *testing.T) {
	k, f, _ := kubeRuntime(t)
	v := desiredWorker()
	_, err := k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	f.mu.Lock()
	object(f.value["spec"])["paused"] = true
	f.mu.Unlock()
	v.Revision, v.Generation = 2, 2
	v.Spec.ConfigSecret = "rotated-config"
	for range 2 {
		out, err := k.Reconcile(context.Background(), v)
		require.NoError(t, err)
		require.Equal(t, "paused", out.State)
	}
	spec := object(f.inspect(t)["spec"])
	require.Equal(t, true, spec["paused"], "djob must not remove an operator's rollout hold")
	require.Equal(t, "rotated-config", object(listEntry(t, workloadPod(spec)["volumes"], "name", "credentials")["secret"])["secretName"])
	require.Equal(t, 1, f.updates, "a paused deployment must not cause repeated writes")
	f.mu.Lock()
	delete(object(f.value["spec"]), "paused")
	f.mu.Unlock()
	out, err := k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	require.Equal(t, "available", out.State)
}

func TestKubernetesRejectsMalformedRolloutPause(t *testing.T) {
	for _, value := range []any{nil, "true", json.Number("1"), []any{}, map[string]any{}} {
		current := map[string]any{"metadata": map[string]any{"generation": json.Number("8")}, "spec": map[string]any{"paused": value}}
		_, err := observeDeployment(Observation{State: "pending"}, current, false)
		require.ErrorContains(t, err, "invalid_response")
	}
}
