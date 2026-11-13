package workercontrol

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKubernetesWaitsForDeletionThenRecreatesLatestDesiredState(t *testing.T) {
	for _, test := range []struct {
		name, state, observed string
		replicas              uint32
	}{
		{"running", "active", "available", 1},
		{"stopped", "active", "stopped", 0},
		{"retired", "retired", "retired", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			k, f, _ := kubeRuntime(t)
			v := desiredWorker()
			_, err := k.Reconcile(context.Background(), v)
			require.NoError(t, err)
			f.mu.Lock()
			metadata := object(f.value["metadata"])
			metadata["deletionTimestamp"] = "2026-11-11T12:34:56Z"
			metadata["finalizers"] = []any{"company.example/private-cleanup-marker"}
			f.mu.Unlock()
			out, err := k.Reconcile(context.Background(), v)
			require.NoError(t, err)
			require.Equal(t, "deleting", out.State, "ready replicas cannot make a terminating Deployment available")
			v.Revision, v.Generation, v.State, v.Spec.Replicas = 2, 2, test.state, test.replicas
			v.Spec.Image = "registry.example/company/worker@sha256:" + strings.Repeat("b", 64)
			out, err = k.Reconcile(context.Background(), v)
			require.NoError(t, err)
			require.Equal(t, "deleting", out.State)
			require.Equal(t, test.replicas, out.Desired)
			require.EqualValues(t, 2, out.Ready, "preserve the counts returned for the terminating object")
			data, err := json.Marshal(out)
			require.NoError(t, err)
			require.Contains(t, string(data), `"deletion_requested_at":"2026-11-11T12:34:56Z"`)
			require.NotContains(t, string(data), "private-cleanup-marker")
			f.mu.Lock()
			creates, updates := f.creates, f.updates
			f.value = nil // Kubernetes has finished finalizing the old object.
			f.mu.Unlock()
			require.Equal(t, 1, creates)
			require.Zero(t, updates, "a changed desired record must not update an object being deleted")
			out, err = k.Reconcile(context.Background(), v)
			require.NoError(t, err)
			require.Equal(t, test.observed, out.State)
			value := f.inspect(t)
			require.Equal(t, "2", object(object(value["metadata"])["annotations"])["djob.io/revision"])
			container := listEntry(t, workloadPod(value["spec"])["containers"], "name", "worker")
			require.Equal(t, v.Spec.Image, container["image"])
			data, err = json.Marshal(out)
			require.NoError(t, err)
			require.NotContains(t, string(data), "deletion_requested_at")
			stale, err := k.Reconcile(context.Background(), desiredWorker())
			require.NoError(t, err)
			require.Equal(t, "superseded", stale.State)
			require.Equal(t, 2, f.creates)
			require.Zero(t, f.updates)
		})
	}
}

func TestKubernetesRejectsInvalidDeletionMetadataBeforeWriting(t *testing.T) {
	for _, timestamp := range []any{"", "private-invalid-timestamp", true, json.Number("1")} {
		k, f, _ := kubeRuntime(t)
		v := desiredWorker()
		_, err := k.Reconcile(context.Background(), v)
		require.NoError(t, err)
		f.mu.Lock()
		object(f.value["metadata"])["deletionTimestamp"] = timestamp
		f.mu.Unlock()
		v.Revision, v.Generation, v.Spec.Replicas = 2, 2, 0
		_, err = k.Reconcile(context.Background(), v)
		require.ErrorContains(t, err, "invalid_response")
		require.NotContains(t, err.Error(), "private-invalid-timestamp")
		require.Zero(t, f.updates)
	}
}

func TestKubernetesDeletionDoesNotBypassOwnershipChecks(t *testing.T) {
	k, f, _ := kubeRuntime(t)
	v := desiredWorker()
	_, err := k.Reconcile(context.Background(), v)
	require.NoError(t, err)
	f.mu.Lock()
	metadata := object(f.value["metadata"])
	metadata["deletionTimestamp"] = "2026-11-11T12:34:56Z"
	object(metadata["annotations"])["djob.io/tenant"] = "another-tenant"
	f.mu.Unlock()
	_, err = k.Reconcile(context.Background(), v)
	require.ErrorContains(t, err, "ownership_conflict")
	require.Zero(t, f.updates)
}
