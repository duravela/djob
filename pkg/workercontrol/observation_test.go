package workercontrol

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKubernetesReportsOnlyCompletedRolloutsAsAvailable(t *testing.T) {
	for _, test := range []struct {
		name, status, state, code string
		desired                   uint32
		retired                   bool
	}{
		{"complete", `{"observedGeneration":8,"replicas":2,"readyReplicas":2,"availableReplicas":2,"updatedReplicas":2}`, "available", "", 2, false},
		{"old ready Pods mask unready replacements", `{"observedGeneration":8,"replicas":4,"readyReplicas":2,"availableReplicas":2,"updatedReplicas":2}`, "pending", "", 2, false},
		{"ready but not available", `{"observedGeneration":8,"replicas":2,"readyReplicas":2,"availableReplicas":1,"updatedReplicas":2}`, "pending", "", 2, false},
		{"old image still running", `{"observedGeneration":8,"replicas":2,"readyReplicas":2,"availableReplicas":2,"updatedReplicas":1}`, "pending", "", 2, false},
		{"replica creation failed", `{"observedGeneration":8,"replicas":1,"conditions":[{"type":"ReplicaFailure","status":"True","reason":"FailedCreate","message":"secret-marker"}]}`, "blocked", "replica_failure", 2, false},
		{"replica set creation failed", `{"observedGeneration":8,"conditions":[{"type":"Progressing","status":"False","reason":"ReplicaSetCreateError","message":"secret-marker"}]}`, "blocked", "rollout_failed", 2, false},
		{"rollout deadline exceeded", `{"observedGeneration":8,"replicas":2,"conditions":[{"type":"ReplicaFailure","status":"True"},{"type":"Progressing","status":"False","reason":"ProgressDeadlineExceeded","message":"secret-marker"}]}`, "stalled", "progress_deadline_exceeded", 2, false},
		{"previous generation failure", `{"observedGeneration":7,"replicas":2,"conditions":[{"type":"Progressing","status":"False","reason":"ProgressDeadlineExceeded"}]}`, "pending", "", 2, false},
		{"old Pods still terminating", `{"observedGeneration":8,"replicas":2,"readyReplicas":2,"availableReplicas":2,"updatedReplicas":2,"terminatingReplicas":1}`, "pending", "", 2, false},
		{"stopping active Pods", `{"observedGeneration":8,"replicas":1}`, "stopping", "", 0, false},
		{"stopping terminating Pods", `{"observedGeneration":8,"replicas":0,"terminatingReplicas":1}`, "stopping", "", 0, true},
		{"stopped", `{"observedGeneration":8,"replicas":0,"terminatingReplicas":0}`, "stopped", "", 0, false},
		{"retired", `{"observedGeneration":8,"replicas":0}`, "retired", "", 0, true},
		{"surge counts remain visible", `{"observedGeneration":8,"replicas":33,"readyReplicas":33,"availableReplicas":33,"updatedReplicas":32}`, "pending", "", 32, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			current, err := decodeObject([]byte(`{"metadata":{"generation":8},"status":` + test.status + `}`))
			require.NoError(t, err)
			out, err := observeDeployment(Observation{State: "pending", Desired: test.desired}, current, test.retired)
			require.NoError(t, err)
			require.Equal(t, test.state, out.State)
			require.Equal(t, test.code, out.Error)
			data, err := json.Marshal(out)
			require.NoError(t, err)
			require.NotContains(t, string(data), "secret-marker")
			if test.name == "surge counts remain visible" {
				require.EqualValues(t, 33, out.Replicas)
				require.EqualValues(t, 33, out.Ready)
			}
			if test.name == "retired" {
				require.Nil(t, out.Terminating, "omitted optional counts must stay unknown")
			}
		})
	}
}

func TestKubernetesRejectsMalformedStatusInsteadOfReportingStopped(t *testing.T) {
	for _, data := range []string{
		`{"metadata":{},"status":{}}`,
		`{"metadata":{"generation":8},"status":{"observedGeneration":8,"replicas":-1}}`,
		`{"metadata":{"generation":8},"status":{"observedGeneration":8,"terminatingReplicas":"unknown"}}`,
		`{"metadata":{"generation":8},"status":{"observedGeneration":8,"readyReplicas":4294967296}}`,
	} {
		current, err := decodeObject([]byte(data))
		require.NoError(t, err)
		_, err = observeDeployment(Observation{State: "pending"}, current, false)
		require.ErrorContains(t, err, "invalid_response")
	}
}

func TestKubernetesDeletionOverridesUnobservedRolloutsAndAllowsNullableTime(t *testing.T) {
	for _, test := range []struct {
		timestamp       any
		observed, state string
	}{
		{"2026-11-11T12:34:56Z", "7", "deleting"},
		{"2026-11-11T12:34:56Z", "0", "deleting"},
		{nil, "8", "available"},
	} {
		current := map[string]any{"metadata": map[string]any{"generation": json.Number("8"), "deletionTimestamp": test.timestamp}, "status": map[string]any{
			"observedGeneration": json.Number(test.observed), "replicas": json.Number("2"), "readyReplicas": json.Number("2"), "availableReplicas": json.Number("2"), "updatedReplicas": json.Number("2"),
		}}
		out, err := observeDeployment(Observation{State: "pending", Desired: 2}, current, false)
		require.NoError(t, err)
		require.Equal(t, test.state, out.State)
		require.EqualValues(t, 2, out.Ready)
		if test.timestamp == nil {
			require.Nil(t, out.DeletionRequestedAt)
		}
	}
}
