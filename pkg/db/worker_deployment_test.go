package db

import (
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func deploymentCommand(id, action string, revision uint64) StateCommand {
	c := &WorkerDeploymentCommand{TenantID: "alpha", ID: id, WorkerID: "worker-one", Action: action, ExpectedRevision: revision, Actor: "platform/alice", Reason: "test worker deployment"}
	if action == "put" {
		c.Spec = &WorkerDeploymentSpec{Image: "registry.example/acme/worker@sha256:" + strings.Repeat("a", 64), ConfigSecret: "worker-one-config", Replicas: 2}
	}
	return StateCommand{WorkerDeployment: c}
}

func TestWorkerDeploymentGenerationGuardsClaimsAndPreservesExistingOwnership(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "bus-group", "work", "driver", "shared-bus")
	h.ok(enrollmentCommand("worker-one", "enroll", 0, "first"))
	created := h.ok(deploymentCommand("line-one", "put", 0)).WorkerDeployment
	require.EqualValues(t, 1, created.Generation)
	require.Equal(t, "default", created.Spec.Pool)
	h.launch("alpha", "work", "run")
	owner := JobOwner{TenantID: "alpha", RunID: "run", AttemptID: "attempt", OwnerID: "remote-worker/1/worker-one/session/incarnation"}
	claim := jc(JobCommand{Claim: &ClaimJob{JobOwner: owner, LeaseUntil: h.at.Add(time.Minute)}})
	claim.WorkerAdmission = &WorkerAdmission{WorkerID: "worker-one", CredentialHash: sha256.Sum256([]byte("first")), Enrolled: true}
	h.reject(claim, "worker_denied") // An external session cannot use a managed identity.
	claim.WorkerAdmission.DeploymentID, claim.WorkerAdmission.DeploymentGeneration = "line-one", 1
	claim.RequestID = "managed-original-claim"
	claim.At = h.at
	require.Equal(t, WorkerDeploymentCommandVersion, claim.CommandVersion())
	h.ok(claim)
	update := deploymentCommand("line-one", "put", 1)
	update.WorkerDeployment.Spec.Image = "registry.example/acme/worker@sha256:" + strings.Repeat("b", 64)
	updated := h.ok(update).WorkerDeployment
	require.EqualValues(t, 2, updated.Generation)
	h.ok(claim) // An exact receipt retry does not acquire another conversation.
	h.ok(jc(JobCommand{Renew: &RenewJob{JobOwner: owner, LeaseUntil: h.at.Add(2 * time.Minute)}}))
	resource, err := h.db.GetJobResource("shared-bus")
	require.NoError(t, err)
	require.Equal(t, ResourceOwned, resource.State)
	require.Error(t, h.db.CheckWorkerDeploymentAdmission("alpha", *claim.WorkerAdmission))
	claim.WorkerAdmission.DeploymentGeneration = 2
	require.NoError(t, h.db.CheckWorkerDeploymentAdmission("alpha", *claim.WorkerAdmission))
	scale := deploymentCommand("line-one", "scale", 2)
	scale.WorkerDeployment.Replicas = 1
	require.EqualValues(t, 2, h.ok(scale).WorkerDeployment.Generation)
	stopped := h.ok(deploymentCommand("line-one", "scale", 3)).WorkerDeployment
	require.EqualValues(t, 3, stopped.Generation)
	require.Error(t, h.db.CheckWorkerDeploymentAdmission("alpha", *claim.WorkerAdmission))
	resource, err = h.db.GetJobResource("shared-bus")
	require.NoError(t, err)
	require.Equal(t, ResourceOwned, resource.State)
	resume := deploymentCommand("line-one", "scale", 4)
	resume.WorkerDeployment.Replicas = 2
	require.EqualValues(t, 3, h.ok(resume).WorkerDeployment.Generation)
	claim.WorkerAdmission.DeploymentGeneration = 3
	require.NoError(t, h.db.CheckWorkerDeploymentAdmission("alpha", *claim.WorkerAdmission))
	h.ok(deploymentCommand("line-one", "retire", 5))
	require.Error(t, h.db.CheckWorkerDeploymentAdmission("alpha", *claim.WorkerAdmission))
	h.reject(deploymentCommand("line-one", "put", 6), "invalid")
	h.reject(deploymentCommand("another-line", "put", 0), "invalid")
}

func TestWorkerDeploymentReceiptsPagesAndExternalClaimCompatibility(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "", "work", "")
	h.ok(enrollmentCommand("worker-one", "enroll", 0, "first"))
	c := deploymentCommand("line-one", "put", 0)
	c.RequestID, c.At = "deployment-once", h.at
	result := h.ok(c)
	require.Equal(t, result.WorkerDeployment, h.ok(c).WorkerDeployment)
	clone := result.Clone()
	clone.WorkerDeployment.Spec.Args = append(clone.WorkerDeployment.Spec.Args, "changed")
	require.Empty(t, result.WorkerDeployment.Spec.Args)
	values, next, err := h.db.ListWorkerDeployments("alpha", "", 32)
	require.NoError(t, err)
	require.Len(t, values, 1)
	require.Empty(t, next)
	values, _, err = h.db.ListWorkerDeployments("beta", "", 32)
	require.NoError(t, err)
	require.Empty(t, values)
	values, _, err = h.db.ListWorkerDeploymentRecords(nil, 32)
	require.NoError(t, err)
	require.Len(t, values, 1)
	legacy := WorkerAdmission{WorkerID: "external", CredentialHash: sha256.Sum256([]byte("static")), Enrolled: true}
	encoded, err := json.Marshal(legacy)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "Deployment")
	claim := jc(JobCommand{Claim: &ClaimJob{JobOwner: JobOwner{TenantID: "alpha", RunID: "run", OwnerID: "owner", AttemptID: "attempt"}, LeaseUntil: h.at.Add(time.Minute)}})
	claim.WorkerAdmission = &legacy
	require.Equal(t, WorkerEnrollmentCommandVersion, claim.CommandVersion())
	legacy.DeploymentGeneration = 1
	require.Error(t, claim.Validate())
	bad := deploymentCommand("bad", "put", 0)
	bad.WorkerDeployment.Spec.Image = "registry.example/acme/worker:latest"
	bad.RequestID, bad.At = "bad", h.at
	require.Error(t, bad.Validate())
}
