package db

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func enrollmentCommand(id, action string, revision uint64, credential string) StateCommand {
	c := &WorkerEnrollmentCommand{TenantID: "alpha", WorkerID: id, Action: action, ExpectedRevision: revision, Actor: "platform/alice", Reason: "test deployment"}
	if action == "enroll" {
		c.Subject = "company/measurement"
	}
	if credential != "" {
		c.CredentialHash = sha256.Sum256([]byte(credential))
	}
	return StateCommand{WorkerEnrollment: c}
}

func TestWorkerEnrollmentLifecycleIsDurableScopedAndExactlyRetried(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "", "work", "")
	c := enrollmentCommand("worker-one", "enroll", 0, "first-secret")
	c.RequestID, c.At = "enroll-once", h.at
	first := h.ok(c).WorkerEnrollment
	require.Equal(t, uint64(1), first.Revision)
	require.Equal(t, "enabled", first.State)
	require.Equal(t, *first, *h.ok(c).WorkerEnrollment)
	changed := c
	copy := *c.WorkerEnrollment
	changed.WorkerEnrollment = &copy
	copy.CredentialHash = sha256.Sum256([]byte("different"))
	h.reject(changed, "request_conflict")
	_, err := h.db.GetWorkerEnrollment("beta", "worker-one")
	require.Error(t, err)
	h.reject(enrollmentCommand("worker-one", "rotate", 2, "next"), "revision_conflict")
}

func TestWorkerEnrollmentRotationDrainAndRevocationRetainExistingAuthority(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "bus-group", "work", "driver", "shared-bus")
	h.ok(enrollmentCommand("worker-one", "enroll", 0, "first"))
	h.launch("alpha", "work", "run")
	hash := sha256.Sum256([]byte("first"))
	claim := jc(JobCommand{Claim: &ClaimJob{JobOwner: JobOwner{TenantID: "alpha", RunID: "run", AttemptID: "attempt", OwnerID: "remote-worker/1/worker-one/session/incarnation"}, LeaseUntil: h.at.Add(time.Minute)}})
	claim.WorkerAdmission = &WorkerAdmission{WorkerID: "worker-one", CredentialHash: hash, Enrolled: true}
	h.ok(enrollmentCommand("worker-one", "drain", 1, ""))
	h.reject(claim, "worker_denied")
	run, err := h.db.GetJobRun("alpha", "run")
	require.NoError(t, err)
	require.Equal(t, JobReady, run.Status)
	resource, err := h.db.GetJobResource("shared-bus")
	require.NoError(t, err)
	require.Equal(t, ResourceAvailable, resource.State)
	h.ok(enrollmentCommand("worker-one", "resume", 2, ""))
	unguarded := claim
	unguarded.WorkerAdmission = nil
	h.reject(unguarded, "worker_denied")
	h.ok(claim)
	h.ok(enrollmentCommand("worker-one", "rotate", 3, "second"))
	_, err = h.db.LookupWorkerCredential(hash)
	require.Error(t, err)
	current, err := h.db.LookupWorkerCredential(sha256.Sum256([]byte("second")))
	require.NoError(t, err)
	require.Equal(t, "company/measurement", current.Subject)
	h.ok(enrollmentCommand("worker-one", "revoke", 4, ""))
	// Enrollment never releases a physical reservation or prevents lease/cleanup
	// progress for a conversation already admitted before the policy change.
	h.ok(jc(JobCommand{Renew: &RenewJob{JobOwner: claim.Job.Claim.JobOwner, LeaseUntil: h.at.Add(2 * time.Minute)}}))
	resource, err = h.db.GetJobResource("shared-bus")
	require.NoError(t, err)
	require.Equal(t, ResourceOwned, resource.State)
	h.reject(enrollmentCommand("worker-one", "resume", 5, ""), "invalid")
	_, err = h.db.LookupWorkerCredential(sha256.Sum256([]byte("second")))
	require.Error(t, err)
	h.reject(enrollmentCommand("new-worker", "enroll", 0, "first"), "invalid")
}

func TestWorkerEnrollmentPagesSnapshotsAndFormatGuard(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "", "work", "")
	for i := 0; i < 35; i++ {
		h.ok(enrollmentCommand(fmt.Sprintf("worker-%02d", i), "enroll", 0, fmt.Sprintf("credential-%d", i)))
	}
	first, cursor, err := h.db.ListWorkerEnrollments("alpha", "", 32)
	require.NoError(t, err)
	require.Len(t, first, 32)
	require.NotEmpty(t, cursor)
	second, next, err := h.db.ListWorkerEnrollments("alpha", cursor, 32)
	require.NoError(t, err)
	require.Len(t, second, 3)
	require.Empty(t, next)
	foreign, _, err := h.db.ListWorkerEnrollments("beta", cursor, 32)
	require.NoError(t, err)
	require.Empty(t, foreign)
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	path := filepath.Join(t.TempDir(), "restored.db")
	restored, err := NewDB(path)
	require.NoError(t, err)
	require.NoError(t, restored.RestoreAt(bytes.NewReader(snapshot.Bytes()), h.index))
	value, err := restored.LookupWorkerCredential(sha256.Sum256([]byte("credential-34")))
	require.NoError(t, err)
	require.Equal(t, "worker-34", value.WorkerID)
	require.NoError(t, restored.ActivateRecoveryFormat(), "recovery must preserve the newer enrollment format")
	require.NoError(t, restored.Close())
	restored, err = NewDB(path)
	require.NoError(t, err)
	_, err = restored.GetWorkerEnrollment("alpha", "worker-00")
	require.NoError(t, err)
	require.NoError(t, restored.db.Update(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{13}, tx.Bucket(metadataBucket).Get(formatKey))
		return tx.Bucket(metadataBucket).Delete(workerEnrollmentVersionKey)
	}))
	require.NoError(t, restored.Close())
	_, err = NewDB(path)
	require.ErrorContains(t, err, "worker enrollment marker")
}

func TestWorkerEnrollmentFingerprintAndClaimVersion(t *testing.T) {
	c := enrollmentCommand("worker-one", "enroll", 0, "private-secret")
	c.At, c.RequestID = time.Date(2026, 11, 11, 12, 0, 0, 1, time.FixedZone("east", 3600)), "enrollment"
	want, err := c.Fingerprint()
	require.NoError(t, err)
	var data bytes.Buffer
	require.NoError(t, gob.NewEncoder(&data).Encode(c))
	require.NotContains(t, data.String(), "private-secret")
	var decoded StateCommand
	require.NoError(t, gob.NewDecoder(&data).Decode(&decoded))
	got, err := decoded.Fingerprint()
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, WorkerEnrollmentCommandVersion, c.CommandVersion())
	require.True(t, c.NeedsAdmission())
	c = enrollmentCommand("worker-one", "revoke", 1, "")
	require.False(t, c.NeedsAdmission())
}
