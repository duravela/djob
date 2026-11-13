package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func (h *channelHarness) publishedRelease(id, worker string) *WorkerDeployment {
	h.t.Helper()
	h.ok(enrollmentCommand(worker, "enroll", 0, "secret-"+worker))
	c := deploymentCommand(id, "put", 0)
	c.WorkerDeployment.WorkerID = worker
	h.ok(c)
	c = deploymentCommand(id, "publish", 1)
	c.WorkerDeployment.WorkerID = worker
	c.RequestID, c.At = "publish-"+id, h.at
	v := h.ok(c).WorkerDeployment
	require.Equal(h.t, v, h.ok(c).WorkerDeployment, "publication retries are exact")
	return v
}
func releaseDefinition(id, release string) StateCommand {
	return jc(JobCommand{PutDefinition: &PutJobDefinition{Definition: JobDefinition{TenantID: "alpha", ID: id, Runtime: StepRuntime, Handler: "test", HandlerVersion: "1", WorkerRelease: release}}})
}
func releaseClaim(run, worker, release string, generation uint64, at time.Time) StateCommand {
	c := jc(JobCommand{Claim: &ClaimJob{JobOwner: JobOwner{TenantID: "alpha", RunID: run, OwnerID: "worker/" + worker, AttemptID: run + "-attempt"}, LeaseUntil: at.Add(time.Minute)}})
	c.WorkerAdmission = &WorkerAdmission{WorkerID: worker, CredentialHash: sha256.Sum256([]byte("secret-" + worker)), Enrolled: true, DeploymentID: release, DeploymentGeneration: generation}
	return c
}
func TestWorkerReleaseCoexistenceRetirementAndResumption(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "", "legacy", "")
	v1 := h.publishedRelease("release-v1", "worker-v1")
	h.publishedRelease("release-v2", "worker-v2")
	require.EqualValues(t, 1, v1.Generation, "publishing must not interrupt existing conversations")
	h.ok(releaseDefinition("work-v1", "release-v1"))
	h.ok(releaseDefinition("work-v2", "release-v2"))
	h.launch("alpha", "work-v1", "waiting")
	h.launch("alpha", "work-v1", "queued")
	h.launch("alpha", "work-v2", "parallel")
	h.launch("alpha", "legacy", "legacy")
	claim := releaseClaim("waiting", "worker-v1", "release-v1", 1, h.at)
	owner := claim.Job.Claim.JobOwner
	h.reject(releaseClaim("waiting", "worker-v2", "release-v2", 1, h.at), "worker_denied")
	h.reject(jc(JobCommand{Claim: claim.Job.Claim}), "worker_denied")
	h.reject(releaseClaim("legacy", "worker-v2", "release-v2", 1, h.at), "worker_denied")
	h.ok(claim)
	wake := h.at.Add(time.Minute)
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobWait, WakeAt: wake}}))
	c := deploymentCommand("release-v1", "retiring", 2)
	c.WorkerDeployment.WorkerID = "worker-v1"
	retiring := h.ok(c).WorkerDeployment
	require.Equal(t, "retiring", retiring.State)
	require.EqualValues(t, 1, retiring.Generation)
	require.EqualValues(t, 2, retiring.OutstandingRuns)
	h.reject(jc(JobCommand{Launch: &LaunchJob{TenantID: "alpha", DefinitionID: "work-v1", DefinitionRevision: 1, RunID: "new-v1"}}), "release_inactive")
	for _, action := range []string{"scale", "retire"} {
		c := deploymentCommand("release-v1", action, 3)
		c.WorkerDeployment.WorkerID = "worker-v1"
		h.reject(c, "release_busy")
	}
	edit := deploymentCommand("release-v1", "put", 3)
	edit.WorkerDeployment.WorkerID = "worker-v1"
	edit.WorkerDeployment.Spec.Image = "registry.example/acme/worker@sha256:" + strings.Repeat("b", 64)
	h.reject(edit, "release_immutable")
	h.ok(releaseClaim("parallel", "worker-v2", "release-v2", 1, h.at))
	// Ownerless waits retain their release, even after its new-run admission closes.
	h.at = wake
	h.ok(jc(JobCommand{Wake: &WakeJob{TenantID: "alpha", RunID: "waiting", WakeAt: wake}}))
	claim = releaseClaim("waiting", "worker-v1", "release-v1", 1, h.at)
	claim.Job.Claim.AttemptID = "resumed"
	h.ok(claim)
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: claim.Job.Claim.JobOwner, ExpectedStep: 1, Kind: JobComplete, Status: JobSucceeded, Quiescent: true}}))
	h.ok(jc(JobCommand{Cancel: &CancelJob{TenantID: "alpha", RunID: "queued", Reason: "withdraw queued work"}}))
	v, err := h.db.GetWorkerDeployment("alpha", "release-v1")
	require.NoError(t, err)
	require.Zero(t, v.OutstandingRuns)
	c = deploymentCommand("release-v1", "retire", 3)
	c.WorkerDeployment.WorkerID = "worker-v1"
	require.Equal(t, "retired", h.ok(c).WorkerDeployment.State)
	run, err := h.db.GetJobRun("alpha", "waiting")
	require.NoError(t, err)
	require.Equal(t, "release-v1", run.Definition.WorkerRelease)
}

func TestWorkerReleaseUnknownOutcomeAndSnapshotKeepRetirementBlocked(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "", "legacy", "", "bus")
	h.publishedRelease("release-v1", "worker-v1")
	definition := releaseDefinition("work", "release-v1")
	definition.Job.PutDefinition.Definition.Resources = []string{"bus"}
	h.ok(definition)
	h.launch("alpha", "work", "uncertain")
	claim := releaseClaim("uncertain", "worker-v1", "release-v1", 1, h.at)
	h.ok(claim)
	h.at = h.at.Add(time.Minute)
	h.ok(jc(JobCommand{Expire: &ExpireJob{JobOwner: claim.Job.Claim.JobOwner, LeaseUntil: h.at}}))
	v, err := h.db.GetWorkerDeployment("alpha", "release-v1")
	require.NoError(t, err)
	require.EqualValues(t, 1, v.OutstandingRuns)
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	path := filepath.Join(t.TempDir(), "restored.db")
	restored, err := NewDB(path)
	require.NoError(t, err)
	require.NoError(t, restored.RestoreAt(bytes.NewReader(snapshot.Bytes()), h.index))
	require.NoError(t, restored.Close())
	compacted, err := CompactFile(context.Background(), path, FileCompactionOptions{})
	require.NoError(t, err)
	require.Equal(t, byte(17), compacted.Format)
	restored, err = NewDB(path)
	require.NoError(t, err)
	defer restored.Close()
	got, err := restored.GetWorkerDeployment("alpha", "release-v1")
	require.NoError(t, err)
	require.Equal(t, v, got)
	require.NoError(t, restored.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{17}, tx.Bucket(metadataBucket).Get(formatKey))
		return nil
	}))
	c := deploymentCommand("release-v1", "retire", 2)
	c.WorkerDeployment.WorkerID = "worker-v1"
	h.reject(c, "release_busy")
	h.ok(jc(JobCommand{Resolve: &ResolveJob{TenantID: "alpha", RunID: "uncertain", ExpectedStep: 0, Status: JobFailed, Actor: "operator", Reason: "confirmed no outstanding effect"}}))
	require.Equal(t, "retired", h.ok(c).WorkerDeployment.State)
}

func TestWorkerReleaseRetirementBlocksCalendarAndNewEventActivation(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "", "legacy", "")
	h.publishedRelease("release-v1", "worker-v1")
	h.ok(releaseDefinition("work", "release-v1"))
	d := JobTriggerDefinition{TenantID: "alpha", ID: "calendar", DefinitionID: "work", DefinitionRevision: 1, MaxPending: 32, Timing: ScheduleTiming{Spec: ScheduleSpec{Kind: ScheduleInterval, StartAt: h.at, Interval: time.Second}, MissedPolicy: MissedCoalesce}}
	trigger := h.ok(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: d, NextAt: h.at}})).Job.Trigger
	event := d
	event.ID = "events"
	event.Timing = ScheduleTiming{}
	event.Event = &EventTriggerSpec{Source: "producer"}
	h.ok(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: event}}))
	activate := jc(JobCommand{Activate: &ActivateJob{TenantID: "alpha", TriggerID: "events", Source: "producer", EventID: "first", OccurredAt: h.at}})
	accepted := h.ok(activate).Job.Activation
	c := deploymentCommand("release-v1", "retiring", 2)
	c.WorkerDeployment.WorkerID = "worker-v1"
	h.ok(c)
	capacity, err := h.db.JobTriggerCapacity("alpha", "calendar")
	require.NoError(t, err)
	require.Zero(t, capacity, "the planner must skip closed releases without rejected log proposals")
	h.reject(jc(triggerPlan(t, *trigger, h.at)), "release_inactive")
	require.Equal(t, accepted, h.ok(activate).Job.Activation, "accepted event redelivery must keep its original identity")
	fresh := *activate.Job.Activate
	fresh.EventID = "next"
	h.reject(jc(JobCommand{Activate: &fresh}), "release_inactive")
	c = deploymentCommand("release-v1", "resume", 3)
	c.WorkerDeployment.WorkerID = "worker-v1"
	h.ok(c)
	result := h.ok(jc(triggerPlan(t, *trigger, h.at)))
	require.Equal(t, 1, result.Job.Created)
	v, err := h.db.GetWorkerDeployment("alpha", "release-v1")
	require.NoError(t, err)
	require.EqualValues(t, 2, v.OutstandingRuns)
}

func TestWorkerReleaseFormatCoexistsWithOptionalConsoleSessions(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "", "legacy", "")
	h.publishedRelease("release-v1", "worker-v1")
	activation := ConsoleSessionCommand{Action: "activate", At: h.at, Policy: sessionPolicy()}
	require.Nil(t, sessionApply(t, h.db, activation).Rejection)
	policy := sessionPolicy()
	policy.MaxRecords++
	require.Nil(t, sessionApply(t, h.db, ConsoleSessionCommand{Action: "configure", At: h.at, Policy: policy, ExpectedPolicyRevision: 1}).Rejection)
	require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{17}, tx.Bucket(metadataBucket).Get(formatKey))
		return nil
	}))
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	restored, err := NewDB(filepath.Join(t.TempDir(), "restored.db"))
	require.NoError(t, err)
	defer restored.Close()
	require.NoError(t, restored.RestoreAt(bytes.NewReader(snapshot.Bytes()), h.db.LastAppliedIndex()))
	got, err := restored.ConsoleSessionPolicy()
	require.NoError(t, err)
	require.EqualValues(t, 2, got.Revision)
}

func TestWorkerReleaseCommandsRequireTheirVersion(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "", "legacy", "")
	h.ok(enrollmentCommand("worker-one", "enroll", 0, "secret"))
	h.ok(deploymentCommand("release", "put", 0))
	for _, c := range []StateCommand{deploymentCommand("release", "publish", 1), releaseDefinition("work", "release")} {
		require.Equal(t, WorkerReleaseCommandVersion, c.CommandVersion())
		c.RequestID, c.At = "old-version", h.at
		applied := h.db.ApplyEntries([]commit.Entry{logEntry(t, h.index+1, Command{Version: WorkerDeploymentCommandVersion, Type: CmdState, State: &c})})
		require.ErrorContains(t, applied.Error, "requires version 22")
	}
}

func TestProtectedReleaseLifecycleRejectsDisruptionAndKeepsHistoricalReplay(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "", "ordinary", "")
	h.publishedRelease("v1", "w1")
	h.ok(releaseDefinition("work", "v1"))
	h.launch("alpha", "work", "queued")
	for _, action := range []string{"restart", "scale", "put"} {
		c := deploymentCommand("v1", action, 2)
		c.WorkerDeployment.WorkerID = "w1"
		c.WorkerDeployment.ProtectOutstanding = true
		if action == "scale" {
			c.WorkerDeployment.Replicas = 1
		}
		if action == "put" {
			c.WorkerDeployment.Spec.Replicas = 1
		}
		require.Equal(t, WorkerLifecycleCommandVersion, c.CommandVersion())
		h.reject(c, "release_busy")
	}
	require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{18}, tx.Bucket(metadataBucket).Get(formatKey))
		return nil
	}))
	increase := deploymentCommand("v1", "scale", 2)
	increase.WorkerDeployment.WorkerID = "w1"
	increase.WorkerDeployment.ProtectOutstanding = true
	increase.WorkerDeployment.Replicas = 3
	require.EqualValues(t, 3, h.ok(increase).WorkerDeployment.Spec.Replicas)
	historical := deploymentCommand("v1", "restart", 3)
	historical.WorkerDeployment.WorkerID = "w1"
	require.Equal(t, WorkerDeploymentCommandVersion, historical.CommandVersion())
	h.ok(historical)
	h.ok(jc(JobCommand{Cancel: &CancelJob{TenantID: "alpha", RunID: "queued", Reason: "withdraw queued work"}}))
	restart := deploymentCommand("v1", "restart", 4)
	restart.WorkerDeployment.WorkerID = "w1"
	restart.WorkerDeployment.ProtectOutstanding = true
	h.ok(restart)
}
