package db

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func jc(c JobCommand) StateCommand { return StateCommand{Job: &c} }

func (h *channelHarness) job(tenant, group, definition, binding string, resources ...string) {
	h.t.Helper()
	if _, err := h.db.GetJobTenant(tenant); err != nil {
		h.ok(jc(JobCommand{PutTenant: &PutJobTenant{Tenant: JobTenant{ID: tenant}}}))
	}
	for _, id := range resources {
		if _, err := h.db.GetJobResource(id); err != nil {
			h.ok(jc(JobCommand{PutResource: &PutJobResource{Spec: ResourceSpec{ID: id, Duplex: "half", Capacity: 1}}}))
		}
		h.ok(jc(JobCommand{Grant: &PutResourceGrant{Grant: ResourceGrant{TenantID: tenant, ResourceID: id, Allowed: true}}}))
	}
	if group != "" {
		h.ok(jc(JobCommand{PutGroup: &PutJobGroup{Group: JobGroup{TenantID: tenant, ID: group, Runtime: StepRuntime, Resources: resources}}}))
	}
	if binding != "" {
		h.ok(jc(JobCommand{PutBinding: &PutJobBinding{Binding: JobBinding{TenantID: tenant, ID: binding, Driver: "test", DriverVersion: "1"}}}))
	}
	h.ok(jc(JobCommand{PutDefinition: &PutJobDefinition{Definition: JobDefinition{TenantID: tenant, ID: definition, GroupID: group, Runtime: StepRuntime, Handler: "test", HandlerVersion: "1", BindingID: binding}}}))
}

func (h *channelHarness) launch(tenant, definition, id string) *JobRun {
	h.t.Helper()
	return h.ok(jc(JobCommand{Launch: &LaunchJob{TenantID: tenant, DefinitionID: definition, DefinitionRevision: 1, RunID: id}})).Job.Run
}

func (h *channelHarness) claimJob(tenant, id, attempt string) JobOwner {
	h.t.Helper()
	owner := JobOwner{TenantID: tenant, RunID: id, AttemptID: attempt, OwnerID: "worker/" + tenant + "/" + id + "/" + attempt}
	h.ok(jc(JobCommand{Claim: &ClaimJob{JobOwner: owner, LeaseUntil: h.at.Add(time.Minute)}}))
	return owner
}

func TestJobsAreTenantScopedAndLaunchesSnapshotBehavior(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "batch", "hash", "")
	h.job("beta", "batch", "hash", "")
	run := h.launch("alpha", "hash", "same-run")
	h.launch("beta", "hash", "same-run")
	require.Empty(t, run.Resources)
	require.Nil(t, run.Binding)
	owner := h.claimJob("alpha", "same-run", "a1")
	h.reject(jc(JobCommand{Advance: &AdvanceJob{JobOwner: JobOwner{TenantID: "beta", RunID: "same-run", AttemptID: owner.AttemptID, OwnerID: owner.OwnerID}, Kind: JobComplete, Status: JobSucceeded, Quiescent: true}}), "stale_owner")
	h.ok(jc(JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: 1, Definition: JobDefinition{TenantID: "alpha", ID: "hash", Runtime: StepRuntime, Handler: "new-handler", HandlerVersion: "2", Input: []byte("new")}}}))
	run, err := h.db.GetJobRun("alpha", "same-run")
	require.NoError(t, err)
	require.Equal(t, uint64(1), run.Definition.Revision)
	require.Equal(t, "test", run.Definition.Handler)
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, ExpectedStep: 0, Kind: JobComplete, Status: JobSucceeded, Result: []byte("digest"), Quiescent: true}}))
	page, err := h.db.ListJobRuns("beta", nil, 1)
	require.NoError(t, err)
	require.Len(t, page.Runs, 1)
	require.Equal(t, JobReady, page.Runs[0].Status)
	_, err = h.db.ListJobRuns("alpha", []byte(jobKey("beta", "same-run")), 1)
	require.ErrorContains(t, err, "another tenant")
	_, err = h.db.GetJobRun("missing", "same-run")
	require.Error(t, err)
	legacy, err := h.db.ListRuns(nil, 256)
	require.NoError(t, err)
	require.Empty(t, legacy.Runs, "generic jobs must not leak into legacy unscoped APIs")
}

func TestJobNamespaceCannotCollideThroughIdentitySeparators(t *testing.T) {
	h := newChannelHarness(t)
	h.job("a", "", "definition", "")
	h.job("a/b", "", "definition", "")
	h.launch("a", "definition", "b/c")
	h.launch("a/b", "definition", "c")
	page, err := h.db.ListJobRuns("a", nil, 256)
	require.NoError(t, err)
	require.Len(t, page.Runs, 1)
	require.Equal(t, "b/c", page.Runs[0].ID)
}

func TestJobResourceClaimsAreAtomicAndSharedWithLegacyGatewayOwners(t *testing.T) {
	h := newChannelHarness(t)
	h.ok(StateCommand{Configure: &ConfigureChannel{ID: "z-gateway", GatewayID: "z-gateway", Protocol: "fake", Endpoint: "localhost:1"}})
	h.ok(jc(JobCommand{PutResource: &PutJobResource{Spec: ResourceSpec{ID: "z-gateway", ChannelID: "z-gateway", Capacity: 1, Duplex: "half"}}}))
	h.job("alpha", "pollers", "collect", "driver", "a-cpu", "z-gateway")
	h.job("beta", "commands", "collect", "driver", "z-gateway")
	h.launch("alpha", "collect", "r1")
	h.launch("beta", "collect", "r2")
	h.ok(StateCommand{Acquire: &AcquireChannel{ChannelID: "z-gateway", OwnerID: "old-worker", LeaseUntil: h.at.Add(time.Minute)}})
	owner := JobOwner{TenantID: "alpha", RunID: "r1", AttemptID: "a1", OwnerID: "new-worker"}
	h.reject(jc(JobCommand{Claim: &ClaimJob{JobOwner: owner, LeaseUntil: h.at.Add(time.Minute)}}), "channel_busy")
	cpu, err := h.db.GetJobResource("a-cpu")
	require.NoError(t, err)
	require.Equal(t, ResourceAvailable, cpu.State, "earlier resources must not be partially reserved")
	require.Zero(t, cpu.Epoch)
	h.ok(StateCommand{Release: &ReleaseChannel{ChannelOwner: ChannelOwner{ChannelID: "z-gateway", Epoch: 1, OwnerID: "old-worker"}, Quiescent: true}})
	owner = h.claimJob("alpha", "r1", "a1")
	legacyOwner := ChannelOwner{ChannelID: "z-gateway", Epoch: 2, OwnerID: owner.OwnerID}
	h.reject(StateCommand{Release: &ReleaseChannel{ChannelOwner: legacyOwner, Quiescent: true}}, "channel_busy")
	h.ok(StateCommand{EnqueueRun: &CreateRun{ID: "legacy-run", ChannelID: "z-gateway", Executor: "fake"}})
	h.reject(claim(legacyOwner, "legacy-run", "legacy-attempt"), "channel_busy")
	h.reject(StateCommand{Acquire: &AcquireChannel{ChannelID: "z-gateway", ExpectedEpoch: 2, OwnerID: "legacy-other", LeaseUntil: h.at.Add(time.Minute)}}, "channel_busy")
	h.reject(jc(JobCommand{Claim: &ClaimJob{JobOwner: JobOwner{TenantID: "beta", RunID: "r2", AttemptID: "b1", OwnerID: "worker-beta"}, LeaseUntil: h.at.Add(time.Minute)}}), "resource_busy")
	for i, op := range []string{"select", "read", "finish"} {
		started := h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, ExpectedStep: uint64(i), Kind: JobDoAction, Operation: op, State: []byte(fmt.Sprint(i))}}))
		h.ok(jc(JobCommand{CompleteAction: &CompleteJobAction{JobOwner: owner, Sequence: started.Job.Action.Sequence, Status: EffectSucceeded, Result: []byte(op)}}))
		h.reject(jc(JobCommand{Claim: &ClaimJob{JobOwner: JobOwner{TenantID: "beta", RunID: "r2", AttemptID: "b1", OwnerID: "worker-beta"}, LeaseUntil: h.at.Add(time.Minute)}}), "resource_busy")
	}
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, ExpectedStep: 3, Kind: JobComplete, Status: JobSucceeded, Quiescent: true}}))
	h.claimJob("beta", "r2", "b1")
	channel, err := h.db.GetChannel("z-gateway")
	require.NoError(t, err)
	require.Equal(t, uint64(3), channel.Epoch)
}

func TestJobGroupAndBindingRequirementsCannotBeRelaxed(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "group", "job", "driver", "bus")
	h.ok(jc(JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: 1, Definition: JobDefinition{TenantID: "tenant", ID: "job", GroupID: "group", Handler: "test", HandlerVersion: "1", BindingID: "driver"}}}))
	run := h.ok(jc(JobCommand{Launch: &LaunchJob{TenantID: "tenant", DefinitionID: "job", DefinitionRevision: 2, RunID: "r"}})).Job.Run
	require.Equal(t, []string{"bus"}, run.Resources)
	h.ok(jc(JobCommand{Grant: &PutResourceGrant{ExpectedRevision: 1, Grant: ResourceGrant{TenantID: "tenant", ResourceID: "bus", Allowed: false}}}))
	h.reject(jc(JobCommand{Claim: &ClaimJob{JobOwner: JobOwner{TenantID: "tenant", RunID: "r", AttemptID: "a", OwnerID: "worker"}, LeaseUntil: h.at.Add(time.Minute)}}), "resource_denied")
	h.reject(jc(JobCommand{PutResource: &PutJobResource{ExpectedRevision: 1, Spec: ResourceSpec{ID: "bus", Capacity: 2, Duplex: "full"}}}), "invalid")
}

func TestGenericRecoveryPreservesNewLegacyAuthority(t *testing.T) {
	for _, legacyFirst := range []bool{true, false} {
		t.Run(fmt.Sprint(legacyFirst), func(t *testing.T) {
			h := newChannelHarness(t)
			h.ok(StateCommand{Configure: &ConfigureChannel{ID: "bus", GatewayID: "bus", Protocol: "fake", Endpoint: "gateway"}})
			h.ok(jc(JobCommand{PutResource: &PutJobResource{Spec: ResourceSpec{ID: "bus", ChannelID: "bus", Duplex: "half", Capacity: 1}}}))
			h.job("tenant", "group", "job", "driver", "bus")
			h.launch("tenant", "job", "r")
			owner := h.claimJob("tenant", "r", "a1")
			h.at = h.at.Add(time.Minute)
			expire := jc(JobCommand{Expire: &ExpireJob{JobOwner: owner, LeaseUntil: h.at}})
			if !legacyFirst {
				h.ok(expire)
			} else {
				h.ok(StateCommand{Expire: &ExpireChannel{ChannelID: "bus", Epoch: 1}})
			}
			h.ok(StateCommand{ResolveChannel: &ResolveChannel{ChannelID: "bus", Epoch: 1, Actor: "operator", Reason: "prior worker excluded and bus idle", Quiescent: true}})
			h.ok(StateCommand{Acquire: &AcquireChannel{ChannelID: "bus", ExpectedEpoch: 1, OwnerID: "new-legacy-owner", LeaseUntil: h.at.Add(time.Hour)}})
			h.ok(StateCommand{EnqueueRun: &CreateRun{ID: "legacy-run", ChannelID: "bus", Executor: "fake"}})
			h.ok(claim(ChannelOwner{ChannelID: "bus", Epoch: 2, OwnerID: "new-legacy-owner"}, "legacy-run", "legacy-attempt"))
			current, err := h.db.GetChannel("bus")
			require.NoError(t, err)
			if legacyFirst {
				h.ok(expire)
			}
			h.ok(jc(JobCommand{ResolveResource: &ResolveJobResource{ResourceID: "bus", Epoch: 1, Actor: "operator", Reason: "old allocation excluded", Quiescent: true}}))
			after, err := h.db.GetChannel("bus")
			require.NoError(t, err)
			require.Equal(t, current, after)
			run, err := h.db.GetJobRun("tenant", "r")
			require.NoError(t, err)
			require.Equal(t, JobUnknown, run.Status)
		})
	}
}

func TestJobInboxSurvivesWaitAndConsumesExactlyOnce(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "", "job", "driver")
	h.launch("tenant", "job", "r")
	owner := h.claimJob("tenant", "r", "a1")
	h.ok(jc(JobCommand{Signal: &SignalJob{TenantID: "tenant", RunID: "r", EventID: "event-1", Key: "reply-42", Data: []byte("answer")}}))
	wait := h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobWait, WaitKey: "reply-42", State: []byte("awaiting")}})).Job.Run
	require.Equal(t, JobReady, wait.Status, "an early event must wake the new wait")
	require.Empty(t, wait.OwnerID)
	owner = h.claimJob("tenant", "r", "a2")
	view, err := h.db.GetJobView("tenant", "r")
	require.NoError(t, err)
	require.Equal(t, []byte("awaiting"), view.Run.State)
	require.Equal(t, []byte("answer"), view.Event.Data)
	h.reject(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, ExpectedStep: 1, Kind: JobComplete, Status: JobSucceeded, Quiescent: true}}), "not_ready")
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, ExpectedStep: 1, EventID: "event-1", Kind: JobComplete, Status: JobSucceeded, Quiescent: true}}))
	event, err := h.db.GetJobEvent("tenant", "r", "event-1")
	require.NoError(t, err)
	require.False(t, event.ConsumedAt.IsZero())
	h.ok(jc(JobCommand{Signal: &SignalJob{TenantID: "tenant", RunID: "r", EventID: "event-1", Key: "reply-42", Data: []byte("answer")}}))
	h.reject(jc(JobCommand{Signal: &SignalJob{TenantID: "tenant", RunID: "r", EventID: "event-1", Key: "reply-42", Data: []byte("changed")}}), "event_conflict")
}

func TestJobQuotasYieldAndTimersUseCommittedState(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "group", "job", "")
	h.ok(jc(JobCommand{PutTenant: &PutJobTenant{ExpectedRevision: 1, Tenant: JobTenant{ID: "tenant", MaxRunning: 1}}}))
	h.launch("tenant", "job", "r1")
	h.launch("tenant", "job", "r2")
	owner := h.claimJob("tenant", "r1", "a1")
	claim2 := JobCommand{Claim: &ClaimJob{JobOwner: JobOwner{TenantID: "tenant", RunID: "r2", AttemptID: "a2", OwnerID: "worker-2"}, LeaseUntil: h.at.Add(time.Minute)}}
	h.reject(jc(claim2), "quota_exceeded")
	wakeAt := h.at.Add(10 * time.Second)
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobWait, WakeAt: wakeAt, State: []byte("waited")}}))
	h.ok(jc(claim2))
	h.reject(jc(JobCommand{Wake: &WakeJob{TenantID: "tenant", RunID: "r1", WakeAt: wakeAt}}), "not_due")
	h.at = wakeAt
	page, err := h.db.ListDueJobTimers(h.at, nil, 2)
	require.NoError(t, err)
	require.Len(t, page.Runs, 1)
	h.ok(jc(JobCommand{Wake: &WakeJob{TenantID: "tenant", RunID: "r1", WakeAt: wakeAt}}))
	page, err = h.db.ListDueJobTimers(h.at, nil, 2)
	require.NoError(t, err)
	require.Empty(t, page.Runs)
}

func TestJobResourceWaitRetainsReservationAndExpiryBlocksIt(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "group", "job", "driver", "bus")
	h.launch("tenant", "job", "r")
	owner := h.claimJob("tenant", "r", "a1")
	wait := h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobWait, WaitKey: "next", State: []byte("device-session")}})).Job.Run
	require.Equal(t, JobWaiting, wait.Status)
	require.Equal(t, owner.OwnerID, wait.OwnerID)
	h.at = wait.LeaseUntil
	expired := h.ok(jc(JobCommand{Expire: &ExpireJob{JobOwner: owner, LeaseUntil: wait.LeaseUntil}})).Job.Run
	require.Equal(t, JobUnknown, expired.Status)
	require.Equal(t, []byte("device-session"), expired.State)
	resource, err := h.db.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, ResourceBlocked, resource.State)
	h.reject(jc(JobCommand{Renew: &RenewJob{JobOwner: owner, LeaseUntil: h.at.Add(time.Minute)}}), "stale_owner")
	h.reject(jc(JobCommand{ResolveResource: &ResolveJobResource{ResourceID: "bus", Epoch: resource.Epoch, Actor: "operator", Reason: "verified", Quiescent: false}}), "not_quiescent")
	h.ok(jc(JobCommand{ResolveResource: &ResolveJobResource{ResourceID: "bus", Epoch: resource.Epoch, Actor: "operator", Reason: "old process terminated and bus idle", Quiescent: true}}))
	h.reject(jc(JobCommand{Resolve: &ResolveJob{TenantID: "tenant", RunID: "r", ExpectedStep: expired.Step, Status: JobReady, Actor: "operator", Reason: "retry"}}), "invalid")
	h.ok(jc(JobCommand{Resolve: &ResolveJob{TenantID: "tenant", RunID: "r", ExpectedStep: expired.Step, Status: JobFailed, Actor: "operator", Reason: "session lost"}}))
}

func TestJobExpiryReplaysOnlyResourceFreeKnownCheckpoints(t *testing.T) {
	for _, effect := range []EffectStatus{"", EffectAuthorized, EffectSucceeded, EffectUnknown} {
		t.Run(string(effect), func(t *testing.T) {
			h := newChannelHarness(t)
			h.job("tenant", "", "job", "driver")
			h.launch("tenant", "job", "r")
			owner := h.claimJob("tenant", "r", "a1")
			if effect != "" {
				h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobDoAction, Operation: "submit"}}))
				if effect != EffectAuthorized {
					h.ok(jc(JobCommand{CompleteAction: &CompleteJobAction{JobOwner: owner, Sequence: 1, Status: effect, Result: []byte("receipt")}}))
				}
			}
			h.at = h.at.Add(time.Minute)
			run := h.ok(jc(JobCommand{Expire: &ExpireJob{JobOwner: owner, LeaseUntil: h.at}})).Job.Run
			if effect == EffectAuthorized || effect == EffectUnknown {
				require.Equal(t, JobUnknown, run.Status)
			} else {
				require.Equal(t, JobReady, run.Status)
			}
			if effect == EffectSucceeded {
				require.Equal(t, EffectSucceeded, run.PendingAction.Status)
			}
		})
	}
}

func TestJobCancellationPreservesEffectsAndPreventsNewActions(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "", "job", "driver")
	h.launch("tenant", "job", "r")
	owner := h.claimJob("tenant", "r", "a1")
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobDoAction, Operation: "write"}}))
	cancel := jc(JobCommand{Cancel: &CancelJob{TenantID: "tenant", RunID: "r", Reason: "stop"}})
	cancel.At = h.at.Add(10 * time.Millisecond)
	h.ok(cancel)
	completion := jc(JobCommand{CompleteAction: &CompleteJobAction{JobOwner: owner, Sequence: 1, Status: EffectSucceeded, Result: []byte("written")}})
	completion.At = h.at.Add(5 * time.Millisecond) // Prepared before cancellation, committed afterward.
	h.ok(completion)
	h.at = h.at.Add(20 * time.Millisecond)
	h.reject(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, ExpectedStep: 1, Kind: JobDoAction, Operation: "another"}}), "cancel_requested")
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, ExpectedStep: 1, Kind: JobComplete, Status: JobCancelled, Quiescent: true}}))
	action, err := h.db.GetJobAction("tenant", "r", 1)
	require.NoError(t, err)
	require.Equal(t, EffectSucceeded, action.Status)
	page, err := h.db.ListExpiredJobClaims(h.at.Add(time.Hour), nil, 10)
	require.NoError(t, err)
	require.Empty(t, page.Runs)
}

func TestGenericJobFingerprintRoundTripsAndClonesNestedData(t *testing.T) {
	command := StateCommand{RequestID: "request", At: time.Now(), Job: &JobCommand{Advance: &AdvanceJob{JobOwner: JobOwner{TenantID: "t", RunID: "r", AttemptID: "a", OwnerID: "o"}, Kind: JobWait, State: []byte{}, WakeAt: time.Now().Add(time.Minute)}}}
	want, err := command.Fingerprint()
	require.NoError(t, err)
	var data bytes.Buffer
	require.NoError(t, gob.NewEncoder(&data).Encode(command))
	var decoded StateCommand
	require.NoError(t, gob.NewDecoder(&data).Decode(&decoded))
	got, err := decoded.Fingerprint()
	require.NoError(t, err)
	require.Equal(t, want, got)
	decoded.Job.Advance.Operation = string([]byte{0xff})
	require.Error(t, decoded.Validate())
	r := StateResult{Job: &JobResult{Run: &JobRun{Definition: JobDefinition{Input: []byte("input")}, PendingAction: &JobAction{Result: []byte("result")}, Resources: []string{"bus"}}}}
	clone := r.Clone()
	clone.Job.Run.Definition.Input[0] = 'x'
	clone.Job.Run.PendingAction.Result[0] = 'x'
	clone.Job.Run.Resources[0] = "changed"
	require.Equal(t, "input", string(r.Job.Run.Definition.Input))
	require.Equal(t, "result", string(r.Job.Run.PendingAction.Result))
	require.Equal(t, "bus", r.Job.Run.Resources[0])
}

func TestGenericJobActivationAndSnapshotFormatGuard(t *testing.T) {
	h := newChannelHarness(t)
	h.reject(jc(JobCommand{Launch: &LaunchJob{TenantID: "absent", DefinitionID: "job", DefinitionRevision: 1, RunID: "r"}}), "not_found")
	require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{2}, tx.Bucket(metadataBucket).Get(formatKey))
		return nil
	}))
	h.job("tenant", "", "job", "driver")
	h.launch("tenant", "job", "r")
	owner := h.claimJob("tenant", "r", "a1")
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobWait, WaitKey: "callback", State: []byte("checkpoint")}}))
	h.ok(jc(JobCommand{Signal: &SignalJob{TenantID: "tenant", RunID: "r", EventID: "e", Key: "callback", Data: []byte("payload")}}))
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	restored, err := NewDB(filepath.Join(t.TempDir(), "restored.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = restored.Close() })
	require.NoError(t, restored.RestoreAt(&snapshot, h.index))
	view, err := restored.GetJobView("tenant", "r")
	require.NoError(t, err)
	require.Equal(t, []byte("checkpoint"), view.Run.State)
	require.Equal(t, []byte("payload"), view.Event.Data)
	require.NoError(t, restored.db.Update(func(tx *bbolt.Tx) error { return tx.Bucket(metadataBucket).Delete(jobStateVersionKey) }))
	require.ErrorContains(t, restored.db.Update(initializeJobState), "missing its format marker")
}
