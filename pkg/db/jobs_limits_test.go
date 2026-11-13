package db

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func (h *channelHarness) limits(v JobLimits) *JobLimits {
	h.t.Helper()
	current, err := h.db.GetJobLimits(v.TenantID)
	require.NoError(h.t, err)
	return h.ok(jc(JobCommand{PutLimits: &PutJobLimits{Limits: v, ExpectedRevision: current.Revision}})).Job.Limits
}
func (h *channelHarness) usage(tenant string) JobUsage {
	h.t.Helper()
	v, err := h.db.GetJobUsage(tenant)
	require.NoError(h.t, err)
	return *v
}

func TestJobLimitsPendingAdmissionIncludesImmediateAndTriggeredUnknownWork(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "group", "work", "", "bus")
	h.limits(JobLimits{TenantID: "tenant", MaxPendingRuns: 2})
	h.launch("tenant", "work", "manual")
	h.eventTrigger("tenant", "ingest", "meters", 2)
	accepted := h.ok(jc(activation("tenant", "ingest", "meters", "reading", h.at))).Job.Activation
	denied := jc(JobCommand{Launch: &LaunchJob{TenantID: "tenant", DefinitionID: "work", DefinitionRevision: 1, RunID: "denied"}})
	h.reject(denied, "quota_exceeded")
	_, err := h.db.GetJobRun("tenant", "denied")
	require.Error(t, err)
	require.Equal(t, uint64(2), h.usage("tenant").PendingRuns)
	require.Equal(t, uint64(2), h.usage("tenant").StoredRuns)
	owner := h.claimJob("tenant", accepted.RunID, "owner")
	h.at = h.at.Add(time.Minute)
	h.ok(jc(JobCommand{Expire: &ExpireJob{JobOwner: owner, LeaseUntil: h.at}}))
	require.Equal(t, uint64(2), h.usage("tenant").PendingRuns, "unknown effects retain admission capacity")
	h.reject(denied, "quota_exceeded")
	h.ok(jc(JobCommand{Resolve: &ResolveJob{TenantID: "tenant", RunID: accepted.RunID, Status: JobFailed, Actor: "operator", Reason: "confirmed outcome"}}))
	h.ok(denied)
	require.Equal(t, uint64(2), h.usage("tenant").PendingRuns)
	require.Equal(t, uint64(3), h.usage("tenant").StoredRuns)
	resource, err := h.db.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, ResourceBlocked, resource.State)
	h.job("other", "", "work", "")
	h.launch("other", "work", "independent")
	require.Equal(t, uint64(1), h.usage("other").PendingRuns)
}

func TestJobLimitsLoweredPolicyAllowsCompletionAndGatewayRelease(t *testing.T) {
	h := newChannelHarness(t)
	h.ok(StateCommand{Configure: &ConfigureChannel{ID: "bus", GatewayID: "bus", Protocol: "fake", Endpoint: "localhost:1"}})
	h.ok(jc(JobCommand{PutResource: &PutJobResource{Spec: ResourceSpec{ID: "bus", ChannelID: "bus", Duplex: "half", Capacity: 1}}}))
	h.job("tenant", "group", "work", "", "bus")
	h.launch("tenant", "work", "one")
	h.launch("tenant", "work", "two")
	owner := h.claimJob("tenant", "one", "owner")
	h.limits(JobLimits{TenantID: "tenant", MaxStoredRuns: 1, MaxPendingRuns: 1})
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobComplete, Status: JobSucceeded, Quiescent: true}}))
	require.Equal(t, uint64(1), h.usage("tenant").PendingRuns)
	require.Equal(t, uint64(2), h.usage("tenant").StoredRuns)
	channel, err := h.db.GetChannel("bus")
	require.NoError(t, err)
	require.Equal(t, ChannelAvailable, channel.State)
	owner = h.claimJob("tenant", "two", "owner-two")
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobComplete, Status: JobSucceeded, Quiescent: true}}))
	h.reject(jc(JobCommand{Launch: &LaunchJob{TenantID: "tenant", DefinitionID: "work", DefinitionRevision: 1, RunID: "three"}}), "quota_exceeded")
	require.Zero(t, h.usage("tenant").PendingRuns, "retained runs keep using stored-run capacity after completion")
	h.limits(JobLimits{TenantID: "tenant"})
	h.launch("tenant", "work", "three")
	require.Equal(t, uint64(3), h.usage("tenant").StoredRuns)
}

func TestJobLimitsInboxRejectsAtomicallyAndCountsConsumedRetention(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "", "work", "")
	h.launch("tenant", "work", "run")
	owner := h.claimJob("tenant", "run", "first")
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobWait, WaitKey: "reply"}}))
	h.limits(JobLimits{TenantID: "tenant", MaxInboxEvents: 1, MaxInboxBytes: 2})
	tooLarge := jc(JobCommand{Signal: &SignalJob{TenantID: "tenant", RunID: "run", EventID: "too-large", Key: "reply", Data: []byte("abc")}})
	h.reject(tooLarge, "quota_exceeded")
	view, err := h.db.GetJobView("tenant", "run")
	require.NoError(t, err)
	require.Equal(t, JobWaiting, view.Run.Status)
	require.Nil(t, view.Event)
	good := jc(JobCommand{Signal: &SignalJob{TenantID: "tenant", RunID: "run", EventID: "accepted", Key: "reply", Data: []byte("ab")}})
	h.ok(good)
	h.ok(good)
	require.Equal(t, uint64(1), h.usage("tenant").InboxEvents)
	require.Equal(t, uint64(2), h.usage("tenant").InboxBytes)
	h.limits(JobLimits{TenantID: "tenant", MaxInboxEvents: 1, MaxInboxBytes: 1})
	owner = h.claimJob("tenant", "run", "second")
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, ExpectedStep: 1, EventID: "accepted", Kind: JobComplete, Status: JobSucceeded, Quiescent: true}}))
	h.ok(good)
	require.Equal(t, uint64(1), h.usage("tenant").InboxEvents)
	require.Equal(t, uint64(2), h.usage("tenant").InboxBytes, "consumption retains stored payload bytes")
	h.launch("tenant", "work", "another")
	h.reject(jc(JobCommand{Signal: &SignalJob{TenantID: "tenant", RunID: "another", EventID: "empty", Key: "reply"}}), "quota_exceeded")
	require.Equal(t, uint64(1), h.usage("tenant").PendingRuns)
}

func TestJobLimitsActivationPayloadAndCountCannotBeEvadedByCompletionOrDuplicates(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "", "work", "")
	h.eventTrigger("tenant", "ingest", "meters", 10)
	h.limits(JobLimits{TenantID: "tenant", MaxActivations: 1, MaxActivationBytes: 2})
	event := activation("tenant", "ingest", "meters", "one", h.at)
	event.Activate.Data = []byte("ab")
	accepted := h.ok(jc(event)).Job.Activation
	h.ok(jc(event))
	h.ok(jc(JobCommand{Cancel: &CancelJob{TenantID: "tenant", RunID: accepted.RunID}}))
	next := activation("tenant", "ingest", "meters", "two", h.at)
	next.Activate.Data = nil
	h.reject(jc(next), "quota_exceeded")
	usage := h.usage("tenant")
	require.Equal(t, uint64(1), usage.Activations)
	require.Equal(t, uint64(2), usage.ActivationBytes)
	require.Zero(t, usage.PendingRuns)
	h.limits(JobLimits{TenantID: "tenant", MaxActivations: 2, MaxActivationBytes: 2})
	next.Activate.Data = []byte("c")
	h.reject(jc(next), "quota_exceeded")
	_, err := h.db.GetJobActivation("tenant", "ingest", "meters", "two")
	require.Error(t, err)
	_, err = h.db.GetJobRun("tenant", EventJobRunID("tenant", "ingest", "meters", "two"))
	require.Error(t, err)
	h.limits(JobLimits{TenantID: "tenant", MaxStoredRuns: 1, MaxActivations: 1, MaxActivationBytes: 1})
	require.Equal(t, accepted, h.ok(jc(event)).Job.Activation, "accepted redelivery is allowed after limits are lowered")
}

func TestJobLimitsMaterializationBatchAndUsageRollbackTogether(t *testing.T) {
	h := newChannelHarness(t)
	trigger := h.trigger("tenant", "batch", 3)
	d := trigger.JobTriggerDefinition
	d.Timing.MissedPolicy = MissedReplay
	d.Timing.CatchUpLimit = 2
	trigger = h.ok(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: d, ExpectedRevision: 1, NextAt: h.at.Add(time.Second)}})).Job.Trigger
	h.limits(JobLimits{TenantID: "tenant", MaxStoredRuns: 1})
	h.at = h.at.Add(2 * time.Second)
	plan := triggerPlan(t, *trigger, h.at)
	h.reject(jc(plan), "quota_exceeded")
	require.Zero(t, h.usage("tenant").StoredRuns)
	require.Zero(t, h.usage("tenant").PendingRuns)
	current, err := h.db.GetJobTrigger("tenant", "batch")
	require.NoError(t, err)
	require.Equal(t, trigger.NextAt, current.NextAt)
	page, err := h.db.ListJobRuns("tenant", nil, 10)
	require.NoError(t, err)
	require.Empty(t, page.Runs)
	h.limits(JobLimits{TenantID: "tenant", MaxStoredRuns: 2})
	require.Equal(t, 2, h.ok(jc(plan)).Job.Created)
	require.Equal(t, uint64(2), h.usage("tenant").StoredRuns)
}

func TestJobLimitsUsageRebuildsForOldWritersAndSurvivesActivatedSnapshot(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "", "work", "")
	h.launch("tenant", "work", "manual")
	h.ok(jc(JobCommand{Signal: &SignalJob{TenantID: "tenant", RunID: "manual", EventID: "event", Key: "reply", Data: []byte("ab")}}))
	h.eventTrigger("tenant", "ingest", "meters", 2)
	h.ok(jc(activation("tenant", "ingest", "meters", "reading", h.at)))
	expected := h.usage("tenant")
	// A pre-v11 writer may leave derived usage stale while format 4 is writable.
	require.NoError(t, h.db.db.Update(func(tx *bbolt.Tx) error {
		return writeRecord(tx, jobUsageBucket, "tenant", &JobUsage{TenantID: "tenant"})
	}))
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	q, err := NewDB(filepath.Join(t.TempDir(), "rebuilt.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	require.NoError(t, q.RestoreAt(&snapshot, h.index))
	actual, err := q.GetJobUsage("tenant")
	require.NoError(t, err)
	require.Equal(t, expected, *actual)
	// Activating a policy makes counters part of the guarded state format.
	h.db = q
	h.limits(JobLimits{TenantID: "tenant", MaxStoredRuns: expected.StoredRuns})
	var activated bytes.Buffer
	require.NoError(t, q.Snapshot(&activated))
	restored, err := NewDB(filepath.Join(t.TempDir(), "guarded.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = restored.Close() })
	require.NoError(t, restored.RestoreAt(&activated, h.index))
	actual, err = restored.GetJobUsage("tenant")
	require.NoError(t, err)
	require.Equal(t, expected, *actual)
	limits, err := restored.GetJobLimits("tenant")
	require.NoError(t, err)
	require.Equal(t, expected.StoredRuns, limits.MaxStoredRuns)
	require.NoError(t, restored.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{5}, tx.Bucket(metadataBucket).Get(formatKey))
		return nil
	}))
	require.NoError(t, restored.db.Update(func(tx *bbolt.Tx) error { return tx.DeleteBucket(jobUsageBucket) }))
	require.ErrorContains(t, restored.db.Update(initializeJobQuotaState), "missing bucket")
}

func TestJobLimitsCASAndRejectionsActivateFormatWithoutChangingOldFingerprints(t *testing.T) {
	h := newChannelHarness(t)
	h.reject(jc(JobCommand{PutLimits: &PutJobLimits{Limits: JobLimits{TenantID: "absent", MaxStoredRuns: 1}}}), "not_found")
	h.job("tenant", "", "work", "")
	defaults, err := h.db.GetJobLimits("tenant")
	require.NoError(t, err)
	require.Equal(t, JobLimits{TenantID: "tenant"}, *defaults)
	applied := h.limits(JobLimits{TenantID: "tenant", MaxStoredRuns: 1})
	require.Equal(t, uint64(1), applied.Revision)
	h.reject(jc(JobCommand{PutLimits: &PutJobLimits{Limits: JobLimits{TenantID: "tenant", MaxStoredRuns: 2}}}), "revision_conflict")
	require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{5}, tx.Bucket(metadataBucket).Get(formatKey))
		return nil
	}))
	limitsRecord, err := h.db.GetJobRecord(JobRecordQuery{Kind: JobLimitsRecord, TenantID: "tenant"})
	require.NoError(t, err)
	require.Equal(t, applied, limitsRecord.Limits)
	_, err = h.db.ListJobRecords(JobRecordQuery{Kind: JobUsageRecord, TenantID: "tenant"}, nil, 1)
	require.Error(t, err)
}
