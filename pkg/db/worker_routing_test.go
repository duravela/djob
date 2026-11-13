package db

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
	"time"
)

func TestWeightedWorkerRoutingPinsChoiceAndSurvivesPolicyChanges(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "", "ordinary", "")
	h.publishedRelease("v1", "w1")
	h.publishedRelease("v2", "w2")
	definition := releaseDefinition("weighted", "")
	definition.Job.PutDefinition.Definition.WorkerReleaseWeights = map[string]uint32{"v1": 7000, "v2": 3000}
	require.Equal(t, WorkerRoutingCommandVersion, definition.CommandVersion())
	h.ok(definition)
	counts := map[string]int{}
	for n := 0; n < 200; n++ {
		id := fmt.Sprintf("r-%03d", n)
		h.launch("alpha", "weighted", id)
		run, err := h.db.GetJobRun("alpha", id)
		require.NoError(t, err)
		require.Equal(t, routedRelease("alpha", id, definition.Job.PutDefinition.Definition), run.Definition.WorkerRelease)
		counts[run.Definition.WorkerRelease]++
	}
	require.Greater(t, counts["v1"], 110)
	require.Greater(t, counts["v2"], 35)
	old, err := h.db.GetJobRun("alpha", "r-000")
	require.NoError(t, err)
	definition.Job.PutDefinition.ExpectedRevision = 1
	definition.Job.PutDefinition.Definition.WorkerReleaseWeights = map[string]uint32{"v2": 10000}
	h.ok(definition)
	retained, err := h.db.GetJobRun("alpha", "r-000")
	require.NoError(t, err)
	require.Equal(t, old.Definition, retained.Definition)
	h.ok(jc(JobCommand{Launch: &LaunchJob{TenantID: "alpha", DefinitionID: "weighted", DefinitionRevision: 2, RunID: "new-policy"}}))
	now, err := h.db.GetJobRun("alpha", "new-policy")
	require.NoError(t, err)
	require.Equal(t, "v2", now.Definition.WorkerRelease)
	worker := "w1"
	if old.Definition.WorkerRelease == "v2" {
		worker = "w2"
	}
	h.ok(releaseClaim("r-000", worker, old.Definition.WorkerRelease, 1, h.at))
}

func TestWeightedWorkerRoutingRejectsInvalidPolicies(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "", "ordinary", "")
	h.publishedRelease("v1", "w1")
	for _, weights := range []map[string]uint32{{"": 10000}, {"v1": 9999}, {"v1": 0}, {"v1": 10001}} {
		c := releaseDefinition("bad", "")
		c.Job.PutDefinition.Definition.WorkerReleaseWeights = weights
		h.reject(c, "invalid")
	}
	c := releaseDefinition("bad", "v1")
	c.Job.PutDefinition.Definition.WorkerReleaseWeights = map[string]uint32{"v1": 10000}
	h.reject(c, "invalid")
}

func TestWeightedScheduledRoutesAndDeduplicationSurviveRestore(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "", "ordinary", "")
	h.publishedRelease("v1", "w1")
	h.publishedRelease("v2", "w2")
	def := releaseDefinition("weighted", "")
	def.Job.PutDefinition.Definition.WorkerReleaseWeights = map[string]uint32{"v1": 5000, "v2": 5000}
	h.ok(def)
	d := JobTriggerDefinition{TenantID: "alpha", ID: "calendar", DefinitionID: "weighted", DefinitionRevision: 1, MaxPending: 32, Timing: ScheduleTiming{Spec: ScheduleSpec{Kind: ScheduleInterval, StartAt: h.at, Interval: time.Second}, MissedPolicy: MissedCoalesce}}
	trigger := h.ok(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: d, NextAt: h.at}})).Job.Trigger
	event := d
	event.ID = "events"
	event.Timing = ScheduleTiming{}
	event.Event = &EventTriggerSpec{Source: "producer"}
	h.ok(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: event}}))
	activation := jc(JobCommand{Activate: &ActivateJob{TenantID: "alpha", TriggerID: "events", Source: "producer", EventID: "one", OccurredAt: h.at}})
	accepted := h.ok(activation).Job.Activation
	calendar := jc(triggerPlan(t, *trigger, h.at))
	h.ok(calendar)
	ids := []string{accepted.RunID, JobOccurrenceRunID("alpha", "calendar", 1, h.at)}
	original := map[string]*JobRun{}
	for _, id := range ids {
		run, err := h.db.GetJobRun("alpha", id)
		require.NoError(t, err)
		require.Equal(t, routedRelease("alpha", id, run.Definition), run.Definition.WorkerRelease)
		original[id] = run
	}
	def.Job.PutDefinition.ExpectedRevision = 1
	def.Job.PutDefinition.Definition.WorkerReleaseWeights = map[string]uint32{"v2": 10000}
	h.ok(def)
	retiring := deploymentCommand("v1", "retiring", 2)
	retiring.WorkerDeployment.WorkerID = "w1"
	h.ok(retiring)
	require.Equal(t, accepted, h.ok(jc(*activation.Job)).Job.Activation)
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	restored, err := NewDB(filepath.Join(t.TempDir(), "restored.db"))
	require.NoError(t, err)
	defer restored.Close()
	require.NoError(t, restored.RestoreAt(&snapshot, h.index))
	for _, id := range ids {
		run, err := restored.GetJobRun("alpha", id)
		require.NoError(t, err)
		require.Equal(t, original[id], run)
	}
	h.reject(jc(JobCommand{Activate: &ActivateJob{TenantID: "alpha", TriggerID: "events", Source: "producer", EventID: "two", OccurredAt: h.at}}), "release_inactive")
}
