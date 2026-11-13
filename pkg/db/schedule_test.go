package db

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/stretchr/testify/require"
)

func putInterval(h *channelHarness, id string) *Schedule {
	h.t.Helper()
	d := ScheduleDefinition{ID: id, ChannelID: "gateway", Executor: "fake", Payload: []byte{0, 255},
		Spec: ScheduleSpec{Kind: ScheduleInterval, StartAt: h.at.Add(-time.Hour), Interval: time.Minute}, MissedPolicy: MissedCoalesce}
	return h.ok(StateCommand{PutSchedule: &PutSchedule{Definition: d, NextAt: d.Spec.StartAt}}).Schedule
}

func TestMaterializationAtomicallyAdvancesCursorAndDeduplicatesOccurrence(t *testing.T) {
	h := newChannelHarness(t)
	h.setup("gateway")
	schedule := putInterval(h, "poll")
	plan, err := IntervalPlan(*schedule, h.at)
	require.NoError(t, err)
	command := StateCommand{RequestID: "materialize", Materialize: &plan}
	first := h.ok(command)
	require.Len(t, first.Runs, 1)
	require.Equal(t, OccurrenceRunID("poll", 1, h.at), first.Runs[0].ID)
	require.Equal(t, []byte{0, 255}, first.Runs[0].Payload)
	require.Equal(t, h.at.Add(time.Minute), first.Schedule.NextAt)
	require.Equal(t, first, h.ok(command), "identical request must return the historical receipt")
	h.reject(StateCommand{Materialize: &plan}, "schedule_conflict")
	page, err := h.db.ListReadyRuns(nil, 10)
	require.NoError(t, err)
	require.Len(t, page.Runs, 1)
	count, err := h.db.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
	ready := first.Runs[0]
	owner := ChannelOwner{ChannelID: "gateway", Epoch: 1, OwnerID: "worker/incarnation-1"}
	h.ok(claim(owner, ready.ID, "attempt"))
	page, err = h.db.ListReadyRuns(nil, 10)
	require.NoError(t, err)
	require.Empty(t, page.Runs)
	count, err = h.db.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestPauseAndRevisionUpdateInvalidateOldPlansAndPreserveExistingRuns(t *testing.T) {
	h := newChannelHarness(t)
	h.setup("gateway")
	schedule := putInterval(h, "poll")
	plan, err := IntervalPlan(*schedule, h.at)
	require.NoError(t, err)
	paused := h.ok(StateCommand{PauseSchedule: &PauseSchedule{ID: "poll", ExpectedRevision: 1, Paused: true}}).Schedule
	require.Equal(t, uint64(2), paused.Revision)
	h.reject(StateCommand{Materialize: &plan}, "schedule_conflict")
	due, err := h.db.ListDueSchedules(h.at, nil, 10)
	require.NoError(t, err)
	require.Empty(t, due.Schedules)
	resumed := h.ok(StateCommand{PauseSchedule: &PauseSchedule{ID: "poll", ExpectedRevision: 2}}).Schedule
	plan, err = IntervalPlan(*resumed, h.at)
	require.NoError(t, err)
	first := h.ok(StateCommand{Materialize: &plan}).Runs[0]
	definition := resumed.ScheduleDefinition
	definition.Payload = []byte("new payload")
	updated := h.ok(StateCommand{PutSchedule: &PutSchedule{Definition: definition, ExpectedRevision: 3, NextAt: h.at.Add(time.Minute)}}).Schedule
	require.Equal(t, uint64(4), updated.Revision)
	original, err := h.db.GetRun(first.ID)
	require.NoError(t, err)
	require.Equal(t, []byte{0, 255}, original.Payload)
	require.Equal(t, uint64(3), original.ScheduleRevision)
	h.reject(StateCommand{PutSchedule: &PutSchedule{Definition: definition, ExpectedRevision: 3, NextAt: h.at.Add(time.Minute)}}, "revision_conflict")
}

func TestMaterializationRollbackIncludesIndexesAndOccurrenceLedger(t *testing.T) {
	h := newChannelHarness(t)
	h.setup("gateway")
	schedule := putInterval(h, "poll")
	plan, err := IntervalPlan(*schedule, h.at)
	require.NoError(t, err)
	command := StateCommand{RequestID: "rollback", At: h.at, Materialize: &plan}
	index := h.db.LastAppliedIndex()
	good := logEntry(t, index+1, Command{Version: SchedulingCommandVersion, Type: CmdState, State: &command})
	bad := logEntry(t, index+2, Command{Version: LegacyCommandVersion, Type: CmdAddJobs, Jobs: []Job{{ID: ""}}})
	result := h.db.ApplyEntries([]commit.Entry{good, bad})
	require.Error(t, result.Error)
	require.Empty(t, result.StateResults)
	require.Equal(t, index, h.db.LastAppliedIndex())
	current, err := h.db.GetSchedule("poll")
	require.NoError(t, err)
	require.Equal(t, schedule.NextAt, current.NextAt)
	page, err := h.db.ListReadyRuns(nil, 10)
	require.NoError(t, err)
	require.Empty(t, page.Runs)
	h.index = index + 2
	require.Len(t, h.ok(command).Runs, 1)
}

func TestReadyCapacityDefersWithoutAdvancingScheduleOrChannelTime(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway")
	schedule := putInterval(h, "poll")
	var entries []commit.Entry
	for i := 0; i < MaxReadyRunsPerChannel; i++ {
		h.index++
		command := StateCommand{RequestID: fmt.Sprintf("fill-%d", i), At: h.at, CreateRun: &CreateRun{ID: fmt.Sprintf("ready-%03d", i), ChannelID: "gateway", Executor: "fake"}}
		entries = append(entries, logEntry(t, h.index, Command{Version: ChannelCommandVersion, Type: CmdState, State: &command}))
	}
	require.NoError(t, h.db.ApplyEntries(entries).Error)
	plan, err := IntervalPlan(*schedule, h.at)
	require.NoError(t, err)
	h.reject(StateCommand{Materialize: &plan}, "backpressure")
	current, err := h.db.GetSchedule("poll")
	require.NoError(t, err)
	require.Equal(t, schedule.NextAt, current.NextAt)
	h.reject(StateCommand{At: h.at.Add(time.Second), CreateRun: &CreateRun{ID: "extra", ChannelID: "gateway", Executor: "fake"}}, "backpressure")
	channel, err := h.db.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, h.at, channel.LastTransition, "rejection must not leak earlier writes")
	h.ok(claim(owner, "ready-000", "attempt"))
	h.ok(StateCommand{Materialize: &plan})
	count, err := h.db.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.Equal(t, uint64(MaxReadyRunsPerChannel), count)
}

func TestSchedulingSnapshotRebuildsOlderReadyStateAndRejectsLostBuckets(t *testing.T) {
	h := newChannelHarness(t)
	h.setup("gateway", "ready")
	schedule := putInterval(h, "poll")
	var data bytes.Buffer
	require.NoError(t, h.db.Snapshot(&data))
	var snapshot Snapshot
	require.NoError(t, gob.NewDecoder(bytes.NewReader(data.Bytes())).Decode(&snapshot))
	var legacy []SnapshotBucket
	for _, bucket := range snapshot.Buckets {
		feature := false
		for _, name := range schedulingBuckets {
			if bytes.Equal(name, bucket.Name) {
				feature = true
			}
		}
		if feature {
			continue
		}
		if bytes.Equal(bucket.Name, metadataBucket) {
			var records []SnapshotRecord
			for _, record := range bucket.Records {
				if !bytes.Equal(record.Key, schedulingVersionKey) {
					records = append(records, record)
				}
			}
			bucket.Records = records
		}
		legacy = append(legacy, bucket)
	}
	// A real previous snapshot cannot contain a schedule. The marker-free image
	// has only the preceding channel/run state, whose ready index is rebuilt.
	snapshot.Buckets = legacy
	encoded, err := encode(snapshot)
	require.NoError(t, err)
	require.NoError(t, h.db.Restore(bytes.NewReader(encoded)))
	first, err := h.db.FirstReadyRun("gateway")
	require.NoError(t, err)
	require.Equal(t, "ready", first.ID)
	count, err := h.db.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
	require.NoError(t, h.db.Restore(bytes.NewReader(data.Bytes())))
	restored, err := h.db.GetSchedule("poll")
	require.NoError(t, err)
	require.Equal(t, schedule, restored)
	require.NoError(t, gob.NewDecoder(bytes.NewReader(data.Bytes())).Decode(&snapshot))
	for i, bucket := range snapshot.Buckets {
		if bytes.Equal(bucket.Name, readyRunsBucket) {
			snapshot.Buckets = append(snapshot.Buckets[:i], snapshot.Buckets[i+1:]...)
			break
		}
	}
	encoded, err = encode(snapshot)
	require.NoError(t, err)
	require.ErrorContains(t, h.db.Restore(bytes.NewReader(encoded)), "missing bucket")
	_, err = h.db.GetSchedule("poll")
	require.NoError(t, err)
}
