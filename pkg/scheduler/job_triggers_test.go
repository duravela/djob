package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/stretchr/testify/require"
)

func genericTriggerDefinition(start time.Time) db.JobTriggerDefinition {
	return db.JobTriggerDefinition{TenantID: "tenant", ID: "trigger", DefinitionID: "hash", DefinitionRevision: 1, Timing: db.ScheduleTiming{Spec: db.ScheduleSpec{Kind: db.ScheduleInterval, StartAt: start, Interval: time.Minute}, MissedPolicy: db.MissedReplay, CatchUpLimit: 3}}
}

func TestGenericTriggerCalendarHasNoExecutionProfileAndHandlesDST(t *testing.T) {
	start := time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC)
	d := genericTriggerDefinition(start)
	d.Timing.Spec = db.ScheduleSpec{Kind: db.ScheduleCron, StartAt: start, Cron: "30  1 * * *", TimeZone: "America/New_York"}
	d.Timing.CatchUpLimit = 2
	put, err := PrepareJobTrigger(d, 0, start)
	require.NoError(t, err)
	require.Equal(t, "30 1 * * *", put.Definition.Timing.Spec.Cron)
	trigger := db.JobTrigger{JobTriggerDefinition: put.Definition, Revision: 1, NextAt: put.NextAt}
	plan, err := PlanJobTrigger(trigger, start.Add(75*time.Minute))
	require.NoError(t, err)
	require.Equal(t, []time.Time{start, start.Add(time.Hour)}, plan.Plan.Occurrences)
	require.False(t, plan.Plan.DeadlineAware)
	require.False(t, plan.Plan.RetryAware)
	require.NotEqual(t, db.JobOccurrenceRunID("tenant", "trigger", 1, start), db.JobOccurrenceRunID("tenant", "trigger", 1, start.Add(time.Hour)))
	for _, zone := range []string{"Local", "Not/AZone"} {
		d.Timing.Spec.TimeZone = zone
		_, err = PrepareJobTrigger(d, 0, start)
		require.Error(t, err)
	}
}

func TestGenericTriggerMissedPoliciesAndUpdatesShareAnchoredCalendar(t *testing.T) {
	start := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	for _, policy := range []db.MissedRunPolicy{db.MissedReplay, db.MissedCoalesce, db.MissedSkip} {
		t.Run(string(policy), func(t *testing.T) {
			d := genericTriggerDefinition(start)
			d.Timing.MissedPolicy = policy
			put, err := PrepareJobTrigger(d, 0, start)
			require.NoError(t, err)
			plan, err := PlanJobTrigger(db.JobTrigger{JobTriggerDefinition: put.Definition, Revision: 1, NextAt: put.NextAt}, start.Add(22*time.Minute+time.Second*2))
			require.NoError(t, err)
			expected := 1
			if policy == db.MissedReplay {
				expected = 3
			}
			if policy == db.MissedSkip {
				expected = 0
			}
			require.Len(t, plan.Plan.Occurrences, expected)
			require.Equal(t, start.Add(23*time.Minute), plan.Plan.NextAt)
			require.False(t, plan.Plan.SkippedThrough.IsZero())
		})
	}
	d := genericTriggerDefinition(start)
	d.MaxPending = 2
	d.Timing.CatchUpLimit = 0
	put, err := PrepareJobTrigger(d, 7, start.Add(17*time.Second))
	require.NoError(t, err)
	require.Equal(t, 2, put.Definition.Timing.CatchUpLimit)
	require.Equal(t, start.Add(time.Minute), put.NextAt)
	d.Timing.Spec.Interval = -1
	_, err = PrepareJobTrigger(d, 0, start)
	require.Error(t, err)
	d = genericTriggerDefinition(start)
	d.Timing.Spec = db.ScheduleSpec{Kind: db.ScheduleOnce, StartAt: start}
	d.Timing.MissedPolicy = ""
	once, err := PrepareJobTrigger(d, 0, start)
	require.NoError(t, err)
	plan, err := PlanJobTrigger(db.JobTrigger{JobTriggerDefinition: once.Definition, Revision: 1, NextAt: start}, start.Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, []time.Time{start}, plan.Plan.Occurrences)
	require.True(t, plan.Plan.NextAt.IsZero())
}

func seedGenericTrigger(t *testing.T, f *runtimeFixture, d db.JobTriggerDefinition) *db.JobTrigger {
	t.Helper()
	f.state(db.StateCommand{Job: &db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: d.TenantID}}}})
	f.state(db.StateCommand{Job: &db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: d.TenantID, ID: d.DefinitionID, Runtime: db.StepRuntime, Handler: "sha256", HandlerVersion: "1", Input: []byte("input")}}}})
	put, err := PrepareJobTrigger(d, 0, time.Now().UTC())
	require.NoError(t, err)
	return f.state(db.StateCommand{Job: &db.JobCommand{PutTrigger: &put}}).Job.Trigger
}

func (b *runtimeBackend) ListDueJobTriggers(at time.Time, after []byte, limit int) (db.JobTriggerPage, error) {
	page, err := b.DB.ListDueJobTriggers(at, after, limit)
	if b.dueBarrier != nil {
		if b.dueReaders.Add(1) == 2 {
			close(b.dueBarrier)
		}
		<-b.dueBarrier
	}
	return page, err
}

func TestGenericTriggerLostAckRetriesExactEnvelopeAndBackpressureAvoidsProposals(t *testing.T) {
	f := newRuntime(t, fake.Options{})
	at := time.Now().UTC()
	d := genericTriggerDefinition(at)
	d.MaxPending = 3
	seedGenericTrigger(t, f, d)
	leader := false
	var observations []MaterializationObservation
	m, err := NewJobTriggerMaterializer(MaterializerConfig{OnMaterialized: func(o MaterializationObservation) { observations = append(observations, o) }}, f.backend, func() bool { return leader })
	require.NoError(t, err)
	stats, err := m.Tick(context.Background(), at.Add(5*time.Minute))
	require.NoError(t, err)
	require.Zero(t, stats.Created)
	require.Empty(t, observations)
	leader = true
	f.backend.loseAck.Store(true)
	stats, err = m.Tick(context.Background(), at.Add(5*time.Minute))
	require.NoError(t, err)
	require.Equal(t, 3, stats.Created)
	require.Len(t, observations, 3)
	for _, o := range observations {
		require.True(t, o.TimeKnown)
	}
	commands := f.backend.commands
	require.Equal(t, commands[len(commands)-2], commands[len(commands)-1])
	page, err := f.backend.ListJobRuns("tenant", nil, 10)
	require.NoError(t, err)
	require.Len(t, page.Runs, 3)
	for _, offset := range []time.Duration{3 * time.Minute, 4 * time.Minute, 5 * time.Minute} {
		run, err := f.backend.GetJobRun("tenant", db.JobOccurrenceRunID("tenant", "trigger", 1, at.Add(offset)))
		require.NoError(t, err)
		require.Equal(t, at.Add(offset), *run.ScheduledAt)
	}
	before := len(f.backend.commands)
	for range 3 {
		stats, err = m.Tick(context.Background(), at.Add(6*time.Minute))
		require.NoError(t, err)
		require.Zero(t, stats.Created)
	}
	require.Len(t, f.backend.commands, before)
	require.Len(t, observations, 3, "backpressure and retries must not inflate materialization observations")
}

func TestGenericTriggerCompetingMaterializersCreateEachOccurrenceOnce(t *testing.T) {
	f := newRuntime(t, fake.Options{})
	at := time.Now().UTC()
	seedGenericTrigger(t, f, genericTriggerDefinition(at))
	f.backend.dueBarrier = make(chan struct{})
	var observations atomic.Uint32
	cfg := MaterializerConfig{OnMaterialized: func(MaterializationObservation) { observations.Add(1) }}
	a, err := NewJobTriggerMaterializer(cfg, f.backend, func() bool { return true })
	require.NoError(t, err)
	b, err := NewJobTriggerMaterializer(cfg, f.backend, func() bool { return true })
	require.NoError(t, err)
	var wg sync.WaitGroup
	stats := make([]MaterializationStats, 2)
	errs := make([]error, 2)
	for i, m := range []*JobTriggerMaterializer{a, b} {
		wg.Add(1)
		go func(i int, m *JobTriggerMaterializer) {
			defer wg.Done()
			stats[i], errs[i] = m.Tick(context.Background(), at.Add(5*time.Minute))
		}(i, m)
	}
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.Equal(t, 3, stats[0].Created+stats[1].Created)
	require.Equal(t, 1, stats[0].Conflicts+stats[1].Conflicts)
	require.EqualValues(t, 3, observations.Load())
	page, err := f.backend.ListJobRuns("tenant", nil, 10)
	require.NoError(t, err)
	require.Len(t, page.Runs, 3)
}

func TestGenericTriggerSweepDoesNotStarveLaterDueTriggers(t *testing.T) {
	f := newRuntime(t, fake.Options{})
	at := time.Now().UTC()
	d := genericTriggerDefinition(at)
	d.Timing.MissedPolicy = db.MissedCoalesce
	d.MaxPending = 1
	seedGenericTrigger(t, f, d)
	put, err := PrepareJobTrigger(d, 0, at)
	require.NoError(t, err)
	put.Definition.ID = "other"
	f.state(db.StateCommand{Job: &db.JobCommand{PutTrigger: &put}})
	m, err := NewJobTriggerMaterializer(MaterializerConfig{BatchSize: 1}, f.backend, func() bool { return true })
	require.NoError(t, err)
	total := 0
	for range 6 {
		stats, err := m.Tick(context.Background(), at.Add(time.Second))
		require.NoError(t, err)
		total += stats.Created
	}
	require.Equal(t, 2, total)
	before := len(f.backend.commands)
	for range 6 {
		stats, err := m.Tick(context.Background(), at.Add(time.Minute))
		require.NoError(t, err)
		require.Zero(t, stats.Created)
	}
	require.Len(t, f.backend.commands, before, "full triggers must be deferred before proposing durable rejections")
}

func TestEventTriggersHaveNoCalendarDefaultsOrPlan(t *testing.T) {
	at := time.Now().UTC()
	d := db.JobTriggerDefinition{TenantID: "tenant", ID: "event", DefinitionID: "hash", DefinitionRevision: 1, Event: &db.EventTriggerSpec{Source: "meters"}}
	put, err := PrepareJobTrigger(d, 0, at)
	require.NoError(t, err)
	require.True(t, put.NextAt.IsZero())
	require.Equal(t, db.ScheduleTiming{}, put.Definition.Timing)
	_, err = PlanJobTrigger(db.JobTrigger{JobTriggerDefinition: put.Definition, Revision: 1}, at)
	require.Error(t, err)
	d.Timing.Spec.StartAt = at
	_, err = PrepareJobTrigger(d, 0, at)
	require.Error(t, err)
}

type retiringTriggerBackend struct {
	*runtimeBackend
	afterList, afterCapacity func()
}

func (b *retiringTriggerBackend) ListDueJobTriggers(at time.Time, after []byte, limit int) (db.JobTriggerPage, error) {
	page, err := b.runtimeBackend.ListDueJobTriggers(at, after, limit)
	if b.afterList != nil {
		b.afterList()
		b.afterList = nil
	}
	return page, err
}

func (b *retiringTriggerBackend) JobTriggerCapacity(tenant, id string) (int, error) {
	capacity, err := b.runtimeBackend.JobTriggerCapacity(tenant, id)
	if b.afterCapacity != nil {
		b.afterCapacity()
		b.afterCapacity = nil
	}
	return capacity, err
}

func TestGenericTriggerRetiredDuringSweepIsAConflictWithoutCreatingWork(t *testing.T) {
	for _, stage := range []string{"after-list", "after-capacity"} {
		t.Run(stage, func(t *testing.T) {
			f := newRuntime(t, fake.Options{})
			at := time.Now().UTC()
			seedGenericTrigger(t, f, genericTriggerDefinition(at))
			retire := func() {
				f.state(db.StateCommand{Job: &db.JobCommand{RetireTrigger: &db.RetireJobTrigger{TenantID: "tenant", ID: "trigger", ExpectedRevision: 1, Actor: "writer", Reason: "retire during a due scan"}}})
			}
			backend := &retiringTriggerBackend{runtimeBackend: f.backend}
			if stage == "after-list" {
				backend.afterList = retire
			} else {
				backend.afterCapacity = retire
			}
			var observations []MaterializationObservation
			m, err := NewJobTriggerMaterializer(MaterializerConfig{OnMaterialized: func(o MaterializationObservation) { observations = append(observations, o) }}, backend, func() bool { return true })
			require.NoError(t, err)
			stats, err := m.Tick(context.Background(), at.Add(5*time.Minute))
			require.NoError(t, err)
			require.Equal(t, 1, stats.Conflicts)
			require.Zero(t, stats.Created)
			require.Empty(t, observations)
			stats, err = m.Tick(context.Background(), at.Add(6*time.Minute))
			require.NoError(t, err)
			require.Zero(t, stats.Created)
			page, err := f.backend.ListJobRuns("tenant", nil, 10)
			require.NoError(t, err)
			require.Empty(t, page.Runs)
		})
	}
}

func TestJobTriggerTenantStorageQuotaDefersBeforeProposalAndResumesAfterIncrease(t *testing.T) {
	f := newRuntime(t, fake.Options{})
	at := time.Now().UTC()
	d := genericTriggerDefinition(at)
	d.Timing.MissedPolicy = db.MissedCoalesce
	seedGenericTrigger(t, f, d)
	f.state(db.StateCommand{Job: &db.JobCommand{PutLimits: &db.PutJobLimits{Limits: db.JobLimits{TenantID: "tenant", MaxStoredRuns: 1}}}})
	m, err := NewJobTriggerMaterializer(MaterializerConfig{}, f.backend, func() bool { return true })
	require.NoError(t, err)
	stats, err := m.Tick(context.Background(), at.Add(time.Second))
	require.NoError(t, err)
	require.Equal(t, 1, stats.Created)
	before := len(f.backend.commands)
	for range 4 {
		stats, err = m.Tick(context.Background(), at.Add(time.Minute))
		require.NoError(t, err)
		require.Zero(t, stats.Created)
	}
	require.Len(t, f.backend.commands, before, "full tenant storage must be deferred before a proposal")
	f.state(db.StateCommand{Job: &db.JobCommand{PutLimits: &db.PutJobLimits{ExpectedRevision: 1, Limits: db.JobLimits{TenantID: "tenant", MaxStoredRuns: 2}}}})
	total := 0
	for range 3 {
		stats, err = m.Tick(context.Background(), at.Add(time.Minute))
		require.NoError(t, err)
		total += stats.Created
	}
	require.Equal(t, 1, total)
	usage, err := f.backend.GetJobUsage("tenant")
	require.NoError(t, err)
	require.Equal(t, uint64(2), usage.StoredRuns)
}
