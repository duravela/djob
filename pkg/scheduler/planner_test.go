package scheduler

import (
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func TestIntervalMissedPoliciesKeepAnchoredBoundedOccurrences(t *testing.T) {
	start := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		policy  db.MissedRunPolicy
		offsets []time.Duration
		skipped time.Duration
	}{
		{db.MissedCoalesce, []time.Duration{20 * time.Minute}, 15 * time.Minute},
		{db.MissedReplay, []time.Duration{10 * time.Minute, 15 * time.Minute, 20 * time.Minute}, 5 * time.Minute},
		{db.MissedSkip, nil, 20 * time.Minute},
	} {
		t.Run(string(test.policy), func(t *testing.T) {
			schedule := db.Schedule{ScheduleDefinition: db.ScheduleDefinition{ID: "poll", ChannelID: "gateway", Executor: "fake",
				Spec: db.ScheduleSpec{Kind: db.ScheduleInterval, StartAt: start, Interval: 5 * time.Minute}, MissedPolicy: test.policy, MisfireGrace: time.Second, CatchUpLimit: 3}, Revision: 1, NextAt: start}
			plan, err := Plan(schedule, start.Add(22*time.Minute))
			require.NoError(t, err)
			require.Len(t, plan.Occurrences, len(test.offsets))
			for i, offset := range test.offsets {
				require.Equal(t, start.Add(offset), plan.Occurrences[i])
			}
			require.Equal(t, start.Add(test.skipped), plan.SkippedThrough)
			require.Equal(t, start.Add(25*time.Minute), plan.NextAt)
		})
	}
}

func TestOneTimeAndSkipGrace(t *testing.T) {
	start := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	schedule := db.Schedule{ScheduleDefinition: db.ScheduleDefinition{ID: "once", ChannelID: "gateway", Executor: "fake", Spec: db.ScheduleSpec{Kind: db.ScheduleOnce, StartAt: start}, MissedPolicy: db.MissedSkip, MisfireGrace: time.Second}, Revision: 1, NextAt: start}
	near, err := Plan(schedule, start.Add(500*time.Millisecond))
	require.NoError(t, err)
	require.Equal(t, []time.Time{start}, near.Occurrences)
	require.True(t, near.NextAt.IsZero())
	late, err := Plan(schedule, start.Add(time.Hour))
	require.NoError(t, err)
	require.Empty(t, late.Occurrences)
	require.Equal(t, start, late.SkippedThrough)
}

func TestCronHandlesDSTGapFoldAndLeapCentury(t *testing.T) {
	t.Run("spring gap", func(t *testing.T) {
		definition := db.ScheduleDefinition{ID: "cron", ChannelID: "gateway", Executor: "fake", Spec: db.ScheduleSpec{Kind: db.ScheduleCron, StartAt: time.Date(2026, 3, 7, 7, 30, 0, 0, time.UTC), Cron: "30 2 * * *", TimeZone: "America/New_York"}, MissedPolicy: db.MissedCoalesce}
		put, err := PreparePut(definition, 0, definition.Spec.StartAt)
		require.NoError(t, err)
		plan, err := Plan(db.Schedule{ScheduleDefinition: put.Definition, Revision: 1, NextAt: put.NextAt}, time.Date(2026, 3, 8, 8, 0, 0, 0, time.UTC))
		require.NoError(t, err)
		require.Equal(t, []time.Time{definition.Spec.StartAt}, plan.Occurrences)
		require.Equal(t, time.Date(2026, 3, 9, 6, 30, 0, 0, time.UTC), plan.NextAt)
	})
	t.Run("fall fold", func(t *testing.T) {
		definition := db.ScheduleDefinition{ID: "cron", ChannelID: "gateway", Executor: "fake", Spec: db.ScheduleSpec{Kind: db.ScheduleCron, StartAt: time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC), Cron: "30 1 * * *", TimeZone: "America/New_York"}, MissedPolicy: db.MissedReplay, CatchUpLimit: 2}
		put, err := PreparePut(definition, 0, definition.Spec.StartAt)
		require.NoError(t, err)
		plan, err := Plan(db.Schedule{ScheduleDefinition: put.Definition, Revision: 1, NextAt: put.NextAt}, time.Date(2026, 11, 1, 6, 45, 0, 0, time.UTC))
		require.NoError(t, err)
		require.Equal(t, []time.Time{definition.Spec.StartAt, definition.Spec.StartAt.Add(time.Hour)}, plan.Occurrences)
		require.NotEqual(t, db.OccurrenceRunID("cron", 1, plan.Occurrences[0]), db.OccurrenceRunID("cron", 1, plan.Occurrences[1]))
	})
	t.Run("century leap gap", func(t *testing.T) {
		definition := db.ScheduleDefinition{ID: "leap", ChannelID: "gateway", Executor: "fake", Spec: db.ScheduleSpec{Kind: db.ScheduleCron, StartAt: time.Date(2096, 2, 29, 0, 0, 0, 0, time.UTC), Cron: "0 0 29 2 *", TimeZone: "UTC"}, MissedPolicy: db.MissedCoalesce}
		put, err := PreparePut(definition, 1, time.Date(2097, 1, 1, 0, 0, 0, 0, time.UTC))
		require.NoError(t, err)
		require.Equal(t, time.Date(2104, 2, 29, 0, 0, 0, 0, time.UTC), put.NextAt)
	})
}

func TestCronLongDowntimeKeepsOnlyNewestBoundedWindow(t *testing.T) {
	start := time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)
	definition := db.ScheduleDefinition{ID: "every-minute", ChannelID: "gateway", Executor: "fake", Spec: db.ScheduleSpec{Kind: db.ScheduleCron, StartAt: start, Cron: "* * * * *", TimeZone: "UTC"}, MissedPolicy: db.MissedReplay, CatchUpLimit: 3}
	put, err := PreparePut(definition, 0, start)
	require.NoError(t, err)
	now := time.Date(2026, 11, 4, 12, 5, 20, 0, time.UTC)
	plan, err := Plan(db.Schedule{ScheduleDefinition: put.Definition, Revision: 1, NextAt: put.NextAt}, now)
	require.NoError(t, err)
	require.Equal(t, []time.Time{now.Truncate(time.Minute).Add(-2 * time.Minute), now.Truncate(time.Minute).Add(-time.Minute), now.Truncate(time.Minute)}, plan.Occurrences)
	require.Equal(t, now.Truncate(time.Minute).Add(time.Minute), plan.NextAt)
	require.Equal(t, now.Truncate(time.Minute).Add(-3*time.Minute), plan.SkippedThrough)
}

func TestCalendarValidationAndUpdates(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	d := db.ScheduleDefinition{ID: "schedule", ChannelID: "gateway", Executor: "fake", Spec: db.ScheduleSpec{Kind: db.ScheduleInterval, StartAt: now.Add(-time.Hour), Interval: time.Minute}, MissedPolicy: db.MissedCoalesce}
	put, err := PreparePut(d, 1, now.Add(17*time.Second))
	require.NoError(t, err)
	require.Equal(t, now.Add(time.Minute), put.NextAt)
	for _, spec := range []db.ScheduleSpec{
		{Kind: db.ScheduleCron, StartAt: now, Cron: "61 * * * *", TimeZone: "UTC"},
		{Kind: db.ScheduleCron, StartAt: now, Cron: "* * * * *", TimeZone: "Local"},
		{Kind: db.ScheduleCron, StartAt: now, Cron: "0 0 30 2 *", TimeZone: "UTC"},
		{Kind: db.ScheduleCron, StartAt: now, Cron: "* * * * *", TimeZone: "Not/AZone"},
	} {
		d.Spec = spec
		_, err := PreparePut(d, 0, now)
		require.Error(t, err)
	}
}
