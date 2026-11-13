package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/stretchr/testify/require"
)

type materializationAckBackend struct {
	*runtimeBackend
	waiting, release chan struct{}
	once             sync.Once
	fail             bool
}

func (b *materializationAckBackend) ProposeStateCommand(ctx context.Context, c db.StateCommand) (db.StateResult, error) {
	r, err := b.runtimeBackend.ProposeStateCommand(ctx, c)
	if err == nil && (c.Materialize != nil || (c.Job != nil && c.Job.Materialize != nil)) {
		b.once.Do(func() { close(b.waiting) })
		select {
		case <-b.release:
		case <-ctx.Done():
			return r, ctx.Err()
		}
		if b.fail {
			return r, context.DeadlineExceeded
		}
	}
	return r, err
}

func TestMaterializationObservationRequiresAcknowledgement(t *testing.T) {
	for _, profile := range []string{"gateway", "generic"} {
		for _, fail := range []bool{false, true} {
			name := profile + "/acknowledged"
			if fail {
				name = profile + "/lost"
			}
			t.Run(name, func(t *testing.T) {
				f := newRuntime(t, fake.Options{}, "gateway")
				b := &materializationAckBackend{runtimeBackend: f.backend, waiting: make(chan struct{}), release: make(chan struct{}), fail: fail}
				observations := make(chan MaterializationObservation, 8)
				cfg := MaterializerConfig{OnMaterialized: func(o MaterializationObservation) { observations <- o }}
				start := time.Now().UTC().Add(-time.Minute)
				var tick func(context.Context, time.Time) (MaterializationStats, error)
				if profile == "gateway" {
					m, err := NewMaterializer(cfg, b, func() bool { return true })
					require.NoError(t, err)
					_, err = m.Put(context.Background(), db.ScheduleDefinition{ID: "once", ChannelID: "gateway", Executor: "fake",
						Spec: db.ScheduleSpec{Kind: db.ScheduleOnce, StartAt: start}, MissedPolicy: db.MissedCoalesce}, 0)
					require.NoError(t, err)
					tick = m.Tick
				} else {
					d := genericTriggerDefinition(start)
					d.Timing.Spec.Kind, d.Timing.Spec.Interval = db.ScheduleOnce, 0
					seedGenericTrigger(t, f, d)
					m, err := NewJobTriggerMaterializer(cfg, b, func() bool { return true })
					require.NoError(t, err)
					tick = m.Tick
				}
				done, finished := make(chan error, 1), make(chan struct{})
				go func() { defer close(finished); _, err := tick(context.Background(), time.Now().UTC()); done <- err }()
				var releaseOnce sync.Once
				t.Cleanup(func() {
					releaseOnce.Do(func() { close(b.release) })
					select {
					case <-finished:
					case <-time.After(3 * time.Second):
						t.Error("materializer did not finish")
					}
				})
				select {
				case <-b.waiting:
				case <-time.After(time.Second):
					t.Fatal("acknowledgement did not stall")
				}
				require.Empty(t, observations)
				releaseOnce.Do(func() { close(b.release) })
				err := <-done
				if fail {
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.Empty(t, observations, "locally applied creation without an acknowledgement is not an observation")
				} else {
					require.NoError(t, err)
					require.Len(t, observations, 1)
					o := <-observations
					require.True(t, o.TimeKnown)
					require.GreaterOrEqual(t, o.Lateness, time.Minute)
				}
			})
		}
	}
}

func TestMaterializationObservationExcludesSkippedOccurrencesAndIncludesExpiredRecords(t *testing.T) {
	for _, skip := range []bool{false, true} {
		name := "expired"
		if skip {
			name = "skipped"
		}
		t.Run(name, func(t *testing.T) {
			f := newRuntime(t, fake.Options{}, "gateway")
			var observations []MaterializationObservation
			m, err := NewMaterializer(MaterializerConfig{OnMaterialized: func(o MaterializationObservation) { observations = append(observations, o) }}, f.backend, func() bool { return true })
			require.NoError(t, err)
			d := db.ScheduleDefinition{ID: "once", ChannelID: "gateway", Executor: "fake", StartWindow: time.Second,
				Spec: db.ScheduleSpec{Kind: db.ScheduleOnce, StartAt: time.Now().Add(-time.Minute)}, MissedPolicy: db.MissedCoalesce}
			if skip {
				d.MissedPolicy, d.MisfireGrace = db.MissedSkip, time.Second
			}
			schedule, err := m.Put(context.Background(), d, 0)
			require.NoError(t, err)
			stats, err := m.Tick(context.Background(), time.Now().UTC())
			require.NoError(t, err)
			if skip {
				require.Zero(t, stats.Created)
				require.Empty(t, observations)
			} else {
				require.Equal(t, 1, stats.Created)
				require.Len(t, observations, 1)
				run, err := f.backend.GetRun(db.OccurrenceRunID(schedule.ID, schedule.Revision, schedule.NextAt))
				require.NoError(t, err)
				require.Equal(t, db.RunExpired, run.State)
			}
		})
	}
}
