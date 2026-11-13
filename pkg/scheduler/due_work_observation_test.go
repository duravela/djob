package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/stretchr/testify/require"
)

type dueWorkAckBackend struct {
	*runtimeBackend
	waiting, release chan struct{}
	once             sync.Once
	mode             string
	commands         []db.StateCommand // Read after the controller finishes.
}

func (b *dueWorkAckBackend) ProposeStateCommand(ctx context.Context, c db.StateCommand) (db.StateResult, error) {
	r, err := b.runtimeBackend.ProposeStateCommand(ctx, c)
	if err != nil {
		return r, err
	}
	b.commands = append(b.commands, c)
	b.once.Do(func() { close(b.waiting) })
	select {
	case <-b.release:
	case <-ctx.Done():
		return db.StateResult{}, ctx.Err()
	}
	switch b.mode {
	case "lost_all":
		return db.StateResult{}, context.DeadlineExceeded
	case "lost_once":
		if len(b.commands) == 1 {
			return db.StateResult{}, context.DeadlineExceeded
		}
	case "wrong_fingerprint":
		r.Fingerprint[0] ^= 1
	case "wrong_due":
		if c.ReleaseRetry != nil {
			r.Attempt.RetryAt = r.Attempt.RetryAt.Add(time.Second)
		} else {
			r.Job.Run.WakeAt = r.Job.Run.WakeAt.Add(time.Second)
		}
	case "compacted":
		r.Compacted = true
	}
	return r, nil
}

func seedDueWorkTimer(t *testing.T, f *runtimeFixture) time.Time {
	t.Helper()
	base := time.Now().UTC().Add(-time.Minute)
	due := base.Add(10 * time.Second)
	owner := db.JobOwner{TenantID: "tenant", RunID: "timer", AttemptID: "attempt", OwnerID: "private-owner"}
	commands := []db.JobCommand{
		{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "tenant"}}},
		{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "tenant", ID: "work", Runtime: db.StepRuntime, Handler: "test", HandlerVersion: "1"}}},
		{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: "work", DefinitionRevision: 1, RunID: "timer"}},
		{Claim: &db.ClaimJob{JobOwner: owner, LeaseUntil: base.Add(2 * time.Minute)}},
		{Advance: &db.AdvanceJob{JobOwner: owner, Kind: db.JobWait, WakeAt: due}},
	}
	for i, job := range commands {
		_, err := f.backend.ProposeStateCommand(context.Background(), db.StateCommand{RequestID: fmt.Sprintf("seed/%d", i), At: base.Add(time.Duration(i) * time.Millisecond), Job: &job})
		require.NoError(t, err)
	}
	return due
}

func TestDueWorkObservationRequiresMatchingAcknowledgementAndUsesLocalTime(t *testing.T) {
	for _, profile := range []string{"gateway", "generic"} {
		for _, mode := range []string{"acknowledged", "lost_once", "lost_all", "wrong_fingerprint", "wrong_due", "compacted"} {
			t.Run(profile+"/"+mode, func(t *testing.T) {
				f := newRuntime(t, fake.Options{}, "gateway")
				b := &dueWorkAckBackend{runtimeBackend: f.backend, waiting: make(chan struct{}), release: make(chan struct{}), mode: mode}
				observations := make(chan DueWorkObservation, 4)
				observe := func(o DueWorkObservation) { observations <- o }
				var due time.Time
				var tick func(context.Context, time.Time) (int, error)
				if profile == "gateway" {
					due = waitingRetry(f, "retry", time.Nanosecond).RetryAt
					p, err := NewRetryPromoter(RetryPromoterConfig{OnReleased: observe}, b, func() bool { return true })
					require.NoError(t, err)
					tick = p.Tick
				} else {
					due = seedDueWorkTimer(t, f)
					p, err := NewJobMaintenance(JobMaintenanceConfig{OnWoken: observe}, b, func() bool { return true })
					require.NoError(t, err)
					tick = p.Tick
				}
				var releaseOnce sync.Once
				finished := make(chan struct{})
				var tickErr error
				go func() {
					defer close(finished)
					// Discovery's clock is deliberately unrelated to the local
					// acknowledgement clock used by the observation.
					_, tickErr = tick(context.Background(), time.Now().UTC().Add(time.Hour))
				}()
				t.Cleanup(func() {
					releaseOnce.Do(func() { close(b.release) })
					select {
					case <-finished:
					case <-time.After(3 * time.Second):
						t.Error("due-work controller did not finish")
					}
				})
				select {
				case <-b.waiting:
				case <-time.After(3 * time.Second):
					t.Fatal("due-work acknowledgement did not stall")
				}
				require.Empty(t, observations, "local application without acknowledgement is not a timing observation")
				minimum := time.Since(due)
				releaseOnce.Do(func() { close(b.release) })
				<-finished
				if mode == "acknowledged" || mode == "lost_once" {
					require.NoError(t, tickErr)
					require.Len(t, observations, 1)
					o := <-observations
					require.True(t, o.TimeKnown)
					require.GreaterOrEqual(t, o.Lateness, minimum, "include acknowledgement delivery delay")
					require.LessOrEqual(t, o.Lateness, time.Since(due), "use local acknowledgement time rather than Tick's supplied timestamp")
				} else {
					require.Empty(t, observations)
				}
				if mode == "lost_once" || mode == "lost_all" {
					require.Len(t, b.commands, 2)
					require.Equal(t, b.commands[0], b.commands[1])
				}
			})
		}
	}
}
