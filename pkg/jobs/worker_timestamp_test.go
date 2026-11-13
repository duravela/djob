package jobs

import (
	"context"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func TestWorkerCommitClampsObservedTimeWithoutExtendingLocalLease(t *testing.T) {
	b := newBackend(t)
	prepare(t, b, "t", "group", "job", "", nil)
	launch(t, b, "t", "job", "r")
	w := worker(t, b, Config{}, Registry{Handlers: map[Version]Handler{{Name: "job", Version: "1"}: oneAction}})
	life, stop := context.WithCancel(context.Background())
	defer stop()
	work, stopWork := context.WithCancel(life)
	defer stopWork()
	future := time.Now().UTC().Add(500 * time.Millisecond)
	e := &execution{w: w, lastAt: future, owner: db.JobOwner{TenantID: "t", RunID: "r", AttemptID: "attempt", OwnerID: "owner"}, life: life, stopLife: stop, work: work, stopWork: stopWork}
	result, err := e.commit(func(at time.Time) db.JobCommand {
		return db.JobCommand{Claim: &db.ClaimJob{JobOwner: e.owner, LeaseUntil: at.Add(w.cfg.LeaseDuration)}}
	}, true)
	require.NoError(t, err)
	require.Equal(t, future, result.Job.Run.LastTransition)
	require.False(t, e.deadline.After(time.Now().Add(w.cfg.LeaseDuration-w.cfg.SafetyMargin)), "a future wall timestamp must not grant additional local execution time")
	require.True(t, e.deadline.Before(result.Job.Run.LeaseUntil.Add(-w.cfg.SafetyMargin)), "clamping durable timestamps must not extend the monotonic local deadline")
	_, err = e.commit(func(time.Time) db.JobCommand {
		return db.JobCommand{Advance: &db.AdvanceJob{JobOwner: e.owner, ExpectedStep: 0, Kind: db.JobCheckpoint}}
	}, false)
	require.NoError(t, err, "a subsequent operation must not backdate the acknowledged claim")
	previous := e.deadline
	_, err = e.commit(func(at time.Time) db.JobCommand {
		return db.JobCommand{Renew: &db.RenewJob{JobOwner: e.owner, LeaseUntil: at.Add(w.cfg.LeaseDuration)}}
	}, true)
	require.NoError(t, err)
	require.True(t, e.deadline.Equal(previous), "an unchanged durable lease must not refresh local authority")
	require.True(t, e.safe())
	commands := b.commands[len(b.commands)-3:]
	require.Equal(t, future, commands[0].At)
	require.Equal(t, future, commands[1].At)
	require.NotEqual(t, commands[0].RequestID, commands[1].RequestID)
}
