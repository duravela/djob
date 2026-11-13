package jobs

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func TestCancellationDuringCleanupFinishesAndReleasesKnownQuiescentResource(t *testing.T) {
	b := newBackend(t)
	setupAction(t, b, true)
	cleaning, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	d := &testDriver{execute: func(context.Context, Action) Effect {
		return Effect{Status: db.EffectSucceeded, Data: []byte("written")}
	}, quiet: func(ctx context.Context) error {
		close(cleaning)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	w := worker(t, b, Config{}, Registry{Handlers: map[Version]Handler{{"job", "1"}: oneAction}, Drivers: map[Version]DriverFactory{{"test", "1"}: func(db.JobRun) (Driver, error) { return d, nil }}})
	type reply struct {
		r   Result
		err error
	}
	done := make(chan reply, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		r, err := w.Execute(context.Background(), "t", "r")
		done <- reply{r, err}
	}()
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("cleanup worker did not stop")
		}
	})
	select {
	case <-cleaning:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not start")
	}
	state(t, b, db.JobCommand{Cancel: &db.CancelJob{TenantID: "t", RunID: "r", Reason: "cancel during cleanup"}})
	once.Do(func() { close(release) })
	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.Equal(t, db.JobCancelled, r.r.Run.Status)
	case <-time.After(time.Second):
		t.Fatal("cancelled job did not finish")
	}
	resource, err := b.GetJobResource("resource")
	require.NoError(t, err)
	require.Equal(t, db.ResourceAvailable, resource.State)
	action, err := b.GetJobAction("t", "r", 1)
	require.NoError(t, err)
	require.Equal(t, db.EffectSucceeded, action.Status)
}
