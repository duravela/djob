package jobs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func dispatchFixture(t *testing.T, b *testBackend, tenant string) {
	t.Helper()
	state(t, b, db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: tenant}}})
	state(t, b, db.JobCommand{PutBinding: &db.PutJobBinding{Binding: db.JobBinding{TenantID: tenant, ID: "driver", Driver: "test", DriverVersion: "1"}}})
	prepare(t, b, tenant, "group", "job", "driver", nil)
}

func TestDispatchBoundsTasksAndSkipsLocallyFullTenants(t *testing.T) {
	b := newBackend(t)
	dispatchFixture(t, b, "a")
	dispatchFixture(t, b, "b")
	for i := range 12 {
		launch(t, b, "a", "job", fmt.Sprintf("r-%03d", i))
	}
	launch(t, b, "b", "job", "r")
	started := make(chan string, 10)
	release := make(chan struct{})
	var once sync.Once
	registry := Registry{Handlers: map[Version]Handler{{"job", "1"}: oneAction}, Drivers: map[Version]DriverFactory{{"test", "1"}: func(db.JobRun) (Driver, error) {
		return &testDriver{execute: func(ctx context.Context, a Action) Effect {
			started <- a.TenantID
			select {
			case <-release:
				return Effect{Status: db.EffectSucceeded}
			case <-ctx.Done():
				return Effect{Status: db.EffectFailed, Error: ctx.Err().Error()}
			}
		}}, nil
	}}}
	d, err := NewDispatcher(b, DispatchConfig{ScanLimit: 1, MaxConcurrent: 2, MaxPerTenant: 1}, Config{}, registry)
	require.NoError(t, err)
	t.Cleanup(func() { once.Do(func() { close(release) }); require.NoError(t, d.Close(context.Background())) })
	n, err := d.Tick(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, n)
	select {
	case tenant := <-started:
		require.Equal(t, "a", tenant)
	case <-time.After(time.Second):
		t.Fatal("first tenant did not start")
	}
	n, err = d.Tick(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, n)
	select {
	case tenant := <-started:
		require.Equal(t, "b", tenant)
	case <-time.After(time.Second):
		t.Fatal("full first tenant hid the second tenant")
	}
	n, err = d.Tick(context.Background())
	require.NoError(t, err)
	require.Zero(t, n)
	require.Equal(t, 2, d.Active())
	for _, tenant := range []string{"a", "b", "unrelated"} {
		active, paused, closed := d.InspectTenant(tenant)
		if tenant == "unrelated" {
			require.Zero(t, active, "inspection must not expose other tenants' tasks")
		} else {
			require.Equal(t, 1, active)
		}
		require.False(t, paused)
		require.False(t, closed)
	}
	once.Do(func() { close(release) })
	require.Eventually(t, func() bool { return d.Active() == 0 }, time.Second, time.Millisecond)
}

func TestDispatchersContendThroughOneCommittedClaim(t *testing.T) {
	b := newBackend(t)
	setupAction(t, b, false)
	var calls atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	registry := Registry{Handlers: map[Version]Handler{{"job", "1"}: oneAction}, Drivers: map[Version]DriverFactory{{"test", "1"}: func(db.JobRun) (Driver, error) {
		return &testDriver{execute: func(context.Context, Action) Effect {
			if calls.Add(1) == 1 {
				close(started)
			}
			<-release
			return Effect{Status: db.EffectSucceeded}
		}}, nil
	}}}
	a, err := NewDispatcher(b, DispatchConfig{}, Config{}, registry)
	require.NoError(t, err)
	c, err := NewDispatcher(b, DispatchConfig{}, Config{}, registry)
	require.NoError(t, err)
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		require.NoError(t, a.Close(context.Background()))
		require.NoError(t, c.Close(context.Background()))
	})
	var ticks sync.WaitGroup
	ticks.Add(2)
	go func() { defer ticks.Done(); _, _ = a.Tick(context.Background()) }()
	go func() { defer ticks.Done(); _, _ = c.Tick(context.Background()) }()
	ticks.Wait()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("no execution started")
	}
	once.Do(func() { close(release) })
	require.Eventually(t, func() bool { return a.Active()+c.Active() == 0 }, time.Second, time.Millisecond)
	require.EqualValues(t, 1, calls.Load())
	view, err := b.GetJobView("t", "r")
	require.NoError(t, err)
	require.EqualValues(t, 1, view.Run.AttemptCount)
}

func TestDispatchSkipsUnsupportedWorkWithoutClaimingIt(t *testing.T) {
	b := newBackend(t)
	prepare(t, b, "tenant", "", "hash", "", nil)
	launch(t, b, "tenant", "hash", "r")
	d, err := NewDispatcher(b, DispatchConfig{}, Config{}, Builtins())
	require.NoError(t, err)
	defer d.Close(context.Background())
	n, err := d.Tick(context.Background())
	require.NoError(t, err)
	require.Zero(t, n)
	run, err := b.GetJobRun("tenant", "r")
	require.NoError(t, err)
	require.Zero(t, run.AttemptCount)
	require.Equal(t, db.JobReady, run.Status)
}

func TestDispatchCloseWaitsForDriverAndCleanup(t *testing.T) {
	b := newBackend(t)
	setupAction(t, b, true)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	registry := Registry{Handlers: map[Version]Handler{{"job", "1"}: oneAction}, Drivers: map[Version]DriverFactory{{"test", "1"}: func(db.JobRun) (Driver, error) {
		return &testDriver{execute: func(context.Context, Action) Effect {
			close(started)
			<-release
			return Effect{Status: db.EffectSucceeded}
		}}, nil
	}}}
	d, err := NewDispatcher(b, DispatchConfig{}, Config{}, registry)
	require.NoError(t, err)
	t.Cleanup(func() { once.Do(func() { close(release) }); require.NoError(t, d.Close(context.Background())) })
	_, err = d.Tick(context.Background())
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("driver did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, d.Close(ctx), context.DeadlineExceeded)
	resource, err := b.GetJobResource("resource")
	require.NoError(t, err)
	require.Equal(t, db.ResourceOwned, resource.State)
	once.Do(func() { close(release) })
	require.NoError(t, d.Close(context.Background()))
	resource, err = b.GetJobResource("resource")
	require.NoError(t, err)
	require.Equal(t, db.ResourceAvailable, resource.State)
}

func TestAdmissionRejectionDoesNotCreateImmediateRetryLoop(t *testing.T) {
	b := newBackend(t)
	setupAction(t, b, true)
	resource := state(t, b, db.JobCommand{Launch: &db.LaunchJob{TenantID: "t", DefinitionID: "job", DefinitionRevision: 1, RunID: "holder"}}).Job.Run
	state(t, b, db.JobCommand{Claim: &db.ClaimJob{JobOwner: db.JobOwner{TenantID: "t", RunID: resource.ID, AttemptID: "holder", OwnerID: "holder"}, LeaseUntil: time.Now().Add(time.Minute)}})
	claims := make(chan struct{}, 16)
	fault := &faultBackend{testBackend: b, before: func(_ context.Context, c db.StateCommand) error {
		if c.Job.Claim != nil && c.Job.Claim.RunID == "r" {
			claims <- struct{}{}
		}
		return nil
	}}
	registry := Registry{Handlers: map[Version]Handler{{"job", "1"}: oneAction}, Drivers: map[Version]DriverFactory{{"test", "1"}: func(db.JobRun) (Driver, error) { return nil, errors.New("must not create a driver") }}}
	d, err := NewDispatcher(fault, DispatchConfig{PollInterval: time.Hour}, Config{}, registry)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()
	t.Cleanup(func() { cancel(); require.NoError(t, d.Close(context.Background())); <-done })
	select {
	case <-claims:
	case <-time.After(time.Second):
		t.Fatal("no claim attempted")
	}
	select {
	case <-claims:
		t.Fatal("admission rejection bypassed the poll interval")
	case <-time.After(30 * time.Millisecond):
	}
}
