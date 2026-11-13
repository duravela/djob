package jobs

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func nextExecutionObservation(t *testing.T, observations <-chan ExecutionObservation) ExecutionObservation {
	t.Helper()
	select {
	case o := <-observations:
		return o
	case <-time.After(3 * time.Second):
		t.Fatal("worker observation did not arrive")
		return ExecutionObservation{}
	}
}

func assertExecutionPending(t *testing.T, observations <-chan ExecutionObservation) {
	t.Helper()
	select {
	case o := <-observations:
		t.Fatalf("observed premature worker return: %+v", o)
	default:
	}
}

func TestExecutionObserverSeparatesWaitingAdmissionAndResume(t *testing.T) {
	b := newBackend(t)
	prepare(t, b, "tenant", "", "conversation", "", nil)
	launch(t, b, "tenant", "conversation", "r")
	var observations []ExecutionObservation
	var admissions []AdmissionObservation
	var actions []ActionObservation
	w := worker(t, b, Config{
		OnExecution: func(o ExecutionObservation) { observations = append(observations, o) },
		OnAdmission: func(o AdmissionObservation) { admissions = append(admissions, o) },
		OnAction:    func(o ActionObservation) { actions = append(actions, o) },
	}, Registry{
		Handlers: map[Version]Handler{{"conversation", "1"}: func(v View) (Decision, error) {
			if v.Event == nil {
				return Decision{Kind: db.JobWait, WaitKey: "reply"}, nil
			}
			return Decision{Kind: db.JobComplete, Status: db.JobSucceeded, Result: v.Event.Data}, nil
		}},
	})
	r, err := w.Execute(context.Background(), "tenant", "r")
	require.NoError(t, err)
	require.True(t, r.Yielded)
	_, err = w.Execute(context.Background(), "tenant", "r")
	require.Error(t, err, "a waiter with no event cannot claim another attempt")
	state(t, b, db.JobCommand{Signal: &db.SignalJob{TenantID: "tenant", RunID: "r", EventID: "reply", Key: "reply", Data: []byte("value")}})
	r, err = w.Execute(context.Background(), "tenant", "r")
	require.NoError(t, err)
	require.Equal(t, db.JobSucceeded, r.Run.Status)
	require.Len(t, observations, 6)
	require.Empty(t, actions, "waits, resumes and rejected claims without driver actions emit no action timing")
	require.Len(t, admissions, 1, "waiting, rejected reclaims and resumes must not repeat launch-to-admission timing")
	require.True(t, admissions[0].QueueTimeKnown)
	require.False(t, admissions[0].Scheduled)
	for i, want := range []string{"waiting", "not_admitted", "succeeded"} {
		require.Equal(t, ExecutionObservation{Phase: ExecutionStarted}, observations[2*i])
		returned := observations[2*i+1]
		require.Equal(t, ExecutionReturned, returned.Phase)
		require.Equal(t, want, returned.Result)
		require.Positive(t, returned.Duration)
		require.False(t, returned.CleanupFailed)
	}
}

func TestExecutionObserverDoesNotCountQuotaRejectionAsCompletion(t *testing.T) {
	b := newBackend(t)
	prepare(t, b, "tenant", "", "compute", "", nil)
	launch(t, b, "tenant", "compute", "held")
	launch(t, b, "tenant", "compute", "r")
	state(t, b, db.JobCommand{PutTenant: &db.PutJobTenant{ExpectedRevision: 1, Tenant: db.JobTenant{ID: "tenant", MaxRunning: 1}}})
	state(t, b, db.JobCommand{Claim: &db.ClaimJob{JobOwner: db.JobOwner{TenantID: "tenant", RunID: "held", AttemptID: "held", OwnerID: "holder"}, LeaseUntil: time.Now().Add(time.Minute)}})
	var observations []ExecutionObservation
	var admissions []AdmissionObservation
	w := worker(t, b, Config{
		OnExecution: func(o ExecutionObservation) { observations = append(observations, o) },
		OnAdmission: func(o AdmissionObservation) { admissions = append(admissions, o) },
	}, Registry{
		Handlers: map[Version]Handler{{"compute", "1"}: func(View) (Decision, error) {
			t.Fatal("quota rejection must precede the handler")
			return Decision{}, nil
		}},
	})
	_, err := w.Execute(context.Background(), "tenant", "r")
	var rejection *db.CommandRejection
	require.ErrorAs(t, err, &rejection)
	require.Equal(t, "quota_exceeded", rejection.Code)
	require.Len(t, observations, 2)
	require.Equal(t, "not_admitted", observations[1].Result)
	require.Empty(t, admissions)
}

func TestExecutionObserverIncludesFallbackCleanupFailureAfterActionCommitRejection(t *testing.T) {
	b := newBackend(t)
	setupAction(t, b, true)
	fault := &faultBackend{testBackend: b, before: func(_ context.Context, c db.StateCommand) error {
		if c.Job.CompleteAction != nil {
			return &db.CommandRejection{Code: "stale_owner"}
		}
		return nil
	}}
	observations := make(chan ExecutionObservation, 8)
	cleaning := make(chan context.Context, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	var closed atomic.Bool
	d := &testDriver{
		execute: func(context.Context, Action) Effect { return Effect{Status: db.EffectSucceeded} },
		quiet: func(ctx context.Context) error {
			cleaning <- ctx
			<-release
			return nil
		},
		close: func(context.Context) error { closed.Store(true); return errors.New("private cleanup detail") },
	}
	w := worker(t, fault, Config{RecoveryTimeout: 30 * time.Millisecond, OnExecution: func(o ExecutionObservation) { observations <- o }}, Registry{
		Handlers: map[Version]Handler{{"job", "1"}: oneAction},
		Drivers:  map[Version]DriverFactory{{"test", "1"}: func(db.JobRun) (Driver, error) { return d, nil }},
	})
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() { defer close(finished); _, err := w.Execute(context.Background(), "t", "r"); done <- err }()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("worker did not finish")
		}
	})
	require.Equal(t, ExecutionStarted, nextExecutionObservation(t, observations).Phase)
	select {
	case ctx := <-cleaning:
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("cleanup deadline did not elapse")
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup did not start")
	}
	assertExecutionPending(t, observations)
	releaseOnce.Do(func() { close(release) })
	returned := nextExecutionObservation(t, observations)
	require.True(t, closed.Load())
	require.True(t, returned.CleanupFailed)
	require.Equal(t, "error", returned.Result, "a rejection after I/O is not an admission rejection")
	require.GreaterOrEqual(t, returned.Duration, 30*time.Millisecond)
	require.Error(t, <-done)
}

func TestExecutionClassificationRequiresAcknowledgedOutcome(t *testing.T) {
	for _, tc := range []struct {
		name    string
		result  Result
		err     error
		claimed bool
		want    string
	}{
		{"other owner finished", Result{Run: &db.JobRun{Status: db.JobSucceeded}}, ErrOwnershipLost, true, "ownership_lost"},
		{"completion ack lost", Result{Run: &db.JobRun{Status: db.JobSucceeded}}, context.DeadlineExceeded, true, "interrupted"},
		{"confirmed yield after cancellation", Result{Yielded: true, Run: &db.JobRun{Status: db.JobReady}}, context.Canceled, true, "yielded"},
		{"postclaim rejection", Result{}, &db.CommandRejection{Code: "stale_owner"}, true, "error"},
		{"unknown completion", Result{Run: &db.JobRun{Status: db.JobUnknown}}, nil, true, "unknown"},
		{"cancelled completion", Result{Run: &db.JobRun{Status: db.JobCancelled}}, nil, true, "cancelled"},
		{"failed completion", Result{Run: &db.JobRun{Status: db.JobFailed}}, nil, true, "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, executionResult(tc.result, tc.err, tc.claimed)) })
	}
}

type monitorReadBackend struct {
	*testBackend
	block   atomic.Bool
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *monitorReadBackend) GetJobView(tenant, run string) (db.JobView, error) {
	if b.block.Load() {
		b.once.Do(func() { close(b.started) })
		<-b.release
	}
	return b.testBackend.GetJobView(tenant, run)
}

func TestExecutionObserverWaitsForMonitorAfterDurableCompletion(t *testing.T) {
	b := newBackend(t)
	prepare(t, b, "tenant", "", "compute", "", nil)
	launch(t, b, "tenant", "compute", "r")
	backend := &monitorReadBackend{testBackend: b, started: make(chan struct{}), release: make(chan struct{})}
	observations := make(chan ExecutionObservation, 8)
	w := worker(t, backend, Config{PollInterval: time.Millisecond, OnExecution: func(o ExecutionObservation) { observations <- o }}, Registry{
		Handlers: map[Version]Handler{{"compute", "1"}: func(View) (Decision, error) {
			backend.block.Store(true)
			select {
			case <-backend.started:
				return Decision{Kind: db.JobComplete, Status: db.JobSucceeded}, nil
			case <-time.After(time.Second):
				return Decision{}, errors.New("monitor did not start reading")
			}
		}},
	})
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() { defer close(finished); _, err := w.Execute(context.Background(), "tenant", "r"); done <- err }()
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(backend.release) })
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("worker did not join monitor")
		}
	})
	require.Equal(t, ExecutionStarted, nextExecutionObservation(t, observations).Phase)
	require.Eventually(t, func() bool {
		run, err := b.GetJobRun("tenant", "r")
		return err == nil && run.Status == db.JobSucceeded
	}, time.Second, time.Millisecond)
	assertExecutionPending(t, observations)
	releaseOnce.Do(func() { close(backend.release) })
	require.Equal(t, "succeeded", nextExecutionObservation(t, observations).Result)
	require.NoError(t, <-done)
}

func TestExecutionObserverDoesNotReportGenericSuccessWithoutCompletionReceipt(t *testing.T) {
	b := newBackend(t)
	prepare(t, b, "tenant", "", "compute", "", nil)
	launch(t, b, "tenant", "compute", "r")
	fault := &faultBackend{testBackend: b, after: func(c db.StateCommand, _ *db.StateResult) error {
		if c.Job.Advance != nil && c.Job.Advance.Kind == db.JobComplete {
			return context.DeadlineExceeded // Lose both acknowledgements after applying.
		}
		return nil
	}}
	var observations []ExecutionObservation
	w := worker(t, fault, Config{OnExecution: func(o ExecutionObservation) { observations = append(observations, o) }}, Registry{
		Handlers: map[Version]Handler{{"compute", "1"}: func(View) (Decision, error) {
			return Decision{Kind: db.JobComplete, Status: db.JobSucceeded}, nil
		}},
	})
	_, err := w.Execute(context.Background(), "tenant", "r")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	run, err := b.GetJobRun("tenant", "r")
	require.NoError(t, err)
	require.Equal(t, db.JobSucceeded, run.Status, "local durable state alone is not this worker's acknowledged result")
	require.Len(t, observations, 2)
	require.Equal(t, "interrupted", observations[1].Result)
}
