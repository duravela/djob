package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func nextActionObservation(t *testing.T, observations <-chan ActionObservation) ActionObservation {
	t.Helper()
	select {
	case o := <-observations:
		return o
	case <-time.After(3 * time.Second):
		t.Fatal("action observation did not arrive")
		return ActionObservation{}
	}
}

func TestActionObservationSeparatesDriverAndRecordingAndWaitsForAcknowledgement(t *testing.T) {
	b := newBackend(t)
	setupAction(t, b, true)
	driving, recording := make(chan struct{}), make(chan struct{})
	releaseDriver, releaseRecord := make(chan struct{}), make(chan struct{})
	var driverOnce, recordOnce sync.Once
	observations := make(chan ActionObservation, 2)
	fault := &faultBackend{testBackend: b, before: func(ctx context.Context, c db.StateCommand) error {
		if c.Job.CompleteAction != nil {
			close(recording)
			select {
			case <-releaseRecord:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}}
	driver := &testDriver{execute: func(context.Context, Action) Effect {
		close(driving)
		<-releaseDriver
		return Effect{Status: db.EffectSucceeded, Data: []byte("private action result")}
	}}
	w := worker(t, fault, Config{OnAction: func(o ActionObservation) { observations <- o }}, Registry{
		Handlers: map[Version]Handler{{"job", "1"}: oneAction},
		Drivers:  map[Version]DriverFactory{{"test", "1"}: func(db.JobRun) (Driver, error) { return driver, nil }},
	})
	done := make(chan error, 1)
	finished := make(chan struct{})
	began := time.Now()
	go func() { defer close(finished); _, err := w.Execute(context.Background(), "t", "r"); done <- err }()
	t.Cleanup(func() {
		driverOnce.Do(func() { close(releaseDriver) })
		recordOnce.Do(func() { close(releaseRecord) })
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("worker did not finish")
		}
	})
	select {
	case <-driving:
	case <-time.After(3 * time.Second):
		t.Fatal("driver did not start")
	}
	assertPending := func() {
		t.Helper()
		select {
		case o := <-observations:
			t.Fatalf("observed action before recording returned: %+v", o)
		default:
		}
	}
	assertPending()
	time.Sleep(15 * time.Millisecond)
	driverOnce.Do(func() { close(releaseDriver) })
	select {
	case <-recording:
	case <-time.After(3 * time.Second):
		t.Fatal("completion recording did not start")
	}
	assertPending()
	action, err := b.GetJobAction("t", "r", 1)
	require.NoError(t, err)
	require.Equal(t, db.EffectAuthorized, action.Status)
	time.Sleep(15 * time.Millisecond)
	recordOnce.Do(func() { close(releaseRecord) })
	o := nextActionObservation(t, observations)
	require.LessOrEqual(t, o.DriverDuration+o.RecordDuration, time.Since(began), "separate stage timings must not overlap")
	require.True(t, o.DriverInvoked)
	require.True(t, o.RecordConfirmed)
	require.Equal(t, db.EffectSucceeded, o.Status)
	require.GreaterOrEqual(t, o.DriverDuration, 15*time.Millisecond)
	require.GreaterOrEqual(t, o.RecordDuration, 15*time.Millisecond)
	action, err = b.GetJobAction("t", "r", 1)
	require.NoError(t, err)
	require.Equal(t, db.EffectSucceeded, action.Status)
	encoded, err := json.Marshal(o)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "private action result")
	// Cleanup joins the worker; a second observation must not appear for its
	// final run completion or resource release.
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not finish")
	}
	assertPending()
}

func TestActionObservationRequiresMatchingAcknowledgementAndDoesNotRepeatIO(t *testing.T) {
	for _, kind := range []string{"retry", "lost", "mismatch", "rejected"} {
		t.Run(kind, func(t *testing.T) {
			b := newBackend(t)
			setupAction(t, b, false)
			var calls, executed atomic.Int32
			fault := &faultBackend{testBackend: b,
				before: func(_ context.Context, c db.StateCommand) error {
					if c.Job.CompleteAction != nil {
						calls.Add(1)
						if kind == "rejected" {
							return &db.CommandRejection{Code: "stale_owner"}
						}
					}
					return nil
				},
				after: func(c db.StateCommand, r *db.StateResult) error {
					if c.Job.CompleteAction != nil {
						switch kind {
						case "retry":
							if calls.Load() == 1 {
								return context.DeadlineExceeded
							}
						case "lost":
							return context.DeadlineExceeded
						case "mismatch":
							r.RequestID = "private mismatched request"
						}
					}
					return nil
				},
			}
			var observations []ActionObservation
			w := worker(t, fault, Config{OnAction: func(o ActionObservation) { observations = append(observations, o) }}, Registry{
				Handlers: map[Version]Handler{{"job", "1"}: oneAction},
				Drivers: map[Version]DriverFactory{{"test", "1"}: func(db.JobRun) (Driver, error) {
					return &testDriver{execute: func(context.Context, Action) Effect {
						executed.Add(1)
						return Effect{Status: db.EffectSucceeded}
					}}, nil
				}},
			})
			_, err := w.Execute(context.Background(), "t", "r")
			if kind == "retry" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.EqualValues(t, 1, executed.Load())
			require.Len(t, observations, 1, "identical proposal retries must not repeat action observations")
			o := observations[0]
			require.Equal(t, db.EffectSucceeded, o.Status)
			require.True(t, o.DriverInvoked)
			require.Positive(t, o.DriverDuration)
			require.Positive(t, o.RecordDuration)
			require.Equal(t, kind == "retry", o.RecordConfirmed)
			action, err := b.GetJobAction("t", "r", 1)
			require.NoError(t, err)
			if kind == "rejected" {
				require.Equal(t, db.EffectAuthorized, action.Status)
			} else {
				require.Equal(t, db.EffectSucceeded, action.Status, "local state must not upgrade an unacknowledged observation")
			}
		})
	}
}

func TestActionObservationDistinguishesFactoryFailureFromDriverExecution(t *testing.T) {
	for _, kind := range []string{"factory_error", "nil_driver", "failed", "unknown", "invalid"} {
		t.Run(kind, func(t *testing.T) {
			b := newBackend(t)
			setupAction(t, b, false)
			var observations []ActionObservation
			w := worker(t, b, Config{OnAction: func(o ActionObservation) { observations = append(observations, o) }}, Registry{
				Handlers: map[Version]Handler{{"job", "1"}: oneAction},
				Drivers: map[Version]DriverFactory{{"test", "1"}: func(db.JobRun) (Driver, error) {
					if kind == "factory_error" {
						return nil, errors.New("private factory failure")
					}
					if kind == "nil_driver" {
						return nil, nil
					}
					return &testDriver{execute: func(context.Context, Action) Effect {
						status := map[string]db.EffectStatus{"failed": db.EffectFailed, "unknown": db.EffectUnknown, "invalid": "private invalid effect"}[kind]
						return Effect{Status: status, Error: "private effect detail"}
					}}, nil
				}},
			})
			_, err := w.Execute(context.Background(), "t", "r")
			require.NoError(t, err)
			require.Len(t, observations, 1)
			o := observations[0]
			require.True(t, o.RecordConfirmed)
			require.Positive(t, o.RecordDuration)
			if kind == "factory_error" || kind == "nil_driver" {
				require.False(t, o.DriverInvoked)
				require.Zero(t, o.DriverDuration)
				require.Equal(t, db.EffectFailed, o.Status)
			} else {
				require.True(t, o.DriverInvoked)
				require.Positive(t, o.DriverDuration)
				if kind == "failed" {
					require.Equal(t, db.EffectFailed, o.Status)
				} else {
					require.Equal(t, db.EffectUnknown, o.Status)
				}
			}
			encoded, err := json.Marshal(o)
			require.NoError(t, err)
			require.NotContains(t, string(encoded), "private")
		})
	}
}

func TestActionObservationWaitsForUncooperativeDriverAfterAuthorityLoss(t *testing.T) {
	b := newBackend(t)
	setupAction(t, b, true)
	cancelled, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var recordCalls atomic.Int32
	fault := &faultBackend{testBackend: b, before: func(_ context.Context, c db.StateCommand) error {
		if c.Job.Renew != nil {
			return &db.CommandRejection{Code: "stale_owner"}
		}
		if c.Job.CompleteAction != nil {
			recordCalls.Add(1)
		}
		return nil
	}}
	observations := make(chan ActionObservation, 2)
	cfg := Config{
		LeaseDuration: 600 * time.Millisecond, SafetyMargin: 150 * time.Millisecond,
		RenewInterval: 80 * time.Millisecond, CommitTimeout: 20 * time.Millisecond,
		PollInterval: 5 * time.Millisecond, OnAction: func(o ActionObservation) { observations <- o },
	}
	w := worker(t, fault, cfg, Registry{
		Handlers: map[Version]Handler{{"job", "1"}: oneAction},
		Drivers: map[Version]DriverFactory{{"test", "1"}: func(db.JobRun) (Driver, error) {
			return &testDriver{execute: func(ctx context.Context, _ Action) Effect {
				<-ctx.Done()
				close(cancelled)
				<-release // Cancellation does not detach this driver or establish quiescence.
				return Effect{Status: db.EffectSucceeded}
			}}, nil
		}},
	})
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() { defer close(finished); _, err := w.Execute(context.Background(), "t", "r"); done <- err }()
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("worker did not finish")
		}
	})
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("authority loss did not cancel the driver's context")
	}
	select {
	case o := <-observations:
		t.Fatalf("driver cancellation must not fabricate its return: %+v", o)
	default:
	}
	once.Do(func() { close(release) })
	o := nextActionObservation(t, observations)
	require.True(t, o.DriverInvoked)
	require.False(t, o.RecordConfirmed)
	require.Equal(t, db.EffectSucceeded, o.Status, "a reported driver success is not a confirmed durable effect")
	require.Zero(t, recordCalls.Load(), "lost authority refuses even the completion proposal")
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrOwnershipLost)
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not return after the driver")
	}
	action, err := b.GetJobAction("t", "r", 1)
	require.NoError(t, err)
	require.Equal(t, db.EffectAuthorized, action.Status)
}
