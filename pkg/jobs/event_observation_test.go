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

func eventConversation(v View) (Decision, error) {
	if len(v.Run.State) == 0 {
		return Decision{Kind: db.JobWait, State: []byte("waiting"), WaitKey: "reply"}, nil
	}
	if v.Event == nil {
		return Decision{}, errors.New("conversation needs its event")
	}
	return Decision{Kind: db.JobComplete, Status: db.JobSucceeded, Result: v.Event.Data}, nil
}

func prepareEventConversation(t *testing.T, b *testBackend, held bool) {
	t.Helper()
	var resources []string
	if held {
		state(t, b, db.JobCommand{PutResource: &db.PutJobResource{Spec: db.ResourceSpec{ID: "bus", Duplex: "half", Capacity: 1}}})
		resources = []string{"bus"}
	}
	prepare(t, b, "tenant", "group", "conversation", "", resources)
	launch(t, b, "tenant", "conversation", "run")
}

func eventRegistry() Registry {
	return Registry{Handlers: map[Version]Handler{{"conversation", "1"}: eventConversation}}
}

func deliverConversationEvent(t *testing.T, b Backend) *db.JobEvent {
	t.Helper()
	r := state(t, b, db.JobCommand{Signal: &db.SignalJob{TenantID: "tenant", RunID: "run", EventID: "private-event", Key: "reply", Data: []byte("private reply")}})
	return r.Job.Event
}

func awaitEventWorker(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("event worker did not return")
	}
}

func TestEventObservationCoversEarlyEventsRestartAndHeldConversations(t *testing.T) {
	for _, kind := range []string{"before_wait", "after_restart", "held"} {
		t.Run(kind, func(t *testing.T) {
			b := newBackend(t)
			prepareEventConversation(t, b, kind == "held")
			observations := make(chan EventConsumptionObservation, 4)
			w := worker(t, b, Config{PollInterval: time.Millisecond, OnEventConsumed: func(o EventConsumptionObservation) { observations <- o }}, eventRegistry())
			var event *db.JobEvent
			if kind == "before_wait" {
				event = deliverConversationEvent(t, b)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if kind == "held" {
				done, finished := make(chan error, 1), make(chan struct{})
				go func() { defer close(finished); _, err := w.Execute(ctx, "tenant", "run"); done <- err }()
				t.Cleanup(func() {
					cancel()
					select {
					case <-finished:
					case <-time.After(3 * time.Second):
						t.Error("held conversation did not stop")
					}
				})
				require.Eventually(t, func() bool {
					r, err := b.GetJobRun("tenant", "run")
					return err == nil && r.Status == db.JobWaiting
				}, time.Second, time.Millisecond)
				require.Empty(t, observations)
				event = deliverConversationEvent(t, b)
				awaitEventWorker(t, done)
			} else {
				r, err := w.Execute(ctx, "tenant", "run")
				require.NoError(t, err)
				require.True(t, r.Yielded)
				require.Empty(t, observations, "delivery or entering a wait alone is not consumption")
				if kind == "after_restart" {
					event = deliverConversationEvent(t, b)
					b.restart(t)
				}
				_, err = w.Execute(ctx, "tenant", "run")
				require.NoError(t, err)
			}
			require.Len(t, observations, 1)
			o := <-observations
			require.True(t, o.TimeKnown)
			require.GreaterOrEqual(t, o.Delay, time.Duration(0))
			run, err := b.GetJobRun("tenant", "run")
			require.NoError(t, err)
			require.Equal(t, db.JobSucceeded, run.Status)
			require.Equal(t, event.Data, run.Result)
			if kind == "held" {
				require.EqualValues(t, 1, run.AttemptCount)
			} else {
				require.EqualValues(t, 2, run.AttemptCount)
			}
			// A duplicate delivery and a rejected terminal claim must not repeat
			// a consumed event's observation or replace its original timestamp.
			duplicate := deliverConversationEvent(t, b)
			require.Equal(t, event.At, duplicate.At)
			require.False(t, duplicate.ConsumedAt.IsZero())
			_, err = w.Execute(ctx, "tenant", "run")
			require.Error(t, err)
			require.Empty(t, observations)
		})
	}
}

func TestEventObservationRequiresSuccessfulMatchingConsumptionReceipt(t *testing.T) {
	for _, kind := range []string{"acknowledged", "lost_once", "lost_all", "wrong_request", "wrong_fingerprint", "compacted", "wrong_step", "rejected"} {
		t.Run(kind, func(t *testing.T) {
			b := newBackend(t)
			prepareEventConversation(t, b, false)
			_, err := worker(t, b, Config{}, eventRegistry()).Execute(context.Background(), "tenant", "run")
			require.NoError(t, err)
			deliverConversationEvent(t, b)
			var calls atomic.Int32
			fault := &faultBackend{testBackend: b,
				before: func(_ context.Context, c db.StateCommand) error {
					if c.Job.Advance != nil && c.Job.Advance.EventID != "" {
						calls.Add(1)
						if kind == "rejected" {
							return &db.CommandRejection{Code: "step_conflict"}
						}
					}
					return nil
				},
				after: func(c db.StateCommand, r *db.StateResult) error {
					if c.Job.Advance != nil && c.Job.Advance.EventID != "" {
						switch kind {
						case "lost_once":
							if calls.Load() == 1 {
								return context.DeadlineExceeded
							}
						case "lost_all":
							return context.DeadlineExceeded
						case "wrong_request":
							r.RequestID = "another request"
						case "wrong_fingerprint":
							r.Fingerprint[0] ^= 1
						case "compacted":
							r.Compacted = true
						case "wrong_step":
							r.Job.Run.Step++
						}
					}
					return nil
				},
			}
			var observations []EventConsumptionObservation
			w := worker(t, fault, Config{OnEventConsumed: func(o EventConsumptionObservation) { observations = append(observations, o) }}, eventRegistry())
			_, err = w.Execute(context.Background(), "tenant", "run")
			if kind == "acknowledged" || kind == "lost_once" || kind == "wrong_step" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if kind == "acknowledged" || kind == "lost_once" {
				require.Len(t, observations, 1)
				require.True(t, observations[0].TimeKnown)
			} else {
				require.Empty(t, observations, "local consumed state alone is not a successful matching acknowledgement")
			}
			if kind == "lost_once" || kind == "lost_all" {
				require.EqualValues(t, 2, calls.Load(), "retry the identical proposal, not the observation")
			}
		})
	}
}

func TestEventObservationIncludesDelayedAcknowledgement(t *testing.T) {
	b := newBackend(t)
	prepareEventConversation(t, b, false)
	_, err := worker(t, b, Config{}, eventRegistry()).Execute(context.Background(), "tenant", "run")
	require.NoError(t, err)
	event := deliverConversationEvent(t, b)
	waiting, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	fault := &faultBackend{testBackend: b, after: func(c db.StateCommand, _ *db.StateResult) error {
		if c.Job.Advance != nil && c.Job.Advance.EventID != "" {
			close(waiting)
			<-release
		}
		return nil
	}}
	observations := make(chan EventConsumptionObservation, 2)
	w := worker(t, fault, Config{OnEventConsumed: func(o EventConsumptionObservation) { observations <- o }}, eventRegistry())
	done, finished := make(chan error, 1), make(chan struct{})
	go func() { defer close(finished); _, err := w.Execute(context.Background(), "tenant", "run"); done <- err }()
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("worker did not finish")
		}
	})
	select {
	case <-waiting:
	case <-time.After(3 * time.Second):
		t.Fatal("consumption acknowledgement did not stall")
	}
	require.Empty(t, observations)
	run, err := b.GetJobRun("tenant", "run")
	require.NoError(t, err)
	require.Equal(t, db.JobSucceeded, run.Status, "consumption already committed while its acknowledgement is delayed")
	time.Sleep(15 * time.Millisecond)
	minimum := time.Since(event.At)
	once.Do(func() { close(release) })
	awaitEventWorker(t, done)
	require.Len(t, observations, 1)
	o := <-observations
	require.True(t, o.TimeKnown)
	require.GreaterOrEqual(t, o.Delay, minimum, "the endpoint is local acknowledgement, not the proposal timestamp")
}

func TestEventObservationTracksConsumptionIndependentlyOfJobSuccess(t *testing.T) {
	for _, kind := range []string{"handler_failure", "cancelled_wait"} {
		t.Run(kind, func(t *testing.T) {
			b := newBackend(t)
			prepareEventConversation(t, b, false)
			var observations []EventConsumptionObservation
			registry := eventRegistry()
			registry.Handlers[Version{"conversation", "1"}] = func(v View) (Decision, error) {
				if v.Event != nil {
					return Decision{}, errors.New("private handler failure")
				}
				return eventConversation(v)
			}
			w := worker(t, b, Config{OnEventConsumed: func(o EventConsumptionObservation) { observations = append(observations, o) }}, registry)
			_, err := w.Execute(context.Background(), "tenant", "run")
			require.NoError(t, err)
			deliverConversationEvent(t, b)
			if kind == "cancelled_wait" {
				state(t, b, db.JobCommand{Cancel: &db.CancelJob{TenantID: "tenant", RunID: "run", Reason: "cancel before consuming"}})
			}
			_, err = w.Execute(context.Background(), "tenant", "run")
			event, readErr := b.GetJobEvent("tenant", "run", "private-event")
			require.NoError(t, readErr)
			if kind == "handler_failure" {
				require.NoError(t, err)
				require.Len(t, observations, 1, "an acknowledged failed decision still consumed its event")
				require.False(t, event.ConsumedAt.IsZero())
			} else {
				require.Error(t, err)
				require.Empty(t, observations, "cancelling a ready wait must not fabricate consumption")
				require.True(t, event.ConsumedAt.IsZero())
			}
		})
	}
}

func TestEventObserverCancellationPreventsSubsequentDriverIO(t *testing.T) {
	b := newBackend(t)
	setupAction(t, b, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var factories atomic.Int32
	observations := make(chan EventConsumptionObservation, 2)
	w := worker(t, b, Config{PollInterval: time.Millisecond, OnEventConsumed: func(o EventConsumptionObservation) {
		cancel() // The newly authorized action still needs a fresh local check.
		observations <- o
	}}, Registry{
		Handlers: map[Version]Handler{{"job", "1"}: func(v View) (Decision, error) {
			if len(v.Run.State) == 0 {
				return Decision{Kind: db.JobWait, State: []byte("waiting"), WaitKey: "reply"}, nil
			}
			return Decision{Kind: db.JobDoAction, State: []byte("next"), Operation: "do"}, nil
		}},
		Drivers: map[Version]DriverFactory{{"test", "1"}: func(db.JobRun) (Driver, error) {
			factories.Add(1)
			return nil, errors.New("driver must not be constructed after cancellation")
		}},
	})
	done, finished := make(chan error, 1), make(chan struct{})
	go func() { defer close(finished); _, err := w.Execute(ctx, "t", "r"); done <- err }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("worker did not finish")
		}
	})
	require.Eventually(t, func() bool {
		r, err := b.GetJobRun("t", "r")
		return err == nil && r.Status == db.JobWaiting
	}, time.Second, time.Millisecond)
	state(t, b, db.JobCommand{Signal: &db.SignalJob{TenantID: "t", RunID: "r", EventID: "reply", Key: "reply"}})
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not finish after observer cancellation")
	}
	require.Len(t, observations, 1)
	require.Zero(t, factories.Load(), "a metrics callback must not bypass cancellation before driver I/O")
	event, err := b.GetJobEvent("t", "r", "reply")
	require.NoError(t, err)
	require.False(t, event.ConsumedAt.IsZero())
}
