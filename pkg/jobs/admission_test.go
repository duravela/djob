package jobs

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func TestAdmissionObservationWaitsForVerifiedReceiptAndCountsClaimRetryOnce(t *testing.T) {
	b := newBackend(t)
	prepare(t, b, "tenant", "", "compute", "", nil)
	launch(t, b, "tenant", "compute", "r")
	admissions := make(chan AdmissionObservation, 4)
	readyAdmissions := make(chan bool, 4)
	waiting, release := make(chan struct{}), make(chan struct{})
	var claims atomic.Uint32
	var releaseOnce sync.Once
	fault := &faultBackend{testBackend: b, after: func(c db.StateCommand, _ *db.StateResult) error {
		if c.Job.Claim != nil {
			switch claims.Add(1) {
			case 1:
				return context.DeadlineExceeded
			case 2:
				close(waiting)
				<-release
			}
		}
		return nil
	}}
	w := worker(t, fault, Config{OnAdmission: func(o AdmissionObservation) { admissions <- o }, OnReadyAdmission: func(_ time.Duration, known bool) { readyAdmissions <- known }}, Registry{
		Handlers: map[Version]Handler{{"compute", "1"}: func(View) (Decision, error) {
			return Decision{Kind: db.JobComplete, Status: db.JobSucceeded}, nil
		}},
	})
	done, finished := make(chan error, 1), make(chan struct{})
	go func() { defer close(finished); _, err := w.Execute(context.Background(), "tenant", "r"); done <- err }()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("worker did not finish")
		}
	})
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("claim acknowledgement did not stall")
	}
	require.Empty(t, admissions)
	require.Empty(t, readyAdmissions)
	run, err := b.GetJobRun("tenant", "r")
	require.NoError(t, err)
	require.EqualValues(t, 1, run.AttemptCount)
	minimumAge := time.Since(run.CreatedAt)
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-done)
	require.Len(t, admissions, 1)
	require.Len(t, readyAdmissions, 1)
	require.True(t, <-readyAdmissions)
	o := <-admissions
	require.True(t, o.QueueTimeKnown)
	require.GreaterOrEqual(t, o.QueueAge, minimumAge)
	require.False(t, o.Scheduled)
	require.EqualValues(t, 2, claims.Load())
}

func TestAdmissionObservationRejectsMismatchedClaimReceipt(t *testing.T) {
	b := newBackend(t)
	prepare(t, b, "tenant", "", "compute", "", nil)
	launch(t, b, "tenant", "compute", "r")
	fault := &faultBackend{testBackend: b, after: func(c db.StateCommand, r *db.StateResult) error {
		if c.Job.Claim != nil {
			r.RequestID = "wrong-receipt"
		}
		return nil
	}}
	var admissions []AdmissionObservation
	w := worker(t, fault, Config{OnAdmission: func(o AdmissionObservation) { admissions = append(admissions, o) }}, Registry{
		Handlers: map[Version]Handler{{"compute", "1"}: func(View) (Decision, error) {
			t.Fatal("an unverified claim must not invoke the handler")
			return Decision{}, nil
		}},
	})
	_, err := w.Execute(context.Background(), "tenant", "r")
	require.Error(t, err)
	require.Empty(t, admissions)
}
