package executor_test

import (
	"context"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/stretchr/testify/require"
)

func TestAdmissionObservationWaitsForClaimAcknowledgement(t *testing.T) {
	observations := make(chan executor.AdmissionObservation, 4)
	f := newFixture(t, fake.Options{}, func(c *executor.Config) {
		c.OnAdmission = func(o executor.AdmissionObservation) { observations <- o }
	})
	require.NoError(t, f.executor.Close(context.Background()))
	gate := newHold(true)
	t.Cleanup(func() {
		select {
		case <-gate.release:
		default:
			close(gate.release)
		}
	})
	b := &deadlineAckBackend{backend: f.backend, claimAck: gate}
	var err error
	f.executor, err = executor.New(context.Background(), f.cfg, b, f.gateway.Factory)
	require.NoError(t, err)
	replies := executeAsync(f.executor, context.Background(), "slave-3")
	await(t, gate.started)
	run, err := f.backend.GetRun("slave-3")
	require.NoError(t, err)
	require.Equal(t, db.RunRunning, run.State, "the claim applied but has not been acknowledged")
	select {
	case o := <-observations:
		t.Fatalf("unacknowledged admission was observed: %+v", o)
	default:
	}
	require.Zero(t, f.gateway.Stats().Started)
	minimumAge := time.Since(run.CreatedAt)
	close(gate.release)
	call := receiveCall(t, f.gateway)
	require.Len(t, observations, 1)
	o := <-observations
	require.True(t, o.QueueTimeKnown)
	require.GreaterOrEqual(t, o.QueueAge, minimumAge, "queue age includes the claim acknowledgement wait")
	require.False(t, o.Scheduled)
	finishCall(t, call, executor.Outcome{Status: db.AttemptSucceeded})
	require.NoError(t, receiveReply(t, replies).err)
	_, err = f.executor.Execute(context.Background(), "slave-3")
	require.Error(t, err)
	require.Empty(t, observations, "rejected repeated execution adds no first-admission sample")
}
