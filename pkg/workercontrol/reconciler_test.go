package workercontrol

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

type deploymentScan struct{}

func (deploymentScan) ListWorkerDeploymentRecords([]byte, int) ([]db.WorkerDeployment, []byte, error) {
	return []db.WorkerDeployment{{TenantID: "alpha", ID: "one", Revision: 1, Generation: 1}, {TenantID: "alpha", ID: "two", Revision: 1, Generation: 1}}, nil, nil
}

func (deploymentScan) GetWorkerDeployment(tenant, id string) (*db.WorkerDeployment, error) {
	return &db.WorkerDeployment{TenantID: tenant, ID: id, Revision: 1, Generation: 1}, nil
}

type runtimeCall func(context.Context, db.WorkerDeployment) (Observation, error)

func (f runtimeCall) Reconcile(ctx context.Context, v db.WorkerDeployment) (Observation, error) {
	return f(ctx, v)
}

func TestReconcilerRechecksQuorumBeforeEveryDeploymentAndStopsOnCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var probes, calls atomic.Int32
		terminating := uint32(2)
		deletingAt := time.Now().UTC()
		originalDeletionTime := deletingAt
		lost := make(chan struct{})
		r := &Reconciler{Backend: deploymentScan{}, Eligible: func(context.Context) bool {
			if probes.Add(1) == 3 {
				close(lost)
				return false
			}
			return true
		}, Runtime: runtimeCall(func(_ context.Context, v db.WorkerDeployment) (Observation, error) {
			calls.Add(1)
			return Observation{State: "deleting", Terminating: &terminating, DeletionRequestedAt: &deletingAt}, nil
		})}
		done := make(chan struct{})
		go func() { defer close(done); r.run(ctx, 1) }()
		select {
		case <-lost:
		case <-time.After(time.Second):
			t.Fatal("did not recheck eligibility before the second deployment")
		}
		synctest.Wait()
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("reconciler did not stop")
		}
		require.EqualValues(t, 1, calls.Load())
		observation, ok := r.Observation("alpha", "one")
		require.True(t, ok)
		require.EqualValues(t, 1, observation.Revision)
		terminating = 9
		deletingAt = deletingAt.Add(time.Hour)
		require.Equal(t, originalDeletionTime, *observation.DeletionRequestedAt)
		require.EqualValues(t, 2, *observation.Terminating)
		*observation.Terminating = 8
		*observation.DeletionRequestedAt = deletingAt
		again, _ := r.Observation("alpha", "one")
		require.EqualValues(t, 2, *again.Terminating, "observers cannot change cached runtime counts")
		require.Equal(t, originalDeletionTime, *again.DeletionRequestedAt, "observers cannot change cached deletion timestamps")
		_, ok = r.Observation("alpha", "two")
		require.False(t, ok)
	})
}
