package workercontrol

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

type deploymentCatalog struct {
	mu       sync.Mutex
	values   map[string]db.WorkerDeployment
	failPage bool
	reads    int
}

func newDeploymentCatalog(count int) *deploymentCatalog {
	c := &deploymentCatalog{values: make(map[string]db.WorkerDeployment)}
	for i := range count {
		id := fmt.Sprintf("worker-%04d", i)
		c.values[id] = db.WorkerDeployment{TenantID: "alpha", ID: id, WorkerID: id, Revision: 1, Generation: 1, Spec: db.WorkerDeploymentSpec{Replicas: 1}}
	}
	return c
}

func (c *deploymentCatalog) ListWorkerDeploymentRecords(cursor []byte, limit int) ([]db.WorkerDeployment, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	start := 0
	if len(cursor) != 0 {
		start, _ = strconv.Atoi(string(cursor))
	}
	if start != 0 && c.failPage {
		return nil, nil, errors.New("private-state-marker")
	}
	keys := make([]string, 0, len(c.values))
	for key := range c.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	end := min(start+limit, len(keys))
	values := make([]db.WorkerDeployment, 0, end-start)
	for _, key := range keys[start:end] {
		values = append(values, c.values[key])
	}
	var next []byte
	if end < len(keys) {
		next = []byte(fmt.Sprintf("%06d", end))
	}
	return values, next, nil
}

func (c *deploymentCatalog) GetWorkerDeployment(tenant, id string) (*db.WorkerDeployment, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads++
	v, ok := c.values[id]
	if !ok || tenant != v.TenantID {
		return nil, errors.New("not found")
	}
	return &v, nil
}

func (c *deploymentCatalog) stop(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := c.values[id]
	v.Revision++
	v.Generation++
	v.Spec.Replicas = 0
	c.values[id] = v
}

func runReconciler(t *testing.T, r *Reconciler) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	return cancel, done
}

func TestReconcilerProcessesAllCatalogPagesWithoutPageDelays(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		catalog := newDeploymentCatalog(70)
		var calls atomic.Int32
		r := &Reconciler{Backend: catalog, Eligible: func(context.Context) bool { return true }, Runtime: runtimeCall(func(context.Context, db.WorkerDeployment) (Observation, error) {
			calls.Add(1)
			return Observation{State: "pending"}, nil
		})}
		started := time.Now()
		cancel, done := runReconciler(t, r)
		defer func() { cancel(); <-done }()
		synctest.Wait()
		require.EqualValues(t, 70, calls.Load())
		require.Equal(t, started, time.Now(), "later pages must not wait for the refresh timer")
		_, ok := r.Observation("alpha", "worker-0069")
		require.True(t, ok)
	})
}

func TestReconcilerSharesSlotsAcrossPoolsBeforeAnUnhealthyCatalogFinishes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		catalog := newDeploymentCatalog(32)
		for key, v := range catalog.values {
			v.Spec.Pool = "slow"
			if key == "worker-0031" {
				v.Spec.Pool = "healthy"
			}
			catalog.values[key] = v
		}
		var healthy atomic.Int32
		r := &Reconciler{Backend: catalog, Eligible: func(context.Context) bool { return true }, Runtime: runtimeCall(func(ctx context.Context, v db.WorkerDeployment) (Observation, error) {
			if v.Spec.Pool == "healthy" {
				healthy.Add(1)
				return Observation{State: "available"}, nil
			}
			<-ctx.Done()
			return Observation{}, runtimeFailure("unavailable")
		})}
		cancel, done := runReconciler(t, r)
		defer func() { cancel(); <-done }()
		synctest.Wait()
		require.Positive(t, healthy.Load(), "a new healthy pool must receive a slot before the earlier unhealthy records exhaust the catalog")
		time.Sleep(reconcileTimeout + reconcileInterval)
		synctest.Wait()
		require.GreaterOrEqual(t, healthy.Load(), int32(2), "an established pool must keep refreshing while another pool still has untried records")
	})
}

func TestSlowDeploymentDoesNotBlockUnrelatedWorkAndShutdownJoinsIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		catalog := newDeploymentCatalog(8)
		var active, maximum, completed atomic.Int32
		r := &Reconciler{Backend: catalog, Eligible: func(context.Context) bool { return true }, Runtime: runtimeCall(func(ctx context.Context, v db.WorkerDeployment) (Observation, error) {
			n := active.Add(1)
			defer active.Add(-1)
			for old := maximum.Load(); n > old; old = maximum.Load() {
				if maximum.CompareAndSwap(old, n) {
					break
				}
			}
			if v.ID == "worker-0000" {
				<-ctx.Done()
				return Observation{}, ctx.Err()
			}
			completed.Add(1)
			return Observation{State: "pending"}, nil
		})}
		cancel, done := runReconciler(t, r)
		synctest.Wait()
		require.EqualValues(t, 7, completed.Load())
		require.GreaterOrEqual(t, maximum.Load(), int32(2))
		require.LessOrEqual(t, maximum.Load(), int32(reconcileConcurrency))
		cancel()
		<-done
		require.Zero(t, active.Load(), "shutdown must join the canceled runtime operation")
	})
}

func TestQueuedDeploymentReadsCurrentDesiredStateAfterItsQuorumCheck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		catalog := newDeploymentCatalog(5)
		release := make(chan struct{})
		var observed db.WorkerDeployment
		r := &Reconciler{Backend: catalog, Eligible: func(context.Context) bool { return true }, Runtime: runtimeCall(func(ctx context.Context, v db.WorkerDeployment) (Observation, error) {
			if v.ID == "worker-0004" {
				observed = v
				return Observation{State: "stopped"}, nil
			}
			select {
			case <-release:
			case <-ctx.Done():
			}
			return Observation{}, ctx.Err()
		})}
		cancel, done := runReconciler(t, r)
		defer func() { cancel(); <-done }()
		synctest.Wait()
		catalog.stop("worker-0004")
		close(release)
		synctest.Wait()
		require.EqualValues(t, 2, observed.Revision)
		require.Zero(t, observed.Spec.Replicas, "the old catalog page must not restart this worker")
		cached, ok := r.Observation("alpha", "worker-0004")
		require.True(t, ok)
		require.EqualValues(t, 2, cached.Revision)
	})
}

func TestChangedDeploymentTakesNextSlotAheadOfUntouchedCatalog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		catalog := newDeploymentCatalog(80)
		release := make(chan struct{}, 1)
		var starts atomic.Int32
		var stopped atomic.Bool
		r := &Reconciler{Backend: catalog, Eligible: func(context.Context) bool { return true }, Runtime: runtimeCall(func(ctx context.Context, v db.WorkerDeployment) (Observation, error) {
			starts.Add(1)
			if v.ID == "worker-0079" {
				stopped.Store(v.Spec.Replicas == 0 && v.Revision == 2)
				return Observation{State: "stopped"}, nil
			}
			select {
			case <-release:
			case <-ctx.Done():
			}
			return Observation{}, ctx.Err()
		})}
		cancel, done := runReconciler(t, r)
		defer func() { cancel(); <-done }()
		synctest.Wait()
		require.EqualValues(t, 4, starts.Load())
		catalog.stop("worker-0079")
		time.Sleep(reconcileInterval)
		synctest.Wait()
		release <- struct{}{}
		synctest.Wait()
		require.True(t, stopped.Load(), "a stop must pass the still-untouched first-attempt backlog")
		require.LessOrEqual(t, starts.Load(), int32(6))
	})
}

func TestFailedRequestsBackOffButNewRevisionBypassesTheDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		catalog := newDeploymentCatalog(1)
		var calls, errorsObserved atomic.Int32
		r := &Reconciler{Backend: catalog, Eligible: func(context.Context) bool { return true }, OnError: func(err error) { require.NotContains(t, err.Error(), "private-marker"); errorsObserved.Add(1) }, Runtime: runtimeCall(func(_ context.Context, v db.WorkerDeployment) (Observation, error) {
			calls.Add(1)
			if v.Revision == 1 {
				return Observation{}, errors.New("private-marker")
			}
			return Observation{State: "stopped"}, nil
		})}
		cancel, done := runReconciler(t, r)
		defer func() { cancel(); <-done }()
		synctest.Wait()
		for attempt := int32(1); attempt <= 4; attempt++ {
			require.Equal(t, attempt, calls.Load())
			observation, ok := r.Observation("alpha", "worker-0000")
			require.True(t, ok)
			require.Equal(t, "unavailable", observation.Error)
			require.NotNil(t, observation.RetryAfter)
			delay := observation.RetryAfter.Sub(time.Now())
			require.Positive(t, delay)
			require.LessOrEqual(t, delay, 30*time.Second)
			if attempt == 4 {
				require.Greater(t, delay, 2*reconcileInterval)
				break
			}
			time.Sleep(delay / 2)
			synctest.Wait()
			require.Equal(t, attempt, calls.Load(), "refreshes must respect an unchanged failure's retry delay")
			time.Sleep(delay - delay/2)
			synctest.Wait()
		}
		catalog.stop("worker-0000")
		time.Sleep(reconcileInterval)
		synctest.Wait()
		require.EqualValues(t, 5, calls.Load())
		require.EqualValues(t, 4, errorsObserved.Load())
		observation, _ := r.Observation("alpha", "worker-0000")
		require.Equal(t, "stopped", observation.State)
		require.EqualValues(t, 2, observation.Revision)
		require.Nil(t, observation.RetryAfter)
	})
}

func TestOldInFlightFailureDoesNotBackOffANewerStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		catalog := newDeploymentCatalog(1)
		release := make(chan struct{})
		var stopped atomic.Bool
		r := &Reconciler{Backend: catalog, Eligible: func(context.Context) bool { return true }, Runtime: runtimeCall(func(ctx context.Context, v db.WorkerDeployment) (Observation, error) {
			if v.Revision == 2 {
				stopped.Store(true)
				return Observation{State: "stopped"}, nil
			}
			select {
			case <-release:
			case <-ctx.Done():
			}
			return Observation{}, runtimeFailure("unavailable")
		})}
		cancel, done := runReconciler(t, r)
		defer func() { cancel(); <-done }()
		synctest.Wait()
		catalog.stop("worker-0000")
		time.Sleep(reconcileInterval)
		synctest.Wait()
		require.False(t, stopped.Load(), "one deployment must never have concurrent runtime operations")
		close(release)
		synctest.Wait()
		require.True(t, stopped.Load())
		observation, _ := r.Observation("alpha", "worker-0000")
		require.EqualValues(t, 2, observation.Revision)
		require.Nil(t, observation.RetryAfter)
	})
}

func TestReconcilerRejectsAnOversizedCatalogBeforeStartingWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		catalog := newDeploymentCatalog(db.MaxWorkerDeployments + 1)
		var calls, failures atomic.Int32
		r := &Reconciler{Backend: catalog, Eligible: func(context.Context) bool { return true }, OnError: func(error) { failures.Add(1) }, Runtime: runtimeCall(func(context.Context, db.WorkerDeployment) (Observation, error) {
			calls.Add(1)
			return Observation{}, nil
		})}
		cancel, done := runReconciler(t, r)
		defer func() { cancel(); <-done }()
		synctest.Wait()
		require.Zero(t, calls.Load())
		require.EqualValues(t, 1, failures.Load())
	})
}

func TestLeadershipLossCancelsOutstandingReconciliationAndResumesFresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		catalog := newDeploymentCatalog(4)
		var eligible atomic.Bool
		eligible.Store(true)
		var active, calls atomic.Int32
		r := &Reconciler{Backend: catalog, Eligible: func(context.Context) bool { return eligible.Load() }, Runtime: runtimeCall(func(ctx context.Context, v db.WorkerDeployment) (Observation, error) {
			calls.Add(1)
			active.Add(1)
			defer active.Add(-1)
			<-ctx.Done()
			return Observation{}, ctx.Err()
		})}
		cancel, done := runReconciler(t, r)
		defer func() { cancel(); <-done }()
		synctest.Wait()
		require.EqualValues(t, 4, active.Load())
		eligible.Store(false)
		time.Sleep(reconcileInterval)
		synctest.Wait()
		require.Zero(t, active.Load())
		before := calls.Load()
		time.Sleep(2 * reconcileInterval)
		synctest.Wait()
		require.Equal(t, before, calls.Load())
		eligible.Store(true)
		time.Sleep(reconcileInterval)
		synctest.Wait()
		require.EqualValues(t, 4, active.Load())
		require.Equal(t, before+4, calls.Load())
	})
}

func TestIncompleteCatalogNeverDispatchesItsPartialPage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		catalog := newDeploymentCatalog(40)
		catalog.failPage = true
		var calls, failures atomic.Int32
		r := &Reconciler{Backend: catalog, Eligible: func(context.Context) bool { return true }, OnError: func(err error) { require.NotContains(t, err.Error(), "private-state-marker"); failures.Add(1) }, Runtime: runtimeCall(func(context.Context, db.WorkerDeployment) (Observation, error) {
			calls.Add(1)
			return Observation{}, nil
		})}
		cancel, done := runReconciler(t, r)
		defer func() { cancel(); <-done }()
		synctest.Wait()
		require.Zero(t, calls.Load())
		require.EqualValues(t, 1, failures.Load())
		catalog.mu.Lock()
		catalog.failPage = false
		catalog.mu.Unlock()
		time.Sleep(reconcileInterval)
		synctest.Wait()
		require.EqualValues(t, 40, calls.Load())
	})
}
