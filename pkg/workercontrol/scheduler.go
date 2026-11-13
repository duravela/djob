package workercontrol

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/duravela/djob/pkg/db"
)

const (
	reconcileConcurrency = 4
	reconcileInterval    = 2 * time.Second
	reconcileTimeout     = 10 * time.Second
)

// Only bounded identity metadata is queued. The worker reads the current spec
// after its quorum check, rather than retaining a possibly stale page of specs.
type deploymentRef struct {
	tenant, id           string
	revision, generation uint64
	replicas             uint32
	pool                 string
}

func (v deploymentRef) key() string { return v.tenant + "\x00" + v.id }

type scheduledDeployment struct {
	ref              deploymentRef
	lastAttempt      uint64
	failures         uint8
	due              time.Time
	running, present bool
}

type scheduledPool struct {
	running      int
	lastDispatch uint64
}

type reconcileTask struct {
	ref   deploymentRef
	epoch uint64
	ctx   context.Context
}

type reconcileResult struct {
	task                 reconcileTask
	ref                  deploymentRef
	observation          Observation
	err                  error
	ineligible, canceled bool
}

func (r *Reconciler) catalog(ctx context.Context) ([]deploymentRef, error) {
	var cursor []byte
	refs := make([]deploymentRef, 0, 32)
	seen := make(map[string]bool)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		values, next, err := r.Backend.ListWorkerDeploymentRecords(cursor, 32)
		if err != nil {
			return nil, err
		}
		for _, v := range values {
			ref := deploymentRef{v.TenantID, v.ID, v.Revision, v.Generation, v.Spec.Replicas, v.Spec.Pool}
			if ref.tenant == "" || ref.id == "" || ref.revision == 0 || seen[ref.key()] || len(refs) == db.MaxWorkerDeployments {
				return nil, runtimeFailure("state_unavailable")
			}
			seen[ref.key()] = true
			refs = append(refs, ref)
		}
		if len(next) == 0 {
			return refs, nil
		}
		if len(values) == 0 || len(next) > 1024 || bytes.Compare(next, cursor) <= 0 {
			return nil, runtimeFailure("state_unavailable")
		}
		cursor = bytes.Clone(next)
	}
}

func (r *Reconciler) run(ctx context.Context, concurrency int) {
	if r.Backend == nil || r.Runtime == nil || r.Eligible == nil || concurrency < 1 || concurrency > reconcileConcurrency {
		r.report(runtimeFailure("invalid_configuration"))
		return
	}
	workersContext, stopWorkers := context.WithCancel(ctx)
	epochContext, stopEpoch := context.WithCancel(workersContext)
	jobs := make(chan reconcileTask)
	results := make(chan reconcileResult, concurrency)
	var workers sync.WaitGroup
	for range concurrency {
		workers.Add(1)
		go func() { defer workers.Done(); r.reconcileWorker(workersContext, jobs, results) }()
	}
	defer func() { stopEpoch(); stopWorkers(); workers.Wait() }()
	entries := make(map[string]*scheduledDeployment)
	pools := make(map[string]scheduledPool)
	var dispatched uint64
	var epoch uint64
	enabled := false
	pause := func() { enabled = false; stopEpoch() }
	refresh := func() {
		probe, stop := context.WithTimeout(workersContext, 3*time.Second)
		allowed := r.Eligible(probe)
		stop()
		if !allowed {
			pause()
			return
		}
		refs, err := r.catalog(workersContext)
		if err != nil {
			pause()
			if workersContext.Err() == nil {
				r.report(runtimeFailure("state_unavailable"))
			}
			return
		}
		now := time.Now()
		for _, entry := range entries {
			entry.present = false
		}
		for _, ref := range refs {
			entry := entries[ref.key()]
			if entry == nil {
				entry = &scheduledDeployment{ref: ref, due: now}
				entries[ref.key()] = entry
			} else if ref.revision > entry.ref.revision {
				entry.ref, entry.due, entry.failures = ref, now, 0
			}
			entry.present = true
		}
		presentPools := make(map[string]bool)
		for key, entry := range entries {
			if !entry.present {
				r.mu.Lock()
				delete(r.observations, key)
				r.mu.Unlock()
				if !entry.running {
					delete(entries, key)
					continue
				}
			}
			presentPools[entry.ref.pool] = true
		}
		for pool, state := range pools {
			if !presentPools[pool] && state.running == 0 {
				delete(pools, pool)
			}
		}
		if !enabled {
			stopEpoch()
			epochContext, stopEpoch = context.WithCancel(workersContext)
			epoch++
			for _, entry := range entries {
				entry.due, entry.failures = now, 0
			}
			enabled = true
		}
	}
	refresh()
	scan := time.NewTicker(reconcileInterval)
	defer scan.Stop()
	wake := time.NewTimer(time.Hour)
	defer wake.Stop()
	for {
		if workersContext.Err() != nil {
			return
		}
		wake.Stop()
		var send chan reconcileTask
		var task reconcileTask
		var wakeAt <-chan time.Time
		if enabled {
			now := time.Now()
			entry, next := nextDeployment(entries, pools, now)
			if entry != nil {
				send, task = jobs, reconcileTask{ref: entry.ref, epoch: epoch, ctx: epochContext}
			} else if !next.IsZero() {
				wake.Reset(max(time.Duration(0), next.Sub(now)))
				wakeAt = wake.C
			}
		}
		select {
		case <-workersContext.Done():
			return
		case <-scan.C:
			refresh()
		case <-wakeAt:
		case send <- task:
			entries[task.ref.key()].running = true
			dispatched++
			pool := pools[task.ref.pool]
			pool.running++
			pool.lastDispatch = dispatched
			pools[task.ref.pool] = pool
		case result := <-results:
			pool := pools[result.task.ref.pool]
			pool.running--
			pools[result.task.ref.pool] = pool
			entry := entries[result.task.ref.key()]
			if entry == nil {
				continue
			}
			entry.running = false
			if !entry.present {
				delete(entries, result.task.ref.key())
				continue
			}
			if result.ref.revision > entry.ref.revision {
				entry.ref = result.ref
			}
			if result.ineligible && result.task.epoch == epoch {
				pause()
			}
			if result.canceled || result.ineligible || result.task.epoch != epoch || !enabled {
				entry.due = time.Now()
				continue
			}
			now := time.Now()
			entry.lastAttempt = result.ref.revision
			observation := result.observation
			observation.Revision, observation.Generation, observation.Desired, observation.ObservedAt = result.ref.revision, result.ref.generation, result.ref.replicas, now.UTC()
			observation.RetryAfter = nil
			if result.err != nil {
				observation.State, observation.Error = "error", "unavailable"
				var failure *runtimeError
				if errors.As(result.err, &failure) {
					observation.Error = failure.code
				}
				r.report(runtimeFailure(observation.Error))
			}
			if result.ref.revision < entry.ref.revision {
				// A newer stop/rollout arrived during the operation. An old
				// failure must not back off that newer intent.
				entry.due, entry.failures = now, 0
			} else if result.err != nil {
				entry.failures = min(entry.failures+1, uint8(6))
				entry.due = now.Add(reconcileBackoff(entry.failures))
				after := entry.due.UTC()
				observation.RetryAfter = &after
			} else {
				entry.due, entry.failures = now.Add(reconcileInterval), 0
			}
			r.publish(result.ref.tenant, result.ref.id, observation)
		}
	}
}

func deploymentPriority(entry *scheduledDeployment) int {
	if entry.ref.revision > 1 && entry.ref.revision != entry.lastAttempt {
		return 0
	}
	if entry.lastAttempt == 0 {
		return 1
	}
	return 2
}

func nextDeployment(entries map[string]*scheduledDeployment, pools map[string]scheduledPool, now time.Time) (*scheduledDeployment, time.Time) {
	var best *scheduledDeployment
	var next time.Time
	for _, entry := range entries {
		if !entry.present || entry.running {
			continue
		}
		if entry.due.After(now) {
			if next.IsZero() || entry.due.Before(next) {
				next = entry.due
			}
			continue
		}
		if best == nil || deploymentBefore(entry, best, pools) {
			best = entry
		}
	}
	return best, next
}

func deploymentBefore(entry, best *scheduledDeployment, pools map[string]scheduledPool) bool {
	priority, bestPriority := deploymentPriority(entry), deploymentPriority(best)
	// Stops and other changed revisions retain priority across the whole catalog.
	if (priority == 0) != (bestPriority == 0) {
		return priority == 0
	}
	pool, bestPool := pools[entry.ref.pool], pools[best.ref.pool]
	if pool.running != bestPool.running {
		return pool.running < bestPool.running
	}
	if pool.lastDispatch != bestPool.lastDispatch {
		return pool.lastDispatch < bestPool.lastDispatch
	}
	// Within a pool, keep first attempts ahead of routine refreshes. Between
	// pools, rotate available slots so a large stalled catalog cannot starve one.
	if priority != bestPriority {
		return priority < bestPriority
	}
	return entry.due.Before(best.due) || entry.due.Equal(best.due) && entry.ref.key() < best.ref.key()
}

func reconcileBackoff(failures uint8) time.Duration {
	base := min(reconcileInterval<<min(failures-1, uint8(4)), 30*time.Second)
	return base/2 + time.Duration(rand.Int64N(int64(base/2)+1))
}

func (r *Reconciler) reconcileWorker(ctx context.Context, jobs <-chan reconcileTask, results chan<- reconcileResult) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-jobs:
			result := r.reconcileOne(task)
			select {
			case results <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (r *Reconciler) reconcileOne(task reconcileTask) reconcileResult {
	result := reconcileResult{task: task, ref: task.ref}
	probe, stop := context.WithTimeout(task.ctx, 3*time.Second)
	allowed := r.Eligible(probe)
	stop()
	if !allowed {
		result.ineligible, result.canceled = true, task.ctx.Err() != nil
		return result
	}
	if task.ctx.Err() != nil {
		result.canceled = true
		return result
	}
	v, err := r.Backend.GetWorkerDeployment(task.ref.tenant, task.ref.id)
	if err != nil || v == nil || v.TenantID != task.ref.tenant || v.ID != task.ref.id || v.Revision < task.ref.revision || v.Generation == 0 {
		result.err = runtimeFailure("state_unavailable")
		return result
	}
	result.ref = deploymentRef{v.TenantID, v.ID, v.Revision, v.Generation, v.Spec.Replicas, v.Spec.Pool}
	if task.ctx.Err() != nil {
		result.canceled = true
		return result
	}
	operation, cancel := context.WithTimeout(task.ctx, reconcileTimeout)
	result.observation, result.err = r.Runtime.Reconcile(operation, *v)
	if result.err == nil {
		result.err = operation.Err()
	}
	cancel()
	result.canceled = task.ctx.Err() != nil
	return result
}
