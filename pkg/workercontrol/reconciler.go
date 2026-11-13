package workercontrol

import (
	"context"
	"sync"
	"time"

	"github.com/duravela/djob/pkg/db"
)

type Observation struct {
	Pool                string     `json:"pool"`
	Namespace           string     `json:"namespace"`
	Name                string     `json:"name"`
	Revision            uint64     `json:"revision,string"`
	Generation          uint64     `json:"generation,string"`
	Desired             uint32     `json:"desired_replicas"`
	Replicas            uint32     `json:"replicas"`
	Ready               uint32     `json:"ready_replicas"`
	Available           uint32     `json:"available_replicas"`
	Updated             uint32     `json:"updated_replicas"`
	Terminating         *uint32    `json:"terminating_replicas,omitempty"`
	RolloutPaused       bool       `json:"rollout_paused,omitempty"`
	State               string     `json:"state"`
	Error               string     `json:"error,omitempty"`
	ObservedAt          time.Time  `json:"observed_at"`
	RetryAfter          *time.Time `json:"retry_after,omitempty"`
	DeletionRequestedAt *time.Time `json:"deletion_requested_at,omitempty"`
}

type Runtime interface {
	// Reconcile must honor cancellation and return after all request goroutines
	// it started have stopped. Shutdown joins every outstanding operation.
	Reconcile(context.Context, db.WorkerDeployment) (Observation, error)
}
type Backend interface {
	ListWorkerDeploymentRecords([]byte, int) ([]db.WorkerDeployment, []byte, error)
	GetWorkerDeployment(string, string) (*db.WorkerDeployment, error)
}

type Reconciler struct {
	Backend Backend
	Runtime Runtime
	// Eligible must confirm leadership, quorum and local application before
	// each deployment. The runtime also uses monotonic revisions and resource CAS.
	Eligible func(context.Context) bool
	// OnError receives sanitized runtime or state-access errors. It must be
	// bounded and return promptly, like other background-worker observers.
	OnError      func(error)
	mu           sync.Mutex
	observations map[string]Observation
}

func (r *Reconciler) Observation(tenant, id string) (Observation, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.observations[tenant+"\x00"+id]
	return cloneObservation(v), ok
}

func cloneObservation(v Observation) Observation {
	if v.Terminating != nil {
		n := *v.Terminating
		v.Terminating = &n
	}
	if v.RetryAfter != nil {
		at := *v.RetryAfter
		v.RetryAfter = &at
	}
	if v.DeletionRequestedAt != nil {
		at := *v.DeletionRequestedAt
		v.DeletionRequestedAt = &at
	}
	return v
}

// Run keeps a bounded process-local work set. Catalog refreshes do not wait for
// Kubernetes calls, so a changed revision can take the next free worker slot.
func (r *Reconciler) Run(ctx context.Context) {
	r.run(ctx, reconcileConcurrency)
}

func (r *Reconciler) publish(tenant, id string, observation Observation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.observations == nil {
		r.observations = make(map[string]Observation)
	}
	r.observations[tenant+"\x00"+id] = cloneObservation(observation)
}

func (r *Reconciler) report(err error) {
	if r.OnError != nil {
		r.OnError(err)
	}
}

// Observations returns independent snapshots without exposing mutable pointers.
func (r *Reconciler) Observations() []Observation {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Observation, 0, len(r.observations))
	for _, v := range r.observations {
		out = append(out, cloneObservation(v))
	}
	return out
}
