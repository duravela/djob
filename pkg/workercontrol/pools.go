package workercontrol

import (
	"context"
	"fmt"
	"time"

	"github.com/duravela/djob/pkg/db"
)

// MaxKubernetesPools bounds the controller's local runtime configuration.
const MaxKubernetesPools = 16

// KubernetesPools routes a deployment to its explicitly configured pool. Each
// pool has its own namespace, API trust and service-account credential. The
// routing table is immutable; all pools share the reconciler's work limits.
type KubernetesPools struct {
	runtimes map[string]*Kubernetes
}

// NewKubernetesPools validates and constructs an immutable set of worker pools.
func NewKubernetesPools(configs []KubernetesConfig) (*KubernetesPools, error) {
	if len(configs) == 0 || len(configs) > MaxKubernetesPools {
		return nil, fmt.Errorf("configure 1 to %d Kubernetes worker pools", MaxKubernetesPools)
	}
	pools := &KubernetesPools{runtimes: make(map[string]*Kubernetes, len(configs))}
	for _, cfg := range configs {
		name := cfg.Pool
		if name == "" {
			name = "default"
		}
		if _, duplicate := pools.runtimes[name]; duplicate {
			pools.Close()
			return nil, fmt.Errorf("duplicate Kubernetes worker pool %q", name)
		}
		runtime, err := NewKubernetes(cfg)
		if err != nil {
			pools.Close()
			return nil, err
		}
		pools.runtimes[name] = runtime
	}
	return pools, nil
}

func (p *KubernetesPools) Close() {
	for _, runtime := range p.runtimes {
		runtime.Close()
	}
}

func (p *KubernetesPools) Reconcile(ctx context.Context, v db.WorkerDeployment) (Observation, error) {
	if runtime, configured := p.runtimes[v.Spec.Pool]; configured {
		return runtime.Reconcile(ctx, v)
	}
	// An unknown pool must not contact or fall back to another runtime.
	return Observation{Pool: v.Spec.Pool, Revision: v.Revision, Generation: v.Generation, Desired: v.Spec.Replicas, ObservedAt: time.Now().UTC(), State: "pending"}, runtimeFailure("pool_unavailable")
}
