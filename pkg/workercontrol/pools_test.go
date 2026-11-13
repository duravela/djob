package workercontrol

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func poolConfig(t *testing.T, pool, namespace string) (KubernetesConfig, *kubeFixture) {
	t.Helper()
	k, f, _ := kubeRuntime(t)
	f.mu.Lock()
	f.namespace = namespace
	f.token = "service-account-" + pool
	token := f.token
	f.mu.Unlock()
	cfg := k.cfg
	cfg.Pool, cfg.Namespace = pool, namespace
	require.NoError(t, os.WriteFile(cfg.TokenFile, []byte(token), 0600))
	return cfg, f
}

func TestKubernetesPoolsRouteNamespacesAndCredentialsWithoutFallback(t *testing.T) {
	alpha, fa := poolConfig(t, "alpha", "alpha-workers")
	beta, fb := poolConfig(t, "beta", "beta-workers")
	pools, err := NewKubernetesPools([]KubernetesConfig{alpha, beta})
	require.NoError(t, err)
	t.Cleanup(pools.Close)
	v := desiredWorker()
	for _, test := range []struct{ pool, namespace, secret string }{
		{"alpha", "alpha-workers", "alpha-config"},
		{"beta", "beta-workers", "beta-config"},
	} {
		v.Spec.Pool, v.Spec.ConfigSecret = test.pool, test.secret
		out, err := pools.Reconcile(context.Background(), v)
		require.NoError(t, err)
		require.Equal(t, "available", out.State)
		require.Equal(t, test.pool, out.Pool)
		require.Equal(t, test.namespace, out.Namespace)
	}
	for _, f := range []*kubeFixture{fa, fb} {
		require.Equal(t, 1, f.creates)
		require.Zero(t, f.updates)
	}
	for _, name := range []string{"missing", "default", ""} {
		v.Spec.Pool = name
		out, err := pools.Reconcile(context.Background(), v)
		require.ErrorContains(t, err, "pool_unavailable")
		require.Equal(t, name, out.Pool)
		require.Empty(t, out.Namespace, "an unconfigured pool must not inherit another pool's namespace")
	}
	// Each pool rereads its own credential. Losing access to one API must not
	// prevent the other pool from observing its existing workload.
	fa.mu.Lock()
	fa.token = "rotated-service-account"
	fa.mu.Unlock()
	v.Spec.Pool, v.Spec.ConfigSecret = "alpha", "alpha-config"
	_, err = pools.Reconcile(context.Background(), v)
	require.ErrorContains(t, err, "forbidden")
	v.Spec.Pool, v.Spec.ConfigSecret = "beta", "beta-config"
	out, err := pools.Reconcile(context.Background(), v)
	require.NoError(t, err)
	require.Equal(t, "available", out.State)
	require.Zero(t, fb.updates)
}

func TestKubernetesPoolsDoNotSerializeUnrelatedAPIsAndHonorCancellation(t *testing.T) {
	slow, fs := poolConfig(t, "slow", "slow-workers")
	fast, _ := poolConfig(t, "fast", "fast-workers")
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	fs.mu.Lock()
	fs.mutate = func(map[string]any) { close(started); <-release }
	fs.mu.Unlock()
	pools, err := NewKubernetesPools([]KubernetesConfig{slow, fast})
	require.NoError(t, err)
	t.Cleanup(pools.Close)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	v := desiredWorker()
	v.Spec.Pool = "slow"
	slowWorker := v
	go func() { _, err := pools.Reconcile(ctx, slowWorker); done <- err }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("slow pool did not start its API operation")
	}
	v.Spec.Pool = "fast"
	probe, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	_, err = pools.Reconcile(probe, v)
	require.NoError(t, err, "a stalled pool must not block another pool")
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("pool operation did not return after cancellation")
	}
	unblock()
}

func TestKubernetesPoolsValidateUniqueNamesAndBoundConfiguration(t *testing.T) {
	cfg := KubernetesConfig{ServerURL: "https://kubernetes.example", ClusterID: "plant", Namespace: "workers", TokenFile: "/private/token"}
	_, err := NewKubernetesPools(nil)
	require.Error(t, err)
	configs := make([]KubernetesConfig, MaxKubernetesPools+1)
	for i := range configs {
		configs[i] = cfg
		configs[i].Pool = fmt.Sprintf("pool-%d", i)
	}
	_, err = NewKubernetesPools(configs)
	require.Error(t, err)
	pools, err := NewKubernetesPools(configs[:MaxKubernetesPools])
	require.NoError(t, err)
	pools.Close()
	other := cfg
	other.Pool = "default"
	_, err = NewKubernetesPools([]KubernetesConfig{cfg, other})
	require.ErrorContains(t, err, "duplicate Kubernetes worker pool")
	other.Pool = "invalid_pool"
	_, err = NewKubernetesPools([]KubernetesConfig{cfg, other})
	require.Error(t, err)
}
