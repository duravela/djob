// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package worker_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/sdk/go"
	"github.com/duravela/djob/sdk/go/worker"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
)

type retryCloseDriver struct {
	*device
	failure      error
	retryStarted chan struct{}
	releaseRetry chan struct{}
}

func (d *retryCloseDriver) Close(context.Context) error {
	if d.closed.Add(1) == 1 {
		return d.failure
	}
	close(d.retryStarted)
	<-d.releaseRetry
	return nil
}

func TestReportedCloseFailureStopsBeforeAnotherJobOrRegistration(t *testing.T) {
	for _, mode := range []string{"run", "serve"} {
		for _, fault := range []string{"none", "close acknowledgement", "drain acknowledgement"} {
			t.Run(mode+"/"+fault, func(t *testing.T) {
				var primaryRegistrations, fallbackRegistrations, effects, factories, retries atomic.Int32
				failure := errors.New("local driver close failed")
				d := &retryCloseDriver{device: &device{execute: func(context.Context, djob.Action) djob.Effect {
					effects.Add(1)
					return djob.Effect{Status: djob.EffectSucceeded}
				}}, failure: failure, retryStarted: make(chan struct{}), releaseRetry: make(chan struct{})}
				c := &controller{commands: []wire.Command{action(1), cleanup(2, wire.Quiesce), cleanup(3, wire.Close), {
					ID: strings.Repeat("f", 64), Sequence: 4, Kind: wire.Evaluate, Timeout: time.Second, Budget: time.Second,
					View: &djob.View{Run: djob.JobRun{TenantID: "alpha", ID: "next-job", Definition: djob.JobDefinition{Handler: "test", HandlerVersion: "1"}}},
				}}, resultStatus: func(result wire.Result) int {
					if result.ID == cleanup(3, wire.Close).ID && fault == "close acknowledgement" {
						return http.StatusServiceUnavailable
					}
					return http.StatusNoContent
				}}
				primary := tlsController(t, func(w http.ResponseWriter, r *http.Request) {
					if strings.HasSuffix(r.URL.Path, "/drain") && fault == "drain acknowledgement" {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					if strings.HasSuffix(r.URL.Path, "/register") {
						primaryRegistrations.Add(1)
					}
					c.handler(w, r)
				})
				fallback := tlsController(t, func(w http.ResponseWriter, r *http.Request) {
					if strings.HasSuffix(r.URL.Path, "/register") {
						fallbackRegistrations.Add(1)
					}
					(&controller{}).handler(w, r)
				})
				cfg := supervisedConfig(t, primary, fallback)
				cfg.Registry.Drivers = map[djob.Version]djob.DriverFactory{{Name: "device", Version: "1"}: func(djob.JobRun) (djob.Driver, error) {
					factories.Add(1)
					return d, nil
				}}
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				done := make(chan error, 1)
				go func() {
					defer close(done)
					if mode == "run" {
						done <- worker.Run(ctx, cfg)
						return
					}
					retry := smallBackoff()
					retry.OnRetry = func(worker.RetryObservation) { retries.Add(1) }
					done <- worker.Serve(ctx, cfg, retry)
				}()
				t.Cleanup(func() {
					select {
					case <-d.releaseRetry:
					default:
						close(d.releaseRetry)
					}
					cancel()
					select {
					case <-done:
					case <-time.After(2 * time.Second):
						t.Error("worker did not join cleanup")
					}
				})
				select {
				case <-d.retryStarted:
				case <-time.After(2 * time.Second):
					t.Fatal("local close retry did not start")
				}
				select {
				case err := <-done:
					t.Fatalf("worker detached cleanup: %v", err)
				default:
				}
				close(d.releaseRetry)
				select {
				case err := <-done:
					if !errors.Is(err, worker.ErrCleanupFailed) || !errors.Is(err, failure) {
						t.Fatalf("close failure was lost after retry succeeded: %v", err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("worker did not return its close failure")
				}
				if primaryRegistrations.Load() != 1 || fallbackRegistrations.Load() != 0 || retries.Load() != 0 || effects.Load() != 1 || factories.Load() != 1 {
					t.Fatalf("unsafe continuation: primary=%d fallback=%d retries=%d effects=%d factories=%d", primaryRegistrations.Load(), fallbackRegistrations.Load(), retries.Load(), effects.Load(), factories.Load())
				}
				c.mu.Lock()
				defer c.mu.Unlock()
				wantResults := 3
				if fault == "drain acknowledgement" {
					wantResults = 2
				}
				if len(c.commands) != 1 || len(c.results) != wantResults || (wantResults == 3 && c.results[2].Error != failure.Error()) {
					t.Fatalf("close failure was not reported before stopping: queued=%d results=%v", len(c.commands), c.results)
				}
			})
		}
	}
}

func TestQuiesceFailureWithConfirmedCloseAllowsIndependentWork(t *testing.T) {
	failure := errors.New("device quiescence is unproven")
	d := &cleanupDriver{device: &device{execute: func(context.Context, djob.Action) djob.Effect {
		return djob.Effect{Status: djob.EffectSucceeded}
	}}, quiesceErr: failure}
	c := &controller{commands: []wire.Command{action(1), cleanup(2, wire.Quiesce), cleanup(3, wire.Close), {
		ID: strings.Repeat("f", 64), Sequence: 4, Kind: wire.Evaluate, Timeout: time.Second, Budget: time.Second,
		View: &djob.View{Run: djob.JobRun{TenantID: "alpha", ID: "independent", Definition: djob.JobDefinition{Handler: "test", HandlerVersion: "1"}}},
	}}}
	cfg, _ := setup(t, c, d.device)
	cfg.Registry.Drivers[djob.Version{Name: "device", Version: "1"}] = func(djob.JobRun) (djob.Driver, error) { return d, nil }
	if err := worker.Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.results) != 4 || c.results[1].Error != failure.Error() || c.results[3].Decision == nil || c.results[3].Decision.Status != djob.JobSucceeded {
		t.Fatalf("quiescence report or independent evaluation lost: %v", c.results)
	}
	if d.closed.Load() != 1 {
		t.Fatalf("local driver was not closed once: %d", d.closed.Load())
	}
}
