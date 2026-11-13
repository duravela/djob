// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package worker_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/sdk/go"
	"github.com/duravela/djob/sdk/go/worker"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
)

type device struct {
	execute          func(context.Context, djob.Action) djob.Effect
	quiesced, closed atomic.Int32
}

func (d *device) Execute(ctx context.Context, a djob.Action) djob.Effect { return d.execute(ctx, a) }
func (d *device) Quiesce(context.Context) error                          { d.quiesced.Add(1); return nil }
func (d *device) Close(context.Context) error                            { d.closed.Add(1); return nil }

type controller struct {
	mu            sync.Mutex
	commands      []wire.Command
	results       []wire.Result
	delay         time.Duration
	budget        time.Duration
	failHeartbeat func() bool
	resultStatus  func(wire.Result) int
	drained       chan struct{}
	drainOnce     sync.Once
}

func (c *controller) handler(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer "+strings.Repeat("s", 32) {
		w.WriteHeader(401)
		return
	}
	write := func(v any) { w.Header().Set("Content-Type", "application/json"); _ = json.NewEncoder(w).Encode(v) }
	switch {
	case strings.HasSuffix(r.URL.Path, "/register"):
		write(wire.Registered{Protocol: wire.Version, SessionID: strings.Repeat("a", 64), TenantID: "alpha", HeartbeatEvery: 20 * time.Millisecond})
	case strings.HasSuffix(r.URL.Path, "/heartbeat"):
		if c.failHeartbeat != nil && c.failHeartbeat() {
			w.WriteHeader(503)
			return
		}
		var q wire.Heartbeat
		_ = json.NewDecoder(r.Body).Decode(&q)
		budget := c.budget
		if budget == 0 {
			budget = time.Second
		}
		write(wire.HeartbeatReply{CommandID: q.CommandID, Budget: budget})
	case strings.HasSuffix(r.URL.Path, "/poll"):
		c.mu.Lock()
		if len(c.commands) == 0 {
			c.mu.Unlock()
			write(wire.Command{Kind: wire.Drained})
			return
		}
		command := c.commands[0]
		c.commands = c.commands[1:]
		delay := c.delay
		c.delay = 0
		c.mu.Unlock()
		if delay > 0 {
			time.Sleep(delay)
		}
		write(command)
	case strings.HasSuffix(r.URL.Path, "/result"):
		var result wire.Result
		_ = json.NewDecoder(r.Body).Decode(&result)
		c.mu.Lock()
		c.results = append(c.results, result)
		c.mu.Unlock()
		status := http.StatusNoContent
		if c.resultStatus != nil {
			status = c.resultStatus(result)
		}
		w.WriteHeader(status)
	case strings.HasSuffix(r.URL.Path, "/drain"):
		if c.drained != nil {
			c.drainOnce.Do(func() { close(c.drained) })
		}
		w.WriteHeader(204)
	case strings.HasSuffix(r.URL.Path, "/disconnect"):
		w.WriteHeader(204)
	default:
		w.WriteHeader(404)
	}
}

func setup(t *testing.T, c *controller, d *device) (worker.Config, *atomic.Int32) {
	t.Helper()
	s := httptest.NewTLSServer(http.HandlerFunc(c.handler))
	t.Cleanup(s.Close)
	factories := &atomic.Int32{}
	return worker.Config{Transport: worker.TransportHTTPSV1, ServerURL: s.URL, WorkerID: "test-worker", TenantID: "alpha", Token: strings.Repeat("s", 32), Client: s.Client(), DrainTimeout: time.Second, Registry: djob.Registry{
		Handlers: map[djob.Version]djob.Handler{{Name: "test", Version: "1"}: func(djob.View) (djob.Decision, error) {
			return djob.Decision{Kind: djob.JobComplete, Status: djob.JobSucceeded}, nil
		}},
		Drivers: map[djob.Version]djob.DriverFactory{{Name: "device", Version: "1"}: func(djob.JobRun) (djob.Driver, error) { factories.Add(1); return d, nil }},
	}}, factories
}

func action(sequence uint64) wire.Command {
	run := &djob.JobRun{TenantID: "alpha", ID: "run", AttemptID: "attempt", OwnerID: "owner", Binding: &djob.JobBinding{Driver: "device", DriverVersion: "1"}, PendingAction: &djob.JobAction{TenantID: "alpha", RunID: "run", AttemptID: "attempt", Sequence: sequence, Operation: "measure", Status: djob.EffectAuthorized}}
	return wire.Command{ID: strings.Repeat(string(rune('a'+sequence)), 64), Sequence: sequence, Kind: wire.Execute, Timeout: 2 * time.Second, Budget: time.Second, Run: run, Action: &djob.Action{TenantID: "alpha", RunID: "run", AttemptID: "attempt", Sequence: sequence, Operation: "measure", IdempotencyKey: "stable-action-key"}}
}
func cleanup(sequence uint64, kind wire.Kind) wire.Command {
	return wire.Command{ID: strings.Repeat(string(rune('a'+sequence)), 64), Sequence: sequence, Kind: kind, Timeout: time.Second, Budget: 500 * time.Millisecond, Run: action(1).Run}
}

func TestRelatedActionsKeepOneDriverAndAcknowledgeCleanup(t *testing.T) {
	var effects atomic.Int32
	d := &device{execute: func(context.Context, djob.Action) djob.Effect {
		effects.Add(1)
		return djob.Effect{Status: djob.EffectSucceeded}
	}}
	c := &controller{commands: []wire.Command{action(1), action(2), cleanup(3, wire.Quiesce), cleanup(4, wire.Close)}}
	cfg, factories := setup(t, c, d)
	if err := worker.Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if effects.Load() != 2 || factories.Load() != 1 || d.closed.Load() != 1 || d.quiesced.Load() != 1 {
		t.Fatalf("effects=%d factories=%d closed=%d quiesced=%d", effects.Load(), factories.Load(), d.closed.Load(), d.quiesced.Load())
	}
}

func TestReplayedCommandNeverRepeatsAnEffect(t *testing.T) {
	var effects atomic.Int32
	d := &device{execute: func(context.Context, djob.Action) djob.Effect {
		effects.Add(1)
		return djob.Effect{Status: djob.EffectSucceeded}
	}}
	c := &controller{commands: []wire.Command{action(1), action(1)}}
	cfg, _ := setup(t, c, d)
	if err := worker.Run(context.Background(), cfg); err == nil {
		t.Fatal("replayed sequence was accepted")
	}
	if effects.Load() != 1 || d.closed.Load() != 1 {
		t.Fatalf("effects=%d closed=%d", effects.Load(), d.closed.Load())
	}
}

func TestNetworkDelayConsumesAuthorizationBeforeFactoryOrIO(t *testing.T) {
	cmd := action(1)
	cmd.Budget = time.Millisecond
	c := &controller{commands: []wire.Command{cmd}, delay: 60 * time.Millisecond}
	d := &device{execute: func(context.Context, djob.Action) djob.Effect {
		t.Error("expired permission started I/O")
		return djob.Effect{}
	}}
	cfg, factories := setup(t, c, d)
	if err := worker.Run(context.Background(), cfg); err == nil {
		t.Fatal("expired permission was accepted")
	}
	if factories.Load() != 0 {
		t.Fatal("expired permission allocated a driver")
	}
}

func TestHeartbeatLossCancelsAndJoinsIOBeforeReturn(t *testing.T) {
	var running atomic.Bool
	stopped := make(chan struct{})
	d := &device{execute: func(ctx context.Context, _ djob.Action) djob.Effect {
		running.Store(true)
		<-ctx.Done()
		close(stopped)
		return djob.Effect{Status: djob.EffectUnknown}
	}}
	c := &controller{commands: []wire.Command{action(1)}, failHeartbeat: running.Load}
	cfg, _ := setup(t, c, d)
	if err := worker.Run(context.Background(), cfg); err == nil {
		t.Fatal("connection loss was hidden")
	}
	select {
	case <-stopped:
	default:
		t.Fatal("Run detached active I/O")
	}
	if d.closed.Load() != 1 || d.quiesced.Load() != 1 {
		t.Fatal("connection loss did not recover and close the driver")
	}
}

func TestFreshHeartbeatPermissionSupportsLongerActions(t *testing.T) {
	cmd := action(1)
	cmd.Budget = 150 * time.Millisecond
	d := &device{execute: func(ctx context.Context, _ djob.Action) djob.Effect {
		timer := time.NewTimer(450 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			t.Error("fresh permission did not renew the action")
			return djob.Effect{Status: djob.EffectUnknown}
		case <-timer.C:
			return djob.Effect{Status: djob.EffectSucceeded}
		}
	}}
	c := &controller{commands: []wire.Command{cmd, cleanup(2, wire.Quiesce), cleanup(3, wire.Close)}, budget: 300 * time.Millisecond}
	cfg, _ := setup(t, c, d)
	if err := worker.Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDrainsTheCurrentConversation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	drainSeen := make(chan struct{})
	started := make(chan struct{})
	d := &device{execute: func(ctx context.Context, _ djob.Action) djob.Effect {
		close(started)
		select {
		case <-ctx.Done():
			t.Error("shutdown interrupted I/O before draining")
		case <-drainSeen:
		}
		return djob.Effect{Status: djob.EffectSucceeded}
	}}
	c := &controller{commands: []wire.Command{action(1), cleanup(2, wire.Quiesce), cleanup(3, wire.Close)}, drained: drainSeen}
	cfg, _ := setup(t, c, d)
	go func() { <-started; cancel() }()
	if err := worker.Run(parent, cfg); err != nil {
		t.Fatal(err)
	}
	if d.closed.Load() != 1 {
		t.Fatal("drain did not close the session")
	}
}

func TestConnectionConfigurationRejectsPlaintextAndUnverifiedTLS(t *testing.T) {
	d := &device{execute: func(context.Context, djob.Action) djob.Effect { return djob.Effect{} }}
	cfg, _ := setup(t, &controller{}, d)
	for _, url := range []string{"http://localhost:8080", "https://secret-marker@example.com", "https://example.com?secret-marker"} {
		candidate := cfg
		candidate.ServerURL = url
		if err := worker.Run(context.Background(), candidate); err == nil || strings.Contains(err.Error(), "secret-marker") {
			t.Fatalf("invalid server configuration: %v", err)
		}
	}
	cfg.Client = &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	if err := worker.Run(context.Background(), cfg); err == nil {
		t.Fatal("unverified TLS accepted")
	}
	path := filepath.Join(t.TempDir(), "worker.json")
	if err := os.WriteFile(path, []byte(`{"token":"secret-marker"}`), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.LoadConfig(path); err == nil || strings.Contains(err.Error(), "secret-marker") {
		t.Fatalf("unsafe credential file: %v", err)
	}
}
