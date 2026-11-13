// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/duravela/djob/sdk/go"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
)

type authorityKey struct{}

// Gateways returns the current action's controller-verified gateway profile.
// It is available only inside Driver.Execute. The driver must still honor ctx;
// neither the snapshot nor a logical epoch supplies a physical fence.
func Gateways(ctx context.Context) ([]wire.Gateway, bool) {
	v, ok := ctx.Value(authorityKey{}).([]wire.Gateway)
	return append([]wire.Gateway(nil), v...), ok && ctx.Err() == nil
}

type callLease struct {
	id     string
	cancel context.CancelFunc
	mu     sync.Mutex
	timer  *time.Timer
	closed bool
}

// Renewal starts before its request. Network and server delays consume
// permission rather than creating a fresh lifetime when a response arrives.
func (l *callLease) renew(started time.Time, budget time.Duration) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	remaining := time.Until(started.Add(budget))
	if l.closed || budget <= 0 || remaining <= 0 {
		l.cancel()
		return false
	}
	if l.timer != nil {
		l.timer.Stop()
	}
	l.timer = time.AfterFunc(remaining, l.cancel)
	return true
}

func (l *callLease) stop() {
	l.mu.Lock()
	l.closed = true
	if l.timer != nil {
		l.timer.Stop()
	}
	l.cancel()
	l.mu.Unlock()
}

type client struct {
	rpc       *workerRPC
	cfg       Config
	sessionID string
	mu        sync.Mutex
	active    *callLease
	driver    djob.Driver
	run       *djob.JobRun
	// A failed reported Close ends this session even if later local cleanup
	// succeeds or the result acknowledgement is lost.
	closeFailure error
}

func (c *client) exchange(ctx context.Context, path string, body, reply any) (int, error) {
	if c.rpc != nil {
		return c.exchangeGRPC(ctx, path, body, reply)
	}
	return c.exchangeHTTPS(ctx, path, body, reply)
}

func (c *client) exchangeHTTPS(ctx context.Context, path string, body, reply any) (int, error) {
	data, err := json.Marshal(body)
	if err != nil || len(data) > wire.MaxResultBytes {
		return 0, fmt.Errorf("worker message exceeds its limit")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.ServerURL+wire.Path+path, bytes.NewReader(data))
	if err != nil {
		return 0, ErrSessionLost
	}
	request.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	request.Header.Set("Content-Type", "application/json")
	response, err := c.cfg.Client.Do(request)
	if err != nil {
		return 0, transportError(ctx, err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		if reply != nil && !strings.HasSuffix(path, "/poll") {
			return 0, fmt.Errorf("invalid worker response")
		}
		return response.StatusCode, nil
	}
	if response.StatusCode != http.StatusOK {
		// Do not echo a proxy/server response or URL into job logs or stderr.
		return response.StatusCode, rejected(path, response.StatusCode)
	}
	data, err = io.ReadAll(io.LimitReader(response.Body, wire.MaxCommandBytes+1))
	if err != nil {
		return 0, transportError(ctx, err)
	}
	if len(data) > wire.MaxCommandBytes || reply == nil || strict(data, reply) != nil {
		return 0, fmt.Errorf("invalid worker response")
	}
	return response.StatusCode, nil
}

// Run serves one fresh worker session. It returns on disconnection and never
// resumes a session or resends an external command. All driver callbacks finish
// before Run returns, including recovery/close after a lost connection. Callers
// supervising sessions must stop on ErrCleanupFailed; Serve handles that policy.
func Run(parent context.Context, cfg Config) error { return runSession(parent, cfg, nil) }

func runSession(parent context.Context, cfg Config, observe func(time.Duration)) (err error) {
	if err := cfg.validate(); err != nil {
		return err
	}
	// A caller cannot mutate the live capability maps during execution.
	registry := djob.Registry{Handlers: make(map[djob.Version]djob.Handler), Drivers: make(map[djob.Version]djob.DriverFactory)}
	registration := wire.Register{DeploymentID: cfg.DeploymentID, DeploymentGeneration: cfg.DeploymentGeneration, Protocol: wire.Version, WorkerID: cfg.WorkerID, TenantID: cfg.TenantID}
	for v, handler := range cfg.Registry.Handlers {
		registration.Handlers = append(registration.Handlers, v)
		registry.Handlers[v] = handler
	}
	for v, factory := range cfg.Registry.Drivers {
		registration.Drivers = append(registration.Drivers, v)
		registry.Drivers[v] = factory
	}
	cfg.Registry = registry
	c := &client{cfg: cfg}
	if cfg.Transport == TransportGRPC {
		c.rpc, err = openWorkerRPC(parent, cfg)
		if err != nil {
			return err
		}
		defer c.rpc.close()
	}
	var registered wire.Registered
	if _, err := c.exchange(parent, "register", registration, &registered); err != nil {
		return err
	}
	if registered.DeploymentID != cfg.DeploymentID || registered.DeploymentGeneration != cfg.DeploymentGeneration || registered.Protocol != wire.Version || registered.TenantID != cfg.TenantID || !wire.ValidNonce(registered.SessionID) || registered.HeartbeatEvery < 10*time.Millisecond || registered.HeartbeatEvery > 3*time.Second {
		return fmt.Errorf("invalid worker registration response")
	}
	c.sessionID = registered.SessionID
	if err := parent.Err(); err != nil {
		ctx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		_, _ = c.exchange(ctx, c.sessionID+"/disconnect", struct{}{}, nil)
		return err
	}
	established := time.Now()
	life, cancel := context.WithCancelCause(context.Background())
	var heartbeat sync.WaitGroup
	heartbeat.Add(1)
	go func() {
		defer heartbeat.Done()
		c.heartbeat(life, cancel, registered.HeartbeatEvery)
	}()
	heartbeat.Add(1)
	go func() {
		defer heartbeat.Done()
		select {
		case <-life.Done():
			return
		case <-parent.Done():
		}
		request, stop := context.WithTimeout(life, 2*time.Second)
		_, err := c.exchange(request, c.sessionID+"/drain", struct{}{}, nil)
		stop()
		if err != nil {
			cancel(err)
			return
		}
		timer := time.NewTimer(cfg.DrainTimeout)
		defer timer.Stop()
		select {
		case <-life.Done():
		case <-timer.C:
			cancel(parent.Err())
		}
	}()
	defer func() {
		if observe != nil {
			observe(time.Since(established))
		}
		// A heartbeat can reject credentials while a poll/result response is
		// already in flight. Do not turn that terminal rejection into a retry
		// merely because the other request reports a transient error later.
		if cause := context.Cause(life); err != nil && cause != nil && !errors.Is(cause, ErrSessionLost) {
			err = cause
		}
		if c.closeFailure != nil && !errors.Is(err, ErrCleanupFailed) {
			err = errors.Join(err, c.closeFailure)
		}
		cancel(context.Canceled)
		heartbeat.Wait()
		if c.driver != nil {
			ctx, stop := context.WithTimeout(context.Background(), cfg.CleanupTimeout)
			recoverErr := c.driver.Quiesce(ctx)
			stop()
			ctx, stop = context.WithTimeout(context.Background(), cfg.CleanupTimeout)
			closeErr := c.driver.Close(ctx)
			stop()
			if recoverErr != nil || closeErr != nil {
				err = errors.Join(err, ErrCleanupFailed, recoverErr, closeErr)
			}
		}
		ctx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		_, _ = c.exchange(ctx, c.sessionID+"/disconnect", struct{}{}, nil)
	}()
	var sequence uint64
	for life.Err() == nil {
		started := time.Now()
		var command wire.Command
		status, err := c.exchange(life, c.sessionID+"/poll", struct{}{}, &command)
		if err != nil {
			return err
		}
		if status == http.StatusNoContent {
			continue
		}
		if command.Kind == wire.Drained {
			return nil
		}
		if command.Sequence != sequence+1 || !wire.ValidNonce(command.ID) || command.Timeout <= 0 || command.Timeout > time.Hour || command.Budget <= 0 || command.Budget > command.Timeout {
			return fmt.Errorf("invalid or replayed worker command")
		}
		sequence = command.Sequence
		operation, stop := context.WithDeadline(life, started.Add(command.Timeout))
		lease := &callLease{id: command.ID, cancel: stop}
		if !lease.renew(started, command.Budget) || operation.Err() != nil {
			lease.stop()
			return ErrSessionLost
		}
		c.mu.Lock()
		c.active = lease
		c.mu.Unlock()
		result, executeErr := c.execute(operation, command)
		lease.stop()
		c.mu.Lock()
		c.active = nil
		c.mu.Unlock()
		if executeErr != nil {
			return executeErr
		}
		if c.closeFailure != nil {
			// Pause admission before completing the current cleanup call, even
			// when connected to an older version-1 controller. If drain cannot
			// be confirmed, disconnect without completing that call.
			request, stop := context.WithTimeout(life, 2*time.Second)
			_, drainErr := c.exchange(request, c.sessionID+"/drain", struct{}{}, nil)
			stop()
			if drainErr != nil {
				return drainErr
			}
		}
		if _, err := c.exchange(life, c.sessionID+"/result", result, nil); err != nil {
			return err
		}
		if c.closeFailure != nil {
			return c.closeFailure
		}
	}
	if parent.Err() != nil {
		return parent.Err()
	}
	return context.Cause(life)
}

func (c *client) heartbeat(ctx context.Context, cancel context.CancelCauseFunc, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		c.mu.Lock()
		active := c.active
		c.mu.Unlock()
		request := wire.Heartbeat{}
		if active != nil {
			request.CommandID = active.id
		}
		started := time.Now()
		var response wire.HeartbeatReply
		if _, err := c.exchange(ctx, c.sessionID+"/heartbeat", request, &response); err != nil {
			cancel(err)
			return
		}
		if response.CommandID != request.CommandID || response.Budget < 0 || response.Budget > time.Hour {
			cancel(fmt.Errorf("invalid worker heartbeat response"))
			return
		}
		if active != nil && response.CommandID == active.id {
			if response.Cancel {
				active.cancel()
			} else {
				active.renew(started, response.Budget)
			}
		}
	}
}

func sameAttempt(a, b *djob.JobRun) bool {
	return a != nil && b != nil && a.TenantID == b.TenantID && a.ID == b.ID && a.OwnerID == b.OwnerID && a.AttemptID == b.AttemptID
}

func (c *client) execute(ctx context.Context, command wire.Command) (wire.Result, error) {
	r := wire.Result{ID: command.ID}
	switch command.Kind {
	case wire.Evaluate:
		if command.View == nil || command.View.Run.TenantID != c.cfg.TenantID || (c.driver != nil && !sameAttempt(c.run, &command.View.Run)) {
			return r, fmt.Errorf("worker view does not match its session")
		}
		handler := c.cfg.Registry.Handlers[djob.Version{Name: command.View.Run.Definition.Handler, Version: command.View.Run.Definition.HandlerVersion}]
		if handler == nil {
			return r, fmt.Errorf("unregistered worker handler")
		}
		decision, err := handler(*command.View)
		if err != nil {
			r.Error = err.Error()
		} else {
			r.Decision = &decision
		}
	case wire.Execute:
		run, action := command.Run, command.Action
		if run == nil || action == nil || run.Binding == nil || run.TenantID != c.cfg.TenantID || run.ID == "" || run.AttemptID == "" || run.OwnerID == "" || action.TenantID != run.TenantID || action.RunID != run.ID || action.AttemptID != run.AttemptID || run.PendingAction == nil || run.PendingAction.TenantID != run.TenantID || run.PendingAction.RunID != run.ID || run.PendingAction.AttemptID != run.AttemptID || run.PendingAction.Status != djob.EffectAuthorized || action.Sequence == 0 || action.Sequence != run.PendingAction.Sequence || action.Operation != run.PendingAction.Operation || !bytes.Equal(action.Input, run.PendingAction.Input) || action.IdempotencyKey == "" || (c.driver != nil && !sameAttempt(c.run, run)) {
			return r, fmt.Errorf("worker action does not match its authorized run")
		}
		factory := c.cfg.Registry.Drivers[djob.Version{Name: run.Binding.Driver, Version: run.Binding.DriverVersion}]
		if factory == nil {
			return r, fmt.Errorf("unregistered worker driver")
		}
		var err error
		if c.driver == nil {
			c.driver, err = factory(*run)
			c.run = run
			if err == nil && c.driver == nil {
				err = fmt.Errorf("worker driver factory returned nil")
			}
		}
		effect := djob.Effect{Status: djob.EffectFailed}
		if err != nil {
			effect.Error = err.Error()
		} else if ctx.Err() != nil {
			effect.Error = "worker action expired before I/O"
		} else {
			ctx = context.WithValue(ctx, authorityKey{}, command.Gateways)
			effect = c.driver.Execute(ctx, *action)
		}
		r.Effect = &effect
	case wire.Quiesce, wire.Close:
		if c.run != nil && !sameAttempt(c.run, command.Run) {
			return r, fmt.Errorf("worker cleanup does not match its attempt")
		}
		if c.driver != nil {
			var err error
			if command.Kind == wire.Quiesce {
				err = c.driver.Quiesce(ctx)
			} else {
				err = c.driver.Close(ctx)
				if err == nil {
					c.driver, c.run = nil, nil
				} else {
					c.closeFailure = errors.Join(ErrCleanupFailed, err)
				}
			}
			if err != nil {
				r.Error = err.Error()
			}
		}
	default:
		return r, fmt.Errorf("unsupported worker command")
	}
	if len(r.Error) > 4096 {
		r.Error = r.Error[:4096]
	}
	if r.Effect != nil && (len(r.Effect.Data) > wire.MaxPayloadBytes || len(r.Effect.Error) > 4096) {
		return wire.Result{}, fmt.Errorf("worker effect exceeds the protocol limit")
	}
	if r.Decision != nil && (len(r.Decision.State)+len(r.Decision.Input)+len(r.Decision.Result) > wire.MaxPayloadBytes || len(r.Decision.Error) > 4096) {
		return wire.Result{}, fmt.Errorf("worker decision exceeds the protocol limit")
	}
	return r, nil
}
