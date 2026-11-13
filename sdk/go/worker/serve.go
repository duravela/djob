// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package worker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/duravela/djob/sdk/go"
)

// ReconnectConfig bounds retries between fully closed sessions. Zero values use
// 500ms initial backoff, a 30s maximum, and a reset after a 30s session.
type ReconnectConfig struct {
	MinBackoff time.Duration
	MaxBackoff time.Duration
	ResetAfter time.Duration
	// OnRetry reports scheduling of a fresh session. It contains no endpoint,
	// token, payload or raw error. The callback must be quick and bounded.
	OnRetry func(RetryObservation)
}

// RetryObservation describes the next fresh-session attempt after cleanup.
type RetryObservation struct {
	Attempt         uint64
	Delay           time.Duration
	ControllerIndex int // Zero is ServerURL; subsequent indexes are FallbackURLs.
}

func (r *ReconnectConfig) defaults() error {
	if r.MinBackoff == 0 {
		r.MinBackoff = 500 * time.Millisecond
	}
	if r.MaxBackoff == 0 {
		r.MaxBackoff = 30 * time.Second
	}
	if r.ResetAfter == 0 {
		r.ResetAfter = 30 * time.Second
	}
	if r.MinBackoff < 100*time.Millisecond || r.MaxBackoff < r.MinBackoff || r.MaxBackoff > time.Minute || r.ResetAfter < time.Second || r.ResetAfter > time.Hour {
		return fmt.Errorf("invalid worker reconnect backoff or reset interval")
	}
	return nil
}

// Serve supervises fresh worker sessions. Transient failures retry with bounded
// exponential backoff and jitter, rotating through explicitly trusted origins.
// Run always finishes driver I/O and cleanup first. Cleanup, authentication, TLS
// and protocol failures are terminal. No session, result or action is resumed.
func Serve(ctx context.Context, cfg Config, reconnect ReconnectConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if err := reconnect.defaults(); err != nil {
		return err
	}
	registry := djob.Registry{Handlers: make(map[djob.Version]djob.Handler), Drivers: make(map[djob.Version]djob.DriverFactory)}
	for version, handler := range cfg.Registry.Handlers {
		registry.Handlers[version] = handler
	}
	for version, factory := range cfg.Registry.Drivers {
		registry.Drivers[version] = factory
	}
	cfg.Registry = registry
	origins := append([]string{cfg.ServerURL}, cfg.FallbackURLs...)
	cfg.FallbackURLs = nil
	backoff := reconnect.MinBackoff
	var attempt uint64
	index := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		cfg.ServerURL = origins[index]
		var connected time.Duration
		err := runSession(ctx, cfg, func(duration time.Duration) { connected = duration })
		if errors.Is(err, ErrCleanupFailed) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil || !errors.Is(err, ErrSessionLost) {
			return err
		}
		if connected >= reconnect.ResetAfter {
			backoff = reconnect.MinBackoff
		}
		// Equal jitter avoids synchronized reconnect storms while retaining a
		// positive minimum delay even when all controllers are unavailable.
		delay := backoff/2 + time.Duration(rand.Int64N(int64(backoff-backoff/2)))
		index = (index + 1) % len(origins)
		attempt++
		if reconnect.OnRetry != nil {
			reconnect.OnRetry(RetryObservation{Attempt: attempt, Delay: delay, ControllerIndex: index})
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, reconnect.MaxBackoff)
	}
}
