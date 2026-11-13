// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"github.com/duravela/djob/pkg/server/serverctl"
	"sync"
)

type stopTrigger struct {
	mu     sync.Mutex
	seen   bool
	forced bool
	c      chan serverctl.ShutdownRequest
}

func newStopTrigger() *stopTrigger { return &stopTrigger{c: make(chan serverctl.ShutdownRequest, 2)} }
func (s *stopTrigger) signalStop(ctx context.Context, r serverctl.ShutdownRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	force := !r.TerminateUsingGracefulDrain()
	if s.seen && (s.forced || !force) {
		return
	}
	s.seen = true
	s.forced = force
	s.c <- r
}
func (s *stopTrigger) C() <-chan serverctl.ShutdownRequest { return s.c }
