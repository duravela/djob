// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package stop

import (
	"context"
	"errors"
	"sync"
)

var ErrUnavailable = errors.New("stopper no longer accepts tasks")

type Closer interface{ Close() }
type CloserFn func()

func (f CloserFn) Close() { f() }

// Stopper joins admitted tasks before closing resources in reverse registration order.
// Its zero value is not usable; construct it with NewStopper. Tasks must return
// when ShouldQuiesce closes. A task must not synchronously call Stop or Quiesce,
// since both wait for that task to finish. Closers must not reenter Stop.
type Stopper struct {
	mu      sync.Mutex
	tasks   sync.WaitGroup
	once    sync.Once
	quiesce chan struct{}
	done    chan struct{}
	closing bool
	closers []Closer
}

func NewStopper() *Stopper                        { return &Stopper{quiesce: make(chan struct{}), done: make(chan struct{})} }
func (s *Stopper) ShouldQuiesce() <-chan struct{} { return s.quiesce }
func (s *Stopper) IsStopped() <-chan struct{}     { return s.done }
func (s *Stopper) admit() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.quiesce:
		return ErrUnavailable
	default:
		s.tasks.Add(1)
		return nil
	}
}
func (s *Stopper) RunTask(ctx context.Context, name string, f func(context.Context)) error {
	if err := s.admit(); err != nil {
		return err
	}
	defer s.tasks.Done()
	f(ctx)
	return nil
}
func (s *Stopper) RunAsyncTask(ctx context.Context, name string, f func(context.Context)) error {
	if err := s.admit(); err != nil {
		return err
	}
	go func() { defer s.tasks.Done(); f(ctx) }()
	return nil
}
func (s *Stopper) Quiesce(ctx context.Context) {
	s.mu.Lock()
	select {
	case <-s.quiesce:
	default:
		close(s.quiesce)
	}
	s.mu.Unlock()
	s.tasks.Wait()
}
func (s *Stopper) AddCloser(c Closer) {
	s.mu.Lock()
	if !s.closing {
		s.closers = append(s.closers, c)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	c.Close()
}

// Stop waits for cleanup even if ctx is cancelled; cancellation cannot establish quiescence.
func (s *Stopper) Stop(ctx context.Context) {
	s.once.Do(func() {
		s.Quiesce(ctx)
		s.mu.Lock()
		s.closing = true
		closers := s.closers
		s.closers = nil
		s.mu.Unlock()
		for i := len(closers) - 1; i >= 0; i-- {
			closers[i].Close()
		}
		close(s.done)
	})
	<-s.done
}
