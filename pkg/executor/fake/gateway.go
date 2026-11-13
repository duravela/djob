// Package fake supplies a controllable gateway for executor tests and examples.
package fake

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
)

type Response struct {
	Outcome executor.Outcome
	Err     error
}

// Call exposes the separate exchange, protocol recovery, and close phases.
// Gates are buffered so a test can arrange a response before a phase begins.
type Call struct {
	Request         executor.Request
	Context         context.Context
	Reply           chan Response
	RecoveryStarted chan struct{}
	Recover         chan error
	RecoveryContext context.Context
	CloseStarted    chan struct{}
	Close           chan error
	CloseContext    context.Context
}

type Options struct {
	IgnoreExchangeCancellation bool
	IgnoreRecoveryCancellation bool
	IgnoreCloseCancellation    bool
}

type Stats struct {
	Started   int
	Active    int
	MaxActive int
}

// Gateway counts occupancy until Close actually succeeds, including recovery.
// Share one Gateway for sessions that represent the same physical bus.
type Gateway struct {
	Calls   <-chan *Call
	calls   chan *Call
	options Options
	mu      sync.Mutex
	stats   Stats
	all     []*Call
}

func New(options Options) *Gateway {
	calls := make(chan *Call, 32)
	return &Gateway{Calls: calls, calls: calls, options: options}
}

func (g *Gateway) Factory(db.CommunicationChannel) (executor.Transport, error) {
	return &transport{gateway: g}, nil
}

func (g *Gateway) Stats() Stats { g.mu.Lock(); defer g.mu.Unlock(); return g.stats }

// ReleaseAll lets teardown finish sessions even when the assertion that normally
// controls a gate failed. It deliberately reports unknown exchange outcomes.
func (g *Gateway) ReleaseAll() {
	g.mu.Lock()
	calls := append([]*Call(nil), g.all...)
	g.mu.Unlock()
	for _, call := range calls {
		select {
		case call.Reply <- Response{Outcome: executor.Outcome{Status: db.AttemptUnknown}, Err: context.Canceled}:
		default:
		}
		select {
		case call.Recover <- nil:
		default:
		}
		select {
		case call.Close <- nil:
		default:
		}
	}
}

type transport struct {
	gateway *Gateway
	call    *Call
	closed  bool
}

func (t *transport) Exchange(ctx context.Context, request executor.Request) (executor.Outcome, error) {
	if t.call != nil {
		return executor.Outcome{}, fmt.Errorf("fake transport cannot exchange twice")
	}
	request.Payload = bytes.Clone(request.Payload)
	t.call = &Call{Request: request, Context: ctx, Reply: make(chan Response, 1), RecoveryStarted: make(chan struct{}),
		Recover: make(chan error, 1), CloseStarted: make(chan struct{}), Close: make(chan error, 1)}
	t.gateway.mu.Lock()
	t.gateway.stats.Started++
	t.gateway.all = append(t.gateway.all, t.call)
	t.gateway.stats.Active++
	if t.gateway.stats.Active > t.gateway.stats.MaxActive {
		t.gateway.stats.MaxActive = t.gateway.stats.Active
	}
	t.gateway.mu.Unlock()
	var canceled <-chan struct{}
	if !t.gateway.options.IgnoreExchangeCancellation {
		canceled = ctx.Done()
	}
	select {
	case t.gateway.calls <- t.call:
	case <-canceled:
		return executor.Outcome{Status: db.AttemptUnknown}, ctx.Err()
	}
	select {
	case reply := <-t.call.Reply:
		return reply.Outcome, reply.Err
	case <-canceled:
		return executor.Outcome{Status: db.AttemptUnknown}, ctx.Err()
	}
}

func (t *transport) Recover(ctx context.Context) error {
	if t.call == nil {
		return nil
	}
	t.call.RecoveryContext = ctx
	close(t.call.RecoveryStarted)
	var canceled <-chan struct{}
	if !t.gateway.options.IgnoreRecoveryCancellation {
		canceled = ctx.Done()
	}
	select {
	case err := <-t.call.Recover:
		return err
	case <-canceled:
		return ctx.Err()
	}
}

func (t *transport) Close(ctx context.Context) error {
	if t.call == nil {
		return nil
	}
	t.call.CloseContext = ctx
	close(t.call.CloseStarted)
	var canceled <-chan struct{}
	if !t.gateway.options.IgnoreCloseCancellation {
		canceled = ctx.Done()
	}
	var err error
	select {
	case err = <-t.call.Close:
	case <-canceled:
		err = ctx.Err()
	}
	if err == nil && !t.closed {
		t.closed = true
		t.gateway.mu.Lock()
		t.gateway.stats.Active--
		t.gateway.mu.Unlock()
	}
	return err
}
