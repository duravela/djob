// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package rpc

import (
	"context"
	"fmt"
	"github.com/duravela/djob/pkg/base"
	"github.com/duravela/djob/pkg/util/stop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
)

// Context is a compatibility client for loopback test services, not a peer authority or heartbeat validator.
type ContextOptions struct {
	NodeID  *uint64
	Stopper *stop.Stopper
}
type Context struct{ ContextOptions }

func NewContext(ctx context.Context, o ContextOptions) *Context {
	if o.NodeID == nil {
		o.NodeID = new(uint64)
	}
	return &Context{o}
}

type ClientConnConfig struct{ ServerAddr string }

func MakeClientConnConfigFromBaseConfig(c base.Config) ClientConnConfig {
	return ClientConnConfig{c.Addr}
}
func NewClientContext(ctx context.Context, _ ClientConnConfig) (*Context, *stop.Stopper) {
	s := stop.NewStopper()
	return NewContext(ctx, ContextOptions{Stopper: s}), s
}

type GRPCConnection struct {
	owner   *Context
	address string
}

func (c *Context) GRPCDialNode(address string, nodeID *uint64) *GRPCConnection {
	return &GRPCConnection{c, address}
}
func (c *Context) GRPCUnvalidatedDial(address string) *GRPCConnection {
	return &GRPCConnection{c, address}
}
func (c *GRPCConnection) Connect(ctx context.Context) (*grpc.ClientConn, error) {
	host, _, err := net.SplitHostPort(c.address)
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, fmt.Errorf("legacy RPC helper permits loopback only; use the authenticated public client for remote services")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.owner.Stopper == nil {
		return nil, fmt.Errorf("RPC helper requires a stopper")
	}
	select {
	case <-c.owner.Stopper.ShouldQuiesce():
		return nil, stop.ErrUnavailable
	default:
	}
	conn, err := grpc.NewClient(c.address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
	if err == nil {
		c.owner.Stopper.AddCloser(stop.CloserFn(func() { _ = conn.Close() }))
	}
	return conn, err
}
