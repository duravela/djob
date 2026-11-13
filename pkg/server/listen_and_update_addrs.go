// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"fmt"
	"github.com/pires/go-proxyproto"
	"net"
)

type ListenError struct {
	cause error
	Addr  string
}

func (e *ListenError) Error() string { return e.cause.Error() }
func (e *ListenError) Unwrap() error { return e.cause }
func ListenAndUpdateAddrs(ctx context.Context, addr, advertise *string, name string, proxy bool) (net.Listener, error) {
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		return nil, &ListenError{err, *addr}
	}
	if err = UpdateAddrs(ctx, addr, advertise, listener.Addr()); err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("%s listener address: %w", name, err)
	}
	if proxy {
		return &proxyproto.Listener{Listener: listener}, nil
	}
	return listener, nil
}
func UpdateAddrs(ctx context.Context, addr, advertise *string, bound net.Addr) error {
	host, port, err := net.SplitHostPort(bound.String())
	if err != nil {
		return err
	}
	advertisedHost, advertisedPort, err := net.SplitHostPort(*advertise)
	if err != nil {
		return err
	}
	if advertisedPort == "" || advertisedPort == "0" {
		advertisedPort = port
	}
	*addr = net.JoinHostPort(host, port)
	*advertise = net.JoinHostPort(advertisedHost, advertisedPort)
	return nil
}
