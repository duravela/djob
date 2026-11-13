// Portions Copyright (c) Philip O'Toole 2015 - 2024.
// Adapted from rqlite; upstream portions retain the MIT license in
// licenses/MIT-rqlite.txt. Modified for djob; new engine code uses root LICENSE.

package store

import (
	"net"

	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
)

// Layer is the interface expected by the Store for network communication
// between nodes, which is used for Raft distributed consensus.
type Layer interface {
	net.Listener
}

// Transport is the network service provided to Raft, and wraps a Listener.
type Transport struct {
	ly Layer
}

// NewTransport returns an initialized Transport.
func NewTransport(ly Layer) *Transport {
	return &Transport{
		ly: ly,
	}
}

// Accept waits for the next connection.
func (t *Transport) Accept() (net.Conn, error) {
	return t.ly.Accept()
}

// Close closes the transport
func (t *Transport) Close() error {
	return t.ly.Close()
}

// Addr returns the binding address of the transport.
func (t *Transport) Addr() net.Addr {
	return t.ly.Addr()
}

type NodeTransport struct {
	*rafthttp.Transport
	done   chan struct{}
	closed bool
}

// NewNodeTransport returns an initialized NodeTransport.
func NewNodeTransport(transport *rafthttp.Transport) *NodeTransport {
	return &NodeTransport{
		Transport: transport,
		done:      make(chan struct{}),
	}
}

// Close closes the transport
func (n *NodeTransport) Close() error {
	if n.closed {
		return nil
	}
	n.closed = true

	close(n.done)
	if n.Transport == nil {
		return nil
	}
	n.Transport.Stop()
	return nil
}
