// Copyright (c) 2013 Fatih Arslan
// Portions Copyright (c) Philip O'Toole 2015 - 2024.
// SPDX-License-Identifier: MIT
// Adapted for djob from rqlite's fork of fatih/pool; see licenses/MIT-fatih-pool.txt
// and licenses/MIT-rqlite.txt. Modified interfaces and statistics for djob.

package pool

import (
	"net"
	"sync"
)

// Conn is a wrapper around net.Conn to modify the behavior of
// net.Conn's Close() method.
type Conn struct {
	net.Conn
	mu       sync.Mutex
	c        *channelPool
	unusable bool
}

// Close puts the given connection back into the pool instead of closing it.
func (p *Conn) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.unusable {
		if p.Conn != nil {
			return p.Conn.Close()
		}
		return nil
	}
	return p.c.put(p.Conn)
}

// MarkUnusable marks the connection not usable anymore, to let the pool close it instead of returning it to pool.
func (p *Conn) MarkUnusable() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.unusable = true
}
