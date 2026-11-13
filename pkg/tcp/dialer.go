// Portions Copyright (c) Philip O'Toole 2015 - 2024.
// Adapted from rqlite; upstream portions retain the MIT license in
// licenses/MIT-rqlite.txt. Modified for djob; new engine code uses root LICENSE.

package tcp

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"
)

type Dialer interface {
	Dial(address string, timeout time.Duration) (net.Conn, error)
}

// NewNetDialer returns an initialized Dialer.
func NewNetDialer(header byte, tlsConfig *tls.Config) *NetDialer {
	return &NetDialer{
		header:    header,
		tlsConfig: tlsConfig,
	}
}

// NetDialer supports dialing a cluster service.
type NetDialer struct {
	header    byte
	tlsConfig *tls.Config
}

// Dial dials the cluster service at the given addr and returns a connection.
func (d *NetDialer) Dial(addr string, timeout time.Duration) (conn net.Conn, retErr error) {
	dialer := &net.Dialer{Timeout: timeout}

	if d.tlsConfig == nil {
		conn, retErr = dialer.Dial("tcp", addr)
	} else {
		conn, retErr = tls.DialWithDialer(dialer, "tcp", addr, d.tlsConfig)
	}
	if retErr != nil {
		return nil, retErr
	}

	defer func() {
		if retErr != nil && conn != nil {
			conn.Close()
		}
	}()

	// Write a marker byte to indicate message type.
	if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("failed to set WriteDeadline for header: %s", err.Error())
	}
	if _, err := conn.Write([]byte{d.header}); err != nil {
		return nil, err
	}
	return conn, nil
}
