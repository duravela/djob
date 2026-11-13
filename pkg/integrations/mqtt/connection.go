package mqttdriver

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
)

func dial(ctx context.Context, profile Profile) (net.Conn, error) {
	u, err := url.Parse(profile.Broker)
	if err != nil {
		return nil, err
	}
	dialer := &net.Dialer{}
	if u.Scheme == "tls" {
		config := &tls.Config{MinVersion: tls.VersionTLS12}
		if profile.TLSConfig != nil {
			config = profile.TLSConfig.Clone()
			config.MinVersion = max(config.MinVersion, tls.VersionTLS12)
		}
		return (&tls.Dialer{NetDialer: dialer, Config: config}).DialContext(ctx, "tcp", u.Host)
	}
	return dialer.DialContext(ctx, "tcp", u.Host)
}

// packetConn provides Paho's packet-write lock and rejects oversized incoming
// MQTT packets before the parser allocates their advertised body. Announcing a
// maximum in CONNECT alone cannot constrain a faulty or malicious broker.
type packetConn struct {
	net.Conn
	sync.Mutex
	phase, lengthBytes int
	remaining, length  int
	multiplier         int
}

func (c *packetConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if c.phase == 2 {
		n, err := c.Conn.Read(p[:min(len(p), c.remaining)])
		c.remaining -= n
		if c.remaining == 0 {
			c.phase = 0
		}
		return n, err
	}
	n, err := c.Conn.Read(p[:1])
	if err != nil || n == 0 {
		return n, err
	}
	if c.phase == 0 {
		c.phase, c.lengthBytes, c.length, c.multiplier = 1, 0, 0, 1
		return n, nil
	}
	c.length += int(p[0]&127) * c.multiplier
	c.multiplier *= 128
	c.lengthBytes++
	if c.length > maxPacket || (c.lengthBytes == 4 && p[0]&128 != 0) {
		return 0, fmt.Errorf("MQTT packet exceeds the receive limit or has invalid framing")
	}
	if p[0]&128 == 0 {
		c.remaining, c.phase = c.length, 2
		if c.remaining == 0 {
			c.phase = 0
		}
	}
	return n, nil
}

var _ io.Reader = (*packetConn)(nil)
