// Portions Copyright (c) Philip O'Toole 2015 - 2024.
// Adapted from rqlite; upstream portions retain the MIT license in
// licenses/MIT-rqlite.txt. Modified for djob; new engine code uses root LICENSE.

package tcp

import (
	"errors"
	"net"
	"testing"
	"time"
)

func Test_NewDialer(t *testing.T) {
	d := NewNetDialer(1, nil)
	if d == nil {
		t.Fatal("failed to create a dialer")
	}
}

func Test_DialerNoConnect(t *testing.T) {
	d := NewNetDialer(87, nil)
	_, err := d.Dial("127.0.0.1:0", 5*time.Second)
	if err == nil {
		t.Fatalf("no error connecting to bad address")
	}
}

func Test_DialerHeader(t *testing.T) {
	s := mustNewEchoServer()
	defer s.Close()
	go s.Start(t)

	d := NewNetDialer(64, nil)
	conn, err := d.Dial(s.Addr(), 10*time.Second)
	if err != nil {
		t.Fatalf("failed to dial echo server: %s", err.Error())
	}

	buf := make([]byte, 1)

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read from echo server: %s", err.Error())
	}
	if exp, got := buf[0], byte(64); exp != got {
		t.Fatalf("got wrong response from echo server, exp %d, got %d", exp, got)
	}
}

func mustNewEchoServer() *echoServer {
	return &echoServer{
		ln: mustTCPListener("127.0.0.1:0"),
	}
}

type echoServer struct {
	ln net.Listener
}

// Addr returns the address of the echo server.
func (e *echoServer) Addr() string {
	return e.ln.Addr().String()
}

// Start starts the echo server.
func (e *echoServer) Start(t *testing.T) {
	for {
		conn, err := e.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				// Successful testing can cause this.
				return
			}
			t.Logf("failed to accept a connection: %s", err.Error())
		}
		go func(c net.Conn) {
			buf := make([]byte, 1)
			_, err := c.Read(buf)
			if err != nil {
				return
			}
			_, err = c.Write(buf)
			if err != nil {
				t.Logf("failed to write byte: %s", err.Error())
			}
			c.Close()
		}(conn)
	}
}

// Close closes the echo server.
func (e *echoServer) Close() {
	e.ln.Close()
}
