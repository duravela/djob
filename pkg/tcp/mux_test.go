// Portions Copyright (c) Philip O'Toole 2015 - 2024.
// Adapted from rqlite; upstream portions retain the MIT license in
// licenses/MIT-rqlite.txt. Modified for djob; new engine code uses root LICENSE.

package tcp

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"testing/quick"
	"time"

	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// func TestMux(t *testing.T) {
// 	peers := []string{"http://127.0.0.1:12345"}
// 	cfg := Config{
// 		RaftAddr: peers[0],
// 	}
// 	tcpListener := mustTCPListener(cfg.RaftAddr)
// 	defer tcpListener.Close()

// 	mux, err := startNodeMux(&cfg, tcpListener)
// 	if err != nil {
// 		log.Fatal().Msgf("failed to start node mux: %s", err.Error())
// 	}
// 	// Raft internode layer
// 	raftLn := mux.Listen(1)
// 	raftTn := NewLayer(raftLn)
// 	nt := store.NewTransport(raftTn)

// 	transport := &rafthttp.Transport{
// 		Logger:      rc.logger,
// 		ID:          types.ID(cfg.RaftAddr),
// 		ClusterID:   0x1000,
// 		Raft:        rc,
// 		ServerStats: stats.NewServerStats("", ""),
// 		LeaderStats: stats.NewLeaderStats(zap.NewExample(), fmt.Sprint(cfg.RaftAddr)),
// 		ErrorC:      make(chan error),
// 	}

// 	store.NewNodeTransport(transport)
// }

type fakeRaft struct {
	recvc     chan<- raftpb.Message
	err       error
	removedID uint64
}

func (p *fakeRaft) Process(ctx context.Context, m raftpb.Message) error {
	select {
	case p.recvc <- m:
	default:
	}
	return p.err
}

func (p *fakeRaft) IsIDRemoved(id uint64) bool { return id == p.removedID }

func (p *fakeRaft) ReportUnreachable(id uint64) {}

func (p *fakeRaft) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

func newServerStats() *stats.ServerStats {
	return stats.NewServerStats("", "")
}

func TestLayerBasicRouting(t *testing.T) {
	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()

	cfg := &Config{RaftAddr: tcpListener.Addr().String()}
	mux, err := startNodeMux(cfg, tcpListener)
	if err != nil {
		t.Fatalf("failed to create mux: %s", err.Error())
	}
	defer mux.Close()

	raftListener := mux.Listen(0x01)
	dialer := NewNetDialer(0x01, nil)
	layer := NewLayer(raftListener, dialer)
	testLayerRouting(t, layer, "Hello, World!", nil)
}

func TestLayerWithStoppableListenerBasicRouting(t *testing.T) {
	stopc := make(chan struct{})
	// httpdonec := make(chan struct{})
	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()

	// Create a stoppable listener
	stoppableListener, err := toStoppableListener(&tcpListener, stopc)
	if err != nil {
		t.Fatalf("failed to create stoppable listener: %s", err.Error())
	}

	cfg := &Config{RaftAddr: tcpListener.Addr().String()}
	mux, err := startNodeMux(cfg, stoppableListener)
	if err != nil {
		t.Fatalf("failed to create mux: %s", err.Error())
	}
	defer mux.Close()

	raftListener := mux.Listen(0x01)
	dialer := NewNetDialer(0x01, nil)
	layer := NewLayer(raftListener, dialer)
	testLayerRouting(t, layer, "Hello, World!", nil)
	close(stopc)
}

func TestMuxWithRaftHTTPTransport(t *testing.T) {
	// Create a channel to signal shutdown
	// stopc := make(chan struct{})
	// httpdonec := make(chan struct{})
	// tc := make(chan net.Conn)
	// Create a TCP listener
	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()

	// Create a stoppable listener
	// stoppableListener, err := toStoppableListener(&tcpListener, stopc)
	// if err != nil {
	// 	t.Fatalf("failed to create stoppable listener: %s", err.Error())
	// }

	// // Create a rafthttp.Transport
	// transport := &rafthttp.Transport{
	// 	Logger:      zap.NewExample(),
	// 	ClusterID:   0x1000,
	// 	ServerStats: stats.NewServerStats("", ""),
	// 	LeaderStats: stats.NewLeaderStats(zap.NewExample(), "1"),
	// 	ErrorC:      make(chan error),
	// }

	// // Start the transport
	// if err := transport.Start(); err != nil {
	// 	t.Fatalf("failed to start transport: %s", err.Error())
	// }
	// defer transport.Stop()

	cfg := &Config{RaftAddr: tcpListener.Addr().String()}
	mux, err := startNodeMux(cfg, tcpListener)
	if err != nil {
		t.Fatalf("failed to create mux: %s", err.Error())
	}
	defer mux.Close()
	// Register a listener for Raft traffic
	raftListener := mux.Listen(0x01)
	dialer := NewNetDialer(0x01, nil)
	layer := NewLayer(raftListener, dialer)
	testLayerRouting(t, layer, "Hello, World!", nil)
	// testMuxRouting(t, raftListener.Addr().String(), raftListener, 0x01, "Hello, World!")
	// conn, err := layer.Dial(cfg.RaftAddr, 5*time.Second)
	// // // Start an HTTP server to handle Raft traffic
	// // go func() {
	// // 	err = (&http.Server{Handler: transport.Handler()}).Serve(raftListener)
	// // 	select {
	// // 	case <-stopc:
	// // 		zap.NewExample().Warn("stopped")
	// // 	default:
	// // 		zap.NewExample().Warn("failed to serve raft", zap.Error(err))
	// // 	}
	// // 	close(httpdonec)
	// // }()

	// // conn := <-tc
	// // Simulate a client connection to send a Raft message
	// if err != nil {
	// 	t.Fatalf("failed to dial multiplexer: %s", err.Error())
	// }
	// defer conn.Close()

	// // Write a Raft message header and payload
	// message := []byte("raft-message")
	// if _, err := conn.Write(append([]byte{0x01}, message...)); err != nil {
	// 	t.Fatalf("failed to write message: %s", err.Error())
	// }

	// // Ensure the connection is closed
	// // buf := make([]byte, 1)
	// var resp [2]byte
	// _, err = io.ReadFull(conn, resp[:])
	// if err == nil {
	// 	t.Fatalf("expected connection to be closed, but read succeeded")
	// }

	// stopc <- struct{}{}
	// <-httpdonec
	// transport.Stop()
}

func TestMuxBasicRoutingStoppableListener(t *testing.T) {
	stopc := make(chan struct{})
	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()

	stoppableListener, err := toStoppableListener(&tcpListener, stopc)
	if err != nil {
		t.Fatalf("failed to create stoppable listener: %s", err.Error())
	}

	mux, err := NewMux(stoppableListener, nil)
	if err != nil {
		t.Fatalf("failed to create mux: %s", err.Error())
	}
	defer mux.Close()

	// Register listeners for header bytes 0x01 and 0x02
	listener1 := mux.Listen(0x01)
	listener2 := mux.Listen(0x02)

	go mux.Serve()

	// Test routing logic
	testMuxRouting(t, tcpListener.Addr().String(), listener1, 0x01, "Message for listener 1")
	testMuxRouting(t, tcpListener.Addr().String(), listener2, 0x02, "Message for listener 2")
}

func TestMuxGracefulShutdownWithStoppableListener(t *testing.T) {
	stopc := make(chan struct{})
	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()

	stoppableListener, err := toStoppableListener(&tcpListener, stopc)
	if err != nil {
		t.Fatalf("failed to create stoppable listener: %s", err.Error())
	}

	mux, err := NewMux(stoppableListener, nil)
	if err != nil {
		t.Fatalf("failed to create mux: %s", err.Error())
	}
	defer mux.Close()

	// Register a listener for header byte 0x01
	listener := mux.Listen(0x01)

	go mux.Serve()

	// Simulate a client connection
	conn, err := net.Dial("tcp", tcpListener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial multiplexer: %s", err.Error())
	}

	// Write a message with header byte 0x01
	message := []byte("test-message")
	if _, err := conn.Write(append([]byte{0x01}, message...)); err != nil {
		t.Fatalf("failed to write message: %s", err.Error())
	}

	// Accept the connection on the listener
	clientConn, err := listener.Accept()
	if err != nil {
		t.Fatalf("failed to accept connection: %s", err.Error())
	}
	_ = clientConn.SetDeadline(time.Now().Add(3 * time.Second))
	// Read the message
	buf := make([]byte, len(message))
	if _, err := io.ReadFull(clientConn, buf); err != nil {
		t.Fatalf("failed to read message: %s", err.Error())
	}
	if string(buf) != string(message) {
		t.Fatalf("unexpected message: got %s, want %s", buf, message)
	}

	// Stop the multiplexer
	// close(stopc)
	stoppableListener.Close()

	// Ensure no new connections are accepted
	_, err = net.Dial("tcp", tcpListener.Addr().String())
	if err == nil {
		t.Fatalf("expected connection to be refused, but it succeeded")
	}

	// The application owns connections that were already accepted.
	defer conn.Close()
	defer clientConn.Close()
	if _, err := conn.Write(message); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(clientConn, buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(message, buf) {
		t.Fatalf("accepted connection changed ownership")
	}
}

func TestMuxInvalidHeaderWithStoppableListener(t *testing.T) {
	stopc := make(chan struct{})
	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()

	stoppableListener, err := toStoppableListener(&tcpListener, stopc)
	if err != nil {
		t.Fatalf("failed to create stoppable listener: %s", err.Error())
	}

	mux, err := NewMux(stoppableListener, nil)
	if err != nil {
		t.Fatalf("failed to create mux: %s", err.Error())
	}
	defer mux.Close()

	go mux.Serve()

	// Dial the multiplexer and send an invalid header byte
	conn, err := net.Dial("tcp", tcpListener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial multiplexer: %s", err.Error())
	}
	defer conn.Close()

	if _, err := conn.Write([]byte{0xFF}); err != nil {
		t.Fatalf("failed to write invalid header: %s", err.Error())
	}

	// Expect the connection to be closed
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatalf("expected connection to be closed, but read succeeded")
	}
}

func TestMuxConcurrentConnectionsWithStoppableListener(t *testing.T) {
	stopc := make(chan struct{})
	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()

	stoppableListener, err := toStoppableListener(&tcpListener, stopc)
	if err != nil {
		t.Fatalf("failed to create stoppable listener: %s", err.Error())
	}

	mux, err := NewMux(stoppableListener, nil)
	if err != nil {
		t.Fatalf("failed to create mux: %s", err.Error())
	}
	defer mux.Close()

	// Register listeners for header bytes 0x01 and 0x02
	listener1 := mux.Listen(0x01)
	listener2 := mux.Listen(0x02)

	go mux.Serve()

	var wg sync.WaitGroup
	wg.Add(2)

	// Simulate two concurrent connections
	go func() {
		defer wg.Done()
		testMuxRouting(t, tcpListener.Addr().String(), listener1, 0x01, "Message for listener 1")
	}()
	go func() {
		defer wg.Done()
		testMuxRouting(t, tcpListener.Addr().String(), listener2, 0x02, "Message for listener 2")
	}()

	wg.Wait()
}

func TestMuxFuzzWithStoppableListener(t *testing.T) {
	if err := quick.Check(func(header byte, message []byte) bool {
		stopc := make(chan struct{})
		tcpListener := mustTCPListener("127.0.0.1:0")
		defer tcpListener.Close()

		stoppableListener, err := toStoppableListener(&tcpListener, stopc)
		if err != nil {
			t.Fatalf("failed to create stoppable listener: %s", err.Error())
		}

		mux, err := NewMux(stoppableListener, nil)
		if err != nil {
			t.Fatalf("failed to create mux: %s", err.Error())
		}
		defer mux.Close()

		listener := mux.Listen(header)
		go mux.Serve()

		// Simulate a client connection
		conn, err := net.Dial("tcp", tcpListener.Addr().String())
		if err != nil {
			t.Fatalf("failed to dial multiplexer: %s", err.Error())
		}
		defer conn.Close()

		// Write the header and message
		if _, err := conn.Write(append([]byte{header}, message...)); err != nil {
			t.Fatalf("failed to write message: %s", err.Error())
		}

		// Read the message on the listener
		clientConn, err := listener.Accept()
		if err != nil {
			t.Fatalf("failed to accept connection: %s", err.Error())
		}
		defer clientConn.Close()

		buf := make([]byte, len(message))
		if _, err := io.ReadFull(clientConn, buf); err != nil {
			t.Fatalf("failed to read message: %s", err.Error())
		}

		return bytes.Equal(message, buf)
	}, nil); err != nil {
		t.Error(err)
	}
}

func TestMuxBasicRouting(t *testing.T) {
	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()

	mux, err := NewMux(tcpListener.(*net.TCPListener), nil)
	if err != nil {
		t.Fatalf("failed to create mux: %s", err.Error())
	}
	defer mux.Close()

	// Register listeners for header bytes 0x01 and 0x02
	listener1 := mux.Listen(0x01)
	listener2 := mux.Listen(0x02)

	go mux.Serve()

	// Test routing logic
	testMuxRouting(t, tcpListener.Addr().String(), listener1, 0x01, "Message for listener 1")
	testMuxRouting(t, tcpListener.Addr().String(), listener2, 0x02, "Message for listener 2")
}

func TestMuxInvalidHeader(t *testing.T) {
	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()

	mux, err := NewMux(tcpListener.(*net.TCPListener), nil)
	if err != nil {
		t.Fatalf("failed to create mux: %s", err.Error())
	}
	defer mux.Close()

	go mux.Serve()

	// Dial the multiplexer and send an invalid header byte
	conn, err := net.Dial("tcp", tcpListener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial multiplexer: %s", err.Error())
	}
	defer conn.Close()

	if _, err := conn.Write([]byte{0xFF}); err != nil {
		t.Fatalf("failed to write invalid header: %s", err.Error())
	}

	// Expect the connection to be closed
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatalf("expected connection to be closed, but read succeeded")
	}
}

func TestMuxGracefulShutdown(t *testing.T) {
	tcpListener := mustTCPListener("127.0.0.1:0")
	mux, err := NewMux(tcpListener, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mux.Close()
	done := make(chan error, 1)
	go func() { done <- mux.Serve() }()
	conn, err := net.Dial("tcp", tcpListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := mux.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("mux did not stop its pending accept/header read")
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err := conn.Read(b[:]); err == nil {
		t.Fatal("undelivered connection survived shutdown")
	}
}

func TestMuxGracefulShutdownConcurrentConnections(t *testing.T) {
	stopc := make(chan struct{})
	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()

	stoppableListener, err := toStoppableListener(&tcpListener, stopc)
	if err != nil {
		t.Fatalf("failed to create stoppable listener: %s", err.Error())
	}

	mux, err := NewMux(stoppableListener, nil)
	if err != nil {
		t.Fatalf("failed to create mux: %s", err.Error())
	}
	defer mux.Close()

	// Register listeners for header bytes 0x01 and 0x02
	listener1 := mux.Listen(0x01)
	listener2 := mux.Listen(0x02)

	go mux.Serve()

	var wg sync.WaitGroup
	wg.Add(2)

	// Simulate two concurrent connections
	go func() {
		defer wg.Done()
		testMuxRouting(t, tcpListener.Addr().String(), listener1, 0x01, "Message for listener 1")
	}()
	go func() {
		defer wg.Done()
		testMuxRouting(t, tcpListener.Addr().String(), listener2, 0x02, "Message for listener 2")
	}()

	wg.Wait()

	// Signal the multiplexer to stop
	close(stopc)
	stoppableListener.Close()

	// Ensure no new connections are accepted
	_, err = net.Dial("tcp", tcpListener.Addr().String())
	if err == nil {
		t.Fatalf("expected connection to be refused, but it succeeded")
	}
}

func TestMuxGracefulShutdownNoConnections(t *testing.T) {
	stopc := make(chan struct{})
	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()

	stoppableListener, err := toStoppableListener(&tcpListener, stopc)
	if err != nil {
		t.Fatalf("failed to create stoppable listener: %s", err.Error())
	}

	mux, err := NewMux(stoppableListener, nil)
	if err != nil {
		t.Fatalf("failed to create mux: %s", err.Error())
	}
	defer mux.Close()

	go mux.Serve()

	// Signal the multiplexer to stop
	close(stopc)
	stoppableListener.Close()

	// Ensure no new connections are accepted
	_, err = net.Dial("tcp", tcpListener.Addr().String())
	if err == nil {
		t.Fatalf("expected connection to be refused, but it succeeded")
	}
}

func TestMuxFuzz(t *testing.T) {
	if err := quick.Check(func(header byte, message []byte) bool {
		tcpListener := mustTCPListener("127.0.0.1:0")
		defer tcpListener.Close()

		mux, err := NewMux(tcpListener.(*net.TCPListener), nil)
		if err != nil {
			t.Fatalf("failed to create mux: %s", err.Error())
		}
		defer mux.Close()

		listener := mux.Listen(header)
		go mux.Serve()

		// Simulate a client connection
		conn, err := net.Dial("tcp", tcpListener.Addr().String())
		if err != nil {
			t.Fatalf("failed to dial multiplexer: %s", err.Error())
		}
		defer conn.Close()

		// Write the header and message
		if _, err := conn.Write(append([]byte{header}, message...)); err != nil {
			t.Fatalf("failed to write message: %s", err.Error())
		}

		// Read the message on the listener
		clientConn, err := listener.Accept()
		if err != nil {
			t.Fatalf("failed to accept connection: %s", err.Error())
		}
		defer clientConn.Close()

		buf := make([]byte, len(message))
		if _, err := io.ReadFull(clientConn, buf); err != nil {
			t.Fatalf("failed to read message: %s", err.Error())
		}

		return bytes.Equal(message, buf)
	}, nil); err != nil {
		t.Error(err)
	}
}

func testMuxRouting(t *testing.T, addr string, listener net.Listener, header byte, message string) {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		conn, err := listener.Accept()
		if err != nil {
			t.Fatalf("failed to accept connection: %s", err.Error())
		}
		defer conn.Close()

		// Read the message
		buf := make([]byte, len(message))
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatalf("failed to read message: %s", err.Error())
		}

		if string(buf) != message {
			t.Fatalf("unexpected message: got %s, want %s", buf, message)
		}
	}()

	// Dial the multiplexer and send the message
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial multiplexer: %s", err.Error())
	}
	defer conn.Close()

	// Write the header byte and message
	if _, err := conn.Write(append([]byte{header}, []byte(message)...)); err != nil {
		t.Fatalf("failed to write message: %s", err.Error())
	}

	wg.Wait()
}

func testLayerRouting(t *testing.T, ly *Layer, message string, stopc chan struct{}) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := ly.Accept()
		if err != nil {
			t.Fatalf("failed to accept connection: %s", err.Error())
		}
		defer conn.Close()

		// Read the message
		buf := make([]byte, len(message))
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatalf("failed to read message: %s", err.Error())
		}

		if string(buf) != message {
			t.Fatalf("unexpected message: got %s, want %s", buf, message)
		}
	}()

	// Dial the multiplexer and send the message
	conn, err := ly.Dial(ly.addr.String(), 15*time.Second)
	if err != nil {
		t.Fatalf("failed to dial multiplexer: %s", err.Error())
	}
	defer conn.Close()

	// Write the header byte and message
	if _, err := conn.Write([]byte(message)); err != nil {
		t.Fatalf("failed to write message: %s", err.Error())
	}

	wg.Wait()
}

// // func TestMuxSimple(t *testing.T) {
// // 	if err := quick.Check(func(n uint8, msg []byte) bool {
// // 		var wg sync.WaitGroup
// // 		tcpListener := mustTCPListener("127.0.0.1:0")
// // 		defer tcpListener.Close()

// // 		mux, err := NewMux(tcpListener.(*net.TCPListener), nil)
// // 		if err != nil {
// // 			t.Fatalf("failed to create mux: %s", err.Error())
// // 		}
// // 		defer mux.Close()
// // 		mux.Timeout = 200 * time.Millisecond

// // 		ln := mux.Listen(0)
// // 		wg.Add(1)
// // 		go func(ln net.Listener) {
// // 			defer wg.Done()

// // 			conn, err := ln.Accept()
// // 			if conn != nil {
// // 				defer conn.Close()
// // 			}
// // 			if len(msg) == 0 || msg[0] != 0 {
// // 				if err == nil || err.Error() != "network connection closed" {
// // 					t.Logf("unexpected error: %s", err)
// // 				}
// // 				return
// // 			}
// // 			// If the header byte matches this listener
// // 			// then expect a connection and read the message.
// // 			var buf bytes.Buffer
// // 			if _, err := io.CopyN(&buf, conn, int64(len(msg)-1)); err != nil {
// // 				t.Log(err)
// // 			} else if !bytes.Equal(msg[1:], buf.Bytes()) {
// // 				t.Logf("message mismatch:\n\nexp=%x\n\ngot=%x\n\n", msg[1:], buf.Bytes())
// // 			}

// // 			// Write response.
// // 			if _, err := conn.Write([]byte("OK")); err != nil {
// // 				t.Log(err)
// // 			}
// // 		}(ln)
// // 		go mux.Serve()
// // 		conn, err := net.Dial("tcp", tcpListener.Addr().String())
// // 		if err != nil {
// // 			t.Fatal(err)
// // 		} else if _, err = conn.Write(msg); err != nil {
// // 			t.Fatal(err)
// // 		}

// // 		// Read the response into the buffer.
// // 		var resp [2]byte
// // 		_, err = io.ReadFull(conn, resp[:])

// // 		// If the message header is less than n then expect a response.
// // 		// Otherwise, we should get an EOF because the mux closed.
// // 		if len(msg) > 0 && msg[0] < n {
// // 			if string(resp[:]) != `OK` {
// // 				t.Fatalf("unexpected response: %s", resp[:])
// // 			}
// // 		} else {
// // 			if err == nil || (err != io.EOF && !(strings.Contains(err.Error(), "connection reset by peer") ||
// // 				strings.Contains(err.Error(), "closed by the remote host"))) {
// // 				t.Fatalf("unexpected error: %s", err)
// // 			}
// // 		}

// // 		// Close connection.
// // 		if err := conn.Close(); err != nil {
// // 			t.Fatal(err)
// // 		}

// // 		tcpListener.Close()
// // 		wg.Wait()

// // 		return true
// // 	}, nil); err != nil {
// // 		t.Error(err)
// // 	}
// // }

// // func TestMux4Fail(t *testing.T) {
// 	var msg []byte
// 	// tr := &rafthttp.Transport{
// 	// 	ID:          types.ID(1),
// 	// 	ClusterID:   types.ID(1),
// 	// 	Raft:        &fakeRaft{},
// 	// 	ServerStats: newServerStats(),
// 	// 	LeaderStats: stats.NewLeaderStats(zaptest.NewLogger(t), "1"),
// 	// }
// 	// tr.Start()
// 	// srv := httptest.NewServer(tr.Handler())
// 	// defer srv.Close()

// 	httpstopc := make(chan struct{})
// 	httpdonec := make(chan struct{})
// 	// url, err := url.Parse("http://127.0.0.1:10001")
// 	// if err != nil {
// 	// 	t.Fatal("failed to parse URL", zap.Error(err))
// 	// }

// 	var wg sync.WaitGroup

// 	// ln, err := raftnode.NewStoppableListener(url.Host, httpstopc)
// 	// if err != nil {
// 	// 	t.Fatal("failed to listen", zap.Error(err))
// 	// }
// 	ln := mustTCPListener("127.0.0.1:0")
// 	mux, _ := NewMux(ln, nil)
// 	mux.stopc = httpstopc
// 	muxLn := mux.Listen(1)
// 	// go func(t *testing.T) {
// 	// 	_ = srv.Config.Serve(muxLn)
// 	// 	select {
// 	// 	case <-httpstopc:
// 	// 		fmt.Printf("Stopped")
// 	// 	default:
// 	// 		httpdonec <- struct{}{}
// 	// 		zap.NewExample().Fatal("failed to serve raft", zap.Error(err))
// 	// 	}
// 	// 	close(httpdonec)
// 	// }(t)

// 	wg.Add(1)
// 	go func(ln net.Listener) {
// 		defer wg.Done()

// 		conn, err := ln.Accept()
// 		if conn != nil {
// 			defer conn.Close()
// 		}
// 		// If there is no message or the header byte
// 		// doesn't match then expect close.
// 		if len(msg) == 0 || msg[0] != 1 {
// 			if err == nil || err.Error() != "network connection closed" {
// 				t.Logf("unexpected error: %s", err)
// 			}
// 			return
// 		}

// 		// If the header byte matches this listener
// 		// then expect a connection and read the message.
// 		var buf bytes.Buffer
// 		if _, err := io.CopyN(&buf, conn, int64(len(msg)-1)); err != nil {
// 			t.Log(err)
// 		} else if !bytes.Equal(msg[1:], buf.Bytes()) {
// 			t.Logf("message mismatch:\n\nexp=%x\n\ngot=%x\n\n", msg[1:], buf.Bytes())
// 		}

// 		// Write response.
// 		if _, err := conn.Write([]byte("OK")); err != nil {
// 			t.Log(err)
// 		}
// 	}(muxLn)

// 	// Begin serving from the listener.
// 	go mux.Serve()

// 	// Write message to TCP listener and read OK response.
// 	conn, err := net.Dial("tcp", ln.Addr().String())
// 	if err != nil {
// 		t.Fatal(err)
// 	} else if _, err = conn.Write(msg); err != nil {
// 		t.Fatal(err)
// 	}

// 	// Read the response into the buffer.
// 	var resp [2]byte
// 	_, err = io.ReadFull(conn, resp[:])

// 	if len(msg) > 0 && msg[0] < 1 {
// 		if string(resp[:]) != `OK` {
// 			t.Fatalf("unexpected response: %s", resp[:])
// 		}
// 	} else {
// 		if err == nil || (err != io.EOF && !(strings.Contains(err.Error(), "connection reset by peer") ||
// 			strings.Contains(err.Error(), "closed by the remote host"))) {
// 			t.Fatalf("unexpected error: %s", err)
// 		}
// 	}

// 	// tr.Stop()
// 	close(httpstopc)
// 	wg.Wait()
// 	<-httpdonec
// }
// func TestMux3(t *testing.T) {
// 	tcpListener := mustTCPListener("127.0.0.1:0")
// 	defer tcpListener.Close()

// 	transport := &rafthttp.Transport{
// 		Logger:      zap.NewExample(),
// 		ClusterID:   0x1000,
// 		ServerStats: stats.NewServerStats("", ""),
// 		ErrorC:      make(chan error),
// 	}
// 	mux, err := NewMux(tcpListener.(*net.TCPListener), nil)
// 	if err != nil {
// 		t.Fatalf("failed to create mux: %s", err.Error())
// 	}
// 	defer mux.Close()

// 	if err := transport.Start(); err != nil {
// 		t.Fatal("failed to start transport", zap.Error(err))
// 	}

// 	ln := mux.Listen(1)
// 	err = (&http.Server{Handler: transport.Handler()}).Serve(ln)
// 	select {
// 	case <-mux.stopc:
// 	default:
// 		t.Fatal("failed to serve raft", zap.Error(err))
// 	}
// 	mux.Close()
// }

// func TestMux2(t *testing.T) {
// 	if err := quick.Check(func(n uint8, msg []byte) bool {
// 		if testing.Verbose() {
// 			if len(msg) == 0 {
// 				log.Printf("n=%d, <no message>", n)
// 			} else {
// 				log.Printf("n=%d, hdr=%d, len=%d", n, msg[0], len(msg))
// 			}
// 		}

// 		var wg sync.WaitGroup

// 		// Open single listener on random port.
// 		tcpListener := mustTCPListener("127.0.0.1:0")
// 		defer tcpListener.Close()

// 		// Setup muxer & listeners.
// 		mux, err := NewMux(tcpListener.(*net.TCPListener), nil)
// 		if err != nil {
// 			t.Fatalf("failed to create mux: %s", err.Error())
// 		}
// 		defer mux.Close()
// 		mux.Timeout = 200 * time.Millisecond
// 		if !testing.Verbose() {
// 			logger := log.With().Str("component", "[mux]").Logger()
// 			mux.Logger = &logger
// 		}
// 		for i := uint8(0); i < n; i++ {
// 			ln := mux.Listen(i)

// 			wg.Add(1)
// 			go func(i uint8, ln net.Listener) {
// 				defer wg.Done()

// 				// Wait for a connection for this listener.
// 				conn, err := ln.Accept()
// 				if conn != nil {
// 					defer conn.Close()
// 				}

// 				// If there is no message or the header byte
// 				// doesn't match then expect close.
// 				if len(msg) == 0 || msg[0] != i {
// 					if err == nil || err.Error() != "network connection closed" {
// 						t.Logf("unexpected error: %s", err)
// 					}
// 					return
// 				}

// 				// If the header byte matches this listener
// 				// then expect a connection and read the message.
// 				var buf bytes.Buffer
// 				if _, err := io.CopyN(&buf, conn, int64(len(msg)-1)); err != nil {
// 					t.Log(err)
// 				} else if !bytes.Equal(msg[1:], buf.Bytes()) {
// 					t.Logf("message mismatch:\n\nexp=%x\n\ngot=%x\n\n", msg[1:], buf.Bytes())
// 				}

// 				// Write response.
// 				if _, err := conn.Write([]byte("OK")); err != nil {
// 					t.Log(err)
// 				}
// 			}(i, ln)
// 		}

// 		// Begin serving from the listener.
// 		go mux.Serve()

// 		// Write message to TCP listener and read OK response.
// 		conn, err := net.Dial("tcp", tcpListener.Addr().String())
// 		if err != nil {
// 			t.Fatal(err)
// 		} else if _, err = conn.Write(msg); err != nil {
// 			t.Fatal(err)
// 		}

// 		// Read the response into the buffer.
// 		var resp [2]byte
// 		_, err = io.ReadFull(conn, resp[:])

// 		// If the message header is less than n then expect a response.
// 		// Otherwise, we should get an EOF because the mux closed.
// 		if len(msg) > 0 && msg[0] < n {
// 			if string(resp[:]) != `OK` {
// 				t.Fatalf("unexpected response: %s", resp[:])
// 			}
// 		} else {
// 			if err == nil || (err != io.EOF && !(strings.Contains(err.Error(), "connection reset by peer") ||
// 				strings.Contains(err.Error(), "closed by the remote host"))) {
// 				t.Fatalf("unexpected error: %s", err)
// 			}
// 		}

// 		// Close connection.
// 		if err := conn.Close(); err != nil {
// 			t.Fatal(err)
// 		}

// 		// Close original TCP listener and wait for all goroutines to close.
// 		tcpListener.Close()
// 		wg.Wait()

// 		return true
// 	}, nil); err != nil {
// 		t.Error(err)
// 	}
// }

// func TestMuxWithStoppableListener(t *testing.T) {
// 	if err := quick.Check(func(n uint8, msg []byte) bool {
// 		if testing.Verbose() {
// 			if len(msg) == 0 {
// 				log.Printf("n=%d, <no message>", n)
// 			} else {
// 				log.Printf("n=%d, hdr=%d, len=%d", n, msg[0], len(msg))
// 			}
// 		}

// 		var wg sync.WaitGroup

// 		// Open single listener on random port.
// 		tcpListener := mustTCPListener("127.0.0.1:0")
// 		defer tcpListener.Close()

// 		// Setup muxer & listeners.
// 		mux, err := NewMux(tcpListener.(*net.TCPListener), nil)
// 		if err != nil {
// 			t.Fatalf("failed to create mux: %s", err.Error())
// 		}
// 		defer mux.Close()
// 		mux.Timeout = 200 * time.Millisecond
// 		if !testing.Verbose() {
// 			logger := log.With().Str("component", "[mux]").Logger()
// 			mux.Logger = &logger
// 		}
// 		for i := uint8(0); i < n; i++ {
// 			ln := mux.Listen(i)

// 			wg.Add(1)
// 			go func(i uint8, ln net.Listener) {
// 				defer wg.Done()

// 				// Wait for a connection for this listener.
// 				conn, err := ln.Accept()
// 				if conn != nil {
// 					defer conn.Close()
// 				}

// 				// If there is no message or the header byte
// 				// doesn't match then expect close.
// 				if len(msg) == 0 || msg[0] != i {
// 					if err == nil || err.Error() != "network connection closed" {
// 						t.Logf("unexpected error: %s", err)
// 					}
// 					return
// 				}

// 				// If the header byte matches this listener
// 				// then expect a connection and read the message.
// 				var buf bytes.Buffer
// 				if _, err := io.CopyN(&buf, conn, int64(len(msg)-1)); err != nil {
// 					t.Log(err)
// 				} else if !bytes.Equal(msg[1:], buf.Bytes()) {
// 					t.Logf("message mismatch:\n\nexp=%x\n\ngot=%x\n\n", msg[1:], buf.Bytes())
// 				}

// 				// Write response.
// 				if _, err := conn.Write([]byte("OK")); err != nil {
// 					t.Log(err)
// 				}
// 			}(i, ln)
// 		}

// 		// Begin serving from the listener.
// 		go mux.Serve()

// 		// Write message to TCP listener and read OK response.
// 		conn, err := net.Dial("tcp", tcpListener.Addr().String())
// 		if err != nil {
// 			t.Fatal(err)
// 		} else if _, err = conn.Write(msg); err != nil {
// 			t.Fatal(err)
// 		}

// 		// Read the response into the buffer.
// 		var resp [2]byte
// 		_, err = io.ReadFull(conn, resp[:])

// 		// If the message header is less than n then expect a response.
// 		// Otherwise, we should get an EOF because the mux closed.
// 		if len(msg) > 0 && msg[0] < n {
// 			if string(resp[:]) != `OK` {
// 				t.Fatalf("unexpected response: %s", resp[:])
// 			}
// 		} else {
// 			if err == nil || (err != io.EOF && !(strings.Contains(err.Error(), "connection reset by peer") ||
// 				strings.Contains(err.Error(), "closed by the remote host"))) {
// 				t.Fatalf("unexpected error: %s", err)
// 			}
// 		}

// 		// Close connection.
// 		if err := conn.Close(); err != nil {
// 			t.Fatal(err)
// 		}

// 		// Close original TCP listener and wait for all goroutines to close.
// 		tcpListener.Close()
// 		wg.Wait()

// 		return true
// 	}, nil); err != nil {
// 		t.Error(err)
// 	}
// }

// mustTCPListener returns a listener on bind, or panics.
func mustTCPListener(bind string) net.Listener {
	l, err := net.Listen("tcp", bind)
	if err != nil {
		panic(err)
	}
	return l
}
