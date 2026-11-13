package tcp

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.uber.org/zap/zaptest"
)

func TestStoppableListenerStopsEveryPendingAccept(t *testing.T) {
	stop := make(chan struct{})
	ln, err := NewStoppableListener("127.0.0.1:0", stop)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	const count = 8
	started, finished := make(chan struct{}, count), make(chan error, count)
	for range count {
		go func() {
			started <- struct{}{}
			conn, err := ln.Accept()
			if conn != nil {
				_ = conn.Close()
			}
			finished <- err
		}()
	}
	for range count {
		<-started
	}
	close(stop)
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for range count {
		select {
		case err := <-finished:
			if !errors.Is(err, net.ErrClosed) {
				t.Fatalf("pending accept: %v", err)
			}
		case <-deadline.C:
			t.Fatal("stop did not unblock every pending TCP accept")
		}
	}
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	if conn, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		if conn != nil {
			_ = conn.Close()
		}
		t.Fatalf("accept after stop: %v", err)
	}
}

// These are the HTTP shutdown cases from listener2, moved to the current module
// and exercised with an actual Raft HTTP handler, ephemeral ports and joined
// goroutines. The same stop signal must work directly and through the mux.
func TestStoppableListenerStopsRaftHTTP(t *testing.T) {
	for _, multiplexed := range []bool{false, true} {
		name := "direct"
		if multiplexed {
			name = "multiplexed"
		}
		t.Run(name, func(t *testing.T) {
			logger := zaptest.NewLogger(t)
			tr := &rafthttp.Transport{Logger: logger, ID: types.ID(1), ClusterID: types.ID(1), Raft: &fakeRaft{},
				ServerStats: newServerStats(), LeaderStats: stats.NewLeaderStats(logger, "1")}
			if err := tr.Start(); err != nil {
				t.Fatal(err)
			}
			defer tr.Stop()
			stop := make(chan struct{})
			ln, err := NewStoppableListener("127.0.0.1:0", stop)
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			var serving net.Listener = ln
			transport := &http.Transport{}
			defer transport.CloseIdleConnections()
			var muxDone chan error
			if multiplexed {
				mux, err := NewMux(ln, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer mux.Close()
				serving = mux.Listen(1)
				muxDone = make(chan error, 1)
				go func() { muxDone <- mux.Serve() }()
				dialer := NewNetDialer(1, nil)
				transport.DialContext = func(_ context.Context, _, addr string) (net.Conn, error) {
					return dialer.Dial(addr, time.Second)
				}
			}
			server := &http.Server{Handler: tr.Handler(), ReadHeaderTimeout: time.Second}
			defer server.Close()
			done := make(chan error, 1)
			go func() { done <- server.Serve(serving) }()
			client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
			response, err := client.Get("http://" + ln.Addr().String() + "/raft/probing")
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("Raft probe status: %d", response.StatusCode)
			}
			close(stop)
			select {
			case err := <-done:
				if !errors.Is(err, net.ErrClosed) && !errors.Is(err, http.ErrServerClosed) {
					t.Fatalf("HTTP shutdown: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("HTTP server did not stop with its listener")
			}
			if muxDone != nil {
				select {
				case <-muxDone:
				case <-time.After(2 * time.Second):
					t.Fatal("mux did not stop with its TCP listener")
				}
			}
		})
	}
}

type observedHeaderListener struct {
	net.Listener
	header chan struct{}
}

func (ln observedHeaderListener) Accept() (net.Conn, error) {
	conn, err := ln.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return observedHeaderConn{Conn: conn, header: ln.header}, nil
}

type observedHeaderConn struct {
	net.Conn
	header chan struct{}
}

func (conn observedHeaderConn) SetReadDeadline(at time.Time) error {
	err := conn.Conn.SetReadDeadline(at)
	if at.IsZero() {
		conn.header <- struct{}{}
	}
	return err
}

func TestMuxCloseJoinsPendingHeaderAndHandoff(t *testing.T) {
	for _, waitingForAccept := range []bool{false, true} {
		name := "header"
		if waitingForAccept {
			name = "handoff"
		}
		t.Run(name, func(t *testing.T) {
			ln := mustTCPListener("127.0.0.1:0")
			header := make(chan struct{}, 1)
			mux, err := NewMux(observedHeaderListener{Listener: ln, header: header}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer mux.Close()
			mux.Listen(1) // Deliberately never accepted by an application.
			served := make(chan error, 1)
			go func() { served <- mux.Serve() }()
			conn, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if waitingForAccept {
				if _, err := conn.Write([]byte{1}); err != nil {
					t.Fatal(err)
				}
				select {
				case <-header:
				case <-time.After(2 * time.Second):
					t.Fatal("mux did not consume the routing header")
				}
			} else {
				deadline := time.Now().Add(2 * time.Second)
				for {
					mux.mu.Lock()
					pending := len(mux.pending)
					mux.mu.Unlock()
					if pending == 1 {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("mux did not accept the unframed connection")
					}
					time.Sleep(time.Millisecond)
				}
			}
			closed := make(chan error, 1)
			go func() { closed <- mux.Close() }()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("close detached or waited indefinitely for an undelivered connection")
			}
			select {
			case <-served:
			case <-time.After(2 * time.Second):
				t.Fatal("mux acceptance did not finish")
			}
			_ = conn.SetReadDeadline(time.Now().Add(time.Second))
			var b [1]byte
			_, err = conn.Read(b[:])
			if err == nil {
				t.Fatal("undelivered connection survived close")
			}
			var timeout net.Error
			if errors.As(err, &timeout) && timeout.Timeout() {
				t.Fatal("close left the socket open until its read deadline")
			}
		})
	}
}

func TestMuxStopAndConcurrentRegistrationCloseAllListeners(t *testing.T) {
	ln := mustTCPListener("127.0.0.1:0")
	mux, err := NewMux(ln, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mux.Close()
	var wg sync.WaitGroup
	listeners := make(chan net.Listener, 32)
	wg.Add(33)
	for i := range 32 {
		go func() { defer wg.Done(); listeners <- mux.Listen(byte(i)) }()
	}
	go func() { defer wg.Done(); mux.Stop(); mux.Stop() }()
	wg.Wait()
	close(listeners)
	for ln := range listeners {
		if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("registered listener survived stop: %v", err)
		}
	}
}
