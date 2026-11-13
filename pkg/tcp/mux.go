// Portions Copyright (c) Philip O'Toole 2015 - 2024.
// Adapted from rqlite; upstream portions retain the MIT license in
// licenses/MIT-rqlite.txt. Modified for djob; new engine code uses root LICENSE.

package tcp

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.etcd.io/etcd/client/pkg/v3/transport"
)

const (
	// DefaultTimeout is the default length of time to wait for first byte.
	DefaultTimeout = 30 * time.Second
)

// Layer represents the connection between nodes. It can be both used to
// make connections to other nodes (client), and receive connections from other
// nodes (server)
type Layer struct {
	ln     net.Listener
	addr   net.Addr
	dialer *NetDialer
}

// NewLayer returns a new instance of Layer.
func NewLayer(ln net.Listener, dialer *NetDialer) *Layer {
	return &Layer{
		ln:     ln,
		addr:   ln.Addr(),
		dialer: dialer,
	}
}

// Dial creates a new network connection.
func (l *Layer) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	return l.dialer.Dial(addr, timeout)
}

// Accept waits for the next connection.
func (l *Layer) Accept() (net.Conn, error) { return l.ln.Accept() }

// Close closes the layer.
func (l *Layer) Close() error { return l.ln.Close() }

// Addr returns the local address for the layer.
func (l *Layer) Addr() net.Addr {
	return l.addr
}

// RaftListener is retained for compatibility. Mux uses concrete sublisteners;
// raw TCP listeners and demultiplexed listeners have different lifecycles.
type RaftListener interface {
	net.Listener
	C() chan net.Conn
}

type listener struct {
	c         chan net.Conn
	addr      net.Addr
	done      chan struct{}
	closeOnce sync.Once
}

func (ln *listener) Accept() (net.Conn, error) {
	select {
	case <-ln.done:
		return nil, net.ErrClosed
	default:
	}
	select {
	case <-ln.done:
		return nil, net.ErrClosed
	case conn := <-ln.c:
		select {
		case <-ln.done:
			_ = conn.Close()
			return nil, net.ErrClosed
		default:
			return conn, nil
		}
	}
}

func (ln *listener) Close() error {
	ln.closeOnce.Do(func() { close(ln.done) })
	return nil
}
func (ln *listener) C() chan net.Conn { return ln.c }
func (ln *listener) Addr() net.Addr   { return ln.addr }

// NewStoppableListener binds TCP and closes the listener when stopc signals.
// Closing it unblocks all pending accepts; accepted connections belong to callers.
func NewStoppableListener(addr string, stopc <-chan struct{}) (*stoppableListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return wrapStoppableListener(ln.(*net.TCPListener), stopc), nil
}

type stoppableListener struct {
	*net.TCPListener
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func wrapStoppableListener(ln *net.TCPListener, stopc <-chan struct{}) *stoppableListener {
	sl := &stoppableListener{TCPListener: ln, done: make(chan struct{})}
	if stopc != nil {
		go func() {
			select {
			case <-stopc:
				_ = sl.Close()
			case <-sl.done:
			}
		}()
	}
	return sl
}

func toStoppableListener(ln *net.Listener, stopc <-chan struct{}) (*stoppableListener, error) {
	if ln == nil {
		return nil, fmt.Errorf("TCP listener is required")
	}
	tcp, ok := (*ln).(*net.TCPListener)
	if !ok || tcp == nil {
		return nil, fmt.Errorf("TCP listener is required")
	}
	return wrapStoppableListener(tcp, stopc), nil
}

func (sl *stoppableListener) Accept() (net.Conn, error) {
	conn, err := sl.AcceptTCP()
	if err != nil {
		return nil, err
	}
	select {
	case <-sl.done:
		_ = conn.Close()
		return nil, net.ErrClosed
	default:
	}
	_ = conn.SetKeepAlive(true)
	_ = conn.SetKeepAlivePeriod(3 * time.Minute)
	return conn, nil
}

func (sl *stoppableListener) Close() error {
	sl.closeOnce.Do(func() {
		close(sl.done)
		sl.closeErr = sl.TCPListener.Close()
		if errors.Is(sl.closeErr, net.ErrClosed) {
			sl.closeErr = nil
		}
	})
	return sl.closeErr
}

// Config represents all configuration options.
type Config struct {
	// Raft communication bind address
	RaftAddr string

	tlsConfig *transport.TLSInfo // TLS information used when creating connection
}

// Mux multiplexes a network connection.
type Mux struct {
	ln   net.Listener
	addr net.Addr
	m    map[byte]*listener

	mu              sync.Mutex
	wg              sync.WaitGroup
	pending         map[net.Conn]struct{}
	serving, closed bool
	stopc           chan struct{}
	closeOnce       sync.Once
	closeErr        error

	// The amount of time to wait for the first header byte.
	Timeout time.Duration

	// Out-of-band error logger
	Logger *zerolog.Logger

	tlsConfig *transport.TLSInfo // TLS information used when creating connection
}

// NewMux returns a new instance of Mux for ln. If adv is nil,
// then the addr of ln is used.
func NewMux(ln net.Listener, adv net.Addr) (*Mux, error) {
	addr := adv
	if addr == nil {
		addr = ln.Addr()
	}

	logger := log.With().Str("component", "[mux]").Logger()
	return &Mux{
		ln:      ln,
		addr:    addr,
		m:       make(map[byte]*listener),
		pending: make(map[net.Conn]struct{}),
		stopc:   make(chan struct{}),
		Timeout: DefaultTimeout,
		Logger:  &logger,
	}, nil
}

func NewTLSMux(ln net.Listener, addr net.Addr) (*Mux, error) {
	return nil, nil
}

// startNodeMux starts the TCP mux on the given listener, which should be already
// bound to the relevant interface.
func startNodeMux(cfg *Config, ln net.Listener) (*Mux, error) {
	var err error
	var mux *Mux

	if cfg.tlsConfig != nil {

	} else {
		mux, err = NewMux(ln, ln.Addr())
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create node-to-node mux: %s", err.Error())
	}
	go mux.Serve()
	return mux, nil
}

// Listen returns a Listener associated with the given header. Any connection
// accepted by mux is multiplexed based on the initial header byte.
func (mux *Mux) Listen(header byte) net.Listener {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	if _, ok := mux.m[header]; ok {
		panic(fmt.Sprintf("listener already registered under header byte: %d", header))
	}
	ln := &listener{c: make(chan net.Conn), addr: mux.addr, done: make(chan struct{})}
	mux.m[header] = ln
	if mux.closed {
		_ = ln.Close()
	}
	return ln
}

// Close closes the accepting socket, sublisteners and undelivered connections,
// then joins demultiplexing. Connections already accepted by an application
// remain that application's responsibility. Close is safe to call repeatedly.
func (mux *Mux) Close() error {
	mux.closeOnce.Do(func() {
		mux.mu.Lock()
		mux.closed = true
		close(mux.stopc)
		for _, ln := range mux.m {
			_ = ln.Close()
		}
		for conn := range mux.pending {
			_ = conn.Close()
		}
		mux.mu.Unlock()
		mux.closeErr = mux.ln.Close()
		if errors.Is(mux.closeErr, net.ErrClosed) {
			mux.closeErr = nil
		}
	})
	mux.wg.Wait()
	return mux.closeErr
}

// Stop stops acceptance and waits for all pending handoffs to finish.
func (mux *Mux) Stop() { _ = mux.Close() }

// Serve routes connections by their initial byte. It can be called once.
func (mux *Mux) Serve() error {
	mux.mu.Lock()
	if mux.serving {
		mux.mu.Unlock()
		return fmt.Errorf("mux is already serving")
	}
	if mux.closed {
		mux.mu.Unlock()
		return net.ErrClosed
	}
	mux.serving = true
	mux.mu.Unlock()
	defer mux.Close()
	mux.Logger.Printf("mux serving on %s, advertising %s", mux.ln.Addr().String(), mux.addr)
	for {
		conn, err := mux.ln.Accept()
		if err != nil {
			mux.mu.Lock()
			closed := mux.closed
			mux.mu.Unlock()
			if closed {
				return nil
			}
			if temporary, ok := err.(interface{ Temporary() bool }); ok && temporary.Temporary() {
				continue
			}
			return err
		}
		mux.mu.Lock()
		if mux.closed {
			mux.mu.Unlock()
			_ = conn.Close()
			return nil
		}
		mux.pending[conn] = struct{}{}
		mux.wg.Add(1)
		mux.mu.Unlock()
		go mux.handleConn(conn)
	}
}

func (mux *Mux) handleConn(conn net.Conn) {
	delivered := false
	defer func() {
		mux.mu.Lock()
		delete(mux.pending, conn)
		mux.mu.Unlock()
		if !delivered {
			_ = conn.Close()
		}
		mux.wg.Done()
	}()
	if err := conn.SetReadDeadline(time.Now().Add(mux.Timeout)); err != nil {
		return
	}
	var typ [1]byte
	if _, err := io.ReadFull(conn, typ[:]); err != nil {
		return
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return
	}
	mux.mu.Lock()
	handler := mux.m[typ[0]]
	// No more socket reads remain. Close can interrupt the following handoff
	// through stopc without closing a connection transferred to the application.
	delete(mux.pending, conn)
	mux.mu.Unlock()
	if handler == nil {
		return
	}
	select {
	case <-mux.stopc:
	case <-handler.done:
	case handler.c <- conn:
		delivered = true
	}
}
