// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package worker

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
	pb "github.com/duravela/djob/sdk/go/worker/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Capture typed TLS failures before gRPC turns them into an Unavailable status.
// Certificate/authentication failures must stop Serve instead of retry forever.
type workerCredentials struct {
	credentials.TransportCredentials
	observation *workerTLSObservation
}

type workerTLSObservation struct {
	mu      sync.Mutex
	failure error
}

func (o *workerTLSObservation) observe(err error) {
	if err == nil {
		return
	}
	classified := transportError(context.Background(), err)
	if !errors.Is(classified, ErrSessionLost) {
		o.mu.Lock()
		o.failure = classified
		o.mu.Unlock()
	}
}

// TLS 1.3 may report a client-certificate refusal on the first record read,
// after ClientHandshake has returned. Observe records before gRPC erases the
// typed alert into a generic Unavailable status.
type workerTLSConn struct {
	net.Conn
	observation *workerTLSObservation
	firstRead   sync.Once
	readDone    chan struct{}
}

func (c *workerTLSConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.observation.observe(err)
	c.firstRead.Do(func() { close(c.readDone) })
	return n, err
}

func (c *workerTLSConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.observation.observe(err)
	if err != nil {
		// The HTTP/2 preface write can fail before the reader consumes a TLS
		// 1.3 refusal. gRPC then closes the connection and loses that alert.
		// Let the already-started reader report its first result before gRPC
		// tears down a failed startup. Established connections never wait.
		select {
		case <-c.readDone:
		default:
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-c.readDone:
			case <-timer.C:
			}
			timer.Stop()
		}
	}
	return n, err
}

func (c *workerCredentials) ClientHandshake(ctx context.Context, authority string, conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	secured, info, err := c.TransportCredentials.ClientHandshake(ctx, authority, conn)
	if err != nil && ctx.Err() == nil {
		c.observation.observe(err)
	}
	if err == nil {
		secured = &workerTLSConn{Conn: secured, observation: c.observation, readDone: make(chan struct{})}
	}
	return secured, info, err
}

func (c *workerCredentials) Clone() credentials.TransportCredentials {
	return &workerCredentials{TransportCredentials: c.TransportCredentials.Clone(), observation: c.observation}
}

type grpcResponseError struct{ code codes.Code }

func (e *grpcResponseError) Error() string {
	return fmt.Sprintf("worker request rejected (gRPC %s)", e.code)
}
func (e *grpcResponseError) Unwrap() error {
	switch e.code {
	case codes.Canceled, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Internal, codes.Unavailable, codes.NotFound:
		return ErrSessionLost
	}
	return nil
}

type workerRPC struct {
	conn        *grpc.ClientConn
	stream      grpc.BidiStreamingClient[pb.ClientFrame, pb.ServerFrame]
	credentials *workerCredentials
	cancel      context.CancelFunc
	send        sync.Mutex
	mu          sync.Mutex
	sequence    uint64
	pending     map[uint64]chan *pb.ServerFrame
	done        chan struct{}
	received    chan struct{}
	err         error
}

func (c *workerRPC) classify(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	c.credentials.observation.mu.Lock()
	failure := c.credentials.observation.failure
	c.credentials.observation.mu.Unlock()
	if failure != nil {
		return failure
	}
	// Remote status messages may include secrets or proxy text. Retain only the
	// code, with conservative terminal handling for protocol and access errors.
	code := status.Code(err)
	if code == codes.Unknown || code == codes.OK {
		return ErrSessionLost
	}
	return &grpcResponseError{code: code}
}

func openWorkerRPC(parent context.Context, cfg Config) (*workerRPC, error) {
	tlsConfig := cfg.TLSConfig
	if tlsConfig == nil {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	c := &workerRPC{credentials: &workerCredentials{TransportCredentials: credentials.NewTLS(tlsConfig.Clone()), observation: new(workerTLSObservation)}, pending: make(map[uint64]chan *pb.ServerFrame), done: make(chan struct{}), received: make(chan struct{})}
	origin, _ := url.Parse(cfg.ServerURL)
	conn, err := grpc.NewClient("passthrough:///"+origin.Host,
		grpc.WithTransportCredentials(c.credentials), grpc.WithDisableRetry(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(wire.MaxCommandBytes), grpc.MaxCallSendMsgSize(wire.MaxResultBytes)))
	if err != nil {
		return nil, fmt.Errorf("cannot configure worker gRPC connection")
	}
	c.conn = conn
	// A parent cancellation starts graceful drain after registration. Give the
	// stream its own lifetime, but bound initial connection establishment.
	ctx, cancel := context.WithCancel(metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+cfg.Token)))
	c.cancel = cancel
	connecting, stop := context.WithTimeout(parent, cfg.RequestTimeout)
	interrupt := context.AfterFunc(connecting, cancel)
	c.stream, err = pb.NewWorkersClient(conn).Connect(ctx)
	interrupt()
	stop()
	if err != nil {
		cancel()
		_ = conn.Close()
		return nil, c.classify(parent, err)
	}
	go c.receive()
	return c, nil
}

func (c *workerRPC) fail(err error) {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
		close(c.done)
	}
	c.mu.Unlock()
	c.cancel()
}

func (c *workerRPC) receive() {
	defer close(c.received)
	for {
		frame, err := c.stream.Recv()
		if err != nil {
			c.fail(c.classify(context.Background(), err))
			return
		}
		c.mu.Lock()
		waiting := c.pending[frame.RequestId]
		delete(c.pending, frame.RequestId)
		c.mu.Unlock()
		if waiting == nil {
			c.fail(fmt.Errorf("invalid or replayed worker gRPC response"))
			return
		}
		waiting <- frame
	}
}

func (c *workerRPC) close() {
	c.cancel()
	_ = c.conn.Close()
	<-c.received
}

func (c *workerRPC) roundTrip(ctx context.Context, frame *pb.ClientFrame) (*pb.ServerFrame, error) {
	if proto.Size(frame) > wire.MaxResultBytes {
		return nil, fmt.Errorf("worker message exceeds its limit")
	}
	// Cancelling the request cancels the stream, including a blocked Send. A
	// timed-out request is never left live or retried behind the caller's back.
	interrupt := context.AfterFunc(ctx, c.cancel)
	defer interrupt()
	c.send.Lock()
	if ctx.Err() != nil {
		c.send.Unlock()
		return nil, context.Cause(ctx)
	}
	c.mu.Lock()
	if c.err != nil || c.sequence == math.MaxUint64 || len(c.pending) >= 3 {
		err := c.err
		if err == nil {
			err = fmt.Errorf("worker gRPC request limit reached")
		}
		c.mu.Unlock()
		c.send.Unlock()
		return nil, err
	}
	c.sequence++
	frame.RequestId = c.sequence
	waiting := make(chan *pb.ServerFrame, 1)
	c.pending[frame.RequestId] = waiting
	c.mu.Unlock()
	err := c.stream.Send(frame)
	c.send.Unlock()
	if err != nil {
		// Recv contains the authoritative gRPC status after a failed Send.
		select {
		case <-c.done:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}
	select {
	case reply := <-waiting:
		return reply, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-c.done:
		// A terminal status can follow the final response immediately. Prefer
		// that already-delivered response (notably a successful drain).
		select {
		case reply := <-waiting:
			return reply, nil
		default:
		}
		c.mu.Lock()
		err := c.err
		c.mu.Unlock()
		return nil, err
	}
}

func (c *client) exchangeGRPC(ctx context.Context, path string, body, reply any) (int, error) {
	frame := new(pb.ClientFrame)
	operation := path[strings.LastIndex(path, "/")+1:]
	switch operation {
	case "register":
		request := body.(wire.Register)
		request.Protocol = pb.ProtocolVersion
		frame.Payload = &pb.ClientFrame_Register{Register: pb.EncodeRegister(&request)}
	case "poll":
		frame.Payload = &pb.ClientFrame_Poll{Poll: &pb.Poll{}}
	case "heartbeat":
		request := body.(wire.Heartbeat)
		frame.Payload = &pb.ClientFrame_Heartbeat{Heartbeat: pb.EncodeHeartbeat(&request)}
	case "result":
		request := body.(wire.Result)
		frame.Payload = &pb.ClientFrame_Result{Result: pb.EncodeResult(&request)}
	case "drain":
		frame.Payload = &pb.ClientFrame_Drain{Drain: &pb.Drain{}}
	case "disconnect":
		frame.Payload = &pb.ClientFrame_Disconnect{Disconnect: &pb.Disconnect{}}
	default:
		return 0, fmt.Errorf("unsupported worker operation")
	}
	request, stop := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer stop()
	response, err := c.rpc.roundTrip(request, frame)
	if err != nil {
		var rejection *grpcResponseError
		if operation == "register" && errors.As(err, &rejection) && rejection.code == codes.NotFound {
			return 0, fmt.Errorf("worker gRPC registration unavailable")
		}
		if request.Err() != nil && ctx.Err() == nil {
			return 0, ErrSessionLost
		}
		return 0, err
	}
	invalid := fmt.Errorf("invalid worker gRPC response")
	switch operation {
	case "register":
		message := response.GetRegistered()
		if message == nil || message.Protocol != pb.ProtocolVersion {
			return 0, invalid
		}
		registered, err := pb.DecodeRegistered(message)
		if err != nil {
			return 0, invalid
		}
		registered.Protocol = wire.Version
		*reply.(*wire.Registered) = *registered
	case "poll":
		if response.GetIdle() != nil {
			return http.StatusNoContent, nil
		}
		command, err := pb.DecodeCommand(response.GetCommand())
		if err != nil || command == nil {
			return 0, invalid
		}
		*reply.(*wire.Command) = *command
	case "heartbeat":
		heartbeat, err := pb.DecodeHeartbeatReply(response.GetHeartbeat())
		if err != nil || heartbeat == nil {
			return 0, invalid
		}
		*reply.(*wire.HeartbeatReply) = *heartbeat
	default:
		if response.GetAcknowledgement() == nil {
			return 0, invalid
		}
		return http.StatusNoContent, nil
	}
	return http.StatusOK, nil
}
