package server

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
	pb "github.com/duravela/djob/sdk/go/worker/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type workerGRPCServer struct {
	pb.UnimplementedWorkersServer
	hub *remoteWorkers
}

func (h *remoteWorkers) newGRPCTransport() *grpc.Server {
	s := grpc.NewServer(grpc.MaxRecvMsgSize(wire.MaxResultBytes), grpc.MaxSendMsgSize(wire.MaxCommandBytes), grpc.MaxConcurrentStreams(uint32(h.cfg.MaxSessions)))
	pb.RegisterWorkersServer(s, &workerGRPCServer{hub: h})
	return s
}

// Worker gRPC shares the TLS API origin. Only its exact service path bypasses
// ordinary HTTP body/deadline limits; protobuf messages and session inactivity
// have their own bounds. The controller's other gRPC APIs keep their limits.
func (h *remoteWorkers) serveGRPC(w http.ResponseWriter, r *http.Request) {
	if r.TLS == nil || r.ProtoMajor != 2 || !strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
		workerError(w, http.StatusBadRequest, "worker gRPC requires HTTP/2 and TLS")
		return
	}
	controller := http.NewResponseController(w)
	if controller.SetReadDeadline(time.Time{}) != nil || controller.SetWriteDeadline(time.Time{}) != nil {
		workerError(w, http.StatusServiceUnavailable, "worker stream deadlines unavailable")
		return
	}
	h.rpc.ServeHTTP(w, r)
}

func grpcWorkerFault(fault *workerFault) error {
	code := codes.Internal
	switch fault.status {
	case 400:
		code = codes.InvalidArgument
	case 401:
		code = codes.Unauthenticated
	case 403:
		code = codes.PermissionDenied
	case 404, 410:
		code = codes.NotFound
	case 408:
		code = codes.DeadlineExceeded
	case 409:
		code = codes.FailedPrecondition
	case 426:
		code = codes.Unimplemented
	case 429:
		code = codes.ResourceExhausted
	case 503:
		code = codes.Unavailable
	}
	return status.Error(code, fault.message)
}

type workerStreamInput struct {
	frame *pb.ClientFrame
	err   error
}

type workerPollResult struct {
	command *wire.Command
	err     error
}

func sendWorkerPoll(send func(*pb.ServerFrame) error, id uint64, result workerPollResult) error {
	if result.err != nil {
		return result.err
	}
	frame := &pb.ServerFrame{RequestId: id}
	if result.command == nil {
		frame.Payload = &pb.ServerFrame_Idle{Idle: &pb.Idle{}}
	} else {
		frame.Payload = &pb.ServerFrame_Command{Command: pb.EncodeCommand(result.command)}
	}
	if proto.Size(frame) > wire.MaxCommandBytes {
		return status.Error(codes.ResourceExhausted, "worker command exceeds limit")
	}
	return send(frame)
}

func (g *workerGRPCServer) Connect(stream grpc.BidiStreamingServer[pb.ClientFrame, pb.ServerFrame]) error {
	h := g.hub
	p, ok := peer.FromContext(stream.Context())
	if !ok {
		return status.Error(codes.Unauthenticated, "worker TLS required")
	}
	if _, ok := p.AuthInfo.(credentials.TLSInfo); !ok {
		return status.Error(codes.Unauthenticated, "worker TLS required")
	}
	select {
	case h.streamStarts <- struct{}{}:
	default:
		return status.Error(codes.ResourceExhausted, "worker registration capacity reached")
	}
	starting := true
	defer func() {
		if starting {
			<-h.streamStarts
		}
	}()
	md, _ := metadata.FromIncomingContext(stream.Context())
	principal, fault := h.authenticateWorker(stream.Context(), md.Get("authorization"))
	if fault != nil {
		return grpcWorkerFault(fault)
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	type outbound struct {
		frame *pb.ServerFrame
		sent  chan error
	}
	output := make(chan outbound)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case message := <-output:
				message.sent <- stream.Send(message.frame)
			}
		}
	}()
	send := func(frame *pb.ServerFrame) error {
		// A peer that stops reading must not retain a stream or a blocked
		// controller goroutine indefinitely. Returning ends the gRPC context
		// and unblocks the transport's Send/Recv operations.
		deadline := time.NewTimer(h.cfg.IdleTimeout)
		defer deadline.Stop()
		message := outbound{frame, make(chan error, 1)}
		select {
		case output <- message:
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return status.Error(codes.DeadlineExceeded, "worker stream stalled")
		}
		select {
		case err := <-message.sent:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return status.Error(codes.DeadlineExceeded, "worker stream stalled")
		}
	}
	input := make(chan workerStreamInput)
	go func() {
		for {
			frame, err := stream.Recv()
			select {
			case input <- workerStreamInput{frame, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	var first workerStreamInput
	select {
	case first = <-input:
	case <-timer.C:
		return status.Error(codes.DeadlineExceeded, "worker registration timed out")
	case <-ctx.Done():
		return ctx.Err()
	}
	if first.err != nil {
		return first.err
	}
	registration := first.frame.GetRegister()
	if first.frame.RequestId != 1 || registration == nil || proto.Size(first.frame) > wire.MaxControlBytes {
		return status.Error(codes.InvalidArgument, "first worker frame must register")
	}
	if registration.Protocol != pb.ProtocolVersion {
		return status.Error(codes.Unimplemented, "unsupported worker protocol")
	}
	request, err := pb.DecodeRegister(registration)
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid worker registration")
	}
	request.Protocol = wire.Version // The shared execution model is transport-free.
	session, registered, fault := h.newSession(*request, principal)
	if fault != nil {
		return grpcWorkerFault(fault)
	}
	defer session.stop()
	<-h.streamStarts
	starting = false
	registered.Protocol = pb.ProtocolVersion
	if err := send(&pb.ServerFrame{RequestId: 1, Payload: &pb.ServerFrame_Registered{Registered: pb.EncodeRegistered(&registered)}}); err != nil {
		return err
	}
	// Exactly one poll may wait while heartbeats, results and drain requests
	// continue. One writer sends frames; there is no unbounded goroutine fan-out.
	polled := make(chan workerPollResult, 1)
	var pollID uint64
	var previous uint64 = 1
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-session.life.Done():
			// A drained poll owns its final response even though it stops the
			// session. Wait for that bounded operation before ending the stream.
			if pollID == 0 {
				return status.Error(codes.Unavailable, "worker session ended")
			}
			select {
			case result := <-polled:
				return sendWorkerPoll(send, pollID, result)
			case <-ctx.Done():
				return ctx.Err()
			}
		case result := <-polled:
			if err := sendWorkerPoll(send, pollID, result); err != nil {
				return err
			}
			pollID = 0
		case item := <-input:
			if item.err != nil {
				if errors.Is(item.err, io.EOF) {
					return nil
				}
				return item.err
			}
			frame := item.frame
			if previous == math.MaxUint64 || frame.RequestId != previous+1 {
				return status.Error(codes.InvalidArgument, "invalid or replayed worker frame")
			}
			previous = frame.RequestId
			if frame.GetResult() == nil && proto.Size(frame) > 1024 {
				return status.Error(codes.InvalidArgument, "worker control frame exceeds limit")
			}
			session.mu.Lock()
			session.lastSeen = time.Now()
			session.mu.Unlock()
			session.enrollmentAllows()
			reply := &pb.ServerFrame{RequestId: frame.RequestId}
			switch message := frame.Payload.(type) {
			case *pb.ClientFrame_Poll:
				if pollID != 0 {
					return status.Error(codes.FailedPrecondition, "worker poll already active")
				}
				pollID = frame.RequestId
				go func() {
					command, fault := session.nextCommand(ctx)
					result := workerPollResult{command: command}
					if fault != nil {
						result.err = grpcWorkerFault(fault)
					}
					polled <- result
				}()
				continue
			case *pb.ClientFrame_Heartbeat:
				response := wire.HeartbeatReply{CommandID: message.Heartbeat.CommandId, Cancel: message.Heartbeat.CommandId != ""}
				session.mu.Lock()
				if pending := session.pending; pending != nil && pending.command.ID == response.CommandID {
					response.Budget = pending.budget()
					response.Cancel = response.Budget <= 0
				}
				session.mu.Unlock()
				reply.Payload = &pb.ServerFrame_Heartbeat{Heartbeat: pb.EncodeHeartbeatReply(&response)}
			case *pb.ClientFrame_Result:
				result, err := pb.DecodeResult(message.Result)
				if err != nil || result == nil {
					return status.Error(codes.InvalidArgument, "invalid worker result")
				}
				if fault := session.acceptResult(*result); fault != nil {
					return grpcWorkerFault(fault)
				}
				reply.Payload = &pb.ServerFrame_Acknowledgement{Acknowledgement: &pb.Acknowledgement{}}
			case *pb.ClientFrame_Drain:
				session.drain()
				reply.Payload = &pb.ServerFrame_Acknowledgement{Acknowledgement: &pb.Acknowledgement{}}
			case *pb.ClientFrame_Disconnect:
				session.stop()
				return send(&pb.ServerFrame{RequestId: frame.RequestId, Payload: &pb.ServerFrame_Acknowledgement{Acknowledgement: &pb.Acknowledgement{}}})
			default:
				return status.Error(codes.InvalidArgument, "unsupported worker frame")
			}
			if err := send(reply); err != nil {
				return err
			}
		}
	}
}
