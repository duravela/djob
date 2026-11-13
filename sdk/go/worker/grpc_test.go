// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package worker_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	djob "github.com/duravela/djob/sdk/go"
	"github.com/duravela/djob/sdk/go/worker"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
	pb "github.com/duravela/djob/sdk/go/worker/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type testWorkerService struct {
	pb.UnimplementedWorkersServer
	connect func(grpc.BidiStreamingServer[pb.ClientFrame, pb.ServerFrame]) error
}

func (s *testWorkerService) Connect(stream grpc.BidiStreamingServer[pb.ClientFrame, pb.ServerFrame]) error {
	return s.connect(stream)
}

func grpcController(t *testing.T, connect func(grpc.BidiStreamingServer[pb.ClientFrame, pb.ServerFrame]) error, clientAuth bool) *httptest.Server {
	t.Helper()
	rpc := grpc.NewServer()
	pb.RegisterWorkersServer(rpc, &testWorkerService{connect: connect})
	s := httptest.NewUnstartedServer(rpc)
	s.EnableHTTP2 = true
	if clientAuth {
		s.TLS = &tls.Config{ClientAuth: tls.RequireAnyClientCert}
	}
	s.StartTLS()
	t.Cleanup(func() { rpc.Stop(); s.Close() })
	return s
}

func grpcConfig(t *testing.T, server *httptest.Server, d djob.Driver) worker.Config {
	t.Helper()
	cfg := supervisedConfig(t, server)
	cfg.Transport = worker.TransportGRPC
	if d != nil {
		cfg.Registry.Drivers = map[djob.Version]djob.DriverFactory{{Name: "device", Version: "1"}: func(djob.JobRun) (djob.Driver, error) { return d, nil }}
	}
	return cfg
}

// This peer uses native protobuf only. The older JSON fixtures continue to
// exercise explicit HTTPS v1 compatibility separately.
func serveCommands(stream grpc.BidiStreamingServer[pb.ClientFrame, pb.ServerFrame], commands []wire.Command, result func(*pb.Result) error, heartbeatFailure bool, delay time.Duration) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer "+strings.Repeat("s", 32) {
		return status.Error(codes.Unauthenticated, "secret-marker")
	}
	for {
		frame, err := stream.Recv()
		if err != nil {
			return err
		}
		reply := &pb.ServerFrame{RequestId: frame.RequestId}
		switch message := frame.Payload.(type) {
		case *pb.ClientFrame_Register:
			if message.Register.Protocol != pb.ProtocolVersion {
				return status.Error(codes.Unimplemented, "wrong protocol")
			}
			reply.Payload = &pb.ServerFrame_Registered{Registered: &pb.Registered{Protocol: pb.ProtocolVersion, SessionId: strings.Repeat("a", 64), TenantId: "alpha", HeartbeatEveryNs: int64(20 * time.Millisecond)}}
		case *pb.ClientFrame_Poll:
			command := wire.Command{Kind: wire.Drained}
			if len(commands) > 0 {
				command, commands = commands[0], commands[1:]
			}
			if delay > 0 {
				time.Sleep(delay)
				delay = 0
			}
			reply.Payload = &pb.ServerFrame_Command{Command: pb.EncodeCommand(&command)}
		case *pb.ClientFrame_Heartbeat:
			if heartbeatFailure && message.Heartbeat.CommandId != "" {
				return status.Error(codes.Unavailable, "connection lost")
			}
			reply.Payload = &pb.ServerFrame_Heartbeat{Heartbeat: &pb.HeartbeatReply{CommandId: message.Heartbeat.CommandId, BudgetNs: int64(time.Second)}}
		case *pb.ClientFrame_Result:
			if result != nil {
				if err := result(message.Result); err != nil {
					return err
				}
			}
			reply.Payload = &pb.ServerFrame_Acknowledgement{Acknowledgement: &pb.Acknowledgement{}}
		case *pb.ClientFrame_Drain:
			reply.Payload = &pb.ServerFrame_Acknowledgement{Acknowledgement: &pb.Acknowledgement{}}
		case *pb.ClientFrame_Disconnect:
			return stream.Send(&pb.ServerFrame{RequestId: frame.RequestId, Payload: &pb.ServerFrame_Acknowledgement{Acknowledgement: &pb.Acknowledgement{}}})
		default:
			return status.Error(codes.InvalidArgument, "unsupported frame")
		}
		if err := stream.Send(reply); err != nil {
			return err
		}
	}
}

func TestGRPCPreservesConversationAndRejectsReplayedCommands(t *testing.T) {
	for _, replay := range []bool{false, true} {
		t.Run(map[bool]string{false: "conversation", true: "replay"}[replay], func(t *testing.T) {
			var effects, factories, reports atomic.Int32
			d := &device{execute: func(context.Context, djob.Action) djob.Effect {
				effects.Add(1)
				return djob.Effect{Status: djob.EffectSucceeded}
			}}
			commands := []wire.Command{action(1), action(2), cleanup(3, wire.Quiesce), cleanup(4, wire.Close)}
			if replay {
				commands = []wire.Command{action(1), action(1)}
			}
			s := grpcController(t, func(stream grpc.BidiStreamingServer[pb.ClientFrame, pb.ServerFrame]) error {
				return serveCommands(stream, commands, func(*pb.Result) error { reports.Add(1); return nil }, false, 0)
			}, false)
			cfg := grpcConfig(t, s, d)
			cfg.Transport = "" // gRPC is the default, including for existing configurations.
			cfg.Registry.Drivers[djob.Version{Name: "device", Version: "1"}] = func(djob.JobRun) (djob.Driver, error) { factories.Add(1); return d, nil }
			err := worker.Run(context.Background(), cfg)
			if replay {
				if err == nil || effects.Load() != 1 || reports.Load() != 1 {
					t.Fatalf("replayed effect: effects=%d reports=%d err=%v", effects.Load(), reports.Load(), err)
				}
			} else if err != nil || effects.Load() != 2 || reports.Load() != 4 {
				t.Fatalf("conversation: effects=%d reports=%d err=%v", effects.Load(), reports.Load(), err)
			}
			if factories.Load() != 1 || d.quiesced.Load() != 1 || d.closed.Load() != 1 {
				t.Fatalf("driver lifetime: factories=%d quiesce=%d close=%d", factories.Load(), d.quiesced.Load(), d.closed.Load())
			}
		})
	}
}

func TestGRPCTransitDelayConsumesExecutionPermission(t *testing.T) {
	var effects atomic.Int32
	d := &device{execute: func(context.Context, djob.Action) djob.Effect {
		effects.Add(1)
		return djob.Effect{Status: djob.EffectSucceeded}
	}}
	command := action(1)
	command.Budget = 20 * time.Millisecond
	s := grpcController(t, func(stream grpc.BidiStreamingServer[pb.ClientFrame, pb.ServerFrame]) error {
		return serveCommands(stream, []wire.Command{command}, nil, false, 70*time.Millisecond)
	}, false)
	if err := worker.Run(context.Background(), grpcConfig(t, s, d)); !errors.Is(err, worker.ErrSessionLost) || effects.Load() != 0 {
		t.Fatalf("delayed permission executed: effects=%d err=%v", effects.Load(), err)
	}
}

func TestGRPCHeartbeatLossCancelsAndJoinsDeviceIO(t *testing.T) {
	started, stopped, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	d := &device{execute: func(ctx context.Context, _ djob.Action) djob.Effect {
		close(started)
		<-ctx.Done()
		close(stopped)
		<-release
		return djob.Effect{Status: djob.EffectUnknown}
	}}
	s := grpcController(t, func(stream grpc.BidiStreamingServer[pb.ClientFrame, pb.ServerFrame]) error {
		return serveCommands(stream, []wire.Command{action(1)}, nil, true, 0)
	}, false)
	done := make(chan error, 1)
	go func() { done <- worker.Run(context.Background(), grpcConfig(t, s, d)) }()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("I/O did not start")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("lost heartbeat did not cancel I/O")
	}
	select {
	case err := <-done:
		t.Fatalf("worker detached live I/O: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, worker.ErrSessionLost) || d.closed.Load() != 1 {
			t.Fatalf("cleanup: close=%d err=%v", d.closed.Load(), err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not join I/O")
	}
}

func TestGRPCReconnectNeverRepeatsAnUnacknowledgedEffect(t *testing.T) {
	var sessions, effects, reports atomic.Int32
	d := &device{execute: func(context.Context, djob.Action) djob.Effect {
		effects.Add(1)
		return djob.Effect{Status: djob.EffectSucceeded}
	}}
	s := grpcController(t, func(stream grpc.BidiStreamingServer[pb.ClientFrame, pb.ServerFrame]) error {
		if sessions.Add(1) == 1 {
			return serveCommands(stream, []wire.Command{action(1)}, func(*pb.Result) error { reports.Add(1); return status.Error(codes.Unavailable, "lost receipt") }, false, 0)
		}
		return serveCommands(stream, nil, nil, false, 0)
	}, false)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := worker.Serve(ctx, grpcConfig(t, s, d), smallBackoff()); err != nil {
		t.Fatal(err)
	}
	if sessions.Load() != 2 || effects.Load() != 1 || reports.Load() != 1 || d.closed.Load() != 1 {
		t.Fatalf("replay after receipt loss: sessions=%d effects=%d reports=%d closed=%d", sessions.Load(), effects.Load(), reports.Load(), d.closed.Load())
	}
}

func TestGRPCPermanentErrorsDoNotRetryOrExposeStatusMessages(t *testing.T) {
	for _, code := range []codes.Code{codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument, codes.Unimplemented, codes.FailedPrecondition} {
		t.Run(code.String(), func(t *testing.T) {
			var calls, retries atomic.Int32
			s := grpcController(t, func(stream grpc.BidiStreamingServer[pb.ClientFrame, pb.ServerFrame]) error {
				calls.Add(1)
				_, _ = stream.Recv()
				return status.Error(code, "secret-marker")
			}, false)
			retry := smallBackoff()
			retry.OnRetry = func(worker.RetryObservation) { retries.Add(1) }
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := worker.Serve(ctx, grpcConfig(t, s, nil), retry)
			if err == nil || errors.Is(err, worker.ErrSessionLost) || strings.Contains(err.Error(), "secret-marker") || calls.Load() != 1 || retries.Load() != 0 {
				t.Fatalf("permanent failure: calls=%d retries=%d err=%v", calls.Load(), retries.Load(), err)
			}
		})
	}
}

func TestGRPCTLSFailuresAreTerminal(t *testing.T) {
	for _, clientAuth := range []bool{false, true} {
		t.Run(map[bool]string{false: "untrusted server", true: "client certificate required"}[clientAuth], func(t *testing.T) {
			s := grpcController(t, func(grpc.BidiStreamingServer[pb.ClientFrame, pb.ServerFrame]) error {
				t.Error("unverified TLS reached worker service")
				return nil
			}, clientAuth)
			cfg := grpcConfig(t, s, nil)
			if !clientAuth {
				cfg.Client = &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: x509.NewCertPool()}}}
			}
			var retries int
			retry := smallBackoff()
			retry.OnRetry = func(worker.RetryObservation) { retries++ }
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := worker.Serve(ctx, cfg, retry)
			if err == nil || errors.Is(err, worker.ErrSessionLost) || errors.Is(err, context.DeadlineExceeded) || retries != 0 {
				t.Fatalf("TLS failure retried: retries=%d err=%v", retries, err)
			}
		})
	}
}
