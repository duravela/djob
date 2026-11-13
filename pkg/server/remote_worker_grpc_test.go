package server

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	pb "github.com/duravela/djob/sdk/go/worker/protocol/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func workerGRPCStream(t *testing.T, s *Server, client *http.Client, token string) grpc.BidiStreamingClient[pb.ClientFrame, pb.ServerFrame] {
	t.Helper()
	tlsConfig := client.Transport.(*http.Transport).TLSClientConfig.Clone()
	conn, err := grpc.NewClient("passthrough:///"+s.HTTPAddr(), grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), grpc.WithDisableRetry())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	t.Cleanup(cancel)
	stream, err := pb.NewWorkersClient(conn).Connect(metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+token)))
	require.NoError(t, err)
	return stream
}

func grpcRegistration() *pb.ClientFrame {
	return &pb.ClientFrame{RequestId: 1, Payload: &pb.ClientFrame_Register{Register: &pb.Register{Protocol: pb.ProtocolVersion, WorkerId: "test-worker", TenantId: "alpha", Handlers: []*pb.Version{{Name: "sha256", Version: "1"}}}}}
}

func TestGRPCWorkerRegistrationEnforcesIdentityAndProtocol(t *testing.T) {
	s, client := workerAPI(t)
	for _, test := range []struct {
		name, token string
		edit        func(*pb.ClientFrame)
		code        codes.Code
	}{
		{"missing credential", "", nil, codes.Unauthenticated},
		{"ordinary API credential", genericToken("ops/alice"), nil, codes.PermissionDenied},
		{"wrong tenant", workerToken("beta"), nil, codes.PermissionDenied},
		{"unsupported version", workerToken("alpha"), func(f *pb.ClientFrame) { f.GetRegister().Protocol = 99 }, codes.Unimplemented},
		{"bad initial sequence", workerToken("alpha"), func(f *pb.ClientFrame) { f.RequestId = 2 }, codes.InvalidArgument},
		{"missing registration", workerToken("alpha"), func(f *pb.ClientFrame) { f.Payload = &pb.ClientFrame_Poll{Poll: &pb.Poll{}} }, codes.InvalidArgument},
		{"duplicate capabilities", workerToken("alpha"), func(f *pb.ClientFrame) {
			f.GetRegister().Handlers = append(f.GetRegister().Handlers, &pb.Version{Name: "sha256", Version: "1"})
		}, codes.InvalidArgument},
	} {
		t.Run(test.name, func(t *testing.T) {
			stream := workerGRPCStream(t, s, client, test.token)
			frame := grpcRegistration()
			if test.edit != nil {
				test.edit(frame)
			}
			_ = stream.Send(frame) // An authentication status may race Send.
			_, err := stream.Recv()
			require.Equal(t, test.code, status.Code(err))
		})
	}
	for _, test := range []struct {
		name  string
		frame *pb.ClientFrame
		code  codes.Code
	}{
		{"replayed sequence", &pb.ClientFrame{RequestId: 1, Payload: &pb.ClientFrame_Poll{Poll: &pb.Poll{}}}, codes.InvalidArgument},
		{"registration in live session", &pb.ClientFrame{RequestId: 2, Payload: grpcRegistration().Payload}, codes.InvalidArgument},
		{"oversized heartbeat", &pb.ClientFrame{RequestId: 2, Payload: &pb.ClientFrame_Heartbeat{Heartbeat: &pb.Heartbeat{CommandId: strings.Repeat("a", 2048)}}}, codes.InvalidArgument},
		{"unsolicited result", &pb.ClientFrame{RequestId: 2, Payload: &pb.ClientFrame_Result{Result: &pb.Result{Id: strings.Repeat("a", 64)}}}, codes.FailedPrecondition},
	} {
		t.Run(test.name, func(t *testing.T) {
			stream := workerGRPCStream(t, s, client, workerToken("alpha"))
			require.NoError(t, stream.Send(grpcRegistration()))
			registered, err := stream.Recv()
			require.NoError(t, err)
			require.EqualValues(t, pb.ProtocolVersion, registered.GetRegistered().Protocol)
			require.NoError(t, stream.Send(test.frame))
			_, err = stream.Recv()
			require.Equal(t, test.code, status.Code(err))
		})
	}
}

func TestGRPCWorkerHeartbeatContinuesWhilePollWaits(t *testing.T) {
	s, client := workerAPI(t, func(c *Config) { c.RemoteWorkers.PollWait = time.Second })
	stream := workerGRPCStream(t, s, client, workerToken("alpha"))
	require.NoError(t, stream.Send(grpcRegistration()))
	_, err := stream.Recv()
	require.NoError(t, err)
	require.NoError(t, stream.Send(&pb.ClientFrame{RequestId: 2, Payload: &pb.ClientFrame_Poll{Poll: &pb.Poll{}}}))
	require.NoError(t, stream.Send(&pb.ClientFrame{RequestId: 3, Payload: &pb.ClientFrame_Heartbeat{Heartbeat: &pb.Heartbeat{}}}))
	frame, err := stream.Recv()
	require.NoError(t, err)
	require.EqualValues(t, 3, frame.RequestId)
	require.NotNil(t, frame.GetHeartbeat())
	frame, err = stream.Recv()
	require.NoError(t, err)
	require.EqualValues(t, 2, frame.RequestId)
	require.NotNil(t, frame.GetIdle())
	require.NoError(t, stream.Send(&pb.ClientFrame{RequestId: 4, Payload: &pb.ClientFrame_Drain{Drain: &pb.Drain{}}}))
	_, err = stream.Recv()
	require.NoError(t, err)
	require.NoError(t, stream.Send(&pb.ClientFrame{RequestId: 5, Payload: &pb.ClientFrame_Poll{Poll: &pb.Poll{}}}))
	frame, err = stream.Recv()
	require.NoError(t, err)
	require.Equal(t, "drained", frame.GetCommand().Kind)
}

func TestGRPCWorkerStreamSurvivesOrdinaryHTTPDeadline(t *testing.T) {
	s, client := workerAPI(t)
	remotePrepare(t, s)
	_, done := runRemoteWorker(t, s, client, "alpha", remoteRegistry(&remoteDevice{}))
	require.Eventually(t, func() bool { return len(s.remoteWorkers.snapshot()) == 1 }, 2*time.Second, 10*time.Millisecond)
	// Ordinary API requests have ten-second read/write deadlines. A healthy
	// worker stream must remain usable beyond both, without a fresh session.
	sessionID := s.remoteWorkers.snapshot()[0].id
	select {
	case err := <-done:
		t.Fatalf("stream ended: %v", err)
	case <-time.After(11 * time.Second):
	}
	require.Len(t, s.remoteWorkers.snapshot(), 1)
	require.Equal(t, sessionID, s.remoteWorkers.snapshot()[0].id)
	remoteLaunch(t, s, "alpha", "compute", "after-http-deadline")
	remoteState(t, s, "alpha", "after-http-deadline", db.JobSucceeded)
}
