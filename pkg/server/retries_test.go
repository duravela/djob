package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/duravela/djob/pkg/executor/modbustcp"
	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/duravela/djob/pkg/scheduler"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func publicRetryPolicy(delay time.Duration) *pb.RetryPolicy {
	return &pb.RetryPolicy{MaxAttempts: 3, InitialBackoff: durationpb.New(delay), MaxBackoff: durationpb.New(2 * delay)}
}

func TestPublicModbusNoSendRetrySurvivesRestartAndWritesOnce(t *testing.T) {
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := reservation.Addr().String()
	require.NoError(t, reservation.Close())
	cfg := apiTestConfig(t.TempDir())
	cfg.TransportFactory = modbustcp.Factory
	cfg.Retries.PollInterval = 5 * time.Millisecond
	releases := make(chan scheduler.DueWorkObservation, 4)
	cfg.Retries.OnReleased = func(o scheduler.DueWorkObservation) { releases <- o }
	s := startAPI(t, cfg)
	client := apiClient(t, s)
	configureModbus(t, client, "gateway", "tcp://"+address+"?turnaround=1ms")
	request := modbusRun("retry-write", "gateway", 7, true)
	request.RetryPolicy = publicRetryPolicy(2 * time.Second)
	var receipt pb.RunResponse
	httpProto(t, s, "PUT", "/api/v1/runs/retry-write", request, &receipt)
	require.True(t, proto.Equal(request.RetryPolicy, receipt.Run.RetryPolicy))
	waiting := awaitModbusRun(t, client, "retry-write", "retry_wait")
	require.EqualValues(t, 1, waiting.AttemptCount)
	first, err := client.GetAttempt(context.Background(), &pb.GetAttemptRequest{Id: waiting.LastAttemptId})
	require.NoError(t, err)
	require.Equal(t, "failed", first.Attempt.Outcome)
	require.Equal(t, "not_sent", first.Attempt.RetryClass)
	require.True(t, first.Attempt.Quiescent)
	require.True(t, proto.Equal(waiting.RetryAt, first.Attempt.RetryAt))
	require.NoError(t, s.Close(context.Background()))
	gateway := startModbusSimulatorAt(t, address)
	restarted := startAPI(t, cfg)
	client = apiClient(t, restarted)
	call := gateway.next(t)
	require.Equal(t, byte(7), call.frame[6])
	require.Equal(t, []byte{16, 0, 1, 0, 2, 4, 0x12, 0x34, 0, 0}, call.frame[7:])
	call.reply(t, []byte{16, 0, 1, 0, 2})
	run := awaitModbusRun(t, client, "retry-write", "succeeded")
	select {
	case observation := <-releases:
		require.True(t, observation.TimeKnown)
		require.GreaterOrEqual(t, observation.Lateness, time.Duration(0))
	case <-time.After(3 * time.Second):
		t.Fatal("recovered retry did not produce a release observation")
	}
	require.Empty(t, releases)
	require.EqualValues(t, 2, run.AttemptCount)
	require.NotEqual(t, waiting.LastAttemptId, run.LastAttemptId)
	require.Nil(t, run.RetryAt)
	retained, err := client.GetAttempt(context.Background(), &pb.GetAttemptRequest{Id: waiting.LastAttemptId})
	require.NoError(t, err)
	require.True(t, proto.Equal(first, retained))
	retried, err := client.CreateRun(context.Background(), request)
	require.NoError(t, err)
	require.True(t, proto.Equal(&receipt, retried), "retrying the original submission cannot repeat a confirmed write")
	select {
	case <-gateway.calls:
		t.Fatal("confirmed write repeated")
	default:
	}
}

func TestPublicCancellationAndExpiryStopWaitingRetries(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "expiry", true: "cancel"}[cancel], func(t *testing.T) {
			cfg := apiTestConfig(t.TempDir())
			cfg.Deadlines.PollInterval, cfg.Retries.PollInterval = 5*time.Millisecond, 5*time.Millisecond
			gateway := fake.New(fake.Options{})
			cfg.TransportFactory = gateway.Factory
			s := startAPI(t, cfg, gateway)
			client := apiClient(t, s)
			configureAPI(t, client, "gateway")
			request := &pb.CreateRunRequest{Command: meta("create"), Id: "run", ChannelId: "gateway", Executor: "fake", RetryPolicy: publicRetryPolicy(2 * time.Second)}
			if !cancel {
				request.StartDeadline = timestamppb.New(time.Now().Add(time.Second))
			}
			_, err := client.CreateRun(context.Background(), request)
			require.NoError(t, err)
			call := apiCall(t, gateway)
			call.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptFailed, RetryClass: db.RetryNotSent}}
			apiPhase(t, call.RecoveryStarted)
			call.Recover <- nil
			apiPhase(t, call.CloseStarted)
			call.Close <- nil
			waiting := awaitModbusRun(t, client, "run", "retry_wait")
			expected := "expired"
			if cancel {
				response, err := client.CancelRun(context.Background(), &pb.CancelRunRequest{Command: meta("cancel"), Id: "run"})
				require.NoError(t, err)
				require.Equal(t, "cancelled", response.Run.State)
				expected = "cancelled"
			}
			final := awaitModbusRun(t, client, "run", expected)
			require.EqualValues(t, 1, final.AttemptCount)
			require.Nil(t, final.RetryAt)
			_, err = s.retryPromoter.Tick(context.Background(), waiting.RetryAt.AsTime().Add(time.Hour))
			require.NoError(t, err)
			pending, err := s.application.Store.PendingRunCount("gateway")
			require.NoError(t, err)
			require.Zero(t, pending)
			require.Equal(t, 1, gateway.Stats().Started)
		})
	}
}

func TestPublicRetryPolicyValidationAndScheduleRoundTrip(t *testing.T) {
	s := startAPI(t, apiTestConfig(t.TempDir()))
	client := apiClient(t, s)
	configureAPI(t, client, "gateway")
	for _, p := range []*pb.RetryPolicy{
		{}, {MaxAttempts: 33, InitialBackoff: durationpb.New(time.Second), MaxBackoff: durationpb.New(time.Second)},
		{MaxAttempts: 3, InitialBackoff: durationpb.New(time.Second), MaxBackoff: durationpb.New(time.Millisecond)},
		{MaxAttempts: 3, InitialBackoff: durationpb.New(time.Second), MaxBackoff: durationpb.New(2 * time.Hour)},
	} {
		_, err := client.CreateRun(context.Background(), &pb.CreateRunRequest{Command: meta("invalid"), Id: "invalid", ChannelId: "gateway", Executor: "fake", RetryPolicy: p})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	request := apiSchedule("poll", time.Now().Add(time.Hour))
	request.Definition.RetryPolicy = publicRetryPolicy(time.Second)
	var response pb.ScheduleResponse
	httpProto(t, s, "PUT", "/api/v1/schedules/poll", request, &response)
	require.True(t, proto.Equal(request.Definition.RetryPolicy, response.Schedule.Definition.RetryPolicy))
}
