package server

import (
	"context"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/fake"
	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPublicRunDeadlineExpiresOnControllerAndPreservesOriginalReceiptAfterRestart(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.Deadlines.PollInterval = 5 * time.Millisecond
	s := startAPI(t, cfg)
	client := apiClient(t, s)
	configureAPI(t, client, "gateway")
	request := &pb.CreateRunRequest{Command: meta("create-expiring"), Id: "expire/with/slash", ChannelId: "gateway", Executor: "fake",
		StartDeadline: timestamppb.New(time.Now().Add(300 * time.Millisecond))}
	var receipt pb.RunResponse
	httpProto(t, s, "PUT", "/api/v1/runs/expire/with/slash", request, &receipt)
	require.Equal(t, "ready", receipt.Run.State)
	require.True(t, proto.Equal(request.StartDeadline, receipt.Run.StartDeadline))
	require.Eventually(t, func() bool {
		r, err := client.GetRun(context.Background(), &pb.GetRunRequest{Id: request.Id})
		return err == nil && r.Run.State == "expired" && r.Run.AttemptCount == 0
	}, 3*time.Second, 5*time.Millisecond)
	channel, err := client.GetChannel(context.Background(), &pb.GetChannelRequest{Id: "gateway"})
	require.NoError(t, err)
	require.Equal(t, "available", channel.Channel.State)
	count, err := s.application.Store.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.Zero(t, count)
	require.NoError(t, s.Close(context.Background()))
	restarted := startAPI(t, cfg)
	client = apiClient(t, restarted)
	retried, err := client.CreateRun(context.Background(), request)
	require.NoError(t, err)
	require.True(t, proto.Equal(&receipt, retried), "retry must retain the historical ready receipt rather than extend its deadline")
	expired, err := client.GetRun(context.Background(), &pb.GetRunRequest{Id: request.Id})
	require.NoError(t, err)
	require.Equal(t, "expired", expired.Run.State)
	require.Zero(t, expired.Run.AttemptCount)
	changed := proto.Clone(request).(*pb.CreateRunRequest)
	changed.StartDeadline = timestamppb.New(time.Now().Add(time.Hour))
	_, err = client.CreateRun(context.Background(), changed)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	cancelled, err := client.CancelRun(context.Background(), &pb.CancelRunRequest{Command: meta("cancel-after-expiry"), Id: request.Id})
	require.NoError(t, err)
	require.Equal(t, "expired", cancelled.Run.State)
	require.False(t, cancelled.Run.CancelRequested)
}

func TestScheduledRunExpiresBehindActiveGatewayWithoutInterruptingItsOwner(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.Deadlines.PollInterval = 5 * time.Millisecond
	gateway := fake.New(fake.Options{})
	cfg.TransportFactory = gateway.Factory
	s := startAPI(t, cfg, gateway)
	client := apiClient(t, s)
	configureAPI(t, client, "gateway")
	_, err := client.CreateRun(context.Background(), &pb.CreateRunRequest{Command: meta("active"), Id: "active", ChannelId: "gateway", Executor: "fake"})
	require.NoError(t, err)
	active := apiCall(t, gateway)
	start := time.Now().Add(100 * time.Millisecond)
	schedule := apiSchedule("stale-poll", start)
	schedule.Definition.StartWindow = durationpb.New(200 * time.Millisecond)
	var receipt pb.ScheduleResponse
	httpProto(t, s, "PUT", "/api/v1/schedules/stale-poll", schedule, &receipt)
	require.True(t, proto.Equal(schedule.Definition.StartWindow, receipt.Schedule.Definition.StartWindow))
	id := db.OccurrenceRunID("stale-poll", 1, start.UTC())
	var expired *pb.Run
	require.Eventually(t, func() bool {
		response, err := client.GetRun(context.Background(), &pb.GetRunRequest{Id: id})
		if err != nil || response.Run.State != "expired" {
			return false
		}
		expired = response.Run
		return true
	}, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, start.UTC().Add(200*time.Millisecond), expired.StartDeadline.AsTime())
	require.Zero(t, expired.AttemptCount)
	require.NoError(t, active.Context.Err())
	channel, err := client.GetChannel(context.Background(), &pb.GetChannelRequest{Id: "gateway"})
	require.NoError(t, err)
	require.Equal(t, active.Request.AttemptID, channel.Channel.ActiveAttemptId)
	require.Equal(t, 1, gateway.Stats().Started)
	active.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded}}
	apiPhase(t, active.RecoveryStarted)
	active.Recover <- nil
	apiPhase(t, active.CloseStarted)
	active.Close <- nil
	require.Eventually(t, func() bool { return s.dispatcher.Pending() == 0 }, 3*time.Second, time.Millisecond)
	require.Equal(t, 1, gateway.Stats().Started)
}

func TestPublicDeadlinesValidateAndAlreadyElapsedSubmissionIsTerminal(t *testing.T) {
	s := startAPI(t, apiTestConfig(t.TempDir()))
	client := apiClient(t, s)
	configureAPI(t, client, "gateway")
	for _, timestamp := range []*timestamppb.Timestamp{timestamppb.New(time.Time{}), {Seconds: 253402300800}, {Nanos: -1}} {
		_, err := client.CreateRun(context.Background(), &pb.CreateRunRequest{Command: meta("invalid-deadline"), Id: "invalid", ChannelId: "gateway", Executor: "fake", StartDeadline: timestamp})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	for _, window := range []*durationpb.Duration{durationpb.New(-time.Second), {Seconds: 1_000_000_000_000}} {
		request := apiSchedule("invalid-window", time.Now().UTC())
		request.Definition.StartWindow = window
		_, err := client.PutSchedule(context.Background(), request)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	response, err := client.CreateRun(context.Background(), &pb.CreateRunRequest{Command: meta("already-expired"), Id: "already-expired", ChannelId: "gateway", Executor: "fake",
		StartDeadline: timestamppb.New(time.Now().Add(-time.Hour))})
	require.NoError(t, err)
	require.Equal(t, "expired", response.Run.State)
	require.Zero(t, response.Run.AttemptCount)
}
