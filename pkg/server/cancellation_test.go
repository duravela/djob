package server

import (
	"context"
	"strings"
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
)

func TestPublicQueuedCancellationSurvivesRestartAndExactRetry(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	s := startAPI(t, cfg)
	client := apiClient(t, s)
	configureAPI(t, client, "gateway")
	_, err := client.CreateRun(context.Background(), &pb.CreateRunRequest{Command: meta("create"), Id: "run/with/slash", ChannelId: "gateway", Executor: "fake"})
	require.NoError(t, err)
	request := &pb.CancelRunRequest{Command: meta("cancel"), Id: "run/with/slash", Reason: "operator cancelled queued work"}
	var receipt pb.RunResponse
	httpProto(t, s, "POST", "/api/v1/runs/run/with/slash/cancel", request, &receipt)
	require.Equal(t, "cancelled", receipt.Run.State)
	require.True(t, receipt.Run.CancelRequested)
	require.Zero(t, receipt.Run.AttemptCount)
	_, err = client.CancelRun(context.Background(), &pb.CancelRunRequest{Command: meta("missing"), Id: "missing"})
	require.Equal(t, codes.NotFound, status.Code(err))
	_, err = client.CancelRun(context.Background(), &pb.CancelRunRequest{Command: meta("reason-too-long"), Id: request.Id, Reason: strings.Repeat("x", 4097)})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.NoError(t, s.Close(context.Background()))
	gateway := fake.New(fake.Options{})
	cfg.TransportFactory = gateway.Factory
	restarted := startAPI(t, cfg, gateway)
	client = apiClient(t, restarted)
	retried, err := client.CancelRun(context.Background(), request)
	require.NoError(t, err)
	require.True(t, proto.Equal(&receipt, retried))
	changed := proto.Clone(request).(*pb.CancelRunRequest)
	changed.Reason = "different"
	_, err = client.CancelRun(context.Background(), changed)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	got, err := client.GetRun(context.Background(), &pb.GetRunRequest{Id: request.Id})
	require.NoError(t, err)
	require.True(t, proto.Equal(&receipt, got))
	count, err := restarted.application.Store.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.Zero(t, count)
	_, err = client.CreateRun(context.Background(), &pb.CreateRunRequest{Command: meta("later"), Id: "later", ChannelId: "gateway", Executor: "fake"})
	require.NoError(t, err)
	call := apiCall(t, gateway)
	require.Equal(t, "later", call.Request.RunID)
	call.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded}}
	apiPhase(t, call.RecoveryStarted)
	call.Recover <- nil
	apiPhase(t, call.CloseStarted)
	call.Close <- nil
	require.Eventually(t, func() bool { return restarted.dispatcher.Pending() == 0 }, 3*time.Second, time.Millisecond)
	require.Equal(t, 1, gateway.Stats().Started)
}

func TestPublicActiveCancellationIsIntentUntilRecoveryAndPreservesUnknownEffect(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	gateway := fake.New(fake.Options{})
	cfg.TransportFactory = gateway.Factory
	s := startAPI(t, cfg, gateway)
	client := apiClient(t, s)
	configureAPI(t, client, "gateway")
	_, err := client.CreateRun(context.Background(), &pb.CreateRunRequest{Command: meta("active"), Id: "active", ChannelId: "gateway", Executor: "fake"})
	require.NoError(t, err)
	call := apiCall(t, gateway)
	_, err = client.CreateRun(context.Background(), &pb.CreateRunRequest{Command: meta("next"), Id: "next", ChannelId: "gateway", Executor: "fake"})
	require.NoError(t, err)
	request := &pb.CancelRunRequest{Command: meta("cancel-active"), Id: "active", Reason: "stop active operation"}
	response, err := client.CancelRun(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, "running", response.Run.State)
	require.True(t, response.Run.CancelRequested)
	apiPhase(t, call.Context.Done())
	apiPhase(t, call.RecoveryStarted)
	require.NoError(t, call.RecoveryContext.Err())
	channel, err := client.GetChannel(context.Background(), &pb.GetChannelRequest{Id: "gateway"})
	require.NoError(t, err)
	require.Equal(t, call.Request.AttemptID, channel.Channel.ActiveAttemptId)
	next, err := client.GetRun(context.Background(), &pb.GetRunRequest{Id: "next"})
	require.NoError(t, err)
	require.Equal(t, "ready", next.Run.State)
	require.Zero(t, next.Run.AttemptCount)
	call.Recover <- nil
	apiPhase(t, call.CloseStarted)
	require.Equal(t, 1, gateway.Stats().Started)
	call.Close <- nil
	second := apiCall(t, gateway)
	require.Equal(t, "next", second.Request.RunID)
	second.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded}}
	apiPhase(t, second.RecoveryStarted)
	second.Recover <- nil
	apiPhase(t, second.CloseStarted)
	second.Close <- nil
	require.Eventually(t, func() bool { return s.dispatcher.Pending() == 0 }, 3*time.Second, time.Millisecond)
	final, err := client.GetRun(context.Background(), &pb.GetRunRequest{Id: "active"})
	require.NoError(t, err)
	require.Equal(t, "unknown", final.Run.State)
	require.True(t, final.Run.CancelRequested)
	attempt, err := client.GetAttempt(context.Background(), &pb.GetAttemptRequest{Id: final.Run.LastAttemptId})
	require.NoError(t, err)
	require.NotNil(t, attempt.Attempt.AuthorizedAt)
	require.False(t, attempt.Attempt.NoDelivery)
	require.True(t, attempt.Attempt.Quiescent)
	var retried pb.RunResponse
	httpProto(t, s, "POST", "/scheduler.Scheduler/CancelRun", request, &retried)
	require.True(t, proto.Equal(response, &retried), "retry retains the original acceptance receipt rather than claiming the operation was undone")
	require.Equal(t, 1, gateway.Stats().MaxActive)
}
