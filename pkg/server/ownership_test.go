package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node"
	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestControllerAutomaticallyBlocksRestoredOwnersWithoutGatewayWork(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.Ownership.PollInterval, cfg.Ownership.BatchSize = 5*time.Millisecond, 2
	cfg.Raft.ID = *cfg.NodeID
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	core, err := node.OpenApplication(ctx, cfg.Raft)
	require.NoError(t, err)
	t.Cleanup(core.Close)
	_, err = core.Raft.WaitForLeaderContext(ctx)
	require.NoError(t, err)
	base := time.Now().UTC().Add(-time.Minute)
	oldLease := base.Add(20 * time.Second)
	var startCommand db.StateCommand
	var original db.StateResult
	for _, id := range []string{"a-idle", "b-active", "c-draining", "d-live", "e-available"} {
		commands := []db.StateCommand{{Configure: &db.ConfigureChannel{ID: id, GatewayID: id, Protocol: "fake", Endpoint: id}}}
		lease := oldLease
		if id == "d-live" {
			lease = time.Now().UTC().Add(time.Hour)
		}
		owner := db.ChannelOwner{ChannelID: id, OwnerID: "old/process", Epoch: 1}
		if id != "e-available" {
			commands = append(commands, db.StateCommand{Acquire: &db.AcquireChannel{ChannelID: id, OwnerID: owner.OwnerID, LeaseUntil: lease}})
		}
		if id == "b-active" {
			commands = append(commands,
				db.StateCommand{EnqueueRun: &db.CreateRun{ID: "run", ChannelID: id, Executor: "fake"}},
				db.StateCommand{Claim: &db.ClaimRun{ChannelOwner: owner, RunID: "run", AttemptID: "first", ClaimToken: "token"}},
				db.StateCommand{StartAttempt: &db.StartAttempt{ChannelOwner: owner, RunID: "run", AttemptID: "first", ClaimToken: "token", DeadlineAware: true, RetryAware: true}},
				db.StateCommand{EnqueueRun: &db.CreateRun{ID: "queued", ChannelID: id, Executor: "fake"}})
		}
		if id == "c-draining" {
			commands = append(commands, db.StateCommand{Drain: &owner})
		}
		for i, command := range commands {
			command.RequestID, command.At = fmt.Sprintf("seed/%s/%d", id, i), base.Add(time.Duration(i)*time.Millisecond)
			result, err := core.Store.ProposeStateCommand(ctx, command)
			require.NoError(t, err)
			if command.StartAttempt != nil {
				startCommand, original = command, result
			}
		}
	}
	core.Close()
	s := startAPI(t, cfg)
	require.Nil(t, s.dispatcher, "controller recovery must work without a transport or dispatcher")
	client := apiClient(t, s)
	require.Eventually(t, func() bool {
		for _, id := range []string{"a-idle", "b-active", "c-draining"} {
			response, err := client.GetChannel(context.Background(), &pb.GetChannelRequest{Id: id})
			if err != nil || response.Channel.State != "blocked" || response.Channel.OwnerId != "old/process" || response.Channel.Epoch != 1 {
				return false
			}
		}
		return true
	}, 3*time.Second, 5*time.Millisecond)
	for id, state := range map[string]string{"d-live": "owned", "e-available": "available"} {
		response, err := client.GetChannel(context.Background(), &pb.GetChannelRequest{Id: id})
		require.NoError(t, err)
		require.Equal(t, state, response.Channel.State)
	}
	run, err := client.GetRun(context.Background(), &pb.GetRunRequest{Id: "run"})
	require.NoError(t, err)
	require.Equal(t, "unknown", run.Run.State)
	require.EqualValues(t, 1, run.Run.AttemptCount)
	attempt, err := client.GetAttempt(context.Background(), &pb.GetAttemptRequest{Id: "first"})
	require.NoError(t, err)
	require.Equal(t, "unknown", attempt.Attempt.Outcome)
	require.False(t, attempt.Attempt.Quiescent)
	require.False(t, attempt.Attempt.FinishedAt.AsTime().Before(oldLease))
	channel, err := client.GetChannel(context.Background(), &pb.GetChannelRequest{Id: "b-active"})
	require.NoError(t, err)
	require.Equal(t, "first", channel.Channel.ActiveAttemptId)
	queued, err := client.GetRun(context.Background(), &pb.GetRunRequest{Id: "queued"})
	require.NoError(t, err)
	require.Equal(t, "ready", queued.Run.State)
	require.Zero(t, queued.Run.AttemptCount)
	replayed, err := s.application.Store.ProposeStateCommand(context.Background(), startCommand)
	require.NoError(t, err)
	require.Equal(t, original, replayed)
	_, err = client.ResolveChannel(context.Background(), &pb.ResolveChannelRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err), "automatic expiry must not enable unauthenticated recovery")
	require.NoError(t, s.Close(context.Background()))
	s = startAPI(t, cfg)
	client = apiClient(t, s)
	restored, err := client.GetAttempt(context.Background(), &pb.GetAttemptRequest{Id: "first"})
	require.NoError(t, err)
	require.True(t, proto.Equal(attempt, restored))
	channel, err = client.GetChannel(context.Background(), &pb.GetChannelRequest{Id: "b-active"})
	require.NoError(t, err)
	require.Equal(t, "blocked", channel.Channel.State)
	require.Equal(t, "first", channel.Channel.ActiveAttemptId)
}

func TestOwnershipSweepConfigurationFailureReleasesServerResources(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.Ownership.BatchSize = -1
	s, err := Start(context.Background(), cfg)
	require.ErrorContains(t, err, "ownership sweeper")
	require.Nil(t, s)
	cfg.Ownership.BatchSize = 2
	s = startAPI(t, cfg)
	require.NotEmpty(t, s.HTTPAddr())
}
