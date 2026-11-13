package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	consoleapp "github.com/duravela/djob/internal/console"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Model a process that disappeared after committed execution authorization.
// No transport is used; the public recovery path must preserve the uncertainty.
func seedCrashedAPIAttempt(t *testing.T, s *Server, gateway, run string) (db.ChannelOwner, time.Time) {
	t.Helper()
	base := time.Now().UTC().Add(-time.Minute)
	owner := db.ChannelOwner{ChannelID: gateway, Epoch: 1, OwnerID: "lost/process"}
	lease := base.Add(20 * time.Second)
	commands := []db.StateCommand{
		{Configure: &db.ConfigureChannel{ID: gateway, GatewayID: gateway, Protocol: "fake", Endpoint: gateway}},
		{EnqueueRun: &db.CreateRun{ID: run, ChannelID: gateway, Executor: "fake"}},
		{Acquire: &db.AcquireChannel{ChannelID: gateway, OwnerID: owner.OwnerID, LeaseUntil: lease}},
		{Claim: &db.ClaimRun{ChannelOwner: owner, RunID: run, AttemptID: run + "/first", ClaimToken: "old-capability"}},
		{StartAttempt: &db.StartAttempt{ChannelOwner: owner, RunID: run, AttemptID: run + "/first", ClaimToken: "old-capability", DeadlineAware: true, RetryAware: true}},
	}
	for i, c := range commands {
		c.RequestID, c.At = fmt.Sprintf("seed/%s/%d", run, i), base.Add(time.Duration(i)*time.Millisecond)
		_, err := s.application.Store.ProposeStateCommand(context.Background(), c)
		require.NoError(t, err)
	}
	return owner, lease
}

func TestPublicOperatorRecoveryKeepsAccessAndOutcomeSeparateAcrossRestart(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.Ownership.PollInterval = time.Hour // Exercise explicit operator expiry without the automatic scan racing it.
	cfg.APIAuth = apiCredentials()
	s := startAPI(t, cfg)
	c := apiClient(t, s)
	owner, lease := seedCrashedAPIAttempt(t, s, "gateway", "uncertain/operation")
	operator := apiAuthContext(apiCredentials().Credentials[2].Token)
	reader := apiAuthContext(apiCredentials().Credentials[0].Token)
	writer := apiAuthContext(apiCredentials().Credentials[1].Token)
	before := &pb.ExpireChannelRequest{Command: &pb.CommandMetadata{RequestId: "too-soon", At: timestamppb.New(lease.Add(-time.Second))}, Id: "gateway", ExpectedEpoch: 1}
	_, err := c.ExpireChannel(operator, before)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	expire := &pb.ExpireChannelRequest{Command: meta("expire-owner"), Id: "gateway", ExpectedEpoch: 1}
	_, err = c.ExpireChannel(writer, expire)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	expired, err := c.ExpireChannel(operator, expire)
	require.NoError(t, err)
	require.Equal(t, "blocked", expired.Channel.State)
	require.Equal(t, "uncertain/operation/first", expired.Channel.ActiveAttemptId)
	unknown, err := c.GetRun(reader, &pb.GetRunRequest{Id: "uncertain/operation"})
	require.NoError(t, err)
	require.Equal(t, "unknown", unknown.Run.State)
	first, err := c.GetAttempt(reader, &pb.GetAttemptRequest{Id: unknown.Run.LastAttemptId})
	require.NoError(t, err)
	require.Equal(t, "unknown", first.Attempt.Outcome)
	require.False(t, first.Attempt.Quiescent)
	_, err = c.ResolveRun(operator, &pb.ResolveRunRequest{Command: meta("retry-before-access-recovery"), Id: unknown.Run.Id, AttemptId: first.Attempt.Id, State: "ready", Reason: "repeat read"})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	resolveChannel := &pb.ResolveChannelRequest{Command: meta("recovery/channel"), Id: "gateway", ExpectedEpoch: 1, Reason: "old process terminated, session revoked, bus verified idle", Quiescent: true}
	unsafe := proto.Clone(resolveChannel).(*pb.ResolveChannelRequest)
	unsafe.Command, unsafe.Quiescent = meta("missing-physical-proof"), false
	_, err = c.ResolveChannel(operator, unsafe)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	stale := proto.Clone(resolveChannel).(*pb.ResolveChannelRequest)
	stale.Command, stale.ExpectedEpoch = meta("wrong-epoch"), 2
	_, err = c.ResolveChannel(operator, stale)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	var channelResolution pb.ChannelResponse
	authenticatedHTTP(t, http.DefaultClient, "http://"+s.HTTPAddr(), http.MethodPost, "/api/v1/channels/gateway/resolve", apiCredentials().Credentials[2].Token,
		resolveChannel, &channelResolution, 200, http.Header{"X-Actor": {"forged-admin"}, "Grpc-Metadata-Actor": {"forged-admin"}})
	require.Equal(t, "available", channelResolution.Channel.State)
	require.Empty(t, channelResolution.Channel.OwnerId)
	stillUnknown, err := c.GetRun(reader, &pb.GetRunRequest{Id: unknown.Run.Id})
	require.NoError(t, err)
	require.Equal(t, "unknown", stillUnknown.Run.State, "physical access recovery must not replay the old operation")
	channelAudit, err := c.GetResolution(reader, &pb.GetResolutionRequest{RequestId: resolveChannel.Command.RequestId})
	require.NoError(t, err)
	require.Equal(t, "ops/alice", channelAudit.Resolution.Actor)
	require.Equal(t, first.Attempt.Id, channelAudit.Resolution.AttemptId)
	require.True(t, channelAudit.Resolution.Quiescent)
	_, err = c.GetResolution(reader, &pb.GetResolutionRequest{RequestId: unsafe.Command.RequestId})
	require.Equal(t, codes.NotFound, status.Code(err), "a rejected resolution must not fabricate audit evidence")
	_, err = s.application.Store.ProposeStateCommand(context.Background(), db.StateCommand{RequestID: "new-owner", At: time.Now().UTC(), Acquire: &db.AcquireChannel{
		ChannelID: "gateway", ExpectedEpoch: 1, OwnerID: "replacement/process", LeaseUntil: time.Now().UTC().Add(time.Minute)}})
	require.NoError(t, err)
	// The original receipt remains available after the gateway changes again.
	repeated, err := c.ResolveChannel(operator, resolveChannel)
	require.NoError(t, err)
	require.True(t, proto.Equal(&channelResolution, repeated))
	_, err = c.ResolveChannel(apiAuthContext(apiCredentials().Credentials[4].Token), resolveChannel)
	require.Equal(t, codes.AlreadyExists, status.Code(err), "another operator cannot reuse the original identity-bearing request")
	_, err = s.application.Store.ProposeStateCommand(context.Background(), db.StateCommand{RequestID: "stale-completion", At: time.Now().UTC(), Finish: &db.FinishAttempt{
		ChannelOwner: owner, RunID: unknown.Run.Id, AttemptID: first.Attempt.Id, ClaimToken: "old-capability", Outcome: db.AttemptSucceeded, Quiescent: true,
		CancellationAware: true, DeadlineAware: true, RetryAware: true}})
	require.Error(t, err, "old capabilities cannot complete work after physical resolution")
	resolveRun := &pb.ResolveRunRequest{Command: meta("recovery/run"), Id: unknown.Run.Id, AttemptId: first.Attempt.Id, State: "ready", Reason: "reconciliation confirms read may repeat"}
	var runResolution pb.RunResponse
	authenticatedHTTP(t, http.DefaultClient, "http://"+s.HTTPAddr(), http.MethodPost, "/scheduler.Scheduler/ResolveRun", apiCredentials().Credentials[2].Token,
		resolveRun, &runResolution, 200, nil)
	require.Equal(t, "ready", runResolution.Run.State)
	require.EqualValues(t, 1, runResolution.Run.AttemptCount)
	unchanged, err := c.GetAttempt(reader, &pb.GetAttemptRequest{Id: first.Attempt.Id})
	require.NoError(t, err)
	require.True(t, proto.Equal(first, unchanged), "resolution must preserve the original uncertain attempt")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	require.NoError(t, s.Close(ctx))
	cancel()
	// Rotate the secret, retaining the same stable audit subject.
	cfg.APIAuth = &APIAuthConfig{Credentials: []APICredential{apiCredentials().Credentials[0], apiCredentials().Credentials[3]}}
	s = startAPI(t, cfg)
	c = apiClient(t, s)
	_, err = c.GetRun(operator, &pb.GetRunRequest{Id: unknown.Run.Id})
	require.Equal(t, codes.Unauthenticated, status.Code(err), "the removed token is no longer accepted")
	retriedRun, err := c.ResolveRun(apiAuthContext(apiCredentials().Credentials[3].Token), resolveRun)
	require.NoError(t, err)
	require.True(t, proto.Equal(&runResolution, retriedRun))
	var restoredAudit pb.ResolutionResponse
	authenticatedHTTP(t, http.DefaultClient, "http://"+s.HTTPAddr(), http.MethodGet, "/api/v1/resolutions/recovery/channel", apiCredentials().Credentials[0].Token,
		nil, &restoredAudit, 200, nil)
	require.True(t, proto.Equal(channelAudit, &restoredAudit))
	runAudit, err := c.GetResolution(reader, &pb.GetResolutionRequest{RequestId: resolveRun.Command.RequestId})
	require.NoError(t, err)
	require.Equal(t, "ops/alice", runAudit.Resolution.Actor)
	require.Equal(t, "ready", runAudit.Resolution.RunState)
	result, err := s.application.Store.ProposeStateCommand(context.Background(), db.StateCommand{RequestID: "fresh-attempt", At: time.Now().UTC(), Claim: &db.ClaimRun{
		ChannelOwner: db.ChannelOwner{ChannelID: "gateway", Epoch: 2, OwnerID: "replacement/process"}, RunID: unknown.Run.Id, AttemptID: "second", ClaimToken: "fresh-capability"}})
	require.NoError(t, err)
	require.EqualValues(t, 2, result.Attempt.Number)
	unchanged, err = c.GetAttempt(reader, &pb.GetAttemptRequest{Id: first.Attempt.Id})
	require.NoError(t, err)
	require.True(t, proto.Equal(first, unchanged))
}

func TestOperatorFinalOutcomeDoesNotClearBlockedGateway(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.Ownership.PollInterval = time.Hour
	cfg.APIAuth = apiCredentials()
	s := startAPI(t, cfg)
	c := apiClient(t, s)
	operator := apiAuthContext(apiCredentials().Credentials[2].Token)
	for _, state := range []string{"succeeded", "failed"} {
		t.Run(state, func(t *testing.T) {
			seedCrashedAPIAttempt(t, s, state, state)
			_, err := c.ExpireChannel(operator, &pb.ExpireChannelRequest{Command: meta("expire/" + state), Id: state, ExpectedEpoch: 1})
			require.NoError(t, err)
			_, err = c.ResolveRun(operator, &pb.ResolveRunRequest{Command: meta("resolve/" + state), Id: state, AttemptId: state + "/first", State: state, Reason: "device inspected and outcome reconciled"})
			require.NoError(t, err)
			channel, err := c.GetChannel(operator, &pb.GetChannelRequest{Id: state})
			require.NoError(t, err)
			require.Equal(t, "blocked", channel.Channel.State)
			attempt, err := c.GetAttempt(operator, &pb.GetAttemptRequest{Id: state + "/first"})
			require.NoError(t, err)
			require.Equal(t, "unknown", attempt.Attempt.Outcome)
		})
	}
}

func TestConsoleGatewayRecoveryAgainstCommittedAuthority(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = apiCredentials()
	cfg.Ownership.PollInterval = time.Hour
	s := startAPI(t, cfg)
	seedCrashedAPIAttempt(t, s, "console-gateway", "console-unknown")
	const origin = "http://127.0.0.1:8899"
	app, err := consoleapp.New(consoleapp.Config{Origin: origin, Controllers: []string{"http://" + s.HTTPAddr()}})
	require.NoError(t, err)
	defer app.Close()
	var cookie *http.Cookie
	var csrf string
	call := func(path string, body any) map[string]any {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		method := http.MethodPost
		if path == "mutations/current" {
			method = http.MethodDelete
		}
		r := httptest.NewRequest(method, origin+"/api/console/v1/"+path, bytes.NewReader(raw))
		r.Header.Set("Origin", origin)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Djob-CSRF", csrf)
		if cookie != nil {
			r.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		app.ServeHTTP(w, r)
		require.Equal(t, 200, w.Code, w.Body.String())
		if cookie == nil {
			cookie = w.Result().Cookies()[0]
		}
		var out map[string]any
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
		return out
	}
	login := call("session", map[string]string{"token": apiCredentials().Credentials[2].Token})
	csrf = login["csrf"].(string)
	channel := call("gateways/get", map[string]string{"id": "console-gateway"})["channel"].(map[string]any)
	require.Equal(t, "owned", channel["state"])
	for _, op := range []string{"expire", "resolve"} {
		pending := call("mutations/gateway", map[string]any{"operation": op, "id": "console-gateway", "expected_epoch": "1", "reason": "old worker excluded and device verified idle", "quiescent": op == "resolve"})
		id := pending["request_id"].(string)
		ack := call("mutations/commit", map[string]string{"request_id": id})
		require.Equal(t, "confirmed", ack["state"])
		if op == "resolve" {
			audit := call("gateways/audit", map[string]string{"id": id})["resolution"].(map[string]any)
			require.Equal(t, "ops/alice", audit["actor"])
			require.Equal(t, true, audit["quiescent"])
		}
		call("mutations/current", map[string]string{"request_id": id})
	}
	channel = call("gateways/get", map[string]string{"id": "console-gateway"})["channel"].(map[string]any)
	require.Equal(t, "available", channel["state"])
	run, err := apiClient(t, s).GetRun(apiAuthContext(apiCredentials().Credentials[0].Token), &pb.GetRunRequest{Id: "console-unknown"})
	require.NoError(t, err)
	require.Equal(t, "unknown", run.Run.State)
}
