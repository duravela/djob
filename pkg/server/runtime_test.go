package server

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/fake"
	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func apiTestConfig(dir string) Config {
	cfg := Config{BaseConfig: MakeBaseConfig()}
	cfg.Admission.Disabled = true // Fixtures also exercise intentionally synthetic command times.
	id := uint64(1)
	cfg.NodeID = &id
	cfg.Addr, cfg.HTTPAddr = "127.0.0.1:0", "127.0.0.1:0"
	cfg.Raft.SetDefaults()
	cfg.Raft.DataDir, cfg.Raft.Peers, cfg.Raft.Logger = dir, []string{"http://127.0.0.1:0"}, zap.NewNop()
	cfg.Raft.BatchTimeout, cfg.Raft.RaftEventLoopHeartbeatInterval = time.Millisecond, 10*time.Millisecond
	cfg.Raft.SnapshotCount, cfg.Raft.CatchUpEntriesN = 2, 1
	cfg.Materializer.PollInterval, cfg.Dispatcher.PollInterval = 5*time.Millisecond, 5*time.Millisecond
	cfg.Dispatcher.Executor = executor.Config{LeaseDuration: 5 * time.Second, LeaseSafetyMargin: time.Second, RenewInterval: time.Second,
		CommitTimeout: 200 * time.Millisecond, OperationTimeout: 3 * time.Second, RecoveryTimeout: 2 * time.Second, CloseTimeout: 2 * time.Second}
	return cfg
}

func startAPI(t *testing.T, cfg Config, gateways ...*fake.Gateway) *Server {
	t.Helper()
	s, err := Start(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		stop := make(chan struct{})
		go func() {
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for {
				for _, g := range gateways {
					g.ReleaseAll()
				}
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
			}
		}()
		defer close(stop)
		// HTTP and gRPC each receive the configured shutdown grace. A caller
		// deadline shorter than that can race a valid five-second HTTP idle wait.
		ctx, cancel := context.WithTimeout(context.Background(), 2*s.cfg.ShutdownTimeout+5*time.Second)
		defer cancel()
		require.NoError(t, s.Close(ctx))
	})
	return s
}

func apiClient(t *testing.T, s *Server) pb.SchedulerClient {
	t.Helper()
	conn, err := grpc.NewClient(s.RPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewSchedulerClient(conn)
}

func meta(id string) *pb.CommandMetadata {
	return &pb.CommandMetadata{RequestId: id, At: timestamppb.Now()}
}
func configureAPI(t *testing.T, c pb.SchedulerClient, id string) {
	t.Helper()
	_, err := c.ConfigureChannel(context.Background(), &pb.ConfigureChannelRequest{Command: meta("configure/" + id), Id: id, GatewayId: id, Protocol: "fake", Endpoint: id + ":502"})
	require.NoError(t, err)
}
func apiSchedule(id string, start time.Time) *pb.PutScheduleRequest {
	return &pb.PutScheduleRequest{Command: meta("schedule/" + id), Definition: &pb.ScheduleDefinition{Id: id, ChannelId: "gateway", Executor: "fake", Payload: []byte{0, 0xff},
		Spec: &pb.ScheduleSpec{Kind: "once", StartAt: timestamppb.New(start)}, MissedPolicy: "coalesce"}}
}
func httpProto(t *testing.T, s *Server, method, path string, input, output proto.Message) {
	t.Helper()
	var data []byte
	var err error
	if input != nil {
		data, err = (protojson.MarshalOptions{UseProtoNames: true}).Marshal(input)
		require.NoError(t, err)
	}
	req, err := http.NewRequest(method, "http://"+s.HTTPAddr()+path, bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 4 * time.Second}).Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	data, err = io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode, string(data))
	if output != nil {
		require.NoError(t, protojson.Unmarshal(data, output), string(data))
	}
}
func apiCall(t *testing.T, g *fake.Gateway) *fake.Call {
	t.Helper()
	select {
	case c := <-g.Calls:
		return c
	case <-time.After(4 * time.Second):
		t.Fatal("scheduled exchange did not start")
		return nil
	}
}
func apiPhase(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("gateway phase did not start")
	}
}

func TestPublicGRPCResourcesRetryRevisionsAndPagination(t *testing.T) {
	s := startAPI(t, apiTestConfig(t.TempDir()))
	c := apiClient(t, s)
	for _, id := range []string{"gateway", "other", "third"} {
		configureAPI(t, c, id)
	}
	first, err := c.ListChannels(context.Background(), &pb.ListChannelsRequest{PageSize: 1})
	require.NoError(t, err)
	require.Len(t, first.Channels, 1)
	require.Equal(t, "gateway", first.Channels[0].Id)
	require.NotEmpty(t, first.NextPageToken)
	last, err := c.ListChannels(context.Background(), &pb.ListChannelsRequest{PageSize: 2, PageToken: first.NextPageToken})
	require.NoError(t, err)
	require.Len(t, last.Channels, 2)
	require.Empty(t, last.NextPageToken)
	request := apiSchedule("poll", time.Now().Add(time.Hour))
	created, err := c.PutSchedule(context.Background(), request)
	require.NoError(t, err)
	require.EqualValues(t, 1, created.Schedule.Revision)
	paused, err := c.PauseSchedule(context.Background(), &pb.PauseScheduleRequest{Command: meta("pause"), Id: "poll", ExpectedRevision: 1, Paused: true})
	require.NoError(t, err)
	require.EqualValues(t, 2, paused.Schedule.Revision)
	retried, err := c.PutSchedule(context.Background(), request)
	require.NoError(t, err)
	require.True(t, proto.Equal(created, retried))
	changed := proto.Clone(request).(*pb.PutScheduleRequest)
	changed.Definition.Payload = []byte("changed")
	_, err = c.PutSchedule(context.Background(), changed)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	changed.Command = meta("replacement")
	changed.ExpectedRevision = 1
	_, err = c.PutSchedule(context.Background(), changed)
	require.Equal(t, codes.Aborted, status.Code(err))
	got, err := c.GetSchedule(context.Background(), &pb.GetScheduleRequest{Id: "poll"})
	require.NoError(t, err)
	require.True(t, got.Schedule.Definition.Paused)
	for _, id := range []string{"a", "z"} {
		_, err = c.PutSchedule(context.Background(), apiSchedule(id, time.Now().Add(time.Hour)))
		require.NoError(t, err)
	}
	page, err := c.ListSchedules(context.Background(), &pb.ListSchedulesRequest{PageSize: 2})
	require.NoError(t, err)
	require.Len(t, page.Schedules, 2)
	end, err := c.ListSchedules(context.Background(), &pb.ListSchedulesRequest{PageSize: 2, PageToken: page.NextPageToken})
	require.NoError(t, err)
	require.Len(t, end.Schedules, 1)
	require.Empty(t, end.NextPageToken)
	_, err = c.ListSchedules(context.Background(), &pb.ListSchedulesRequest{PageToken: first.NextPageToken})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = c.GetRun(context.Background(), &pb.GetRunRequest{Id: "missing"})
	require.Equal(t, codes.NotFound, status.Code(err))
	_, err = c.GetChannel(context.Background(), &pb.GetChannelRequest{Id: "gateway"})
	require.NoError(t, err)
	jobs, err := c.ListJobs(context.Background(), &pb.ListJobsRequest{})
	require.NoError(t, err)
	require.Len(t, jobs.Jobs, 3)
	job, err := c.GetJob(context.Background(), &pb.GetJobRequest{Name: "poll"})
	require.NoError(t, err)
	require.True(t, job.Job.Definition.Paused)
	_, err = c.DeleteJob(context.Background(), &pb.DeleteJobRequest{Name: "poll"})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestPublicAPIRejectsAmbiguousAndUnsupportedInputs(t *testing.T) {
	s := startAPI(t, apiTestConfig(t.TempDir()))
	c := apiClient(t, s)
	configureAPI(t, c, "gateway")
	service := NewSchedulerService(s.application.Store)
	tests := map[string]func() error{
		"nil request": func() error { _, err := service.ConfigureChannel(context.Background(), nil); return err },
		"missing metadata": func() error {
			_, err := c.CreateRun(context.Background(), &pb.CreateRunRequest{Id: "run", ChannelId: "gateway", Executor: "fake"})
			return err
		},
		"invalid timestamp": func() error {
			q := apiSchedule("invalid-time", time.Now())
			q.Command.At = &timestamppb.Timestamp{Seconds: 1 << 62}
			_, err := c.PutSchedule(context.Background(), q)
			return err
		},
		"gateway alias": func() error {
			_, err := c.ConfigureChannel(context.Background(), &pb.ConfigureChannelRequest{Command: meta("alias"), Id: "slave-3", GatewayId: "gateway", Protocol: "fake", Endpoint: "gateway"})
			return err
		},
		"large payload": func() error {
			_, err := c.CreateRun(context.Background(), &pb.CreateRunRequest{Command: meta("large"), Id: "large", ChannelId: "gateway", Executor: "fake", Payload: make([]byte, (64<<10)+1)})
			return err
		},
		"overflowing duration": func() error {
			q := apiSchedule("duration", time.Now())
			q.Definition.Spec.Kind = "interval"
			q.Definition.Spec.Interval = &durationpb.Duration{Seconds: 1 << 40}
			_, err := c.PutSchedule(context.Background(), q)
			return err
		},
		"local cron zone": func() error {
			q := apiSchedule("cron", time.Now())
			q.Definition.Spec.Kind = "cron"
			q.Definition.Spec.Cron = "* * * * *"
			q.Definition.Spec.TimeZone = "Local"
			_, err := c.PutSchedule(context.Background(), q)
			return err
		},
		"reserved run namespace": func() error {
			_, err := c.CreateRun(context.Background(), &pb.CreateRunRequest{Command: meta("reserved"), Id: "scheduled/forged", ChannelId: "gateway", Executor: "fake"})
			return err
		},
		"oversized page": func() error {
			_, err := c.ListRuns(context.Background(), &pb.ListRunsRequest{PageSize: 33})
			return err
		},
		"bad cursor": func() error {
			_, err := c.ListRuns(context.Background(), &pb.ListRunsRequest{PageToken: "not-base64!"})
			return err
		},
		"legacy timing": func() error {
			_, err := c.ScheduleJob(context.Background(), &pb.ScheduleJobRequest{Name: "old", Job: &pb.Job{Schedule: proto.String("@every 1s")}})
			return err
		},
	}
	for name, run := range tests {
		t.Run(name, func(t *testing.T) { require.Equal(t, codes.InvalidArgument, status.Code(run())) })
	}
	page, err := c.ListRuns(context.Background(), &pb.ListRunsRequest{})
	require.NoError(t, err)
	require.Empty(t, page.Runs)
}

func TestHTTPAndGRPCShareDurableSchedulingExecutionAndRestart(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	gateway := fake.New(fake.Options{})
	cfg.TransportFactory = gateway.Factory
	s := startAPI(t, cfg, gateway)
	var channel pb.ChannelResponse
	httpProto(t, s, "PUT", "/api/v1/channels/gateway", &pb.ConfigureChannelRequest{Command: meta("configure"), GatewayId: "gateway", Protocol: "fake", Endpoint: "gateway:502"}, &channel)
	request := apiSchedule("one-time", time.Now().Add(-time.Minute))
	var schedule pb.ScheduleResponse
	httpProto(t, s, "PUT", "/api/v1/schedules/one-time", request, &schedule)
	first := apiCall(t, gateway)
	require.Equal(t, db.OccurrenceRunID("one-time", 1, request.Definition.Spec.StartAt.AsTime()), first.Request.RunID)
	var immediate pb.RunResponse
	httpProto(t, s, "POST", "/scheduler.Scheduler/CreateRun", &pb.CreateRunRequest{Command: meta("immediate"), Id: "slave-7", ChannelId: "gateway", Executor: "fake", Payload: []byte{7}}, &immediate)
	first.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded, Data: []byte{0, 0xff, 0x10}}}
	apiPhase(t, first.RecoveryStarted)
	var run pb.RunResponse
	httpProto(t, s, "GET", "/api/v1/runs/slave-7", nil, &run)
	require.Equal(t, "ready", run.Run.State)
	require.Zero(t, run.Run.AttemptCount)
	first.Recover <- nil
	apiPhase(t, first.CloseStarted)
	first.Close <- nil
	second := apiCall(t, gateway)
	require.Equal(t, "slave-7", second.Request.RunID)
	second.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded}}
	apiPhase(t, second.RecoveryStarted)
	second.Recover <- nil
	apiPhase(t, second.CloseStarted)
	second.Close <- nil
	c := apiClient(t, s)
	require.Eventually(t, func() bool {
		r, err := c.GetRun(context.Background(), &pb.GetRunRequest{Id: second.Request.RunID})
		return err == nil && r.Run.State == "succeeded" && s.dispatcher.Pending() == 0
	}, 3*time.Second, 5*time.Millisecond)
	var attempt pb.AttemptResponse
	httpProto(t, s, "GET", "/api/v1/attempts/"+first.Request.AttemptID, nil, &attempt)
	require.True(t, attempt.Attempt.Quiescent)
	require.Equal(t, []byte{0, 0xff, 0x10}, attempt.Attempt.Result)
	data, err := protojson.Marshal(&attempt)
	require.NoError(t, err)
	require.NotContains(t, string(data), "claim_token")
	require.NotContains(t, string(data), "claimToken")
	httpProto(t, s, "GET", "/api/v1/runs/"+first.Request.RunID, nil, &run)
	require.Equal(t, "succeeded", run.Run.State)
	require.NoError(t, s.Close(context.Background()))
	restarted := startAPI(t, cfg, gateway)
	c = apiClient(t, restarted)
	got, err := c.GetSchedule(context.Background(), &pb.GetScheduleRequest{Id: "one-time"})
	require.NoError(t, err)
	require.True(t, got.Schedule.Completed)
	retried, err := c.PutSchedule(context.Background(), request)
	require.NoError(t, err)
	require.True(t, proto.Equal(&schedule, retried))
	page, err := c.ListRuns(context.Background(), &pb.ListRunsRequest{})
	require.NoError(t, err)
	require.Len(t, page.Runs, 2)
	for _, r := range page.Runs {
		require.Equal(t, "succeeded", r.State)
		require.EqualValues(t, 1, r.AttemptCount)
	}
	require.Equal(t, 1, gateway.Stats().MaxActive)
	require.Equal(t, 2, gateway.Stats().Started)
}

func TestHTTPRejectsBadBodiesPathMismatchAndCursor(t *testing.T) {
	s := startAPI(t, apiTestConfig(t.TempDir()))
	transport := &http.Transport{}
	client := &http.Client{Transport: transport, Timeout: 4 * time.Second}
	t.Cleanup(transport.CloseIdleConnections)
	for _, test := range []struct {
		body string
		want int
	}{
		{`{"unknown":true}`, 400}, {`{"id":"different"}`, 400}, {`{"id":`, 400}, {strings.Repeat(" ", maxAPIRequestBytes+1), 429},
	} {
		req, err := http.NewRequest("PUT", "http://"+s.HTTPAddr()+"/api/v1/channels/gateway", strings.NewReader(test.body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		require.NoError(t, err)
		data, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		require.NoError(t, err)
		require.Equal(t, test.want, response.StatusCode, string(data))
	}
	for _, path := range []string{"/api/v1/runs/?page_size=-1", "/api/v1/schedules/?page_token=bogus", "/api/v1/channels/?page_size=1&page_size=2"} {
		response, err := client.Get("http://" + s.HTTPAddr() + path)
		require.NoError(t, err)
		_ = response.Body.Close()
		require.Equal(t, 400, response.StatusCode)
	}
}

func TestServerCloseDeadlineRetainsRaftUntilUncooperativeCleanupFinishes(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	gateway := fake.New(fake.Options{IgnoreRecoveryCancellation: true})
	cfg.TransportFactory = gateway.Factory
	s := startAPI(t, cfg, gateway)
	c := apiClient(t, s)
	configureAPI(t, c, "gateway")
	for _, id := range []string{"first", "second"} {
		_, err := c.CreateRun(context.Background(), &pb.CreateRunRequest{Command: meta("run/" + id), Id: id, ChannelId: "gateway", Executor: "fake"})
		require.NoError(t, err)
	}
	call := apiCall(t, gateway)
	call.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded}}
	apiPhase(t, call.RecoveryStarted)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := s.Close(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	channel, err := s.application.Store.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, call.Request.AttemptID, channel.ActiveAttemptID)
	require.True(t, s.application.Raft.IsLeader(), "cleanup still needs consensus")
	select {
	case <-s.Done():
		t.Fatal("server detached active cleanup")
	default:
	}
	call.Recover <- nil
	apiPhase(t, call.CloseStarted)
	call.Close <- nil
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, s.Close(ctx))
	require.Equal(t, 1, gateway.Stats().MaxActive)
	require.Equal(t, 1, gateway.Stats().Started)
}

func TestServerReportsStartupFailuresAndReleasesResources(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer occupied.Close()
	for _, target := range []string{"raft", "http", "grpc"} {
		t.Run(target, func(t *testing.T) {
			cfg := apiTestConfig(t.TempDir())
			switch target {
			case "raft":
				cfg.Raft.Peers = []string{"http://" + occupied.Addr().String()}
			case "http":
				cfg.HTTPAddr = occupied.Addr().String()
			case "grpc":
				cfg.Addr = occupied.Addr().String()
			}
			s, err := Start(context.Background(), cfg)
			require.ErrorContains(t, err, "listen")
			require.Nil(t, s)
			cfg.Raft.Peers = []string{"http://127.0.0.1:0"}
			cfg.HTTPAddr = "127.0.0.1:0"
			cfg.Addr = "127.0.0.1:0"
			recovered := startAPI(t, cfg)
			require.NotEmpty(t, recovered.HTTPAddr())
		})
	}
}

func TestServerRejectsConcurrentDataDirectoryReuseAndCancelsStartup(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	first := startAPI(t, cfg)
	other, err := Start(context.Background(), cfg)
	require.ErrorContains(t, err, "lock node data directory")
	require.Nil(t, other)
	require.NoError(t, first.Close(context.Background()))
	startAPI(t, cfg)
	cfg = apiTestConfig(t.TempDir())
	// No other voters are running, so recovery can finish but leader discovery
	// must stop when its caller cancels rather than consuming StartupTimeout.
	cfg.Raft.Peers = make([]string, 3)
	for i := range cfg.Raft.Peers {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		cfg.Raft.Peers[i] = "http://" + listener.Addr().String()
		require.NoError(t, listener.Close())
	}
	ctx, cancel := context.WithCancel(context.Background())
	timer := time.AfterFunc(20*time.Millisecond, cancel)
	defer timer.Stop()
	defer cancel()
	other, err = Start(ctx, cfg)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, other)
}

func TestServerPublicAPIForwardsFollowerWritesAcrossThreeReplicas(t *testing.T) {
	peers := make([]string, 3)
	for i := range peers {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		peers[i] = "http://" + l.Addr().String()
		require.NoError(t, l.Close())
	}
	type answer struct {
		s   *Server
		err error
	}
	answers := make(chan answer, 3)
	for i := range peers {
		cfg := apiTestConfig(t.TempDir())
		id := uint64(i + 1)
		cfg.NodeID = &id
		cfg.Raft.Peers = peers
		go func() { s, err := Start(context.Background(), cfg); answers <- answer{s, err} }()
	}
	var members []*Server
	for range peers {
		a := <-answers
		require.NoError(t, a.err)
		members = append(members, a.s)
		t.Cleanup(func() { require.NoError(t, a.s.Close(context.Background())) })
	}
	var follower *Server
	require.Eventually(t, func() bool {
		for _, s := range members {
			if !s.application.Raft.IsLeader() {
				follower = s
				return true
			}
		}
		return false
	}, 3*time.Second, time.Millisecond)
	c := apiClient(t, follower)
	configureAPI(t, c, "gateway")
	request := apiSchedule("poll", time.Now().Add(time.Hour))
	created, err := c.PutSchedule(context.Background(), request)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		for _, s := range members {
			got, err := s.application.Store.GetSchedule("poll")
			if err != nil || got.Revision != created.Schedule.Revision {
				return false
			}
		}
		return true
	}, 3*time.Second, 5*time.Millisecond)
	for _, s := range members {
		other := apiClient(t, s)
		retried, err := other.PutSchedule(context.Background(), request)
		require.NoError(t, err)
		require.True(t, proto.Equal(created, retried))
	}
}
