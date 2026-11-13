package server

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/duravela/djob/pkg/db"
	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/duravela/djob/pkg/scheduler"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxAPIPageSize = 32

func apiError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, db.ErrAdmissionHeld) {
		return status.Error(codes.Unavailable, err.Error())
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "request canceled; inspect state before retrying a mutation")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "commit acknowledgement timed out; retry the identical request")
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	var rejection *db.CommandRejection
	if errors.As(err, &rejection) {
		code := codes.FailedPrecondition
		switch rejection.Code {
		case "invalid":
			code = codes.InvalidArgument
		case "not_found", "gone":
			code = codes.NotFound
		case "request_conflict", "already_exists", "occurrence_exists", "run_exists", "event_conflict", "activation_conflict":
			code = codes.AlreadyExists
		case "revision_conflict", "schedule_conflict", "stale_time", "step_conflict":
			code = codes.Aborted
		case "backpressure", "exhausted", "quota_exceeded":
			code = codes.ResourceExhausted
		case "resource_denied":
			code = codes.PermissionDenied
		}
		return status.Error(code, rejection.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

func apiID(id string) error {
	if strings.TrimSpace(id) == "" || len(id) > 256 || !utf8.ValidString(id) {
		return status.Error(codes.InvalidArgument, "ID must contain 1 to 256 UTF-8 bytes")
	}
	return nil
}

func commandEnvelope(meta *pb.CommandMetadata) (db.StateCommand, error) {
	if meta == nil || strings.TrimSpace(meta.RequestId) == "" || len(meta.RequestId) > 240 || !utf8.ValidString(meta.RequestId) || meta.At == nil {
		return db.StateCommand{}, status.Error(codes.InvalidArgument, "command requires a request_id of at most 240 UTF-8 bytes and an explicit at timestamp")
	}
	if err := meta.At.CheckValid(); err != nil {
		return db.StateCommand{}, status.Error(codes.InvalidArgument, "invalid command timestamp")
	}
	at := meta.At.AsTime().UTC()
	if at.IsZero() {
		return db.StateCommand{}, status.Error(codes.InvalidArgument, "command timestamp cannot be zero")
	}
	return db.StateCommand{RequestID: "api/" + meta.RequestId, At: at}, nil
}

func (s *schedulerServer) apply(ctx context.Context, command db.StateCommand) (db.StateResult, error) {
	if _, err := s.authorize(ctx, APIWriter); err != nil {
		return db.StateResult{}, err
	}
	if s.backend == nil {
		return db.StateResult{}, status.Error(codes.Unavailable, "scheduler storage is not ready")
	}
	if err := command.Validate(); err != nil {
		return db.StateResult{}, status.Error(codes.InvalidArgument, err.Error())
	}
	if s.server != nil {
		if err := s.server.validateCommandTime(command.At); err != nil {
			return db.StateResult{}, err
		}
	} else if err := validatePublicCommandTime(command.At, 5*time.Second); err != nil {
		return db.StateResult{}, err
	}
	// HTTP requests need a bound too; callers may choose a shorter deadline.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	result, err := s.backend.ProposeStateCommand(ctx, command)
	if err == nil {
		err = result.Err()
	}
	if err != nil {
		return result, apiError(err)
	}
	fingerprint, err := command.Fingerprint()
	if err != nil || result.RequestID != command.RequestID || result.Fingerprint != fingerprint {
		return db.StateResult{}, status.Error(codes.Internal, "commit receipt does not match request")
	}
	return result, nil
}

func (s *schedulerServer) readable(ctx context.Context, id string) error {
	if _, err := s.authorize(ctx, APIReader); err != nil {
		return err
	}
	if s.backend == nil {
		return status.Error(codes.Unavailable, "scheduler storage is not ready")
	}
	return apiID(id)
}

func apiPage(kind string, size uint32, token string) (int, []byte, error) {
	if size == 0 {
		size = maxAPIPageSize
	}
	if size > maxAPIPageSize {
		return 0, nil, status.Errorf(codes.InvalidArgument, "page_size must be at most %d", maxAPIPageSize)
	}
	if token == "" {
		return int(size), nil, nil
	}
	if len(token) > 512 {
		return 0, nil, status.Error(codes.InvalidArgument, "invalid page token")
	}
	data, err := base64.RawURLEncoding.DecodeString(token)
	prefix := "v1/" + kind + "/"
	if err != nil || !strings.HasPrefix(string(data), prefix) {
		return 0, nil, status.Error(codes.InvalidArgument, "invalid page token for this resource")
	}
	cursor := data[len(prefix):]
	if err := apiID(string(cursor)); err != nil {
		return 0, nil, status.Error(codes.InvalidArgument, "invalid page cursor")
	}
	return int(size), cursor, nil
}

func nextPage(kind string, cursor []byte) string {
	if len(cursor) == 0 {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(append([]byte("v1/"+kind+"/"), cursor...))
}

func (s *schedulerServer) ConfigureChannel(ctx context.Context, req *pb.ConfigureChannelRequest) (*pb.ChannelResponse, error) {
	if _, err := s.authorize(ctx, APIWriter); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	c, err := commandEnvelope(req.Command)
	if err != nil {
		return nil, err
	}
	if len(req.Protocol) > 256 || len(req.Endpoint) > 4096 {
		return nil, status.Error(codes.InvalidArgument, "protocol or endpoint is too long")
	}
	c.Configure = &db.ConfigureChannel{ID: req.Id, GatewayID: req.GatewayId, Protocol: req.Protocol, Endpoint: req.Endpoint, ExpectedRevision: req.ExpectedRevision}
	result, err := s.apply(ctx, c)
	if err != nil {
		return nil, err
	}
	return &pb.ChannelResponse{Channel: channelProto(result.Channel)}, nil
}

func (s *schedulerServer) GetChannel(ctx context.Context, req *pb.GetChannelRequest) (*pb.ChannelResponse, error) {
	if err := s.readable(ctx, req.GetId()); err != nil {
		return nil, err
	}
	channel, err := s.backend.GetChannel(req.Id)
	if err != nil {
		return nil, apiError(err)
	}
	return &pb.ChannelResponse{Channel: channelProto(channel)}, nil
}

func (s *schedulerServer) ListChannels(ctx context.Context, req *pb.ListChannelsRequest) (*pb.ListChannelsResponse, error) {
	if err := s.readable(ctx, "list"); err != nil {
		return nil, err
	}
	limit, cursor, err := apiPage("channels", req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	page, err := s.backend.ListChannels(cursor, limit)
	if err != nil {
		return nil, apiError(err)
	}
	response := &pb.ListChannelsResponse{Channels: make([]*pb.Channel, 0, len(page.Channels)), NextPageToken: nextPage("channels", page.NextCursor)}
	for _, channel := range page.Channels {
		response.Channels = append(response.Channels, channelProto(&channel))
	}
	return response, nil
}

func (s *schedulerServer) PutSchedule(ctx context.Context, req *pb.PutScheduleRequest) (*pb.ScheduleResponse, error) {
	if _, err := s.authorize(ctx, APIWriter); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	c, err := commandEnvelope(req.Command)
	if err != nil {
		return nil, err
	}
	definition, err := definitionFromProto(req.Definition)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	put, err := scheduler.PreparePut(definition, req.ExpectedRevision, c.At)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	c.PutSchedule = &put
	result, err := s.apply(ctx, c)
	if err != nil {
		return nil, err
	}
	return &pb.ScheduleResponse{Schedule: scheduleProto(result.Schedule)}, nil
}

func (s *schedulerServer) PauseSchedule(ctx context.Context, req *pb.PauseScheduleRequest) (*pb.ScheduleResponse, error) {
	if _, err := s.authorize(ctx, APIWriter); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	c, err := commandEnvelope(req.Command)
	if err != nil {
		return nil, err
	}
	c.PauseSchedule = &db.PauseSchedule{ID: req.Id, ExpectedRevision: req.ExpectedRevision, Paused: req.Paused}
	result, err := s.apply(ctx, c)
	if err != nil {
		return nil, err
	}
	return &pb.ScheduleResponse{Schedule: scheduleProto(result.Schedule)}, nil
}

func (s *schedulerServer) GetSchedule(ctx context.Context, req *pb.GetScheduleRequest) (*pb.ScheduleResponse, error) {
	if err := s.readable(ctx, req.GetId()); err != nil {
		return nil, err
	}
	schedule, err := s.backend.GetSchedule(req.Id)
	if err != nil {
		return nil, apiError(err)
	}
	return &pb.ScheduleResponse{Schedule: scheduleProto(schedule)}, nil
}

func (s *schedulerServer) ListSchedules(ctx context.Context, req *pb.ListSchedulesRequest) (*pb.ListSchedulesResponse, error) {
	if err := s.readable(ctx, "list"); err != nil {
		return nil, err
	}
	limit, cursor, err := apiPage("schedules", req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	page, err := s.backend.ListSchedules(cursor, limit)
	if err != nil {
		return nil, apiError(err)
	}
	response := &pb.ListSchedulesResponse{Schedules: make([]*pb.Schedule, 0, len(page.Schedules)), NextPageToken: nextPage("schedules", page.NextCursor)}
	for _, schedule := range page.Schedules {
		response.Schedules = append(response.Schedules, scheduleProto(&schedule))
	}
	return response, nil
}

func (s *schedulerServer) CreateRun(ctx context.Context, req *pb.CreateRunRequest) (*pb.RunResponse, error) {
	if _, err := s.authorize(ctx, APIWriter); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	c, err := commandEnvelope(req.Command)
	if err != nil {
		return nil, err
	}
	if len(req.Payload) > 64<<10 {
		return nil, status.Error(codes.InvalidArgument, "run payload exceeds 64 KiB")
	}
	c.EnqueueRun = &db.CreateRun{ID: req.Id, ChannelID: req.ChannelId, Executor: req.Executor, Payload: req.Payload}
	policy, err := retryPolicyFromProto(req.RetryPolicy)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	c.EnqueueRun.RetryPolicy = policy
	if req.StartDeadline != nil {
		if err := req.StartDeadline.CheckValid(); err != nil || req.StartDeadline.AsTime().IsZero() {
			return nil, status.Error(codes.InvalidArgument, "invalid start_deadline timestamp")
		}
		at := req.StartDeadline.AsTime().UTC()
		c.EnqueueRun.StartDeadline = &at
	}
	result, err := s.apply(ctx, c)
	if err != nil {
		return nil, err
	}
	return &pb.RunResponse{Run: runProto(result.Run)}, nil
}

func (s *schedulerServer) GetRun(ctx context.Context, req *pb.GetRunRequest) (*pb.RunResponse, error) {
	if err := s.readable(ctx, req.GetId()); err != nil {
		return nil, err
	}
	run, err := s.backend.GetRun(req.Id)
	if err != nil {
		return nil, apiError(err)
	}
	return &pb.RunResponse{Run: runProto(run)}, nil
}

func (s *schedulerServer) CancelRun(ctx context.Context, req *pb.CancelRunRequest) (*pb.RunResponse, error) {
	if _, err := s.authorize(ctx, APIWriter); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	c, err := commandEnvelope(req.Command)
	if err != nil {
		return nil, err
	}
	c.CancelRun = &db.CancelRun{RunID: req.Id, Reason: req.Reason}
	result, err := s.apply(ctx, c)
	if err != nil {
		return nil, err
	}
	return &pb.RunResponse{Run: runProto(result.Run)}, nil
}

func (s *schedulerServer) ListRuns(ctx context.Context, req *pb.ListRunsRequest) (*pb.ListRunsResponse, error) {
	if err := s.readable(ctx, "list"); err != nil {
		return nil, err
	}
	limit, cursor, err := apiPage("runs", req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	page, err := s.backend.ListRuns(cursor, limit)
	if err != nil {
		return nil, apiError(err)
	}
	response := &pb.ListRunsResponse{Runs: make([]*pb.Run, 0, len(page.Runs)), NextPageToken: nextPage("runs", page.NextCursor)}
	for _, run := range page.Runs {
		response.Runs = append(response.Runs, runProto(&run))
	}
	return response, nil
}

func (s *schedulerServer) GetAttempt(ctx context.Context, req *pb.GetAttemptRequest) (*pb.AttemptResponse, error) {
	if err := s.readable(ctx, req.GetId()); err != nil {
		return nil, err
	}
	attempt, err := s.backend.GetAttempt(req.Id)
	if err != nil {
		return nil, apiError(err)
	}
	return &pb.AttemptResponse{Attempt: attemptProto(attempt)}, nil
}

func timestamp(at time.Time) *timestamppb.Timestamp {
	if at.IsZero() {
		return nil
	}
	return timestamppb.New(at)
}

func duration(value *durationpb.Duration) (time.Duration, error) {
	if value == nil {
		return 0, nil
	}
	if err := value.CheckValid(); err != nil {
		return 0, err
	}
	d := value.AsDuration()
	converted := durationpb.New(d)
	if converted.Seconds != value.Seconds || converted.Nanos != value.Nanos {
		return 0, fmt.Errorf("duration exceeds the supported Go duration range")
	}
	return d, nil
}

func definitionFromProto(d *pb.ScheduleDefinition) (db.ScheduleDefinition, error) {
	if d == nil || d.Spec == nil || d.Spec.StartAt == nil {
		return db.ScheduleDefinition{}, fmt.Errorf("definition, spec, and start_at are required")
	}
	if err := d.Spec.StartAt.CheckValid(); err != nil {
		return db.ScheduleDefinition{}, err
	}
	interval, err := duration(d.Spec.Interval)
	if err != nil {
		return db.ScheduleDefinition{}, err
	}
	grace, err := duration(d.MisfireGrace)
	if err != nil {
		return db.ScheduleDefinition{}, err
	}
	window, err := duration(d.StartWindow)
	if err != nil {
		return db.ScheduleDefinition{}, err
	}
	policy, err := retryPolicyFromProto(d.RetryPolicy)
	if err != nil {
		return db.ScheduleDefinition{}, err
	}
	return db.ScheduleDefinition{ID: d.Id, ChannelID: d.ChannelId, Executor: d.Executor, Payload: d.Payload,
		Spec:         db.ScheduleSpec{Kind: db.ScheduleKind(d.Spec.Kind), StartAt: d.Spec.StartAt.AsTime().UTC(), Interval: interval, Cron: d.Spec.Cron, TimeZone: d.Spec.TimeZone},
		MissedPolicy: db.MissedRunPolicy(d.MissedPolicy), MisfireGrace: grace, CatchUpLimit: int(d.CatchUpLimit), Paused: d.Paused, StartWindow: window, RetryPolicy: policy}, nil
}

func retryPolicyFromProto(p *pb.RetryPolicy) (*db.RetryPolicy, error) {
	if p == nil {
		return nil, nil
	}
	initial, err := duration(p.InitialBackoff)
	if err != nil {
		return nil, err
	}
	maximum, err := duration(p.MaxBackoff)
	if err != nil {
		return nil, err
	}
	policy := &db.RetryPolicy{MaxAttempts: p.MaxAttempts, InitialBackoff: initial, MaxBackoff: maximum}
	return policy, policy.Validate()
}

func retryPolicyProto(p *db.RetryPolicy) *pb.RetryPolicy {
	if p == nil {
		return nil
	}
	return &pb.RetryPolicy{MaxAttempts: p.MaxAttempts, InitialBackoff: durationpb.New(p.InitialBackoff), MaxBackoff: durationpb.New(p.MaxBackoff)}
}

func channelProto(c *db.CommunicationChannel) *pb.Channel {
	if c == nil {
		return nil
	}
	return &pb.Channel{Id: c.ID, GatewayId: c.GatewayID, Protocol: c.Protocol, Endpoint: c.Endpoint, Revision: c.Revision, Epoch: c.Epoch,
		State: string(c.State), OwnerId: c.OwnerID, LeaseUntil: timestamp(c.LeaseUntil), ActiveAttemptId: c.ActiveAttemptID, LastTransition: timestamp(c.LastTransition)}
}

func definitionProto(d db.ScheduleDefinition) *pb.ScheduleDefinition {
	spec := &pb.ScheduleSpec{Kind: string(d.Spec.Kind), StartAt: timestamp(d.Spec.StartAt), Cron: d.Spec.Cron, TimeZone: d.Spec.TimeZone}
	if d.Spec.Interval != 0 {
		spec.Interval = durationpb.New(d.Spec.Interval)
	}
	definition := &pb.ScheduleDefinition{Id: d.ID, ChannelId: d.ChannelID, Executor: d.Executor, Payload: d.Payload, Spec: spec,
		MissedPolicy: string(d.MissedPolicy), CatchUpLimit: uint32(d.CatchUpLimit), Paused: d.Paused, RetryPolicy: retryPolicyProto(d.RetryPolicy)}
	if d.MisfireGrace != 0 {
		definition.MisfireGrace = durationpb.New(d.MisfireGrace)
	}
	if d.StartWindow != 0 {
		definition.StartWindow = durationpb.New(d.StartWindow)
	}
	return definition
}

func scheduleProto(s *db.Schedule) *pb.Schedule {
	if s == nil {
		return nil
	}
	return &pb.Schedule{Definition: definitionProto(s.ScheduleDefinition), Revision: s.Revision, NextAt: timestamp(s.NextAt), Completed: s.Completed,
		LastTransition: timestamp(s.LastTransition), LastOccurrence: timestamp(s.LastOccurrence), SkippedThrough: timestamp(s.SkippedThrough)}
}

func runProto(r *db.Run) *pb.Run {
	if r == nil {
		return nil
	}
	return &pb.Run{Id: r.ID, ChannelId: r.ChannelID, Executor: r.Executor, Payload: r.Payload, State: string(r.State), AttemptCount: r.AttemptCount,
		ActiveAttemptId: r.ActiveAttemptID, LastAttemptId: r.LastAttemptID, CreatedAt: timestamp(r.CreatedAt), LastTransition: timestamp(r.LastTransition),
		ScheduleId: r.ScheduleID, ScheduleRevision: r.ScheduleRevision, ScheduledAt: timestamp(r.ScheduledAt),
		CancelRequested: r.CancellationRequested(), CancelRequestedAt: timestamp(r.CancelRequestedAt), CancelRequestId: r.CancelRequestID, CancelReason: r.CancelReason,
		StartDeadline: timestamp(r.StartDeadline), RetryPolicy: retryPolicyProto(r.RetryPolicy), RetryAt: timestamp(r.RetryAt)}
}

func attemptProto(a *db.Attempt) *pb.Attempt {
	return &pb.Attempt{Id: a.ID, RunId: a.RunID, Number: a.Number, OwnerId: a.OwnerID, Epoch: a.Epoch, StartedAt: timestamp(a.StartedAt), FinishedAt: timestamp(a.FinishedAt),
		Outcome: string(a.Outcome), Result: a.Result, Error: a.Error, Quiescent: a.Quiescent, AuthorizedAt: timestamp(a.AuthorizedAt), NoDelivery: a.NoDelivery, RetryClass: string(a.RetryClass), RetryAt: timestamp(a.RetryAt)}
}

func (s *schedulerServer) ScheduleJob(ctx context.Context, req *pb.ScheduleJobRequest) (*pb.ScheduleJobResponse, error) {
	if _, err := s.authorize(ctx, APIWriter); err != nil {
		return nil, err
	}
	if req == nil || req.Job == nil || req.Job.Definition == nil || req.Metadata == nil {
		return nil, status.Error(codes.InvalidArgument, "use job.definition and metadata, or PutSchedule")
	}
	job := req.Job
	if job.Schedule != nil || job.Repeats != nil || job.DueTime != nil || job.Ttl != nil || job.Data != nil || job.FailurePolicy != nil {
		return nil, status.Error(codes.InvalidArgument, "legacy timing, data, repeat, TTL, and failure-policy fields are unsupported; use definition")
	}
	if req.Name != job.Definition.Id {
		return nil, status.Error(codes.InvalidArgument, "job name must match definition.id")
	}
	response, err := s.PutSchedule(ctx, &pb.PutScheduleRequest{Command: &pb.CommandMetadata{RequestId: req.Metadata.RequestId, At: req.Metadata.At}, Definition: job.Definition, ExpectedRevision: req.Metadata.ExpectedRevision})
	if err != nil {
		return nil, err
	}
	return &pb.ScheduleJobResponse{Schedule: response.Schedule}, nil
}

func (s *schedulerServer) GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error) {
	response, err := s.GetSchedule(ctx, &pb.GetScheduleRequest{Id: req.GetName()})
	if err != nil {
		return nil, err
	}
	return &pb.GetJobResponse{Job: &pb.Job{Definition: response.Schedule.Definition}, Schedule: response.Schedule}, nil
}

func (s *schedulerServer) ListJobs(ctx context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	response, err := s.ListSchedules(ctx, &pb.ListSchedulesRequest{PageSize: req.GetPageSize(), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, err
	}
	jobs := make([]*pb.NamedJob, 0, len(response.Schedules))
	for _, schedule := range response.Schedules {
		jobs = append(jobs, &pb.NamedJob{Name: schedule.Definition.Id, Job: &pb.Job{Definition: schedule.Definition}})
	}
	return &pb.ListJobsResponse{Jobs: jobs, NextPageToken: response.NextPageToken}, nil
}

func (s *schedulerServer) DeleteJob(ctx context.Context, _ *pb.DeleteJobRequest) (*pb.DeleteJobResponse, error) {
	if _, err := s.authorize(ctx, APIWriter); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented, "durable deletion is not implemented; PauseSchedule stops future materialization without deleting runs")
}
