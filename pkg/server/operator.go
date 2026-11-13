package server

import (
	"context"
	"strings"
	"unicode/utf8"

	"github.com/duravela/djob/pkg/db"
	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func resolutionReason(reason string) error {
	if strings.TrimSpace(reason) == "" || len(reason) > 4096 || !utf8.ValidString(reason) {
		return status.Error(codes.InvalidArgument, "resolution requires a reason of 1 to 4096 UTF-8 bytes")
	}
	return nil
}

func (s *schedulerServer) ExpireChannel(ctx context.Context, req *pb.ExpireChannelRequest) (*pb.ChannelResponse, error) {
	if _, err := s.authorize(ctx, APIOperator); err != nil {
		return nil, err
	}
	if req == nil || req.ExpectedEpoch == 0 {
		return nil, status.Error(codes.InvalidArgument, "request and expected_epoch are required")
	}
	c, err := commandEnvelope(req.Command)
	if err != nil {
		return nil, err
	}
	c.Expire = &db.ExpireChannel{ChannelID: req.Id, Epoch: req.ExpectedEpoch}
	result, err := s.apply(ctx, c)
	if err != nil {
		return nil, err
	}
	return &pb.ChannelResponse{Channel: channelProto(result.Channel)}, nil
}

func (s *schedulerServer) ResolveChannel(ctx context.Context, req *pb.ResolveChannelRequest) (*pb.ChannelResponse, error) {
	actor, err := s.authorize(ctx, APIOperator)
	if err != nil {
		return nil, err
	}
	if req == nil || req.ExpectedEpoch == 0 {
		return nil, status.Error(codes.InvalidArgument, "request and expected_epoch are required")
	}
	if err := resolutionReason(req.Reason); err != nil {
		return nil, err
	}
	c, err := commandEnvelope(req.Command)
	if err != nil {
		return nil, err
	}
	c.ResolveChannel = &db.ResolveChannel{ChannelID: req.Id, Epoch: req.ExpectedEpoch, Actor: actor, Reason: req.Reason, Quiescent: req.Quiescent}
	result, err := s.apply(ctx, c)
	if err != nil {
		return nil, err
	}
	return &pb.ChannelResponse{Channel: channelProto(result.Channel)}, nil
}

func (s *schedulerServer) ResolveRun(ctx context.Context, req *pb.ResolveRunRequest) (*pb.RunResponse, error) {
	actor, err := s.authorize(ctx, APIOperator)
	if err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := resolutionReason(req.Reason); err != nil {
		return nil, err
	}
	c, err := commandEnvelope(req.Command)
	if err != nil {
		return nil, err
	}
	c.ResolveRun = &db.ResolveRun{RunID: req.Id, AttemptID: req.AttemptId, State: db.RunState(req.State), Actor: actor, Reason: req.Reason}
	result, err := s.apply(ctx, c)
	if err != nil {
		return nil, err
	}
	return &pb.RunResponse{Run: runProto(result.Run)}, nil
}

func (s *schedulerServer) GetResolution(ctx context.Context, req *pb.GetResolutionRequest) (*pb.ResolutionResponse, error) {
	if err := s.readable(ctx, req.GetRequestId()); err != nil {
		return nil, err
	}
	if len(req.RequestId) > 240 {
		return nil, status.Error(codes.InvalidArgument, "request_id must contain at most 240 UTF-8 bytes")
	}
	r, err := s.backend.GetResolution("api/" + req.RequestId)
	if err != nil {
		return nil, apiError(err)
	}
	return &pb.ResolutionResponse{Resolution: &pb.Resolution{RequestId: strings.TrimPrefix(r.RequestID, "api/"), ChannelId: r.ChannelID,
		RunId: r.RunID, AttemptId: r.AttemptID, Epoch: r.Epoch, RunState: string(r.RunState), At: timestamp(r.At), Actor: r.Actor, Reason: r.Reason, Quiescent: r.Quiescent}}, nil
}
