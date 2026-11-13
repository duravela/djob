package server

import (
	"context"
	"net/http"

	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/duravela/djob/pkg/server/apiconstants"
	"github.com/gorilla/mux"
)

type ApiV1System interface{}
type apiV1ServerOpts struct{ scheduler pb.SchedulerServer }
type apiV1Server struct {
	scheduler pb.SchedulerServer
	mux       *mux.Router
}

func newAPIV1Server(_ context.Context, opts *apiV1ServerOpts) http.Handler {
	a := &apiV1Server{scheduler: opts.scheduler, mux: mux.NewRouter()}
	registerRoutes(a)
	return a
}

func registerRoutes(a *apiV1Server) {
	add := func(method, path string, handler http.HandlerFunc) {
		a.mux.HandleFunc(apiconstants.APIV1Path+path, handler).Methods(method)
	}
	add("PUT", "channels/{id:.+}", jsonRPC(func() *pb.ConfigureChannelRequest { return new(pb.ConfigureChannelRequest) }, a.scheduler.ConfigureChannel,
		func(r *http.Request, q *pb.ConfigureChannelRequest) error { return pathID(r, &q.Id) }))
	add("GET", "channels/{id:.+}", jsonRPC(func() *pb.GetChannelRequest { return new(pb.GetChannelRequest) }, a.scheduler.GetChannel,
		func(r *http.Request, q *pb.GetChannelRequest) error { return pathID(r, &q.Id) }))
	add("GET", "channels/", jsonRPC(func() *pb.ListChannelsRequest { return new(pb.ListChannelsRequest) }, a.scheduler.ListChannels,
		func(r *http.Request, q *pb.ListChannelsRequest) error { return pageQuery(r, &q.PageSize, &q.PageToken) }))
	add("POST", "channels/{id:.+}/expire", jsonRPC(func() *pb.ExpireChannelRequest { return new(pb.ExpireChannelRequest) }, a.scheduler.ExpireChannel,
		func(r *http.Request, q *pb.ExpireChannelRequest) error { return pathID(r, &q.Id) }))
	add("POST", "channels/{id:.+}/resolve", jsonRPC(func() *pb.ResolveChannelRequest { return new(pb.ResolveChannelRequest) }, a.scheduler.ResolveChannel,
		func(r *http.Request, q *pb.ResolveChannelRequest) error { return pathID(r, &q.Id) }))
	add("PUT", "schedules/{id:.+}", jsonRPC(func() *pb.PutScheduleRequest { return new(pb.PutScheduleRequest) }, a.scheduler.PutSchedule,
		func(r *http.Request, q *pb.PutScheduleRequest) error {
			if q.Definition == nil {
				return invalidHTTP("definition is required")
			}
			return pathID(r, &q.Definition.Id)
		}))
	add("POST", "schedules/{id:.+}/pause", jsonRPC(func() *pb.PauseScheduleRequest { return new(pb.PauseScheduleRequest) }, a.scheduler.PauseSchedule,
		func(r *http.Request, q *pb.PauseScheduleRequest) error { return pathID(r, &q.Id) }))
	add("GET", "schedules/{id:.+}", jsonRPC(func() *pb.GetScheduleRequest { return new(pb.GetScheduleRequest) }, a.scheduler.GetSchedule,
		func(r *http.Request, q *pb.GetScheduleRequest) error { return pathID(r, &q.Id) }))
	add("GET", "schedules/", jsonRPC(func() *pb.ListSchedulesRequest { return new(pb.ListSchedulesRequest) }, a.scheduler.ListSchedules,
		func(r *http.Request, q *pb.ListSchedulesRequest) error {
			return pageQuery(r, &q.PageSize, &q.PageToken)
		}))
	add("PUT", "runs/{id:.+}", jsonRPC(func() *pb.CreateRunRequest { return new(pb.CreateRunRequest) }, a.scheduler.CreateRun,
		func(r *http.Request, q *pb.CreateRunRequest) error { return pathID(r, &q.Id) }))
	add("POST", "runs/{id:.+}/cancel", jsonRPC(func() *pb.CancelRunRequest { return new(pb.CancelRunRequest) }, a.scheduler.CancelRun,
		func(r *http.Request, q *pb.CancelRunRequest) error { return pathID(r, &q.Id) }))
	add("POST", "runs/{id:.+}/resolve", jsonRPC(func() *pb.ResolveRunRequest { return new(pb.ResolveRunRequest) }, a.scheduler.ResolveRun,
		func(r *http.Request, q *pb.ResolveRunRequest) error { return pathID(r, &q.Id) }))
	add("GET", "runs/{id:.+}", jsonRPC(func() *pb.GetRunRequest { return new(pb.GetRunRequest) }, a.scheduler.GetRun,
		func(r *http.Request, q *pb.GetRunRequest) error { return pathID(r, &q.Id) }))
	add("GET", "runs/", jsonRPC(func() *pb.ListRunsRequest { return new(pb.ListRunsRequest) }, a.scheduler.ListRuns,
		func(r *http.Request, q *pb.ListRunsRequest) error { return pageQuery(r, &q.PageSize, &q.PageToken) }))
	add("GET", "attempts/{id:.+}", jsonRPC(func() *pb.GetAttemptRequest { return new(pb.GetAttemptRequest) }, a.scheduler.GetAttempt,
		func(r *http.Request, q *pb.GetAttemptRequest) error { return pathID(r, &q.Id) }))
	add("GET", "resolutions/{id:.+}", jsonRPC(func() *pb.GetResolutionRequest { return new(pb.GetResolutionRequest) }, a.scheduler.GetResolution,
		func(r *http.Request, q *pb.GetResolutionRequest) error { return pathID(r, &q.RequestId) }))
	add("GET", "jobs/", jsonRPC(func() *pb.ListJobsRequest { return new(pb.ListJobsRequest) }, a.scheduler.ListJobs,
		func(r *http.Request, q *pb.ListJobsRequest) error { return pageQuery(r, &q.PageSize, &q.PageToken) }))
	add("GET", "jobs/{id:.+}", jsonRPC(func() *pb.GetJobRequest { return new(pb.GetJobRequest) }, a.scheduler.GetJob,
		func(r *http.Request, q *pb.GetJobRequest) error { return pathID(r, &q.Name) }))
	add("POST", "jobs/{id:.+}", jsonRPC(func() *pb.ScheduleJobRequest { return new(pb.ScheduleJobRequest) }, a.scheduler.ScheduleJob,
		func(r *http.Request, q *pb.ScheduleJobRequest) error { return pathID(r, &q.Name) }))
}

func (a *apiV1Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mux.ServeHTTP(w, captureHTTPAuthorization(r))
}
