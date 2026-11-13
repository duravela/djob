package apiserver

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/pool"
	"github.com/duravela/djob/pkg/proto/command/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// JobStore provides the queue operations exposed by the legacy TCP API.
type JobStore interface {
	ProposeCommand(context.Context, db.Command) (map[string]error, error)
	AssignJobs([]string) error
	GetAllJobReports() []*pool.JobResult
}

type JobStoreAdapter struct {
	Store JobStore
}

func (a *JobStoreAdapter) AddJobs(req *proto.AddJobsRequest) (*proto.AddJobsResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("AddJobsRequest is nil")
	}
	// Convert proto jobs to domain jobs
	var jobs []db.Job
	for _, pj := range req.Jobs {
		if pj == nil || pj.Id == "" {
			return nil, fmt.Errorf("job ID is required")
		}
		var deadline time.Time
		if pj.Deadline != nil {
			if err := pj.Deadline.CheckValid(); err != nil {
				return nil, fmt.Errorf("job %s deadline: %w", pj.Id, err)
			}
			deadline = pj.Deadline.AsTime()
		}
		jobs = append(jobs, db.Job{
			ID:       pj.Id,
			Data:     pj.Data,
			Deadline: deadline,
			Priority: int(pj.GetPriority()),
		})
	}
	cmd := db.Command{Type: db.CmdAddJobs, Jobs: jobs}
	result, err := a.Store.ProposeCommand(context.TODO(), cmd)
	if err != nil {
		return nil, err
	}
	resp := &proto.AddJobsResponse{
		AcceptedJobIds: make([]string, 0),
		FailedJobIds:   make([]string, 0),
	}
	for k, v := range result {
		if v != nil {
			resp.FailedJobIds = append(resp.FailedJobIds, k)
		} else {
			resp.AcceptedJobIds = append(resp.AcceptedJobIds, k)
		}
	}
	sort.Strings(resp.AcceptedJobIds)
	sort.Strings(resp.FailedJobIds)
	return resp, err
}

func (a *JobStoreAdapter) RemoveJobs(req *proto.CommandRemoveJobsRequest) (*proto.CommandRemoveJobsResponse, error) {
	var jobs []db.Job
	for _, jid := range req.JobIds {
		jobs = append(jobs, db.Job{
			ID: jid,
			TS: time.Now(),
		})
	}

	cmd := db.Command{Type: db.CmdDeleteJob, Jobs: jobs}
	result, err := a.Store.ProposeCommand(context.TODO(), cmd)
	if err != nil {
		return nil, err
	}
	for id, jobErr := range result {
		if jobErr != nil {
			return nil, fmt.Errorf("remove job %s: %w", id, jobErr)
		}
	}
	resp := &proto.CommandRemoveJobsResponse{}
	resp.Accepted = true

	return resp, nil
}

func (a *JobStoreAdapter) Assign(req *proto.AssignJobsRequest) (*proto.AssignJobsResponse, error) {
	// check if current worker is expected worker req.WorkerId
	var jids []string
	jids = append(jids, req.JobIds...)

	err := a.Store.AssignJobs(jids)
	if err != nil {
		return nil, err
	}
	resp := &proto.AssignJobsResponse{
		Result: &proto.AssignJobsResponse_Success{
			Success: &proto.AssignJobsSuccess{
				Accepted: true,
			},
		},
	}

	return resp, nil
}

func (a *JobStoreAdapter) GetJobResults(_ *proto.CommandGetJobResultsRequest) (*proto.CommandGetJobResultsResponse, error) {
	results := a.Store.GetAllJobReports()

	resp := &proto.CommandGetJobResultsResponse{}
	for _, result := range results {
		jobReport := &proto.JobReport{
			JobId:      result.JobID,
			Group:      result.Group,
			WorkerId:   result.NodeID,
			Status:     result.Status,
			StartedAt:  timestamppb.New(result.StartedAt),
			FinishedAt: timestamppb.New(result.FinishedAt),
			Output:     result.Output,
			Error:      result.Error,
		}
		resp.JobReports = append(resp.JobReports, jobReport)
	}

	return resp, nil
}
