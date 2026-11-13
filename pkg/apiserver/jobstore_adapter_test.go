package apiserver

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/pool"
	"github.com/duravela/djob/pkg/proto/command/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type adapterStore struct {
	command db.Command
	results map[string]error
	err     error
	called  bool
}

func (s *adapterStore) ProposeCommand(_ context.Context, cmd db.Command) (map[string]error, error) {
	s.command = cmd
	s.called = true
	return s.results, s.err
}

func (*adapterStore) AssignJobs([]string) error           { return nil }
func (*adapterStore) GetAllJobReports() []*pool.JobResult { return nil }

func TestJobStoreAdapterAddJobs(t *testing.T) {
	s := &adapterStore{results: map[string]error{
		"accepted-b": nil,
		"rejected":   errors.New("job rejected"),
		"accepted-a": nil,
	}}
	a := JobStoreAdapter{Store: s}
	priority := int32(1)
	deadline := time.Date(2026, time.November, 4, 12, 0, 0, 0, time.UTC)
	resp, err := a.AddJobs(&proto.AddJobsRequest{Jobs: []*proto.Job{
		{Id: "accepted-b", Data: "payload"},
		{Id: "rejected"},
		{Id: "accepted-a", Priority: &priority, Deadline: timestamppb.New(deadline)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(resp.AcceptedJobIds, []string{"accepted-a", "accepted-b"}) ||
		!reflect.DeepEqual(resp.FailedJobIds, []string{"rejected"}) {
		t.Fatalf("unexpected submission results: %v", resp)
	}
	if s.command.Type != db.CmdAddJobs || len(s.command.Jobs) != 3 {
		t.Fatalf("unexpected command: %+v", s.command)
	}
	if got := s.command.Jobs[0]; got.Priority != 0 || !got.Deadline.IsZero() || got.Data != "payload" {
		t.Fatalf("optional fields lost their defaults: %+v", got)
	}
	if got := s.command.Jobs[2]; got.Priority != 1 || !got.Deadline.Equal(deadline) {
		t.Fatalf("explicit fields were not preserved: %+v", got)
	}
}

func TestJobStoreAdapterRejectsInvalidJobsBeforeProposing(t *testing.T) {
	for name, req := range map[string]*proto.AddJobsRequest{
		"nil request":      nil,
		"nil job":          {Jobs: []*proto.Job{nil}},
		"empty ID":         {Jobs: []*proto.Job{{Id: "valid"}, {}}},
		"invalid deadline": {Jobs: []*proto.Job{{Id: "job", Deadline: &timestamppb.Timestamp{Seconds: 253402300800}}}},
	} {
		t.Run(name, func(t *testing.T) {
			s := &adapterStore{}
			a := JobStoreAdapter{Store: s}
			if _, err := a.AddJobs(req); err == nil {
				t.Fatal("expected validation error")
			}
			if s.called {
				t.Fatal("invalid jobs were proposed")
			}
		})
	}
}

func TestJobStoreAdapterPreservesProposalFailure(t *testing.T) {
	want := errors.New("raft proposal timeout")
	a := JobStoreAdapter{Store: &adapterStore{err: want}}
	if _, err := a.AddJobs(&proto.AddJobsRequest{Jobs: []*proto.Job{{Id: "job"}}}); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
}

func TestJobStoreAdapterRejectsFailedRemoval(t *testing.T) {
	want := errors.New("delete failed")
	a := JobStoreAdapter{Store: &adapterStore{results: map[string]error{"job": want}}}
	if _, err := a.RemoveJobs(&proto.CommandRemoveJobsRequest{JobIds: []string{"job"}}); !errors.Is(err, want) {
		t.Fatalf("got %v, want removal failure", err)
	}
}
