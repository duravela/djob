package apiserver

import (
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/apiserver"
	"github.com/duravela/djob/pkg/client"
	"github.com/duravela/djob/pkg/proto/command/proto"
)

const (
	defaultMaxRetries = 3
)

func Test_NewServiceOpenClose(t *testing.T) {
	ml := mustNewMockTransport()
	s := apiserver.New(ml, mustNewMockDatabase(), mustNewMockManager())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	if ml.Addr().String() != s.Addr().String() {
		t.Fatalf("service returned incorrect address")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceTestAssignJobs(t *testing.T) {
	ml := mustNewMockTransport()
	db := mustNewMockDatabase()
	clstr := mustNewMockManager()

	s := apiserver.New(ml, db, clstr)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	cl := client.NewClient(ml, 30*time.Second)
	if err := cl.SetLocal(s.Addr().String()); err != nil {
		t.Fatalf("failed to set cluster client local parameters: %s", err)
	}
	er := &proto.AssignJobsRequest{}
	_, err := cl.AssignJobs(er, s.Addr().String(), 5*time.Second, defaultMaxRetries)
	if err != nil {
		t.Fatal("failed to assign jobs:", err)
	}
}

func Test_NewServiceTestAssignJobs_Fail_NoRegisteredJobs(t *testing.T) {
	ml := mustNewMockTransport()
	db := mustNewMockDatabase()
	clstr := mustNewMockManager()

	clstr.assignFn = func(r *proto.AssignJobsRequest) (*proto.AssignJobsResponse, error) {
		return nil, fmt.Errorf("no jobs registered")
	}

	s := apiserver.New(ml, db, clstr)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	cl := client.NewClient(ml, 30*time.Second)
	if err := cl.SetLocal(s.Addr().String()); err != nil {
		t.Fatalf("failed to set cluster client local parameters: %s", err)
	}
	er := &proto.AssignJobsRequest{}
	resp, err := cl.AssignJobs(er, s.Addr().String(), 5*time.Second, defaultMaxRetries)
	if err != nil {
		t.Fatalf("failed to call AssignJobs: %s", err)
	}

	if resp.GetError() == nil {
		t.Fatal("expected error response, got nil")
	}
}

func Test_NewServiceTestAddJobs(t *testing.T) {
	ml := mustNewMockTransport()
	db := mustNewMockDatabase()
	clstr := mustNewMockManager()

	s := apiserver.New(ml, db, clstr)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	cl := client.NewClient(ml, 30*time.Second)
	if err := cl.SetLocal(s.Addr().String()); err != nil {
		t.Fatalf("failed to set cluster client local parameters: %s", err)
	}
	er := &proto.AddJobsRequest{}
	_, err := cl.AddJobs(er, s.Addr().String(), 5*time.Second, defaultMaxRetries)
	if err != nil {
		t.Fatal("failed to add jobs:", err)
	}
}

func Test_NewServiceTestAddJobs_ProposalFailure(t *testing.T) {
	ml := mustNewMockTransport()
	db := mustNewMockDatabase()
	db.addFn = func(*proto.AddJobsRequest) (*proto.AddJobsResponse, error) {
		return nil, fmt.Errorf("raft proposal timeout")
	}
	s := apiserver.New(ml, db, mustNewMockManager())
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	cl := client.NewClient(ml, time.Second)
	resp, err := cl.AddJobs(&proto.AddJobsRequest{Jobs: []*proto.Job{{Id: "job"}}}, s.Addr().String(), time.Second, 0)
	if err == nil || !strings.Contains(err.Error(), "raft proposal timeout") {
		t.Fatalf("expected proposal failure, got response=%v error=%v", resp, err)
	}
	if resp == nil || resp.Error != "raft proposal timeout" {
		t.Fatalf("proposal failure was not preserved: %v", resp)
	}
}

func Test_NewServiceTestRemoveJobsFailureResponds(t *testing.T) {
	for _, missingRequest := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing_request=%t", missingRequest), func(t *testing.T) {
			ml := mustNewMockTransport()
			db := mustNewMockDatabase()
			db.rmFn = func(*proto.CommandRemoveJobsRequest) (*proto.CommandRemoveJobsResponse, error) {
				return nil, fmt.Errorf("raft proposal timeout")
			}
			s := apiserver.New(ml, db, mustNewMockManager())
			if err := s.Open(); err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			var req *proto.CommandRemoveJobsRequest
			if !missingRequest {
				req = &proto.CommandRemoveJobsRequest{JobIds: []string{"job"}}
			}
			cl := client.NewClient(ml, time.Second)
			resp, err := cl.RemoveJobs(req, s.Addr().String(), time.Second, 0)
			if err != nil {
				t.Fatalf("failed removal did not return a response: %v", err)
			}
			if resp.Accepted {
				t.Fatal("failed removal was accepted")
			}
		})
	}
}

func Test_NewServiceTestRemoveJobs(t *testing.T) {
	ml := mustNewMockTransport()
	db := mustNewMockDatabase()
	clstr := mustNewMockManager()

	s := apiserver.New(ml, db, clstr)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	cl := client.NewClient(ml, 30*time.Second)
	if err := cl.SetLocal(s.Addr().String()); err != nil {
		t.Fatalf("failed to set cluster client local parameters: %s", err)
	}
	er := &proto.CommandRemoveJobsRequest{}
	er.JobIds = []string{"job1", "job2"}
	_, err := cl.RemoveJobs(er, s.Addr().String(), 5*time.Second, defaultMaxRetries)
	if err != nil {
		t.Fatal("failed to remove jobs:", err)
	}
}

func Test_NewServiceTestGetJobResults(t *testing.T) {
	ml := mustNewMockTransport()
	db := mustNewMockDatabase()
	clstr := mustNewMockManager()

	// Set up GetJobResults to return a response with one job report
	clstr.getJobResults = func(r *proto.CommandGetJobResultsRequest) (*proto.CommandGetJobResultsResponse, error) {
		return &proto.CommandGetJobResultsResponse{
			JobReports: []*proto.JobReport{
				{
					JobId:    "job1",
					Status:   "success",
					WorkerId: "worker1",
				},
			},
		}, nil
	}

	s := apiserver.New(ml, db, clstr)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	cl := client.NewClient(ml, 30*time.Second)
	if err := cl.SetLocal(s.Addr().String()); err != nil {
		t.Fatalf("failed to set cluster client local parameters: %s", err)
	}
	resp, err := cl.GetAllJobReports(s.Addr().String(), 5*time.Second, defaultMaxRetries)
	if err != nil {
		t.Fatal("failed to get job results:", err)
	}
	if len(resp.GetJobReports()) != 1 || resp.GetJobReports()[0].JobId != "job1" {
		t.Fatalf("unexpected job reports in response: %+v", resp.GetJobReports())
	}
}

func mustNewMockTransport() *mockTransport {
	tn, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic("failed to create mock listener")
	}
	return &mockTransport{
		tn: tn,
	}
}

type mockTransport struct {
	tn              net.Listener
	remoteEncrypted bool
}

func (ml *mockTransport) Accept() (c net.Conn, err error) {
	return ml.tn.Accept()
}

func (ml *mockTransport) Addr() net.Addr {
	return ml.tn.Addr()
}

func (ml *mockTransport) Close() (err error) {
	return ml.tn.Close()
}

func (ml *mockTransport) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}

	var err error
	var conn net.Conn
	if ml.remoteEncrypted {
		conf := &tls.Config{
			InsecureSkipVerify: true,
		}
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, conf)
	} else {
		conn, err = dialer.Dial("tcp", addr)
	}

	return conn, err
}

type mockDatabase struct {
	addFn    func(*proto.AddJobsRequest) (*proto.AddJobsResponse, error)
	rmFn     func(*proto.CommandRemoveJobsRequest) (*proto.CommandRemoveJobsResponse, error)
	notifyFn func(n *proto.CommandNotifyRequest) error
}

func (m *mockDatabase) AddJobs(r *proto.AddJobsRequest) (*proto.AddJobsResponse, error) {
	return m.addFn(r)
}

func (m *mockDatabase) RemoveJobs(r *proto.CommandRemoveJobsRequest) (*proto.CommandRemoveJobsResponse, error) {
	return m.rmFn(r)
}

func mustNewMockDatabase() *mockDatabase {
	f := func(r *proto.AddJobsRequest) (*proto.AddJobsResponse, error) {
		return &proto.AddJobsResponse{}, nil
	}
	rm := func(r *proto.CommandRemoveJobsRequest) (*proto.CommandRemoveJobsResponse, error) {
		return &proto.CommandRemoveJobsResponse{}, nil
	}
	return &mockDatabase{addFn: f, rmFn: rm}
}

type mockManager struct {
	assignFn      func(*proto.AssignJobsRequest) (*proto.AssignJobsResponse, error)
	getJobResults func(*proto.CommandGetJobResultsRequest) (*proto.CommandGetJobResultsResponse, error)
}

func (m *mockManager) Assign(r *proto.AssignJobsRequest) (*proto.AssignJobsResponse, error) {
	return m.assignFn(r)
}

func (m *mockManager) GetJobResults(r *proto.CommandGetJobResultsRequest) (*proto.CommandGetJobResultsResponse, error) {
	return m.getJobResults(r)
}

func mustNewMockManager() *mockManager {
	a := func(r *proto.AssignJobsRequest) (*proto.AssignJobsResponse, error) {
		return &proto.AssignJobsResponse{}, nil
	}
	r := func(r *proto.CommandGetJobResultsRequest) (*proto.CommandGetJobResultsResponse, error) {
		return &proto.CommandGetJobResultsResponse{}, nil
	}
	return &mockManager{assignFn: a, getJobResults: r}
}
