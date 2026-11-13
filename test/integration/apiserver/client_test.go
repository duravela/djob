package apiserver

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/client"
	"github.com/duravela/djob/pkg/proto/command/proto"
	"github.com/duravela/djob/pkg/util/tcputil"
	"github.com/duravela/djob/test/integration/apiserver/servicetest"
	pb "google.golang.org/protobuf/proto"
)

func TestClient_AddJobs(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			// Error on connection, so give up, as normal
			// test exit can cause that too.
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_ADD_JOBS {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		jobs := c.GetAddJobs()
		if jobs == nil {
			t.Fatal("expected query request, got nil")
		}

		p, err = pb.Marshal(&proto.AddJobsResponse{
			AcceptedJobIds: []string{"accepted"},
			FailedJobIds:   []string{"rejected"},
		})
		if err != nil {
			conn.Close()
		}
		_ = tcputil.WriteBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := client.NewClient(&simpleDialer{}, 0)
	req := &proto.AddJobsRequest{}
	resp, err := c.AddJobs(req, srv.Addr(), time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.AcceptedJobIds) != 1 || resp.AcceptedJobIds[0] != "accepted" ||
		len(resp.FailedJobIds) != 1 || resp.FailedJobIds[0] != "rejected" {
		t.Fatalf("unexpected add response: %v", resp)
	}
}

func TestClient_RemoveJobs(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			// Error on connection, so give up, as normal
			// test exit can cause that too.
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_REMOVE_JOBS {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		jobs := c.GetRemoveJobs()
		if jobs == nil {
			t.Fatal("expected query request, got nil")
		}

		p, err = pb.Marshal(&proto.CommandRemoveJobsResponse{Accepted: true})
		if err != nil {
			conn.Close()
		}
		_ = tcputil.WriteBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := client.NewClient(&simpleDialer{}, 0)
	req := &proto.CommandRemoveJobsRequest{}
	resp, err := c.RemoveJobs(req, srv.Addr(), time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Accepted {
		t.Fatalf("unexpected remove response: %v", resp)
	}
}

func TestClient_AssignJobs(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			// Error on connection, so give up, as normal
			// test exit can cause that too.
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_ASSIGN_JOBS {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		jobs := c.GetAssignJobs()
		if jobs == nil {
			t.Fatal("expected query request, got nil")
		}

		p, err = pb.Marshal(&proto.AssignJobsResponse{
			Result: &proto.AssignJobsResponse_Success{
				Success: &proto.AssignJobsSuccess{Accepted: true},
			},
		})
		if err != nil {
			conn.Close()
		}
		_ = tcputil.WriteBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := client.NewClient(&simpleDialer{}, 0)
	req := &proto.AssignJobsRequest{}
	resp, err := c.AssignJobs(req, srv.Addr(), time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetSuccess().GetAccepted() {
		t.Fatalf("unexpected assign response: %v", resp)
	}
}

// Test that GetAllJobReports calls the underlying transport and returns the response.
func TestClient_GetAllJobReports(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			// Error on connection, so give up, as normal
			// test exit can cause that too.
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_GET_JOB_RESULTS {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		jobs := c.GetGetJobResults()
		if jobs == nil {
			t.Fatal("expected query request, got nil")
		}

		p, err = pb.Marshal(&proto.CommandGetJobResultsResponse{
			JobReports: []*proto.JobReport{{JobId: "job", Status: "success"}},
		})
		if err != nil {
			conn.Close()
		}
		_ = tcputil.WriteBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := client.NewClient(&simpleDialer{}, 0)
	resp, err := c.GetAllJobReports(srv.Addr(), time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.JobReports) != 1 || resp.JobReports[0].JobId != "job" || resp.JobReports[0].Status != "success" {
		t.Fatalf("unexpected reports: %v", resp)
	}
}

func readCommand(conn net.Conn) *proto.Command {
	b := make([]byte, tcputil.ProtoBufferLengthSize)
	_, err := io.ReadFull(conn, b)
	if err != nil {
		return nil
	}
	sz := binary.LittleEndian.Uint64(b[0:])
	p := make([]byte, sz)
	_, err = io.ReadFull(conn, p)
	if err != nil {
		return nil
	}
	c := &proto.Command{}
	err = pb.Unmarshal(p, c)
	if err != nil {
		return nil
	}
	return c
}

type simpleDialer struct {
}

func (s *simpleDialer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
