package apiserver

import (
	"encoding/binary"
	"io"
	"net"
	"time"

	"github.com/duravela/djob/pkg/proto/command/proto"
	"github.com/duravela/djob/pkg/util/tcputil"
	"go.uber.org/zap"
	pb "google.golang.org/protobuf/proto"
)

// Dialer is the interface dialers must implement.
type Dialer interface {
	// Dial is used to create a connection to a service listening
	// on an address.
	Dial(address string, timeout time.Duration) (net.Conn, error)
}

// Database is the interface any job system must implement
type Database interface {
	// For now a simplistic interface for testing purposes
	AddJobs(*proto.AddJobsRequest) (*proto.AddJobsResponse, error)
	RemoveJobs(*proto.CommandRemoveJobsRequest) (*proto.CommandRemoveJobsResponse, error)
}

// Manager provides access to job management related tasks
type Manager interface {

	// Assigns a range of jobs to the node for further processing
	Assign(*proto.AssignJobsRequest) (*proto.AssignJobsResponse, error)

	GetJobResults(*proto.CommandGetJobResultsRequest) (*proto.CommandGetJobResultsResponse, error)

	// StatusJob(*proto.CommandStatusJobRequest) (*proto.CommandStatusJobResponse, error)
}

// Service provides information about the node and cluster.
type Service struct {
	ln   net.Listener // Incoming connections to the service
	addr net.Addr     // Address on which this service is listening

	db  Database // The queryable system.
	mgr Manager  // The job management system

	logger *zap.Logger
}

// New returns a new instance of the cluster service
func New(ln net.Listener, db Database, m Manager) *Service {
	return &Service{
		ln:     ln,
		addr:   ln.Addr(),
		db:     db,
		mgr:    m,
		logger: zap.NewExample().With(zap.String("component", "cluster")),
	}
}

// Open opens the Service.
func (s *Service) Open() error {
	go func() {
		if err := s.serve(); err != nil {
			s.logger.Error("service serve error", zap.Error(err))
		}
	}()
	s.logger.Info("service listening on", zap.String("address", s.Addr().String()))
	return nil
}

// Close closes the service.
func (s *Service) Close() error {
	s.ln.Close()
	return nil
}

// Addr returns the address the service is listening on.
func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
}

func (s *Service) serve() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return err
		}

		go s.handleConn(conn)
	}
}

func (s *Service) handleConn(conn net.Conn) {
	defer conn.Close()

	b := make([]byte, tcputil.ProtoBufferLengthSize)
	for {
		_, err := io.ReadFull(conn, b)
		if err != nil {
			return
		}
		sz := binary.LittleEndian.Uint64(b[0:])

		p := make([]byte, sz)
		_, err = io.ReadFull(conn, p)
		if err != nil {
			return
		}

		c := &proto.Command{}
		err = pb.Unmarshal(p, c)
		if err != nil {
			return
		}

		switch c.Type {
		case proto.Command_COMMAND_TYPE_ADD_JOBS:
			resp := &proto.AddJobsResponse{}
			addJobsRequest := c.GetAddJobs()

			if addJobsRequest == nil {
				resp.Error = "AddJobsRequest is nil"
			} else {
				dbResp, err := s.db.AddJobs(addJobsRequest)
				if err != nil {
					resp.Error = err.Error()
				} else {
					resp = dbResp
				}
			}
			if err := tcputil.MarshalAndWrite(conn, resp); err != nil {
				return
			}
		case proto.Command_COMMAND_TYPE_REMOVE_JOBS:
			resp := &proto.CommandRemoveJobsResponse{}
			removeJobsRequest := c.GetRemoveJobs()
			if removeJobsRequest == nil {
				resp.Accepted = false
			} else {
				dbResp, err := s.db.RemoveJobs(removeJobsRequest)
				if err == nil {
					resp = dbResp
				}
			}
			if err := tcputil.MarshalAndWrite(conn, resp); err != nil {
				return
			}
		case proto.Command_COMMAND_TYPE_ASSIGN_JOBS:
			resp := &proto.AssignJobsResponse{}

			assignJobsRequest := c.GetAssignJobs()
			if assignJobsRequest == nil {
				resp.Result = &proto.AssignJobsResponse_Error{
					Error: &proto.AssignJobsError{
						Message: "AssignJobsRequest is nil",
					},
				}
			} else {
				mgrResp, err := s.mgr.Assign(assignJobsRequest)
				if err != nil {
					resp.Result = &proto.AssignJobsResponse_Error{
						Error: &proto.AssignJobsError{
							Message: err.Error(),
						},
					}
				} else {
					resp.Result = mgrResp.Result
				}
			}

			if err := tcputil.MarshalAndWrite(conn, resp); err != nil {
				return
			}

		case proto.Command_COMMAND_TYPE_GET_JOB_RESULTS:
			getJobResultsRequest := c.GetGetJobResults()
			if getJobResultsRequest != nil {
				mgrResp, err := s.mgr.GetJobResults(getJobResultsRequest)
				if err == nil {
					if err := tcputil.MarshalAndWrite(conn, mgrResp); err != nil {
						return
					}
				}
			}
		}
	}
}
