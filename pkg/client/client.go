package client

import (
	"encoding/binary"
	"errors"
	"expvar"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/duravela/djob/pkg/proto/command/proto"
	"github.com/duravela/djob/pkg/tcp"
	"github.com/duravela/djob/pkg/tcp/pool"
	"github.com/duravela/djob/pkg/util/tcputil"
	pb "google.golang.org/protobuf/proto"
)

// stats captures stats for the Cluster service.
var stats *expvar.Map

const (
	numClientRetries           = "num_client_retries"
	numClientAddJobsRetries    = "num_client_add_jobs_retries"
	numClientRemoveJobsRetries = "num_client_remove_jobs_retries"

	numClientAssignJobsRetries    = "num_client_assign_jobs_retries"
	numClientGetJobResultsRetries = "num_client_get_job_results_retries"

	numClientReadTimeouts  = "num_client_read_timeouts"
	numClientWriteTimeouts = "num_client_write_timeouts"
)

func init() {
	stats = expvar.NewMap("cluster")
	stats.Add(numClientAddJobsRetries, 0)
	stats.Add(numClientRemoveJobsRetries, 0)
	stats.Add(numClientAssignJobsRetries, 0)
	stats.Add(numClientGetJobResultsRetries, 0)
	stats.Add(numClientReadTimeouts, 0)
	stats.Add(numClientWriteTimeouts, 0)
}

// Client allows communicating with a remote node.
type Client struct {
	dialer  tcp.Dialer
	timeout time.Duration

	localMu       sync.RWMutex
	localNodeAddr string

	poolMu sync.RWMutex
	pools  map[string]pool.Pool
}

// NewClient returns a client instance for talking to a remote node.
// Clients will retry certain commands if they fail, to allow for
// remote node restarts. Cluster management operations such as joining
// and removing nodes are not retried, to make it clear to the operator
// that the operation failed. In addition, higher-level code will
// usually retry these operations.
func NewClient(dl tcp.Dialer, t time.Duration) *Client {
	return &Client{
		dialer:  dl,
		timeout: t,
		pools:   make(map[string]pool.Pool),
	}
}

// SetLocal informs the client instance of the node address for the node
// using this client. Along with the Service instance it allows this
// client to serve requests for this node locally without the network hop.
func (c *Client) SetLocal(nodeAddr string) error {
	c.localMu.Lock()
	defer c.localMu.Unlock()
	c.localNodeAddr = nodeAddr
	return nil
}

func (c *Client) AddJobs(ajr *proto.AddJobsRequest, nodeAddr string, timeout time.Duration, retries int) (*proto.AddJobsResponse, error) {
	command := &proto.Command{
		Type:    proto.Command_COMMAND_TYPE_ADD_JOBS,
		Request: &proto.Command_AddJobs{AddJobs: ajr},
	}

	p, nRetries, err := c.retry(command, nodeAddr, timeout, retries)
	stats.Add(numClientAddJobsRetries, int64(nRetries))
	if err != nil {
		return nil, fmt.Errorf("add jobs: %w", err)
	}

	resp := &proto.AddJobsResponse{}
	if err = pb.Unmarshal(p, resp); err != nil {
		return nil, fmt.Errorf("unmarshal add jobs response: %w", err)
	}
	if resp.Error != "" {
		return resp, fmt.Errorf("add jobs: %s", resp.Error)
	}

	return resp, nil
}

func (c *Client) RemoveJobs(r *proto.CommandRemoveJobsRequest, nodeAddr string, timeout time.Duration, retries int) (*proto.CommandRemoveJobsResponse, error) {
	command := &proto.Command{
		Type:    proto.Command_COMMAND_TYPE_REMOVE_JOBS,
		Request: &proto.Command_RemoveJobs{RemoveJobs: r},
	}

	p, nRetries, err := c.retry(command, nodeAddr, timeout, retries)
	stats.Add(numClientRemoveJobsRetries, int64(nRetries))
	if err != nil {
		return nil, fmt.Errorf("remove jobs: %w", err)
	}

	resp := &proto.CommandRemoveJobsResponse{}
	if err = pb.Unmarshal(p, resp); err != nil {
		return nil, fmt.Errorf("unmarshal remove jobs response: %w", err)
	}

	return resp, nil
}

func (c *Client) AssignJobs(ajr *proto.AssignJobsRequest, nodeAddr string, timeout time.Duration, retries int) (*proto.AssignJobsResponse, error) {
	command := &proto.Command{
		Type:    proto.Command_COMMAND_TYPE_ASSIGN_JOBS,
		Request: &proto.Command_AssignJobs{AssignJobs: ajr},
	}

	p, nRetries, err := c.retry(command, nodeAddr, timeout, retries)
	stats.Add(numClientAssignJobsRetries, int64(nRetries))
	if err != nil {
		return nil, fmt.Errorf("assign jobs: %w", err)
	}

	resp := &proto.AssignJobsResponse{}
	if err = pb.Unmarshal(p, resp); err != nil {
		return nil, fmt.Errorf("unmarshal assign jobs response: %w", err)
	}

	return resp, nil
}

func (c *Client) GetAllJobReports(nodeAddr string, timeout time.Duration, retries int) (*proto.CommandGetJobResultsResponse, error) {
	command := &proto.Command{
		Type:    proto.Command_COMMAND_TYPE_GET_JOB_RESULTS,
		Request: &proto.Command_GetJobResults{GetJobResults: &proto.CommandGetJobResultsRequest{}},
	}

	p, nRetries, err := c.retry(command, nodeAddr, timeout, retries)
	stats.Add(numClientGetJobResultsRetries, int64(nRetries))
	if err != nil {
		return nil, fmt.Errorf("get all job reports: %w", err)
	}

	resp := &proto.CommandGetJobResultsResponse{}
	if err = pb.Unmarshal(p, resp); err != nil {
		return nil, fmt.Errorf("unmarshal get all job reports response: %w", err)
	}

	return resp, nil
}

// retry retries a command on a remote node. It does this so we churn through connections
// in the pool if we hit an error, as the remote node may have restarted and the pool's
// connections are now stale.
func (c *Client) retry(command *proto.Command, nodeAddr string, timeout time.Duration, maxRetries int) ([]byte, int, error) {
	var p []byte
	var errOuter error
	var nRetries int

	for {
		p, errOuter = func() ([]byte, error) {
			conn, errInner := c.dial(nodeAddr)
			if errInner != nil {
				return nil, errInner
			}
			defer conn.Close()

			if errInner = writeCommand(conn, command, timeout); errInner != nil {
				handleConnError(conn)
				return nil, errInner
			}

			b, errInner := readResponse(conn, timeout)
			if errInner != nil {
				handleConnError(conn)
				return nil, errInner
			}
			return b, nil
		}()
		if errOuter == nil {
			break
		}
		nRetries++
		stats.Add(numClientRetries, 1)
		if nRetries > maxRetries {
			return nil, nRetries, errOuter
		}
	}
	return p, nRetries, nil
}

func (c *Client) dial(nodeAddr string) (net.Conn, error) {
	var pl pool.Pool
	var ok bool

	c.poolMu.RLock()
	pl, ok = c.pools[nodeAddr]
	c.poolMu.RUnlock()

	if !ok {
		if err := func() error {
			c.poolMu.Lock()
			defer c.poolMu.Unlock()

			if pl, ok = c.pools[nodeAddr]; ok {
				return nil // already created
			}

			// New pool is needed for given address.
			factory := func() (net.Conn, error) { return c.dialer.Dial(nodeAddr, c.timeout) }
			p, err := pool.NewChannelPool(tcputil.MaxPoolCapacity, factory)
			if err != nil {
				return err
			}
			c.pools[nodeAddr] = p
			pl = p
			return nil
		}(); err != nil {
			return nil, err
		}
	}

	// Got pool, now get a connection.
	conn, err := pl.Get()
	if err != nil {
		return nil, fmt.Errorf("pool get: %w", err)
	}
	return conn, nil
}

func writeCommand(conn net.Conn, c *proto.Command, timeout time.Duration) error {
	p, err := pb.Marshal(c)
	if err != nil {
		return fmt.Errorf("command marshal: %w", err)
	}

	// write length of protobuf	message
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	b := make([]byte, tcputil.ProtoBufferLengthSize)
	binary.LittleEndian.PutUint64(b[0:], uint64(len(p)))
	_, err = conn.Write(b)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			stats.Add(numClientWriteTimeouts, 1)
		}
		return fmt.Errorf("write length: %w", err)
	}

	// Write actual protobuf.
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	_, err = conn.Write(p)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			stats.Add(numClientWriteTimeouts, 1)
		}
		return fmt.Errorf("write protobuf bytes: %w", err)
	}
	return nil
}

func readResponse(conn net.Conn, timeout time.Duration) (buf []byte, retErr error) {
	defer func() {
		// Connecting to an open port, may cause a panic
		// when the system tries to read the response. This is a workaround.
		if r := recover(); r != nil {
			retErr = fmt.Errorf("panic reading response from node: %v", r)
		}
	}()

	// Read length of incoming response.
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	b := make([]byte, tcputil.ProtoBufferLengthSize)
	_, err := io.ReadFull(conn, b)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			stats.Add(numClientReadTimeouts, 1)
		}
		return nil, fmt.Errorf("read protobuf length: %w", err)
	}
	sz := binary.LittleEndian.Uint64(b[0:])

	// Read in the actual response.
	p := make([]byte, sz)
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	_, err = io.ReadFull(conn, p)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			stats.Add(numClientReadTimeouts, 1)
		}
		return nil, fmt.Errorf("read protobuf bytes: %w", err)
	}
	return p, nil
}

func handleConnError(conn net.Conn) {
	if pc, ok := conn.(*pool.Conn); ok {
		pc.MarkUnusable()
	}
}
