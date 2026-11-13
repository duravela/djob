package modbustcp

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/modbus"
)

var ErrQuiescence = errors.New("Modbus downstream exchange has not been confirmed complete")

type transport struct {
	channel       db.CommunicationChannel
	address       string
	turnaround    time.Duration
	dial          func(context.Context, string, string) (net.Conn, error)
	conn          net.Conn
	used          bool
	closed        bool
	possibleSend  bool
	terminalReply bool
	repliedAt     time.Time
	closeErr      error
}

// Factory validates configuration and allocates only. Endpoint must be
// tcp://host:port?turnaround=5ms with an explicitly chosen, positive turnaround
// duration (up to one minute). No connection is opened before Exchange.
func Factory(channel db.CommunicationChannel) (executor.Transport, error) {
	if channel.ID == "" || channel.ID != channel.GatewayID {
		return nil, fmt.Errorf("Modbus TCP channel ID must equal the canonical gateway ID")
	}
	if channel.Protocol != Protocol {
		return nil, fmt.Errorf("Modbus TCP adapter requires protocol %q", Protocol)
	}
	u, err := url.Parse(channel.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid Modbus TCP endpoint: %w", err)
	}
	if u.Scheme != "tcp" || u.User != nil || u.Hostname() == "" || u.Path != "" || u.Fragment != "" || u.Opaque != "" {
		return nil, fmt.Errorf("Modbus TCP endpoint must be tcp://host:port?turnaround=duration")
	}
	port, err := strconv.ParseUint(u.Port(), 10, 16)
	if err != nil || port == 0 {
		return nil, fmt.Errorf("Modbus TCP endpoint requires an explicit port in 1..65535")
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil || len(query) != 1 || len(query["turnaround"]) != 1 {
		return nil, fmt.Errorf("Modbus TCP endpoint requires exactly one turnaround parameter")
	}
	turnaround, err := time.ParseDuration(query.Get("turnaround"))
	if err != nil || turnaround <= 0 || turnaround > time.Minute {
		return nil, fmt.Errorf("Modbus TCP turnaround must be positive and at most one minute")
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	return &transport{channel: channel, address: net.JoinHostPort(u.Hostname(), u.Port()), turnaround: turnaround, dial: dialer.DialContext}, nil
}

func (t *transport) Exchange(ctx context.Context, request executor.Request) (executor.Outcome, error) {
	failed := executor.Outcome{Status: db.AttemptFailed}
	if t.used || t.closed {
		return failed, fmt.Errorf("Modbus TCP transport permits exactly one exchange")
	}
	t.used = true
	if request.Channel.ID != t.channel.ID || request.Channel.GatewayID != t.channel.GatewayID ||
		request.Channel.Protocol != t.channel.Protocol || request.Channel.Endpoint != t.channel.Endpoint || request.Channel.Revision != t.channel.Revision {
		return failed, fmt.Errorf("Modbus request does not match configured gateway")
	}
	if request.Executor != ExecutorName {
		return failed, fmt.Errorf("Modbus TCP adapter requires executor %q", ExecutorName)
	}
	op, err := DecodeOperation(request.Payload)
	if err != nil {
		return failed, err
	}
	// Bound direct adapter use too. The executor can impose a shorter deadline.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return failed, err
	}
	var transaction [2]byte
	if _, err := rand.Read(transaction[:]); err != nil {
		return failed, err
	}
	pdu := modbus.RequestPDU(op)
	frame := make([]byte, 7, 7+len(pdu))
	copy(frame[:2], transaction[:])
	binary.BigEndian.PutUint16(frame[4:6], uint16(1+len(pdu)))
	frame[6] = byte(op.UnitID)
	frame = append(frame, pdu...)
	t.conn, err = t.dial(ctx, "tcp", t.address)
	if err != nil {
		if ctx.Err() == nil {
			failed.RetryClass = db.RetryNotSent
		}
		return failed, fmt.Errorf("Modbus TCP dial: %w", err)
	}
	// Join the cancellation callback before returning. No I/O outlives Exchange.
	interrupted := make(chan struct{})
	var interruptErr error
	stop := context.AfterFunc(ctx, func() {
		interruptErr = t.conn.Close()
		close(interrupted)
	})
	defer func() {
		if !stop() {
			<-interrupted
			if !errors.Is(interruptErr, net.ErrClosed) {
				t.closeErr = errors.Join(t.closeErr, interruptErr)
			}
		}
	}()
	deadline, _ := ctx.Deadline()
	if err := t.conn.SetDeadline(deadline); err != nil {
		return failed, fmt.Errorf("Modbus TCP deadline: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return failed, err
	}
	n, err := t.conn.Write(frame)
	t.possibleSend = n > 0
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	if err != nil {
		status := db.AttemptFailed
		if t.possibleSend {
			status = db.AttemptUnknown
		}
		outcome := executor.Outcome{Status: status}
		if !t.possibleSend && ctx.Err() == nil {
			outcome.RetryClass = db.RetryNotSent
		}
		return outcome, errors.Join(fmt.Errorf("Modbus TCP request: %w", err), ctx.Err())
	}
	unknown := executor.Outcome{Status: db.AttemptUnknown}
	header := make([]byte, 7)
	if _, err := io.ReadFull(t.conn, header); err != nil {
		return unknown, errors.Join(fmt.Errorf("Modbus TCP response header: %w", err), ctx.Err())
	}
	length := binary.BigEndian.Uint16(header[4:6])
	if header[0] != transaction[0] || header[1] != transaction[1] || binary.BigEndian.Uint16(header[2:4]) != 0 ||
		header[6] != byte(op.UnitID) || length < 2 || length > 254 {
		return unknown, fmt.Errorf("mismatched or invalid Modbus TCP response header")
	}
	response := make([]byte, int(length)-1)
	if _, err := io.ReadFull(t.conn, response); err != nil {
		return unknown, errors.Join(fmt.Errorf("Modbus TCP response body: %w", err), ctx.Err())
	}
	result, status, terminal, responseErr := modbus.ParseResponse(op, response)
	t.terminalReply = terminal
	t.repliedAt = time.Now()
	data, err := json.Marshal(result)
	return executor.Outcome{Status: status, Data: data}, errors.Join(responseErr, err)
}

func (t *transport) Recover(ctx context.Context) error {
	if t.possibleSend && !t.terminalReply {
		// Neither a closed TCP session nor a quiet delay proves that a gateway
		// has stopped a downstream serial request or a late device response.
		return errors.Join(ErrQuiescence, ctx.Err())
	}
	if !t.possibleSend {
		return ctx.Err()
	}
	timer := time.NewTimer(time.Until(t.repliedAt.Add(t.turnaround)))
	defer timer.Stop()
	select {
	case <-timer.C:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *transport) Close(ctx context.Context) error {
	if !t.closed {
		t.closed = true
		if t.conn != nil {
			if err := t.conn.Close(); !errors.Is(err, net.ErrClosed) {
				t.closeErr = errors.Join(t.closeErr, err)
			}
		}
	}
	return errors.Join(t.closeErr, ctx.Err())
}
