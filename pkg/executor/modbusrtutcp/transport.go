// Package modbusrtutcp exchanges raw RTU frames through a transparent TCP/serial
// bridge. The executor must own the entire gateway, across all downstream units.
package modbusrtutcp

import (
	"context"
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

const Protocol = "modbus-rtu-tcp"

var ErrQuiescence = errors.New("RTU downstream exchange has not been confirmed complete")

type transport struct {
	channel       db.CommunicationChannel
	address       string
	turnaround    time.Duration
	dial          func(context.Context, string, string) (net.Conn, error)
	conn          net.Conn
	used, closed  bool
	possibleSend  bool
	terminalReply bool
	repliedAt     time.Time
	closeErr      error
}

// Factory validates and allocates without I/O. The endpoint must declare the
// bridge's serial baud rate and a turnaround at least the RTU t3.5 minimum:
// tcp://host:port?baud=9600&turnaround=5ms. The bridge must suppress TX echo and
// enforce serial framing/timing; TCP packet timing cannot establish either.
func Factory(channel db.CommunicationChannel) (executor.Transport, error) {
	if channel.ID == "" || channel.ID != channel.GatewayID || channel.Protocol != Protocol {
		return nil, fmt.Errorf("RTU TCP adapter requires a canonical gateway channel with protocol %q", Protocol)
	}
	u, err := url.Parse(channel.Endpoint)
	if err != nil || u.Scheme != "tcp" || u.User != nil || u.Hostname() == "" || u.Path != "" || u.Fragment != "" || u.Opaque != "" {
		return nil, fmt.Errorf("RTU TCP endpoint must be tcp://host:port?baud=rate&turnaround=duration")
	}
	port, err := strconv.ParseUint(u.Port(), 10, 16)
	if err != nil || port == 0 {
		return nil, fmt.Errorf("RTU TCP endpoint requires an explicit port in 1..65535")
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil || len(query) != 2 || len(query["baud"]) != 1 || len(query["turnaround"]) != 1 {
		return nil, fmt.Errorf("RTU TCP endpoint requires exactly one baud and turnaround parameter")
	}
	baud, err := strconv.ParseUint(query.Get("baud"), 10, 32)
	if err != nil || baud == 0 {
		return nil, fmt.Errorf("RTU TCP baud must be a positive 32-bit rate matching the bridge")
	}
	turnaround, err := time.ParseDuration(query.Get("turnaround"))
	if err != nil || turnaround < minimumTurnaround(baud) || turnaround > time.Minute {
		return nil, fmt.Errorf("RTU TCP turnaround must be at least %s for baud %d and at most 1m", minimumTurnaround(baud), baud)
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	return &transport{channel: channel, address: net.JoinHostPort(u.Hostname(), u.Port()), turnaround: turnaround, dial: dialer.DialContext}, nil
}

func minimumTurnaround(baud uint64) time.Duration {
	if baud > 19200 {
		return 1750 * time.Microsecond
	}
	// RTU characters contain 11 bits. Round 3.5 character times upward so the
	// duration never undershoots the serial specification's minimum.
	return time.Duration((38500000000 + baud - 1) / baud)
}

func crc16(data []byte) uint16 {
	crc := uint16(0xffff)
	for _, b := range data {
		crc ^= uint16(b)
		for range 8 {
			if crc&1 != 0 {
				crc = crc>>1 ^ 0xa001
			} else {
				crc >>= 1
			}
		}
	}
	return crc
}

func (t *transport) Exchange(ctx context.Context, request executor.Request) (executor.Outcome, error) {
	failed := executor.Outcome{Status: db.AttemptFailed}
	if t.used || t.closed {
		return failed, fmt.Errorf("RTU TCP transport permits exactly one exchange")
	}
	t.used = true
	if request.Channel.ID != t.channel.ID || request.Channel.GatewayID != t.channel.GatewayID ||
		request.Channel.Protocol != t.channel.Protocol || request.Channel.Endpoint != t.channel.Endpoint || request.Channel.Revision != t.channel.Revision || request.Executor != modbus.ExecutorName {
		return failed, fmt.Errorf("RTU request does not match the configured gateway and Modbus executor")
	}
	op, err := modbus.DecodeOperation(request.Payload)
	if err != nil {
		return failed, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return failed, err
	}
	pdu := modbus.RequestPDU(op)
	frame := append([]byte{byte(op.UnitID)}, pdu...)
	frame = binary.LittleEndian.AppendUint16(frame, crc16(frame))
	t.conn, err = t.dial(ctx, "tcp", t.address)
	if err != nil {
		if ctx.Err() == nil {
			failed.RetryClass = db.RetryNotSent
		}
		return failed, fmt.Errorf("RTU TCP dial: %w", err)
	}
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
		return failed, fmt.Errorf("RTU TCP deadline: %w", err)
	}
	// Observe the initial inter-frame interval locally before the first send.
	// This delay cannot prove the remote bus idle or enforce its byte timing.
	if err := waitUntil(ctx, time.Now().Add(t.turnaround)); err != nil {
		return failed, err
	}
	n, err := t.conn.Write(frame)
	t.possibleSend = n > 0
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	if err != nil {
		if t.possibleSend {
			failed.Status = db.AttemptUnknown
		} else if ctx.Err() == nil {
			failed.RetryClass = db.RetryNotSent
		}
		return failed, errors.Join(fmt.Errorf("RTU TCP request: %w", err), ctx.Err())
	}
	unknown := executor.Outcome{Status: db.AttemptUnknown}
	response, err := readResponse(t.conn, op, pdu[0])
	if err != nil {
		return unknown, errors.Join(err, ctx.Err())
	}
	result, status, terminal, responseErr := modbus.ParseResponse(op, response[1:len(response)-2])
	t.terminalReply, t.repliedAt = terminal, time.Now()
	data, err := json.Marshal(result)
	return executor.Outcome{Status: status, Data: data}, errors.Join(responseErr, err)
}

func readResponse(reader io.Reader, op modbus.Operation, function byte) ([]byte, error) {
	frame := make([]byte, 3, 256)
	if _, err := io.ReadFull(reader, frame); err != nil {
		return nil, fmt.Errorf("RTU response header: %w", err)
	}
	if frame[0] != byte(op.UnitID) || (frame[1] != function && frame[1] != function|0x80) {
		return nil, fmt.Errorf("mismatched RTU unit or function")
	}
	size := 8 // Write acknowledgements echo a five-byte PDU.
	if frame[1] == function|0x80 {
		size = 5
	} else if function >= 1 && function <= 4 {
		count := 2 * int(op.Quantity)
		if function <= 2 {
			count = (int(op.Quantity) + 7) / 8
		}
		if int(frame[2]) != count || count > 250 {
			return nil, fmt.Errorf("mismatched RTU response byte count")
		}
		size = 5 + count
	}
	frame = frame[:size]
	if _, err := io.ReadFull(reader, frame[3:]); err != nil {
		return nil, fmt.Errorf("RTU response body: %w", err)
	}
	if crc16(frame[:len(frame)-2]) != binary.LittleEndian.Uint16(frame[len(frame)-2:]) {
		return nil, fmt.Errorf("invalid RTU response CRC")
	}
	return frame, nil
}

func waitUntil(ctx context.Context, until time.Time) error {
	timer := time.NewTimer(time.Until(until))
	defer timer.Stop()
	select {
	case <-timer.C:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *transport) Recover(ctx context.Context) error {
	if t.possibleSend && !t.terminalReply {
		return errors.Join(ErrQuiescence, ctx.Err())
	}
	if !t.possibleSend {
		return ctx.Err()
	}
	return waitUntil(ctx, t.repliedAt.Add(t.turnaround))
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
