package modbustcp

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/stretchr/testify/require"
)

func testChannel() db.CommunicationChannel {
	return db.CommunicationChannel{ID: "gateway", GatewayID: "gateway", Protocol: Protocol, Endpoint: "tcp://127.0.0.1:502?turnaround=5ms", Revision: 1}
}

func testRequest() executor.Request {
	return executor.Request{Channel: testChannel(), Executor: ExecutorName,
		Payload: payload(`"function":"read_holding_registers","address":0,"quantity":2`)}
}

func readFrame(conn net.Conn) ([]byte, error) {
	header := make([]byte, 7)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	length := int(binary.BigEndian.Uint16(header[4:6]))
	if length < 2 || length > 254 {
		return nil, fmt.Errorf("invalid request length %d", length)
	}
	body := make([]byte, length-1)
	_, err := io.ReadFull(conn, body)
	return append(header, body...), err
}

func replyFrame(request, pdu []byte) []byte {
	header := append([]byte(nil), request[:7]...)
	binary.BigEndian.PutUint16(header[4:6], uint16(1+len(pdu)))
	return append(header, pdu...)
}

// All adapter unit tests use net.Pipe or scripted connections. No real gateway
// is contacted. The public runtime integration test separately uses loopback TCP.
func pipeAdapter(t *testing.T, handler func(net.Conn) error) *transport {
	t.Helper()
	client, server := net.Pipe()
	require.NoError(t, server.SetDeadline(time.Now().Add(3*time.Second)))
	adapter, err := Factory(testChannel())
	require.NoError(t, err)
	tr := adapter.(*transport)
	tr.dial = func(context.Context, string, string) (net.Conn, error) { return client, nil }
	done := make(chan error, 1)
	go func() {
		defer server.Close()
		done <- handler(server)
	}()
	t.Cleanup(func() {
		_ = tr.Close(context.Background())
		_ = client.Close()
		_ = server.Close()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Error("simulator did not stop")
		}
	})
	return tr
}

func TestTCPFragmentedResponseAndTurnaround(t *testing.T) {
	tr := pipeAdapter(t, func(conn net.Conn) error {
		request, err := readFrame(conn)
		if err != nil {
			return err
		}
		if string(request[2:4]) != "\x00\x00" || request[6] != 7 || string(request[7:]) != "\x03\x00\x00\x00\x02" {
			return fmt.Errorf("incorrect MBAP or request PDU: %x", request)
		}
		for _, b := range replyFrame(request, []byte{3, 4, 0x12, 0x34, 0, 0}) {
			if _, err := conn.Write([]byte{b}); err != nil {
				return err
			}
		}
		return nil
	})
	tr.turnaround = 30 * time.Millisecond
	outcome, err := tr.Exchange(context.Background(), testRequest())
	require.NoError(t, err)
	require.Equal(t, db.AttemptSucceeded, outcome.Status)
	var result Result
	require.NoError(t, json.Unmarshal(outcome.Data, &result))
	require.Equal(t, []uint16{0x1234, 0}, result.Registers)
	require.NoError(t, tr.Recover(context.Background()))
	require.False(t, time.Now().Before(tr.repliedAt.Add(tr.turnaround)), "turnaround was skipped")
	require.NoError(t, tr.Close(context.Background()))
	_, err = tr.Exchange(context.Background(), testRequest())
	require.ErrorContains(t, err, "exactly one")
}

func TestMalformedMBAPAndTruncatedRepliesBlockReuse(t *testing.T) {
	for _, variant := range []string{"transaction", "protocol", "unit", "length-small", "length-large", "truncated-header", "truncated-body", "function", "byte-count", "echo"} {
		t.Run(variant, func(t *testing.T) {
			tr := pipeAdapter(t, func(conn net.Conn) error {
				request, err := readFrame(conn)
				if err != nil {
					return err
				}
				response := replyFrame(request, []byte{3, 4, 0, 42, 0, 0})
				switch variant {
				case "transaction":
					response[1] ^= 1
				case "protocol":
					response[3] = 1
				case "unit":
					response[6] = 8
				case "length-small":
					response[5] = 1
				case "length-large":
					response[4], response[5] = 0xff, 0xff
				case "truncated-header":
					response = response[:4]
				case "truncated-body":
					response = response[:9]
				case "function":
					response[7] = 4
				case "byte-count":
					response[8] = 2
				case "echo":
					response = replyFrame(request, []byte{6, 0, 0, 0, 41})
				}
				// net.Pipe writes block until consumed. Invalid headers deliberately
				// leave the body unread, so send only the rejected header there.
				if variant == "transaction" || variant == "protocol" || variant == "unit" || strings.HasPrefix(variant, "length-") {
					response = response[:7]
				}
				_, err = conn.Write(response)
				return err
			})
			request := testRequest()
			if variant == "echo" {
				request.Payload = payload(`"function":"write_single_register","address":0,"registers":[42]`)
			}
			outcome, err := tr.Exchange(context.Background(), request)
			require.Error(t, err)
			require.Equal(t, db.AttemptUnknown, outcome.Status)
			require.ErrorIs(t, tr.Recover(context.Background()), ErrQuiescence)
			require.NoError(t, tr.Close(context.Background()))
			// Closing the socket cannot turn an uncertain serial request safe.
			require.ErrorIs(t, tr.Recover(context.Background()), ErrQuiescence)
		})
	}
}

func TestExceptionReplyControlsQuiescence(t *testing.T) {
	for _, code := range []byte{1, 2, 3, 4, 5, 6, 8, 10, 11, 255} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			tr := pipeAdapter(t, func(conn net.Conn) error {
				request, err := readFrame(conn)
				if err != nil {
					return err
				}
				_, err = conn.Write(replyFrame(request, []byte{0x83, code}))
				return err
			})
			outcome, err := tr.Exchange(context.Background(), testRequest())
			var exception *Exception
			require.ErrorAs(t, err, &exception)
			var result Result
			require.NoError(t, json.Unmarshal(outcome.Data, &result))
			require.Equal(t, code, result.Exception.Code)
			if code >= 1 && code <= 3 {
				require.Equal(t, db.AttemptFailed, outcome.Status)
				require.NoError(t, tr.Recover(context.Background()))
			} else {
				require.Equal(t, db.AttemptUnknown, outcome.Status)
				require.ErrorIs(t, tr.Recover(context.Background()), ErrQuiescence)
			}
		})
	}
}

func TestCancellationStopsIOAndCannotConfirmDownstreamQuiescence(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprint(timeout), func(t *testing.T) {
			received := make(chan struct{})
			tr := pipeAdapter(t, func(conn net.Conn) error {
				if _, err := readFrame(conn); err != nil {
					return err
				}
				close(received)
				_, err := conn.Read(make([]byte, 1))
				if errors.Is(err, io.EOF) {
					return nil
				}
				return fmt.Errorf("expected closed client, got %v", err)
			})
			ctx, cancel := context.WithCancel(context.Background())
			if timeout {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 30*time.Millisecond)
			}
			defer cancel()
			type reply struct {
				outcome executor.Outcome
				err     error
			}
			done := make(chan reply, 1)
			go func() {
				outcome, err := tr.Exchange(ctx, testRequest())
				done <- reply{outcome, err}
			}()
			select {
			case <-received:
			case <-time.After(time.Second):
				t.Fatal("request was not sent")
			}
			if !timeout {
				cancel()
			}
			select {
			case response := <-done:
				require.Error(t, response.err)
				require.Equal(t, db.AttemptUnknown, response.outcome.Status)
				if !timeout {
					require.ErrorIs(t, response.err, context.Canceled)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation did not join exchange")
			}
			require.ErrorIs(t, tr.Recover(context.Background()), ErrQuiescence)
			require.NoError(t, tr.Close(context.Background()))
		})
	}
}

func TestInvalidRequestAndPriorCancellationNeverDial(t *testing.T) {
	for _, variant := range []string{"payload", "executor", "channel", "cancelled", "closed"} {
		t.Run(variant, func(t *testing.T) {
			adapter, err := Factory(testChannel())
			require.NoError(t, err)
			tr := adapter.(*transport)
			tr.dial = func(context.Context, string, string) (net.Conn, error) {
				t.Error("invalid or canceled operation performed I/O")
				return nil, errors.New("unexpected dial")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request := testRequest()
			switch variant {
			case "payload":
				request.Payload = []byte(`{"function":"write_single_register"}`)
			case "executor":
				request.Executor = "echo"
			case "channel":
				request.Channel.ID = "other"
			case "cancelled":
				cancel()
			case "closed":
				require.NoError(t, tr.Close(context.Background()))
			}
			outcome, err := tr.Exchange(ctx, request)
			require.Error(t, err)
			require.Equal(t, db.AttemptFailed, outcome.Status)
			require.NoError(t, tr.Recover(context.Background()))
			require.NoError(t, tr.Close(context.Background()))
		})
	}
}

type scriptedConn struct {
	n, writes int
	writeErr  error
	closeErr  error
}

func (*scriptedConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *scriptedConn) Write([]byte) (int, error) {
	c.writes++
	return c.n, c.writeErr
}
func (c *scriptedConn) Close() error                   { return c.closeErr }
func (*scriptedConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*scriptedConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*scriptedConn) SetDeadline(time.Time) error      { return nil }
func (*scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (*scriptedConn) SetWriteDeadline(time.Time) error { return nil }

func TestFailedDialAndRequestWritesAreNeverRetried(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    int
		err  error
	}{
		{"dial", 0, nil}, {"zero", 0, io.ErrClosedPipe}, {"partial", 3, io.ErrClosedPipe},
		{"short", 3, nil}, {"full-lost-reply", 12, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			adapter, err := Factory(testChannel())
			require.NoError(t, err)
			tr := adapter.(*transport)
			conn := &scriptedConn{n: tc.n, writeErr: tc.err}
			dials := 0
			tr.dial = func(context.Context, string, string) (net.Conn, error) {
				dials++
				if tc.name == "dial" {
					return nil, io.ErrClosedPipe
				}
				return conn, nil
			}
			outcome, err := tr.Exchange(context.Background(), testRequest())
			require.Error(t, err)
			require.Equal(t, 1, dials)
			if tc.name != "dial" {
				require.Equal(t, 1, conn.writes)
			}
			if tc.n == 0 {
				require.Equal(t, db.AttemptFailed, outcome.Status)
				require.Equal(t, db.RetryNotSent, outcome.RetryClass)
				require.NoError(t, tr.Recover(context.Background()))
			} else {
				require.Equal(t, db.AttemptUnknown, outcome.Status)
				require.Empty(t, outcome.RetryClass)
				require.ErrorIs(t, tr.Recover(context.Background()), ErrQuiescence)
			}
			require.NoError(t, tr.Close(context.Background()))
		})
	}
}

func TestRecoveryDeadlineAndCloseFailureRemainVisible(t *testing.T) {
	tr := pipeAdapter(t, func(conn net.Conn) error {
		request, err := readFrame(conn)
		if err != nil {
			return err
		}
		_, err = conn.Write(replyFrame(request, []byte{3, 4, 0, 42, 0, 0}))
		return err
	})
	tr.turnaround = time.Second
	outcome, err := tr.Exchange(context.Background(), testRequest())
	require.NoError(t, err)
	require.Equal(t, db.AttemptSucceeded, outcome.Status)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	require.ErrorIs(t, tr.Recover(ctx), context.DeadlineExceeded)
	require.NoError(t, tr.Close(context.Background()))

	closeErr := errors.New("close failed")
	adapter, err := Factory(testChannel())
	require.NoError(t, err)
	other := adapter.(*transport)
	other.conn = &scriptedConn{closeErr: closeErr}
	require.ErrorIs(t, other.Close(context.Background()), closeErr)
	require.ErrorIs(t, other.Close(context.Background()), closeErr)
}

func TestFactoryRequiresExplicitTransportAndTurnaround(t *testing.T) {
	for _, endpoint := range []string{
		"127.0.0.1:502", "tcp://127.0.0.1:502", "tcp://127.0.0.1:0?turnaround=5ms",
		"tcp://127.0.0.1:65536?turnaround=5ms", "tcp://127.0.0.1?turnaround=5ms", "tcp://:502?turnaround=5ms",
		"http://127.0.0.1:502?turnaround=5ms", "tcp://user:password@127.0.0.1:502?turnaround=5ms",
		"tcp://127.0.0.1:502/path?turnaround=5ms", "tcp://127.0.0.1:502?turnaround=5ms#unit7",
		"tcp://127.0.0.1:502?turnaround=0", "tcp://127.0.0.1:502?turnaround=-1ms",
		"tcp://127.0.0.1:502?turnaround=61s", "tcp://127.0.0.1:502?turnaround=abc",
		"tcp://127.0.0.1:502?turnaround=5ms&turnaround=10ms", "tcp://127.0.0.1:502?turnaround=5ms&retry=1",
		"tcp://127.0.0.1:502?turnaround=%zz",
	} {
		channel := testChannel()
		channel.Endpoint = endpoint
		adapter, err := Factory(channel)
		require.Error(t, err, endpoint)
		require.Nil(t, adapter)
	}
	channel := testChannel()
	channel.Protocol = "rtu-over-tcp"
	_, err := Factory(channel)
	require.Error(t, err)
	channel.Protocol, channel.Endpoint = Protocol, "tcp://[::1]:1502?turnaround=5ms"
	adapter, err := Factory(channel)
	require.NoError(t, err)
	require.Equal(t, "[::1]:1502", adapter.(*transport).address)
	// Factory is inert: a syntactically valid unreachable address is accepted.
	require.NoError(t, adapter.Close(context.Background()))
}

func unhex(t *testing.T, value string) []byte {
	t.Helper()
	data, err := hex.DecodeString(value)
	require.NoError(t, err)
	return data
}

func payload(fields string) []byte { return []byte(`{"version":1,"unit_id":7,` + fields + `}`) }
