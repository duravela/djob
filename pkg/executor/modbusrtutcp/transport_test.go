package modbusrtutcp

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/modbus"
	"github.com/stretchr/testify/require"
)

func channel() db.CommunicationChannel {
	return db.CommunicationChannel{ID: "gateway", GatewayID: "gateway", Protocol: Protocol, Endpoint: "tcp://127.0.0.1:502?baud=115200&turnaround=2ms", Revision: 1}
}

func request(fields string) executor.Request {
	return executor.Request{Channel: channel(), Executor: modbus.ExecutorName, Payload: []byte(`{"version":1,"unit_id":1,` + fields + `}`)}
}

func readRequest(conn net.Conn) ([]byte, error) {
	frame := make([]byte, 6, 256)
	if _, err := io.ReadFull(conn, frame); err != nil {
		return nil, err
	}
	size := 8
	if frame[1] == 15 || frame[1] == 16 {
		frame = frame[:7]
		if _, err := io.ReadFull(conn, frame[6:7]); err != nil {
			return nil, err
		}
		size = 9 + int(frame[6])
		if size > 256 {
			return nil, fmt.Errorf("oversized request")
		}
	}
	read := len(frame)
	frame = frame[:size]
	if _, err := io.ReadFull(conn, frame[read:]); err != nil {
		return nil, err
	}
	if crc16(frame[:size-2]) != binary.LittleEndian.Uint16(frame[size-2:]) {
		return nil, fmt.Errorf("request CRC mismatch")
	}
	return frame, nil
}

func response(unit byte, pdu []byte) []byte {
	frame := append([]byte{unit}, pdu...)
	return binary.LittleEndian.AppendUint16(frame, crc16(frame))
}

func adapter(t *testing.T, handle func(net.Conn) error) *transport {
	t.Helper()
	client, remote := net.Pipe()
	require.NoError(t, remote.SetDeadline(time.Now().Add(4*time.Second)))
	tr, err := Factory(channel())
	require.NoError(t, err)
	worker := tr.(*transport)
	worker.dial = func(context.Context, string, string) (net.Conn, error) { return client, nil }
	done := make(chan error, 1)
	go func() { defer remote.Close(); done <- handle(remote) }()
	t.Cleanup(func() {
		_ = worker.Close(context.Background())
		_ = client.Close()
		_ = remote.Close()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Error("RTU simulator did not stop")
		}
	})
	return worker
}

func TestCRCMatchesPublishedSerialExampleAndWireByteOrder(t *testing.T) {
	// Modbus Serial Line V1.02, Appendix B: 02 07 has CRC 1241, low byte first.
	require.Equal(t, uint16(0x1241), crc16([]byte{2, 7}))
	require.Equal(t, []byte{2, 7, 0x41, 0x12}, response(2, []byte{7}))
	frame, err := hex.DecodeString("01030000000ac5cd")
	require.NoError(t, err)
	require.Equal(t, binary.LittleEndian.Uint16(frame[6:]), crc16(frame[:6]))
}

func TestRTUFunctionsSurviveTCPFragmentationAndRetainQuietIntervals(t *testing.T) {
	for _, tc := range []struct{ fields, pdu, reply string }{
		{`"function":"read_coils","address":19,"quantity":10`, "010013000a", "0102cd01"},
		{`"function":"read_discrete_inputs","address":0,"quantity":8`, "0200000008", "020180"},
		{`"function":"read_holding_registers","address":0,"quantity":2`, "0300000002", "030412340000"},
		{`"function":"read_input_registers","address":65535,"quantity":1`, "04ffff0001", "0402ffff"},
		{`"function":"write_single_coil","address":7,"coils":[true]`, "050007ff00", "050007ff00"},
		{`"function":"write_single_register","address":65535,"registers":[0]`, "06ffff0000", "06ffff0000"},
		{`"function":"write_multiple_coils","address":19,"coils":[true,false,true,true,false,false,true,true,true,false]`, "0f0013000a02cd01", "0f0013000a"},
		{`"function":"write_multiple_registers","address":1,"registers":[4660,65535]`, "1000010002041234ffff", "1000010002"},
	} {
		t.Run(tc.pdu, func(t *testing.T) {
			var received time.Time
			worker := adapter(t, func(conn net.Conn) error {
				frame, err := readRequest(conn)
				if err != nil {
					return err
				}
				received = time.Now()
				if frame[0] != 1 || hex.EncodeToString(frame[1:len(frame)-2]) != tc.pdu {
					return fmt.Errorf("incorrect RTU request: %x", frame)
				}
				pdu, err := hex.DecodeString(tc.reply)
				if err != nil {
					return err
				}
				for _, b := range response(1, pdu) {
					if _, err := conn.Write([]byte{b}); err != nil {
						return err
					}
					// TCP gaps exceed serial t1.5. Only the bridge can measure and
					// enforce byte timing on its actual serial link.
					time.Sleep(time.Millisecond)
				}
				return nil
			})
			began := time.Now()
			outcome, err := worker.Exchange(context.Background(), request(tc.fields))
			require.NoError(t, err)
			require.Equal(t, db.AttemptSucceeded, outcome.Status)
			require.GreaterOrEqual(t, received.Sub(began), worker.turnaround)
			var result modbus.Result
			require.NoError(t, json.Unmarshal(outcome.Data, &result))
			require.EqualValues(t, 1, result.UnitID)
			require.NoError(t, worker.Recover(context.Background()))
			require.False(t, time.Now().Before(worker.repliedAt.Add(worker.turnaround)))
			require.NoError(t, worker.Close(context.Background()))
			_, err = worker.Exchange(context.Background(), request(tc.fields))
			require.Error(t, err, "one transport cannot send another transaction")
		})
	}
}

func TestRTUResponseBoundAndPaddingAreChecked(t *testing.T) {
	worker := adapter(t, func(conn net.Conn) error {
		if _, err := readRequest(conn); err != nil {
			return err
		}
		_, err := conn.Write(response(1, append([]byte{1, 250}, make([]byte, 250)...)))
		return err
	})
	outcome, err := worker.Exchange(context.Background(), request(`"function":"read_coils","address":0,"quantity":2000`))
	require.NoError(t, err)
	require.Equal(t, db.AttemptSucceeded, outcome.Status)
	var result modbus.Result
	require.NoError(t, json.Unmarshal(outcome.Data, &result))
	require.Len(t, result.Coils, 2000)
}

func TestRTUBadFramesNeverPermitGatewayReuse(t *testing.T) {
	for _, variant := range []string{"unit", "function", "byte-count", "crc", "crc-order", "truncated-header", "truncated-body", "echo", "padding"} {
		t.Run(variant, func(t *testing.T) {
			worker := adapter(t, func(conn net.Conn) error {
				if _, err := readRequest(conn); err != nil {
					return err
				}
				frame := response(1, []byte{3, 2, 0, 42})
				switch variant {
				case "unit":
					frame[0], frame = 2, frame[:3]
				case "function":
					frame[1], frame = 4, frame[:3]
				case "byte-count":
					frame[2], frame = 255, frame[:3]
				case "crc":
					frame[len(frame)-1] ^= 1
				case "crc-order":
					frame[len(frame)-1], frame[len(frame)-2] = frame[len(frame)-2], frame[len(frame)-1]
				case "truncated-header":
					frame = frame[:2]
				case "truncated-body":
					frame = frame[:len(frame)-1]
				case "echo":
					frame = response(1, []byte{6, 0, 1, 0, 42})
				case "padding":
					frame = response(1, []byte{1, 1, 0x80})
				}
				_, err := conn.Write(frame)
				return err
			})
			r := request(`"function":"read_holding_registers","address":0,"quantity":1`)
			if variant == "echo" {
				r = request(`"function":"write_single_register","address":0,"registers":[42]`)
			} else if variant == "padding" {
				r = request(`"function":"read_coils","address":0,"quantity":1`)
			}
			outcome, err := worker.Exchange(context.Background(), r)
			require.Error(t, err)
			require.Equal(t, db.AttemptUnknown, outcome.Status)
			require.Empty(t, outcome.RetryClass)
			require.ErrorIs(t, worker.Recover(context.Background()), ErrQuiescence)
			require.NoError(t, worker.Close(context.Background()))
			require.ErrorIs(t, worker.Recover(context.Background()), ErrQuiescence, "a closed TCP session does not prove downstream completion")
		})
	}
}

func TestRTUExceptionsPreserveConservativeEffectClassification(t *testing.T) {
	for _, code := range []byte{0, 1, 2, 3, 4, 5, 6, 8, 10, 11, 255} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			worker := adapter(t, func(conn net.Conn) error {
				if _, err := readRequest(conn); err != nil {
					return err
				}
				_, err := conn.Write(response(1, []byte{0x83, code}))
				return err
			})
			outcome, err := worker.Exchange(context.Background(), request(`"function":"read_holding_registers","address":0,"quantity":1`))
			var exception *modbus.Exception
			require.ErrorAs(t, err, &exception)
			require.Equal(t, code, exception.Code)
			if code >= 1 && code <= 3 {
				require.Equal(t, db.AttemptFailed, outcome.Status)
				require.NoError(t, worker.Recover(context.Background()))
			} else {
				require.Equal(t, db.AttemptUnknown, outcome.Status)
				require.ErrorIs(t, worker.Recover(context.Background()), ErrQuiescence)
			}
		})
	}
}

type scriptedConn struct {
	n, writes int
	writeErr  error
	read      func([]byte) (int, error)
	close     func() error
}

func (c *scriptedConn) Read(b []byte) (int, error) {
	if c.read != nil {
		return c.read(b)
	}
	return 0, io.EOF
}
func (c *scriptedConn) Write([]byte) (int, error) { c.writes++; return c.n, c.writeErr }
func (c *scriptedConn) Close() error {
	if c.close != nil {
		return c.close()
	}
	return nil
}
func (*scriptedConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*scriptedConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*scriptedConn) SetDeadline(time.Time) error      { return nil }
func (*scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (*scriptedConn) SetWriteDeadline(time.Time) error { return nil }

func TestRTUNeverRetriesInsideExchangeAndOnlyZeroBytesCanRequestRetry(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    int
		err  error
	}{{"dial", 0, nil}, {"zero", 0, io.ErrClosedPipe}, {"partial", 3, io.ErrClosedPipe}, {"short", 3, nil}, {"lost-reply", 8, nil}} {
		t.Run(tc.name, func(t *testing.T) {
			tr, err := Factory(channel())
			require.NoError(t, err)
			worker := tr.(*transport)
			conn, dials := &scriptedConn{n: tc.n, writeErr: tc.err}, 0
			worker.dial = func(context.Context, string, string) (net.Conn, error) {
				dials++
				if tc.name == "dial" {
					return nil, io.ErrClosedPipe
				}
				return conn, nil
			}
			outcome, err := worker.Exchange(context.Background(), request(`"function":"read_holding_registers","address":0,"quantity":1`))
			require.Error(t, err)
			require.Equal(t, 1, dials)
			if tc.name != "dial" {
				require.Equal(t, 1, conn.writes)
			}
			if tc.n == 0 {
				require.Equal(t, db.AttemptFailed, outcome.Status)
				require.Equal(t, db.RetryNotSent, outcome.RetryClass)
				require.NoError(t, worker.Recover(context.Background()))
			} else {
				require.Equal(t, db.AttemptUnknown, outcome.Status)
				require.Empty(t, outcome.RetryClass)
				require.ErrorIs(t, worker.Recover(context.Background()), ErrQuiescence)
			}
			require.NoError(t, worker.Close(context.Background()))
		})
	}
}

func TestRTUCancellationJoinsCloseCallbackBeforeReturning(t *testing.T) {
	reading, closeStarted, unblockRead, releaseClose := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var closeOnce, releaseOnce sync.Once
	conn := &scriptedConn{n: 8, read: func([]byte) (int, error) { close(reading); <-unblockRead; return 0, net.ErrClosed },
		close: func() error {
			closeOnce.Do(func() { close(unblockRead); close(closeStarted) })
			<-releaseClose
			return nil
		}}
	tr, err := Factory(channel())
	require.NoError(t, err)
	worker := tr.(*transport)
	worker.dial = func(context.Context, string, string) (net.Conn, error) { return conn, nil }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := worker.Exchange(ctx, request(`"function":"read_holding_registers","address":0,"quantity":1`))
		done <- err
	}()
	t.Cleanup(func() { cancel(); releaseOnce.Do(func() { close(releaseClose) }) })
	select {
	case <-reading:
	case <-time.After(time.Second):
		t.Fatal("exchange did not reach its response read")
	}
	cancel()
	select {
	case <-closeStarted:
	case <-time.After(time.Second):
		t.Fatal("cancellation did not close the connection")
	}
	select {
	case <-done:
		t.Fatal("exchange detached the unfinished cancellation callback")
	default:
	}
	releaseOnce.Do(func() { close(releaseClose) })
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("exchange did not join cancellation")
	}
	require.ErrorIs(t, worker.Recover(context.Background()), ErrQuiescence)
	require.NoError(t, worker.Close(context.Background()))
}

func TestRTUDeadlineDuringInitialQuietPeriodDoesNotSend(t *testing.T) {
	c := channel()
	c.Endpoint = "tcp://127.0.0.1:502?baud=9600&turnaround=1s"
	tr, err := Factory(c)
	require.NoError(t, err)
	worker := tr.(*transport)
	conn := &scriptedConn{n: 8}
	worker.dial = func(context.Context, string, string) (net.Conn, error) { return conn, nil }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	r := request(`"function":"read_holding_registers","address":0,"quantity":1`)
	r.Channel = c
	outcome, err := worker.Exchange(ctx, r)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, db.AttemptFailed, outcome.Status)
	require.Empty(t, outcome.RetryClass)
	require.Zero(t, conn.writes, "cancellation during the initial quiet period must prevent delivery")
	require.NoError(t, worker.Recover(context.Background()))
	require.NoError(t, worker.Close(context.Background()))
}

func TestRTURecoveryDeadlineAndCloseFailureRemainVisible(t *testing.T) {
	closeErr := errors.New("bridge socket could not be closed")
	reader := bytes.NewReader(response(1, []byte{3, 2, 0, 42}))
	conn := &scriptedConn{n: 8, read: reader.Read, close: func() error { return closeErr }}
	tr, err := Factory(channel())
	require.NoError(t, err)
	worker := tr.(*transport)
	worker.dial = func(context.Context, string, string) (net.Conn, error) { return conn, nil }
	outcome, err := worker.Exchange(context.Background(), request(`"function":"read_holding_registers","address":0,"quantity":1`))
	require.NoError(t, err)
	require.Equal(t, db.AttemptSucceeded, outcome.Status)
	worker.turnaround = time.Second
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	require.ErrorIs(t, worker.Recover(ctx), context.DeadlineExceeded)
	require.ErrorIs(t, worker.Close(context.Background()), closeErr)
	require.ErrorIs(t, worker.Close(context.Background()), closeErr, "a repeated close must retain the failed cleanup evidence")
}

func TestRTUFactoryRequiresMatchingBaudAndSufficientTurnaround(t *testing.T) {
	for _, endpoint := range []string{
		"127.0.0.1:502", "tcp://127.0.0.1:502?turnaround=5ms", "tcp://127.0.0.1:502?baud=9600",
		"tcp://127.0.0.1:0?baud=9600&turnaround=5ms", "tcp://127.0.0.1:65536?baud=9600&turnaround=5ms",
		"tcp://:502?baud=9600&turnaround=5ms", "tcp://127.0.0.1?baud=9600&turnaround=5ms",
		"http://127.0.0.1:502?baud=9600&turnaround=5ms", "tcp://user:secret@127.0.0.1:502?baud=9600&turnaround=5ms",
		"tcp://127.0.0.1:502/path?baud=9600&turnaround=5ms", "tcp://127.0.0.1:502?baud=9600&turnaround=5ms#unit",
		"tcp://127.0.0.1:502?baud=0&turnaround=5ms", "tcp://127.0.0.1:502?baud=-1&turnaround=5ms",
		"tcp://127.0.0.1:502?baud=4294967296&turnaround=5ms", "tcp://127.0.0.1:502?baud=9600&baud=9600&turnaround=5ms",
		"tcp://127.0.0.1:502?baud=9600&turnaround=5ms&turnaround=5ms", "tcp://127.0.0.1:502?baud=9600&turnaround=5ms&retry=1",
		"tcp://127.0.0.1:502?baud=9600&turnaround=4.010416ms", "tcp://127.0.0.1:502?baud=19200&turnaround=2.005208ms",
		"tcp://127.0.0.1:502?baud=19201&turnaround=1.749ms", "tcp://127.0.0.1:502?baud=9600&turnaround=61s",
		"tcp://127.0.0.1:502?baud=9600&turnaround=%zz",
	} {
		c := channel()
		c.Endpoint = endpoint
		_, err := Factory(c)
		require.Error(t, err, endpoint)
	}
	for _, params := range []string{"baud=9600&turnaround=4.010417ms", "baud=19200&turnaround=2.005209ms", "baud=19201&turnaround=1.75ms", "baud=300&turnaround=129ms"} {
		c := channel()
		c.Endpoint = "tcp://127.0.0.1:502?" + params
		tr, err := Factory(c)
		require.NoError(t, err)
		require.NoError(t, tr.Close(context.Background()))
	}
	for _, variant := range []string{"protocol", "canonical"} {
		c := channel()
		if variant == "protocol" {
			c.Protocol = "modbus-tcp"
		} else {
			c.GatewayID = "another-bus"
		}
		_, err := Factory(c)
		require.Error(t, err)
	}
}

func TestRTUInvalidInputAndPriorCancellationNeverDial(t *testing.T) {
	for _, variant := range []string{"payload", "broadcast", "executor", "channel", "revision", "endpoint", "cancelled", "closed"} {
		t.Run(variant, func(t *testing.T) {
			tr, err := Factory(channel())
			require.NoError(t, err)
			worker := tr.(*transport)
			worker.dial = func(context.Context, string, string) (net.Conn, error) {
				t.Error("invalid request performed I/O")
				return nil, io.ErrClosedPipe
			}
			r := request(`"function":"read_holding_registers","address":0,"quantity":1`)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch variant {
			case "payload":
				r.Payload = []byte("{}")
			case "broadcast":
				r.Payload = []byte(strings.Replace(string(r.Payload), `"unit_id":1`, `"unit_id":0`, 1))
			case "executor":
				r.Executor = "echo"
			case "channel":
				r.Channel.ID = "other"
			case "revision":
				r.Channel.Revision++
			case "endpoint":
				r.Channel.Endpoint += "&extra=1"
			case "cancelled":
				cancel()
			case "closed":
				require.NoError(t, worker.Close(context.Background()))
			}
			outcome, err := worker.Exchange(ctx, r)
			require.Error(t, err)
			require.Equal(t, db.AttemptFailed, outcome.Status)
			require.NoError(t, worker.Recover(context.Background()))
			require.NoError(t, worker.Close(context.Background()))
		})
	}
}
