package server

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/modbusrtutcp"
	"github.com/duravela/djob/pkg/executor/modbustcp"
	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/stretchr/testify/require"
)

type modbusCall struct {
	rtu        bool
	frame      []byte
	conn       net.Conn
	receivedAt time.Time
	closed     chan struct{}
}

type modbusSimulator struct {
	rtu      bool
	listener net.Listener
	calls    chan *modbusCall
	done     chan struct{}
	accepted chan struct{}
	conns    sync.Map
	workers  sync.WaitGroup
}

func startModbusSimulator(t *testing.T) *modbusSimulator {
	t.Helper()
	return startModbusSimulatorAt(t, "127.0.0.1:0")
}

func startModbusSimulatorAt(t *testing.T, address string) *modbusSimulator {
	t.Helper()
	return startModbusSimulatorFraming(t, address, false)
}

func startModbusSimulatorFraming(t *testing.T, address string, rtu bool) *modbusSimulator {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	require.NoError(t, err)
	sim := &modbusSimulator{rtu: rtu, listener: listener, calls: make(chan *modbusCall, 8), done: make(chan struct{}), accepted: make(chan struct{})}
	go func() {
		defer close(sim.accepted)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			sim.conns.Store(conn, true)
			sim.workers.Add(1)
			go func() {
				defer sim.workers.Done()
				defer sim.conns.Delete(conn)
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(8 * time.Second))
				frame, err := readModbusFrame(conn, rtu)
				if err != nil {
					return
				}
				call := &modbusCall{rtu: rtu, frame: frame, conn: conn, receivedAt: time.Now(), closed: make(chan struct{})}
				defer close(call.closed)
				select {
				case sim.calls <- call:
				case <-sim.done:
					return
				}
				// The test writes a response concurrently. Keep this connection
				// alive and observe the adapter closing it after recovery.
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()
	t.Cleanup(func() {
		close(sim.done)
		_ = listener.Close()
		<-sim.accepted
		sim.conns.Range(func(key, _ any) bool { _ = key.(net.Conn).Close(); return true })
		sim.workers.Wait()
	})
	return sim
}

func (s *modbusSimulator) endpoint(turnaround time.Duration) string {
	if s.rtu {
		return "tcp://" + s.listener.Addr().String() + "?baud=115200&turnaround=" + max(turnaround, 2*time.Millisecond).String()
	}
	return "tcp://" + s.listener.Addr().String() + "?turnaround=" + turnaround.String()
}

func (s *modbusSimulator) next(t *testing.T) *modbusCall {
	t.Helper()
	select {
	case call := <-s.calls:
		return call
	case <-time.After(4 * time.Second):
		t.Fatal("Modbus request did not arrive")
		return nil
	}
}

func (c *modbusCall) reply(t *testing.T, pdu []byte) time.Time {
	t.Helper()
	_, err := c.conn.Write(c.responseFrame(pdu))
	require.NoError(t, err)
	return time.Now()
}

func (c *modbusCall) responseFrame(pdu []byte) []byte {
	if c.rtu {
		frame := append([]byte{c.unit()}, pdu...)
		return binary.LittleEndian.AppendUint16(frame, serialCRC(frame))
	}
	frame := append([]byte(nil), c.frame[:7]...)
	binary.BigEndian.PutUint16(frame[4:6], uint16(1+len(pdu)))
	return append(frame, pdu...)
}

func (c *modbusCall) unit() byte {
	if c.rtu {
		return c.frame[0]
	}
	return c.frame[6]
}

func (c *modbusCall) pdu() []byte {
	if c.rtu {
		return c.frame[1 : len(c.frame)-2]
	}
	return c.frame[7:]
}

func readModbusFrame(conn net.Conn, rtu bool) ([]byte, error) {
	if !rtu {
		header := make([]byte, 7)
		if _, err := io.ReadFull(conn, header); err != nil {
			return nil, err
		}
		length := int(binary.BigEndian.Uint16(header[4:6]))
		if length < 2 || length > 254 {
			return nil, fmt.Errorf("invalid MBAP length")
		}
		body := make([]byte, length-1)
		_, err := io.ReadFull(conn, body)
		return append(header, body...), err
	}
	frame := make([]byte, 7, 256)
	if _, err := io.ReadFull(conn, frame); err != nil {
		return nil, err
	}
	size := 8
	if frame[1] == 15 || frame[1] == 16 {
		size = 9 + int(frame[6])
		if size > 256 {
			return nil, fmt.Errorf("oversized RTU frame")
		}
	}
	frame = frame[:size]
	if _, err := io.ReadFull(conn, frame[7:]); err != nil {
		return nil, err
	}
	if serialCRC(frame[:size-2]) != binary.LittleEndian.Uint16(frame[size-2:]) {
		return nil, fmt.Errorf("RTU request CRC mismatch")
	}
	return frame, nil
}

func serialCRC(frame []byte) uint16 {
	crc := uint16(0xffff)
	for _, b := range frame {
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

func configureModbus(t *testing.T, client pb.SchedulerClient, id, endpoint string) {
	t.Helper()
	configureModbusProtocol(t, client, id, modbustcp.Protocol, endpoint)
}

func configureModbusProtocol(t *testing.T, client pb.SchedulerClient, id, protocol, endpoint string) {
	t.Helper()
	_, err := client.ConfigureChannel(context.Background(), &pb.ConfigureChannelRequest{Command: meta("configure/" + id),
		Id: id, GatewayId: id, Protocol: protocol, Endpoint: endpoint})
	require.NoError(t, err)
}

func modbusRun(id, channel string, unit int, write bool) *pb.CreateRunRequest {
	fields := `"function":"read_holding_registers","address":0,"quantity":2`
	if write {
		fields = `"function":"write_multiple_registers","address":1,"registers":[4660,0]`
	}
	return &pb.CreateRunRequest{Command: meta("create/" + id), Id: id, ChannelId: channel, Executor: modbustcp.ExecutorName,
		Payload: []byte(fmt.Sprintf(`{"version":1,"unit_id":%d,%s}`, unit, fields))}
}

func awaitModbusRun(t *testing.T, client pb.SchedulerClient, id, state string) *pb.Run {
	t.Helper()
	var run *pb.Run
	require.Eventually(t, func() bool {
		response, err := client.GetRun(context.Background(), &pb.GetRunRequest{Id: id})
		if err != nil || response.Run.State != state {
			return false
		}
		run = response.Run
		return true
	}, 4*time.Second, 5*time.Millisecond)
	return run
}

func TestModbusTCPPublicRuntimeSerializesUnitsAndRecoversResults(t *testing.T) {
	testModbusPublicRuntimeSerializesUnitsAndRecoversResults(t, false)
}

func TestModbusRTUTCPPublicRuntimeSerializesUnitsAndRecoversResults(t *testing.T) {
	testModbusPublicRuntimeSerializesUnitsAndRecoversResults(t, true)
}

func testModbusPublicRuntimeSerializesUnitsAndRecoversResults(t *testing.T, rtu bool) {
	gateway, independent := startModbusSimulatorFraming(t, "127.0.0.1:0", rtu), startModbusSimulatorFraming(t, "127.0.0.1:0", rtu)
	cfg := apiTestConfig(t.TempDir())
	cfg.TransportFactory = modbustcp.Factory
	protocol := modbustcp.Protocol
	if rtu {
		cfg.TransportFactory, protocol = modbusrtutcp.Factory, modbusrtutcp.Protocol
	}
	s := startAPI(t, cfg)
	client := apiClient(t, s)
	turnaround := 150 * time.Millisecond
	configureModbusProtocol(t, client, "gateway", protocol, gateway.endpoint(turnaround))
	configureModbusProtocol(t, client, "other", protocol, independent.endpoint(time.Millisecond))
	firstRequest := modbusRun("read-unit-3", "gateway", 3, false)
	_, err := client.CreateRun(context.Background(), firstRequest)
	require.NoError(t, err)
	first := gateway.next(t)
	require.Equal(t, byte(3), first.unit())
	for _, request := range []*pb.CreateRunRequest{modbusRun("write-unit-7", "gateway", 7, true), modbusRun("independent", "other", 4, false)} {
		_, err = client.CreateRun(context.Background(), request)
		require.NoError(t, err)
	}
	other := independent.next(t)
	require.Equal(t, byte(4), other.unit(), "independent gateways can progress while the first bus waits")
	select {
	case <-gateway.calls:
		t.Fatal("a different unit bypassed its active gateway")
	default:
	}
	other.reply(t, []byte{3, 4, 0, 1, 0, 2})
	repliedAt := first.reply(t, []byte{3, 4, 0x12, 0x34, 0, 0})
	second := gateway.next(t)
	require.Equal(t, byte(7), second.unit())
	require.GreaterOrEqual(t, second.receivedAt.Sub(repliedAt), turnaround-time.Millisecond)
	require.Equal(t, []byte{16, 0, 1, 0, 2, 4, 0x12, 0x34, 0, 0}, second.pdu())
	second.reply(t, []byte{16, 0, 1, 0, 2})
	for _, id := range []string{"read-unit-3", "write-unit-7", "independent"} {
		run := awaitModbusRun(t, client, id, "succeeded")
		require.EqualValues(t, 1, run.AttemptCount)
		response, err := client.GetAttempt(context.Background(), &pb.GetAttemptRequest{Id: run.LastAttemptId})
		require.NoError(t, err)
		require.True(t, response.Attempt.Quiescent)
		var result modbustcp.Result
		require.NoError(t, json.Unmarshal(response.Attempt.Result, &result))
		if id != "independent" {
			require.Equal(t, []uint16{0x1234, 0}, result.Registers)
		}
	}
	_, err = client.CreateRun(context.Background(), firstRequest)
	require.NoError(t, err, "exact retry must not repeat physical I/O")
	require.NoError(t, s.Close(context.Background()))
	restarted := startAPI(t, cfg)
	client = apiClient(t, restarted)
	for _, id := range []string{"read-unit-3", "write-unit-7", "independent"} {
		awaitModbusRun(t, client, id, "succeeded")
	}
	_, err = client.CreateRun(context.Background(), modbusRun("after-restart", "gateway", 9, false))
	require.NoError(t, err)
	third := gateway.next(t)
	require.Equal(t, byte(9), third.unit(), "completed requests were replayed after restart")
	third.reply(t, []byte{3, 4, 0, 1, 0, 2})
	awaitModbusRun(t, client, "after-restart", "succeeded")
}

func TestModbusTCPUncertainDeliveryBlocksGatewayAcrossRestart(t *testing.T) {
	testModbusUncertainDeliveryBlocksGatewayAcrossRestart(t, false)
}

func TestModbusRTUTCPUncertainDeliveryBlocksGatewayAcrossRestart(t *testing.T) {
	testModbusUncertainDeliveryBlocksGatewayAcrossRestart(t, true)
}

func testModbusUncertainDeliveryBlocksGatewayAcrossRestart(t *testing.T, rtu bool) {
	variants := []string{"lost-reply", "cancelled", "gateway-target-timeout"}
	if rtu {
		variants = append(variants, "bad-crc")
	}
	for _, variant := range variants {
		t.Run(variant, func(t *testing.T) {
			gateway := startModbusSimulatorFraming(t, "127.0.0.1:0", rtu)
			cfg := apiTestConfig(t.TempDir())
			cfg.TransportFactory = modbustcp.Factory
			protocol := modbustcp.Protocol
			if rtu {
				cfg.TransportFactory, protocol = modbusrtutcp.Factory, modbusrtutcp.Protocol
			}
			start := func() *Server {
				s, err := Start(context.Background(), cfg)
				require.NoError(t, err)
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					err := s.Close(ctx)
					require.True(t, err == nil || errors.Is(err, executor.ErrRecovery), "shutdown: %v", err)
				})
				return s
			}
			s := start()
			client := apiClient(t, s)
			configureModbusProtocol(t, client, "gateway", protocol, gateway.endpoint(time.Millisecond))
			request := modbusRun("uncertain", "gateway", 3, true)
			request.RetryPolicy = publicRetryPolicy(time.Millisecond)
			_, err := client.CreateRun(context.Background(), request)
			require.NoError(t, err)
			call := gateway.next(t)
			_, err = client.CreateRun(context.Background(), modbusRun("waiting", "gateway", 7, false))
			require.NoError(t, err)
			switch variant {
			case "cancelled":
				_, err = client.CancelRun(context.Background(), &pb.CancelRunRequest{Command: meta("cancel"), Id: "uncertain", Reason: "stop active write"})
				require.NoError(t, err)
			case "gateway-target-timeout":
				call.reply(t, []byte{0x90, 11})
			case "bad-crc":
				frame := call.responseFrame([]byte{16, 0, 1, 0, 2})
				frame[len(frame)-1] ^= 1
				_, err = call.conn.Write(frame)
				require.NoError(t, err)
			default:
				require.NoError(t, call.conn.Close())
			}
			run := awaitModbusRun(t, client, "uncertain", "unknown")
			require.Equal(t, variant == "cancelled", run.CancelRequested)
			require.Nil(t, run.RetryAt, "unknown effects must not gain automatic retries")
			attempt, err := client.GetAttempt(context.Background(), &pb.GetAttemptRequest{Id: run.LastAttemptId})
			require.NoError(t, err)
			require.False(t, attempt.Attempt.Quiescent)
			require.False(t, attempt.Attempt.NoDelivery)
			require.Empty(t, attempt.Attempt.RetryClass)
			require.NotEmpty(t, attempt.Attempt.Error)
			channel, err := client.GetChannel(context.Background(), &pb.GetChannelRequest{Id: "gateway"})
			require.NoError(t, err)
			require.Equal(t, "blocked", channel.Channel.State)
			waiting := awaitModbusRun(t, client, "waiting", "ready")
			require.Zero(t, waiting.AttemptCount)
			err = s.Close(context.Background())
			require.True(t, err == nil || errors.Is(err, executor.ErrRecovery), "%v", err)
			restarted := start()
			client = apiClient(t, restarted)
			awaitModbusRun(t, client, "uncertain", "unknown")
			channel, err = client.GetChannel(context.Background(), &pb.GetChannelRequest{Id: "gateway"})
			require.NoError(t, err)
			require.Equal(t, "blocked", channel.Channel.State)
			// Exercise repeated ready scans after recovery; blocked work is not
			// claimed and a new socket is not permission to use the same bus.
			for range 3 {
				_, err = restarted.dispatcher.Tick(context.Background())
				require.NoError(t, err)
			}
			waiting = awaitModbusRun(t, client, "waiting", "ready")
			require.Zero(t, waiting.AttemptCount)
			select {
			case <-gateway.calls:
				t.Fatal("uncertain downstream transaction was automatically retried or bypassed")
			default:
			}
		})
	}
}
