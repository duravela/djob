package cli_test

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type gateway struct {
	ln       net.Listener
	rtu      bool
	calls    chan *wireCall
	problems chan error
	stop     chan struct{}
	accepted chan struct{}
	conns    sync.Map
	workers  sync.WaitGroup
	requests atomic.Int32
	active   atomic.Int32
	maximum  atomic.Int32
}

type wireCall struct {
	frame []byte
	rtu   bool
	reply chan []byte
}

func newGateway(t *testing.T, rtu bool) *gateway {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := &gateway{ln: ln, rtu: rtu, calls: make(chan *wireCall, 16), problems: make(chan error, 16), stop: make(chan struct{}), accepted: make(chan struct{})}
	go func() {
		defer close(g.accepted)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			g.conns.Store(conn, true)
			g.workers.Add(1)
			go func() {
				defer g.workers.Done()
				defer g.conns.Delete(conn)
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
				frame, err := readFrame(conn, rtu)
				if err != nil {
					if err != io.EOF {
						select {
						case g.problems <- err:
						case <-g.stop:
						}
					}
					return
				}
				g.requests.Add(1)
				active := g.active.Add(1)
				defer g.active.Add(-1)
				for old := g.maximum.Load(); active > old; old = g.maximum.Load() {
					if g.maximum.CompareAndSwap(old, active) {
						break
					}
				}
				call := &wireCall{frame: frame, rtu: rtu, reply: make(chan []byte, 1)}
				select {
				case g.calls <- call:
				case <-g.stop:
					return
				}
				select {
				case reply := <-call.reply:
					if rtu {
						for _, b := range reply {
							if _, err = conn.Write([]byte{b}); err != nil {
								break
							}
						}
					} else {
						_, err = conn.Write(reply)
					}
					if err != nil {
						select {
						case g.problems <- err:
						case <-g.stop:
						}
					}
				case <-g.stop:
				}
			}()
		}
	}()
	t.Cleanup(func() {
		close(g.stop)
		_ = g.ln.Close()
		<-g.accepted
		g.conns.Range(func(conn, _ any) bool { _ = conn.(net.Conn).Close(); return true })
		g.workers.Wait()
	})
	return g
}

func readFrame(conn net.Conn, rtu bool) ([]byte, error) {
	frame := make([]byte, 7, 260)
	if _, err := io.ReadFull(conn, frame); err != nil {
		return nil, err
	}
	size := int(binary.BigEndian.Uint16(frame[4:6])) + 6
	if rtu {
		size = 8
		if frame[1] == 15 || frame[1] == 16 {
			size = 9 + int(frame[6])
		}
	}
	if size < 8 || size > 260 || (rtu && size > 256) {
		return nil, fmt.Errorf("invalid frame size %d", size)
	}
	frame = frame[:size]
	if _, err := io.ReadFull(conn, frame[7:]); err != nil {
		return nil, err
	}
	if rtu {
		if crc16(frame[:size-2]) != binary.LittleEndian.Uint16(frame[size-2:]) {
			return nil, fmt.Errorf("invalid request CRC: %x", frame)
		}
	} else if binary.BigEndian.Uint16(frame[2:4]) != 0 {
		return nil, fmt.Errorf("invalid MBAP protocol: %x", frame)
	}
	return frame, nil
}

func crc16(frame []byte) uint16 {
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

func (g *gateway) endpoint() string {
	query := "?turnaround=5ms"
	if g.rtu {
		query += "&baud=9600"
	}
	return "tcp://" + g.ln.Addr().String() + query
}

func (g *gateway) next(t *testing.T) *wireCall {
	t.Helper()
	select {
	case call := <-g.calls:
		return call
	case err := <-g.problems:
		t.Fatalf("gateway wire error: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("gateway request did not arrive")
	}
	return nil
}

func (call *wireCall) unit() byte {
	if call.rtu {
		return call.frame[0]
	}
	return call.frame[6]
}

func (call *wireCall) respond(t *testing.T, corruptCRC bool) {
	t.Helper()
	pdu := call.frame[7:]
	if call.rtu {
		pdu = call.frame[1 : len(call.frame)-2]
	}
	var reply []byte
	switch pdu[0] {
	case 3:
		count := int(binary.BigEndian.Uint16(pdu[3:5]))
		reply = []byte{3, byte(2 * count)}
		for range count {
			reply = append(reply, 0, 42)
		}
	case 16:
		reply = append([]byte(nil), pdu[:5]...)
	default:
		t.Fatalf("unexpected simulated function %d", pdu[0])
	}
	if call.rtu {
		reply = append([]byte{call.unit()}, reply...)
		reply = binary.LittleEndian.AppendUint16(reply, crc16(reply))
		if corruptCRC {
			reply[len(reply)-1] ^= 1
		}
	} else {
		header := append([]byte(nil), call.frame[:7]...)
		binary.BigEndian.PutUint16(header[4:6], uint16(len(reply)+1))
		reply = append(header, reply...)
	}
	call.reply <- reply
}

func (g *gateway) check(t *testing.T, requests int32) {
	t.Helper()
	if got := g.requests.Load(); got != requests {
		t.Fatalf("gateway requests: got %d, want %d", got, requests)
	}
	if maximum := g.maximum.Load(); maximum != 1 {
		t.Fatalf("gateway transaction overlap: maximum %d", maximum)
	}
	select {
	case err := <-g.problems:
		t.Fatal(err)
	default:
	}
}
