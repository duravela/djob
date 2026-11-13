// Test-only Modbus gateway with independent wire framing and fault controls.
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type gateway struct {
	corrupt atomic.Bool
	hold    atomic.Bool
	mu      sync.Mutex
	stats   struct {
		Requests int   `json:"requests"`
		Active   int   `json:"active"`
		Maximum  int   `json:"maximum"`
		Units    []int `json:"units"`
	}
}

func main() {
	rtu := flag.Bool("rtu", false, "Use RTU framing")
	inspect := flag.Bool("process-stats", false, "Print Linux PID 1 resource counters and exit")
	highWater := flag.Bool("process-high-water", false, "Print Linux PID 1 and cgroup memory peaks and exit")
	flag.Parse()
	if *highWater {
		if err := processHighWater(); err != nil {
			panic(err)
		}
		return
	}
	if *inspect {
		if err := processStats(); err != nil {
			panic(err)
		}
		return
	}
	g := &gateway{}
	ln, err := net.Listen("tcp", ":1502")
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go g.exchange(conn, *rtu)
		}
	}()
	http.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		g.mu.Lock()
		defer g.mu.Unlock()
		_ = json.NewEncoder(w).Encode(g.stats)
	})
	http.HandleFunc("POST /corrupt-next", func(w http.ResponseWriter, _ *http.Request) {
		g.corrupt.Store(true)
		w.WriteHeader(http.StatusNoContent)
	})
	http.HandleFunc("POST /hold-next", func(w http.ResponseWriter, _ *http.Request) {
		g.hold.Store(true)
		w.WriteHeader(http.StatusNoContent)
	})
	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}

func (g *gateway) exchange(conn net.Conn, rtu bool) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	size, offset := 12, 7
	if rtu {
		size, offset = 8, 1
	}
	frame := make([]byte, size)
	if _, err := io.ReadFull(conn, frame); err != nil {
		return // A TCP readiness probe need not send a request.
	}
	if (rtu && crc(frame[:6]) != binary.LittleEndian.Uint16(frame[6:])) ||
		(!rtu && (binary.BigEndian.Uint16(frame[2:4]) != 0 || binary.BigEndian.Uint16(frame[4:6]) != 6)) {
		return
	}
	pdu := frame[offset:]
	unit := frame[offset-1]
	quantity := binary.BigEndian.Uint16(pdu[3:5])
	if unit < 1 || unit > 247 || pdu[0] != 3 || quantity < 1 || quantity > 125 {
		return
	}
	g.mu.Lock()
	g.stats.Requests++
	g.stats.Active++
	g.stats.Maximum = max(g.stats.Maximum, g.stats.Active)
	if len(g.stats.Units) < 1000 {
		g.stats.Units = append(g.stats.Units, int(unit))
	}
	g.mu.Unlock()
	defer func() { g.mu.Lock(); g.stats.Active--; g.mu.Unlock() }()
	// Make overlapping transactions observable, rather than replying instantly.
	time.Sleep(30 * time.Millisecond)
	if g.hold.Swap(false) {
		time.Sleep(5 * time.Second)
	}
	reply := []byte{3, byte(quantity * 2)}
	for range quantity {
		reply = append(reply, 0, 42)
	}
	corrupt := g.corrupt.Swap(false)
	if rtu {
		reply = append([]byte{unit}, reply...)
		reply = binary.LittleEndian.AppendUint16(reply, crc(reply))
		if corrupt {
			reply[len(reply)-1] ^= 1
		}
	} else {
		header := append([]byte(nil), frame[:7]...)
		binary.BigEndian.PutUint16(header[4:6], uint16(len(reply)+1))
		if corrupt {
			header[0] ^= 1 // Mismatched transaction ID, not a valid rejection.
		}
		reply = append(header, reply...)
	}
	// Fragment responses independently of transport packet boundaries.
	for _, b := range reply {
		if _, err := conn.Write([]byte{b}); err != nil {
			fmt.Println("reply interrupted:", err)
			return
		}
	}
}

func crc(data []byte) uint16 {
	value := uint16(0xffff)
	for _, b := range data {
		value ^= uint16(b)
		for range 8 {
			if value&1 != 0 {
				value = value>>1 ^ 0xa001
			} else {
				value >>= 1
			}
		}
	}
	return value
}
