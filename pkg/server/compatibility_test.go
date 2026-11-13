package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/duravela/djob/pkg/server/serverctl"
)

func TestStopTriggerPreservesFirstForcedRequestAndEscalation(t *testing.T) {
	forced := serverctl.MakeShutdownRequest(serverctl.ShutdownReasonFatalError, errors.New("fatal"))
	graceful := serverctl.MakeShutdownRequest(serverctl.ShutdownReasonGracefulStopRequestedByOrchestration, nil)
	for _, first := range []serverctl.ShutdownRequest{forced, graceful} {
		s := newStopTrigger()
		s.signalStop(context.Background(), first)
		var workers sync.WaitGroup
		for range 20 {
			workers.Go(func() { s.signalStop(context.Background(), graceful); s.signalStop(context.Background(), forced) })
		}
		workers.Wait()
		if got := <-s.C(); got != first {
			t.Fatalf("lost first request: %v", got)
		}
		if first == graceful {
			if got := <-s.C(); got != forced {
				t.Fatalf("lost escalation: %v", got)
			}
		}
		select {
		case got := <-s.C():
			t.Fatalf("duplicate request: %v", got)
		default:
		}
	}
}

func TestHTTPHandlerPanicPolicy(t *testing.T) {
	for _, value := range []any{"private failure", http.ErrAbortHandler} {
		s := newHTTPServer(BaseConfig{})
		s.mux.HandleFunc("/", func(http.ResponseWriter, *http.Request) { panic(value) })
		w := httptest.NewRecorder()
		var caught any
		func() { defer func() { caught = recover() }(); s.baseHandler(w, httptest.NewRequest("GET", "/", nil)) }()
		if value == http.ErrAbortHandler {
			if caught != value {
				t.Fatal("abort panic was swallowed")
			}
		} else if caught != nil || w.Code != 500 || strings.Contains(w.Body.String(), "private failure") {
			t.Fatalf("panic policy: %v %d %s", caught, w.Code, w.Body.String())
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("missing cache policy")
		}
	}
}

func TestListenerAddressUpdateAndFailureCleanup(t *testing.T) {
	for _, value := range []struct{ bound, advertise, want string }{
		{"127.0.0.1:1234", "public.example:0", "public.example:1234"},
		{"[::1]:1234", "[::1]:5678", "[::1]:5678"},
	} {
		addr, advertise := "unused", value.advertise
		bound, err := net.ResolveTCPAddr("tcp", value.bound)
		if err != nil {
			t.Fatal(err)
		}
		if err = UpdateAddrs(context.Background(), &addr, &advertise, bound); err != nil || addr != value.bound || advertise != value.want {
			t.Fatalf("addresses: %s %s %v", addr, advertise, err)
		}
	}
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reservation.Addr().String()
	_ = reservation.Close()
	invalid := "invalid address"
	if listener, err := ListenAndUpdateAddrs(context.Background(), &addr, &invalid, "test", false); err == nil {
		_ = listener.Close()
		t.Fatal("invalid advertisement accepted")
	}
	again, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("failed setup leaked listener: %v", err)
	}
	_ = again.Close()
}
