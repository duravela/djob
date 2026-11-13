package rpc

import (
	"context"
	"errors"
	"testing"

	"github.com/duravela/djob/pkg/util/stop"
	"google.golang.org/grpc/connectivity"
)

func TestCompatibilityClientIsLocalAndOwned(t *testing.T) {
	owner := stop.NewStopper()
	defer owner.Stop(context.Background())
	c := NewContext(context.Background(), ContextOptions{Stopper: owner})
	for _, address := range []string{"192.0.2.1:1234", "remote.example:1234", "unix:///tmp/remote", "dns:///remote:1234"} {
		if conn, err := c.GRPCUnvalidatedDial(address).Connect(context.Background()); err == nil {
			_ = conn.Close()
			t.Fatalf("non-loopback address allowed: %s", address)
		}
	}
	conn, err := c.GRPCUnvalidatedDial("127.0.0.1:1234").Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	owner.Stop(context.Background())
	if conn.GetState() != connectivity.Shutdown {
		t.Fatal("connection survived owner shutdown")
	}
	if _, err = c.GRPCUnvalidatedDial("127.0.0.1:1234").Connect(context.Background()); !errors.Is(err, stop.ErrUnavailable) {
		t.Fatalf("shutdown owner allowed connection: %v", err)
	}
}
