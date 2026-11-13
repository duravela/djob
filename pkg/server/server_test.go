package server

import (
	"context"
	"testing"

	"github.com/duravela/djob/pkg/base"
	"github.com/duravela/djob/pkg/testutils/serverutils"
	"github.com/duravela/djob/pkg/util/leaktest"
)

func TestSelfBootstrap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, err := serverutils.NewServer(base.TestServerArgs{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stopper().Stop(context.Background())
	ready := make(chan bool, 1)
	s.SetReadyFn(func(waitForInit bool) { ready <- waitForInit })
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case waitForInit := <-ready:
		if waitForInit {
			t.Fatal("started fixture incorrectly reports that it still needs bootstrap")
		}
	default:
		t.Fatal("fixture did not report readiness")
	}
}
