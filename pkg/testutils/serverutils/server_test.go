package serverutils

import (
	"context"
	"testing"

	"github.com/duravela/djob/pkg/base"
)

type fakeServer struct {
	TestServerInterfaceRaw
	stops int
}

func (s *fakeServer) Stop(context.Context) { s.stops++ }

type fakeFactory struct{ server *fakeServer }

func (f fakeFactory) New(base.TestServerArgs) (any, error) { return f.server, nil }

func TestAdapterDelegatesStop(t *testing.T) {
	original := factory.value
	t.Cleanup(func() { InitTestServerFactory(original) })
	raw := new(fakeServer)
	InitTestServerFactory(fakeFactory{raw})
	s, err := NewServer(base.TestServerArgs{})
	if err != nil {
		t.Fatal(err)
	}
	s.Stop(context.Background())
	if raw.stops != 1 || s.ApplicationLayer() != raw {
		t.Fatal("adapter discarded lifecycle or application")
	}
}

func TestURLAddsPathWithoutMutatingBase(t *testing.T) {
	base := NewTestURL("http://127.0.0.1:1234/root?keep=1&replace=old")
	got := base.WithPath("/child?replace=new&extra=2")
	if got.String() != "http://127.0.0.1:1234/root/child?extra=2&keep=1&replace=new" {
		t.Fatal(got)
	}
	if base.String() != "http://127.0.0.1:1234/root?keep=1&replace=old" {
		t.Fatal("base was mutated", base)
	}
}
