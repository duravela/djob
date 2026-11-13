package e2e_test

import (
	"crypto/rand"
	"net/netip"
	"strings"

	dockernetwork "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

// Partition healing must restore the listener's original IP, not merely its
// DNS alias. Docker allows explicit endpoint addresses only on a network with
// an explicitly configured subnet. Randomize the isolated pool and let Docker
// reject overlaps, including races with another concurrently created fixture.
func newRaftNetwork(h *harness) *testcontainers.DockerNetwork {
	h.t.Helper()
	for range 16 {
		var random [2]byte
		if _, err := rand.Read(random[:]); err != nil {
			h.t.Fatal(err)
		}
		subnet := netip.PrefixFrom(netip.AddrFrom4([4]byte{10, 128 + random[0]%64, random[1], 0}), 24)
		nw, err := network.New(h.ctx, network.WithIPAM(&dockernetwork.IPAM{Config: []dockernetwork.IPAMConfig{{Subnet: subnet}}}))
		if err == nil {
			testcontainers.CleanupNetwork(h.t, nw)
			return nw
		}
		if !strings.Contains(strings.ToLower(err.Error()), "overlap") {
			h.t.Fatal(err)
		}
	}
	h.t.Fatal("could not allocate an isolated Raft fixture subnet")
	return nil
}
