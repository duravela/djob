package node

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"
)

type peerClockReply struct {
	NodeID    uint64    `json:"node_id,string"`
	ClusterID uint64    `json:"cluster_id,string"`
	At        time.Time `json:"at"`
}

// PeerClockWithin requires a majority, including this member, whose complete
// request/response uncertainty intervals fit the configured skew budget.
// It checks relative time only, not UTC accuracy. All probe goroutines join.
func (rc *RaftNode) PeerClockWithin(ctx context.Context, skew time.Duration) bool {
	if skew <= 0 || ctx.Err() != nil {
		return false
	}
	voters := bootstrapVoters(*rc.raftCfg)
	configuration := ""
	if rc.raftCfg.DynamicMembership {
		membership := rc.Membership()
		voters, configuration = membership.Voters, membership.Configuration
		if len(voters) == 0 {
			return false
		}
	}
	if len(voters) == 1 && voters[0] == rc.raftCfg.ID {
		return true
	}
	results := make(chan bool, len(voters))
	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	probes := 0
	for i, peer := range rc.raftCfg.Peers {
		if uint64(i+1) == rc.raftCfg.ID || !slices.Contains(voters, uint64(i+1)) {
			continue
		}
		probes++
		go func(id uint64, peer string) {
			valid := false
			defer func() { results <- valid }()
			request, err := http.NewRequestWithContext(probeCtx, http.MethodGet, strings.TrimRight(peer, "/")+"/djob/clock", nil)
			if err != nil {
				return
			}
			before := rc.clockNow()
			response, err := rc.peerClockClient.Do(request)
			if err != nil {
				return
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				return
			}
			var reply peerClockReply
			decoder := json.NewDecoder(io.LimitReader(response.Body, 4096))
			if decoder.Decode(&reply) != nil {
				return
			}
			after := rc.clockNow()
			valid = reply.NodeID == id && reply.ClusterID == rc.raftCfg.ClusterID && !reply.At.IsZero() && !after.Before(before) && reply.At.Sub(before) <= skew && reply.At.Sub(after) >= -skew
		}(uint64(i+1), peer)
	}
	good := 0
	if slices.Contains(voters, rc.raftCfg.ID) {
		good = 1
	}
	for range probes {
		if <-results {
			good++
		}
		if good >= len(voters)/2+1 {
			cancel() // Join cancelled minority probes without consuming the parent deadline.
		}
	}
	return ctx.Err() == nil && good >= len(voters)/2+1 && (configuration == "" || configuration == rc.Membership().Configuration)
}
