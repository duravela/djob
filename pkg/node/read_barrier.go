package node

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"

	"go.etcd.io/raft/v3"
)

const MaxPendingReadBarriers = 32

var (
	ErrReadBarrierUnavailable = errors.New("Raft read barrier unavailable")
	ErrReadBarrierBusy        = errors.New("Raft read barrier capacity exhausted")
	ErrReadBarrierChanged     = errors.New("Raft leadership changed during read barrier")
	ErrReadBarrierUnsafe      = errors.New("Raft read barriers require ReadOnlySafe")
)

// ReadBarrierResult is confirmed by the quorum and acknowledged by the local
// durable application. It is a point-in-time observation, not execution authority.
type ReadBarrierResult struct {
	ConfirmedIndex uint64
	AppliedIndex   uint64
}
type readBarrierReply struct {
	result ReadBarrierResult
	err    error
}
type pendingReadBarrier struct {
	done      chan readBarrierReply
	confirmed bool
	index     uint64
}
type readBarrierState struct {
	mu           sync.Mutex
	leader, term uint64
	pending      map[string]*pendingReadBarrier
}

// ReadBarrier submits a fresh ReadIndex without appending a log entry or receipt.
// Cancelled accepted probes retain their slot until confirmation or leadership
// change: dropping them on timeout would leave unbounded requests inside Raft.
func (rc *RaftNode) ReadBarrier(ctx context.Context) (ReadBarrierResult, error) {
	if err := ctx.Err(); err != nil {
		return ReadBarrierResult{}, err
	}
	if rc.raftCfg == nil || rc.raftCfg.Config == nil {
		return ReadBarrierResult{}, ErrReadBarrierUnavailable
	}
	if rc.raftCfg.ReadOnlyOption != raft.ReadOnlySafe {
		return ReadBarrierResult{}, ErrReadBarrierUnsafe
	}
	select {
	case <-rc.raftReady:
	case <-rc.stopc:
		return ReadBarrierResult{}, ErrReadBarrierUnavailable
	case <-ctx.Done():
		return ReadBarrierResult{}, ctx.Err()
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return ReadBarrierResult{}, err
	}
	key := string(nonce[:])
	pending := &pendingReadBarrier{done: make(chan readBarrierReply, 1)}
	state := &rc.readBarriers
	state.mu.Lock()
	select {
	case <-rc.stopc:
		state.mu.Unlock()
		return ReadBarrierResult{}, ErrReadBarrierUnavailable
	default:
	}
	if state.leader == 0 {
		state.mu.Unlock()
		return ReadBarrierResult{}, ErrReadBarrierUnavailable
	}
	if len(state.pending) >= MaxPendingReadBarriers {
		state.mu.Unlock()
		return ReadBarrierResult{}, ErrReadBarrierBusy
	}
	if state.pending == nil {
		state.pending = make(map[string]*pendingReadBarrier)
	}
	if _, exists := state.pending[key]; exists {
		state.mu.Unlock()
		return ReadBarrierResult{}, ErrReadBarrierBusy
	}
	state.pending[key] = pending
	state.mu.Unlock()
	if err := rc.node.ReadIndex(ctx, nonce[:]); err != nil {
		// A failed submission never entered Raft and can release its slot now.
		state.mu.Lock()
		if state.pending[key] == pending {
			delete(state.pending, key)
		}
		state.mu.Unlock()
		return ReadBarrierResult{}, err
	}
	select {
	case reply := <-pending.done:
		return reply.result, reply.err
	case <-ctx.Done():
		return ReadBarrierResult{}, ctx.Err()
	case <-rc.stopc:
		return ReadBarrierResult{}, ErrReadBarrierUnavailable
	}
}

func (s *readBarrierState) failLocked(err error) {
	for key, p := range s.pending {
		p.done <- readBarrierReply{err: err}
		delete(s.pending, key)
	}
}
func (rc *RaftNode) stopReadBarriers() {
	state := &rc.readBarriers
	state.mu.Lock()
	defer state.mu.Unlock()
	state.leader = 0
	state.failLocked(ErrReadBarrierUnavailable)
}
func (rc *RaftNode) updateReadBarrierLeadership(rd raft.Ready) {
	state := &rc.readBarriers
	state.mu.Lock()
	defer state.mu.Unlock()
	changed := false
	if rd.SoftState != nil && state.leader != rd.SoftState.Lead {
		state.leader = rd.SoftState.Lead
		changed = true
	}
	if rd.HardState.Term != 0 && state.term != rd.HardState.Term {
		state.term = rd.HardState.Term
		changed = true
	}
	if changed {
		state.failLocked(ErrReadBarrierChanged)
	}
}

// Only the Raft application loop calls this, after snapshot/entry application
// acknowledgements. A confirmed index ahead of the local application stays pending.
func (rc *RaftNode) completeReadBarriers(states []raft.ReadState) {
	state := &rc.readBarriers
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, read := range states {
		if p := state.pending[string(read.RequestCtx)]; p != nil {
			if !p.confirmed || read.Index > p.index {
				p.index = read.Index
			}
			p.confirmed = true
		}
	}
	for key, p := range state.pending {
		if p.confirmed && p.index <= rc.appliedIndex {
			p.done <- readBarrierReply{result: ReadBarrierResult{ConfirmedIndex: p.index, AppliedIndex: rc.appliedIndex}}
			delete(state.pending, key)
		}
	}
}
