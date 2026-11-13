package store

import (
	"fmt"

	"github.com/duravela/djob/pkg/db"
)

const maxProposalBytes = 8 << 20
const maxPendingProposalBytes = 32 << 20

// Bound encoded proposals awaiting handoff or acknowledgement. This does not
// include transient encoding allocations, Raft-owned copies or snapshot data.
func (s *Store) reserveProposal(size int) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if size > maxProposalBytes {
		return nil, &db.CommandRejection{Code: "invalid", Message: fmt.Sprintf("encoded proposal exceeds %d bytes", maxProposalBytes)}
	}
	if s.pendingProposalBytes+size > maxPendingProposalBytes {
		return nil, &db.CommandRejection{Code: "backpressure", Message: "pending proposal byte budget exhausted; retry the identical request"}
	}
	s.pendingProposalBytes += size
	return func() { s.mu.Lock(); s.pendingProposalBytes -= size; s.mu.Unlock() }, nil
}
