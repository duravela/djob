package store

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestProposalBudgetRejectsAndReleases(t *testing.T) {
	s := &Store{}
	_, err := s.reserveProposal(maxProposalBytes + 1)
	require.ErrorContains(t, err, "invalid")
	releases := make([]func(), 4)
	for i := range releases {
		releases[i], err = s.reserveProposal(maxProposalBytes)
		require.NoError(t, err)
	}
	_, err = s.reserveProposal(1)
	require.ErrorContains(t, err, "backpressure")
	releases[0]()
	release, err := s.reserveProposal(maxProposalBytes)
	require.NoError(t, err)
	release()
	for _, release := range releases[1:] {
		release()
	}
	require.Zero(t, s.pendingProposalBytes)
}
