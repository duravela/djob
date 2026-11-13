package store

import (
	"context"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

type stateReply struct {
	result db.StateResult
	err    error
}

func proposeStateAsync(s *Store, ctx context.Context, command db.StateCommand) <-chan stateReply {
	reply := make(chan stateReply, 1)
	go func() {
		result, err := s.ProposeStateCommand(ctx, command)
		reply <- stateReply{result: result, err: err}
	}()
	return reply
}

func receiveProposal(t *testing.T, proposals <-chan []byte) []byte {
	t.Helper()
	select {
	case data := <-proposals:
		return data
	case <-time.After(time.Second):
		t.Fatal("proposal was not sent")
		return nil
	}
}

func receiveStateReply(t *testing.T, replies <-chan stateReply) stateReply {
	t.Helper()
	select {
	case reply := <-replies:
		return reply
	case <-time.After(3 * time.Second):
		t.Fatal("state request did not finish")
		return stateReply{}
	}
}

func TestStateWaitersMatchExactRequestsWithinOneCommitBatch(t *testing.T) {
	s, proposals, commits := newTestJobQueue(t)
	command := db.StateCommand{RequestID: "same-ID", At: time.Now().UTC(), Configure: &db.ConfigureChannel{
		ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "first:502",
	}}
	first := proposeStateAsync(s, context.Background(), command)
	firstData := receiveProposal(t, proposals)
	changed := command
	configuration := *command.Configure
	configuration.Endpoint = "conflicting:502"
	changed.Configure = &configuration
	second := proposeStateAsync(s, context.Background(), changed)
	secondData := receiveProposal(t, proposals)
	duplicate := proposeStateAsync(s, context.Background(), command)
	duplicateData := receiveProposal(t, proposals)
	applyCommit(t, commits, &commit.Commit{Entries: []commit.Entry{
		{Index: 10, Data: firstData}, {Index: 11, Data: secondData}, {Index: 12, Data: duplicateData},
	}})
	originalReply := receiveStateReply(t, first)
	require.NoError(t, originalReply.err)
	conflictReply := receiveStateReply(t, second)
	require.Error(t, conflictReply.err)
	require.Equal(t, "request_conflict", conflictReply.result.Rejection.Code)
	duplicateReply := receiveStateReply(t, duplicate)
	require.NoError(t, duplicateReply.err)
	require.Equal(t, originalReply.result, duplicateReply.result)
	originalReply.result.Channel.Endpoint = "caller mutation"
	require.Equal(t, "first:502", duplicateReply.result.Channel.Endpoint)
	stored, err := s.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, "first:502", stored.Endpoint)
	s.mu.RLock()
	require.Empty(t, s.pendingState)
	s.mu.RUnlock()
	select {
	case <-s.stopC:
		t.Fatal("expected rejection stopped the store")
	default:
	}
}

func TestStateRetryAfterLostReplyUsesDurableReceipt(t *testing.T) {
	s, proposals, commits := newTestJobQueue(t)
	command := db.StateCommand{RequestID: "lost-reply", At: time.Now().UTC(), Configure: &db.ConfigureChannel{
		ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "gateway:502",
	}}
	ctx, cancel := context.WithCancel(context.Background())
	first := proposeStateAsync(s, ctx, command)
	data := receiveProposal(t, proposals)
	cancel()
	require.ErrorIs(t, receiveStateReply(t, first).err, context.Canceled)
	applyCommit(t, commits, &commit.Commit{Entries: []commit.Entry{{Index: 3, Data: data}}})
	retry := proposeStateAsync(s, context.Background(), command)
	data = receiveProposal(t, proposals)
	applyCommit(t, commits, &commit.Commit{Entries: []commit.Entry{{Index: 4, Data: data}}})
	reply := receiveStateReply(t, retry)
	require.NoError(t, reply.err)
	require.Equal(t, uint64(1), reply.result.Channel.Revision)
	require.Equal(t, uint64(4), s.LastAppliedIndex())
	s.mu.RLock()
	require.Empty(t, s.pendingState)
	s.mu.RUnlock()
}

func TestInvalidStateEnvelopeIsNotProposed(t *testing.T) {
	s, proposals, _ := newTestJobQueue(t)
	_, err := s.ProposeStateCommand(context.Background(), db.StateCommand{RequestID: "no-operation", At: time.Now()})
	require.Error(t, err)
	select {
	case <-proposals:
		t.Fatal("invalid envelope was proposed")
	default:
	}
	require.Zero(t, s.LastAppliedIndex())
}
