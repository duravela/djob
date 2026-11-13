package node

import (
	"context"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/stretchr/testify/require"
)

func TestRaftExecutorKeepsBusOccupiedThroughCanceledExchangeCleanup(t *testing.T) {
	nodes := startChannelCluster(t)
	at := time.Now().UTC()
	proposeChannel(t, nodes[0], db.StateCommand{RequestID: "executor-configure", At: at,
		Configure: &db.ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "gateway:502"}})
	for _, id := range []string{"slave-3", "slave-7"} {
		proposeChannel(t, nodes[0], db.StateCommand{RequestID: "executor-create-" + id, At: at,
			CreateRun: &db.CreateRun{ID: id, ChannelID: "gateway", Executor: "fake", Payload: []byte{0, 0xff}}})
	}
	gateway := fake.New(fake.Options{})
	configuration := executor.Config{ChannelID: "gateway", LeaseDuration: 10 * time.Second, LeaseSafetyMargin: 2 * time.Second,
		RenewInterval: 2 * time.Second, CommitTimeout: time.Second, OperationTimeout: 2 * time.Second, RecoveryTimeout: time.Second, CloseTimeout: time.Second}
	e, err := executor.New(context.Background(), configuration, nodes[0].store, gateway.Factory)
	require.NoError(t, err)
	t.Cleanup(func() {
		stop := make(chan struct{})
		go func() {
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			for {
				gateway.ReleaseAll()
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		_ = e.Close(ctx)
		close(stop)
	})
	ctx, cancel := context.WithCancel(context.Background())
	firstReply := make(chan error, 1)
	go func() { _, err := e.Execute(ctx, "slave-3"); firstReply <- err }()
	var first *fake.Call
	select {
	case first = <-gateway.Calls:
	case <-time.After(3 * time.Second):
		t.Fatal("first exchange did not start")
	}
	secondReply := make(chan error, 1)
	go func() { _, err := e.Execute(context.Background(), "slave-7"); secondReply <- err }()
	require.Eventually(t, func() bool { return e.Pending() == 1 }, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-firstReply:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("caller was not canceled")
	}
	select {
	case <-first.RecoveryStarted:
	case <-time.After(time.Second):
		t.Fatal("recovery did not start")
	}
	require.NoError(t, first.RecoveryContext.Err())
	require.Eventually(t, func() bool {
		for _, node := range nodes {
			channel, err := node.store.GetChannel("gateway")
			if err != nil || channel.ActiveAttemptID != first.Request.AttemptID {
				return false
			}
			run, err := node.store.GetRun("slave-7")
			if err != nil || run.State != db.RunReady || run.AttemptCount != 0 {
				return false
			}
		}
		return true
	}, 3*time.Second, 10*time.Millisecond)
	other, err := executor.New(context.Background(), configuration, nodes[1].store, gateway.Factory)
	require.ErrorIs(t, err, executor.ErrOwnershipLost)
	require.Nil(t, other)
	require.Equal(t, 1, gateway.Stats().Started)
	first.Recover <- nil
	select {
	case <-first.CloseStarted:
	case <-time.After(time.Second):
		t.Fatal("close did not start")
	}
	select {
	case <-gateway.Calls:
		t.Fatal("next exchange bypassed close")
	default:
	}
	first.Close <- nil
	var second *fake.Call
	select {
	case second = <-gateway.Calls:
	case <-time.After(3 * time.Second):
		t.Fatal("second exchange did not start after committed cleanup")
	}
	require.Equal(t, "slave-7", second.Request.RunID)
	require.Equal(t, 1, gateway.Stats().MaxActive)
	second.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded, Data: []byte{0x12, 0x34}}}
	select {
	case <-second.RecoveryStarted:
	case <-time.After(time.Second):
		t.Fatal("second recovery did not start")
	}
	second.Recover <- nil
	select {
	case <-second.CloseStarted:
	case <-time.After(time.Second):
		t.Fatal("second close did not start")
	}
	second.Close <- nil
	select {
	case err := <-secondReply:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("second result was not committed")
	}
	require.Eventually(t, func() bool {
		for _, node := range nodes {
			firstRun, err := node.store.GetRun("slave-3")
			if err != nil || firstRun.State != db.RunUnknown {
				return false
			}
			attempt, err := node.store.GetAttempt(firstRun.LastAttemptID)
			if err != nil || !attempt.Quiescent || attempt.Error == "" {
				return false
			}
			secondRun, err := node.store.GetRun("slave-7")
			if err != nil || secondRun.State != db.RunSucceeded {
				return false
			}
		}
		return true
	}, 3*time.Second, 10*time.Millisecond)
}
