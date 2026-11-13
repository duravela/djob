package node

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/duravela/djob/pkg/jobs"
	"github.com/duravela/djob/pkg/scheduler"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/pkg/v3/types"
)

type exitAfterRemoteEffect struct{ endpoint string }

func (d *exitAfterRemoteEffect) Execute(ctx context.Context, _ jobs.Action) jobs.Effect {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, d.endpoint, nil)
	if err == nil {
		var reply *http.Response
		reply, err = (&http.Client{Timeout: time.Second}).Do(request)
		if err == nil {
			_ = reply.Body.Close()
			if reply.StatusCode == http.StatusAccepted {
				// The remote process performed the effect, but the worker has
				// not returned to record CompleteJobAction or clean up access.
				os.Exit(0)
			}
		}
	}
	return jobs.Effect{Status: db.EffectUnknown, Error: "test remote request failed"}
}
func (*exitAfterRemoteEffect) Quiesce(context.Context) error { return nil }
func (*exitAfterRemoteEffect) Close(context.Context) error   { return nil }

func TestCrashAfterRemoteEffectBeforeDurableResultNeverReplaysAction(t *testing.T) {
	const helperEnv = "DJOB_TEST_EXIT_AFTER_REMOTE_EFFECT"
	registry := func(endpoint string) jobs.Registry {
		return jobs.Registry{
			Handlers: map[jobs.Version]jobs.Handler{{Name: "write", Version: "1"}: func(jobs.View) (jobs.Decision, error) {
				return jobs.Decision{Kind: db.JobDoAction, Operation: "write"}, nil
			}},
			Drivers: map[jobs.Version]jobs.DriverFactory{{Name: "remote", Version: "1"}: func(db.JobRun) (jobs.Driver, error) {
				return &exitAfterRemoteEffect{endpoint: endpoint}, nil
			}},
		}
	}
	if directory := os.Getenv(helperEnv); directory != "" {
		n := startDurableNode(t, directory, 1000)
		proposeChannel(t, n, db.StateCommand{RequestID: "effect-channel", At: time.Now().UTC(), Configure: &db.ConfigureChannel{ID: "bus", GatewayID: "bus", Protocol: "fake", Endpoint: "bus"}})
		for i, command := range []db.JobCommand{
			{PutResource: &db.PutJobResource{Spec: db.ResourceSpec{ID: "bus", ChannelID: "bus", Duplex: "half", Capacity: 1}}},
			{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "tenant"}}},
			{Grant: &db.PutResourceGrant{Grant: db.ResourceGrant{TenantID: "tenant", ResourceID: "bus", Allowed: true}}},
			{PutBinding: &db.PutJobBinding{Binding: db.JobBinding{TenantID: "tenant", ID: "remote", Driver: "remote", DriverVersion: "1", Resources: []string{"bus"}}}},
			{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "tenant", ID: "write", Runtime: db.StepRuntime, Handler: "write", HandlerVersion: "1", BindingID: "remote"}}},
			{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: "write", DefinitionRevision: 1, RunID: "write"}},
		} {
			genericProposal(t, n, "effect-setup/"+string(rune('a'+i)), command)
		}
		worker, err := jobs.NewWorker(n.store, jobs.Config{LeaseDuration: time.Second, SafetyMargin: 200 * time.Millisecond, RenewInterval: 100 * time.Millisecond, CommitTimeout: 200 * time.Millisecond}, registry(os.Getenv("DJOB_TEST_REMOTE_EFFECT_URL")))
		require.NoError(t, err)
		_, err = worker.Execute(context.Background(), "tenant", "write")
		t.Fatalf("test process failed to exit at the effect boundary: %v", err)
	}
	var calls atomic.Int32
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer remote.Close()
	directory := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCrashAfterRemoteEffectBeforeDurableResultNeverReplaysAction$", "-test.timeout=12s")
	child.Env = append(os.Environ(), helperEnv+"="+directory, "DJOB_TEST_REMOTE_EFFECT_URL="+remote.URL)
	output, err := child.CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.EqualValues(t, 1, calls.Load())
	n := startDurableNode(t, directory, 1000)
	run, err := n.store.GetJobRun("tenant", "write")
	require.NoError(t, err)
	require.Equal(t, db.JobRunning, run.Status)
	require.NotNil(t, run.PendingAction)
	require.Eventually(t, func() bool { return !time.Now().Before(run.LeaseUntil) }, 3*time.Second, 5*time.Millisecond)
	genericProposal(t, n, "expire-after-effect-crash", db.JobCommand{Expire: &db.ExpireJob{JobOwner: db.JobOwner{TenantID: "tenant", RunID: "write", AttemptID: run.AttemptID, OwnerID: run.OwnerID}, LeaseUntil: run.LeaseUntil}})
	run, err = n.store.GetJobRun("tenant", "write")
	require.NoError(t, err)
	require.Equal(t, db.JobUnknown, run.Status)
	require.Equal(t, db.EffectUnknown, run.PendingAction.Status)
	resource, err := n.store.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, db.ResourceBlocked, resource.State)
	channel, err := n.store.GetChannel("bus")
	require.NoError(t, err)
	require.Equal(t, db.ChannelBlocked, channel.State)
	worker, err := jobs.NewWorker(n.store, jobs.Config{}, registry(remote.URL))
	require.NoError(t, err)
	_, err = worker.Execute(context.Background(), "tenant", "write")
	require.Error(t, err)
	require.EqualValues(t, 1, calls.Load(), "an unrecorded remote effect cannot be executed again automatically")
}

func TestPersistenceFailureStopsBeforeAcknowledgementAndExactRetryRecovers(t *testing.T) {
	for _, phase := range []string{"wal_save", "wal_sync"} {
		t.Run(phase, func(t *testing.T) {
			directory := t.TempDir()
			n := startDurableNode(t, directory, 1000)
			seedRaftEventTrigger(t, n)
			var injected atomic.Bool
			n.raft.persistFault.Store(&persistenceFault{before: func(current string) error {
				if current == phase && injected.CompareAndSwap(false, true) {
					return syscall.ENOSPC
				}
				return nil
			}})
			command := db.StateCommand{RequestID: "uncertain-disk-write", At: time.Now().UTC(), Job: &db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "after-fault"}}}}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			_, err := n.store.ProposeStateCommand(ctx, command)
			require.Error(t, err)
			select {
			case <-n.raft.Done():
			case <-time.After(3 * time.Second):
				t.Fatal("member continued after persistence failure")
			}
			require.True(t, injected.Load())
			n.close()
			n = startDurableNode(t, directory, 1000)
			first := proposeChannel(t, n, command)
			second := proposeChannel(t, n, command)
			require.Equal(t, first, second)
			require.Equal(t, uint64(1), second.Job.Tenant.Revision, "uncertain acknowledgement must not apply a second transition")
		})
	}
}

func TestLiveRaftPartitionCannotTakeOverGatewayWithStalledExchange(t *testing.T) {
	nodes := startChannelCluster(t)
	ownerIndex := -1
	for i, n := range nodes {
		if n.raft.IsLeader() {
			ownerIndex = i
			break
		}
	}
	require.NotEqual(t, -1, ownerIndex)
	owner := nodes[ownerIndex]
	proposeChannel(t, owner, db.StateCommand{RequestID: "partition-config", At: time.Now().UTC(), Configure: &db.ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "gateway"}})
	proposeChannel(t, owner, db.StateCommand{RequestID: "partition-run", At: time.Now().UTC(), EnqueueRun: &db.CreateRun{ID: "write", ChannelID: "gateway", Executor: "fake", Payload: []byte("write")}})
	gateway := fake.New(fake.Options{IgnoreExchangeCancellation: true})
	worker, err := executor.New(context.Background(), executor.Config{ChannelID: "gateway", LeaseDuration: time.Second, LeaseSafetyMargin: 200 * time.Millisecond, RenewInterval: 100 * time.Millisecond, CommitTimeout: 150 * time.Millisecond, OperationTimeout: 5 * time.Second, RecoveryTimeout: time.Second, CloseTimeout: time.Second}, owner.store, gateway.Factory)
	require.NoError(t, err)
	t.Cleanup(func() {
		stop := make(chan struct{})
		go func() {
			ticker := time.NewTicker(time.Millisecond)
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
		defer close(stop)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = worker.Close(ctx)
	})
	done := make(chan error, 1)
	go func() { _, err := worker.Execute(context.Background(), "write"); done <- err }()
	var call *fake.Call
	select {
	case call = <-gateway.Calls:
	case <-time.After(3 * time.Second):
		t.Fatal("exchange did not start")
	}
	for i, n := range nodes {
		if i != ownerIndex {
			owner.raft.transport.CutPeer(types.ID(i + 1))
			n.raft.transport.CutPeer(types.ID(ownerIndex + 1))
		}
	}
	t.Cleanup(func() {
		for i, n := range nodes {
			if i != ownerIndex {
				owner.raft.transport.MendPeer(types.ID(i + 1))
				n.raft.transport.MendPeer(types.ID(ownerIndex + 1))
			}
		}
	})
	barrier, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	_, err = owner.raft.ReadBarrier(barrier)
	cancel()
	require.Error(t, err)
	var replacement *durableNode
	require.Eventually(t, func() bool {
		for i, n := range nodes {
			if i != ownerIndex && n.raft.IsLeader() {
				replacement = n
				return true
			}
		}
		return false
	}, 4*time.Second, 5*time.Millisecond)
	channel, err := replacement.store.GetChannel("gateway")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return !time.Now().Before(channel.LeaseUntil) }, 3*time.Second, 5*time.Millisecond)
	sweeper, err := scheduler.NewOwnershipSweeper(scheduler.OwnershipSweeperConfig{}, replacement.store, replacement.raft.IsLeader)
	require.NoError(t, err)
	_, err = sweeper.Tick(context.Background(), time.Now().UTC())
	require.NoError(t, err)
	blocked, err := replacement.store.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, db.ChannelBlocked, blocked.State)
	unknown, err := replacement.store.GetRun("write")
	require.NoError(t, err)
	require.Equal(t, db.RunUnknown, unknown.State)
	second, err := executor.New(context.Background(), executor.Config{ChannelID: "gateway"}, replacement.store, gateway.Factory)
	require.Error(t, err)
	require.Nil(t, second)
	require.Equal(t, 1, gateway.Stats().Active)
	require.Equal(t, 1, gateway.Stats().MaxActive)
	for i, n := range nodes {
		if i != ownerIndex {
			owner.raft.transport.MendPeer(types.ID(i + 1))
			n.raft.transport.MendPeer(types.ID(ownerIndex + 1))
		}
	}
	require.Eventually(t, func() bool {
		v, err := owner.store.GetChannel("gateway")
		return err == nil && v.State == db.ChannelBlocked
	}, 3*time.Second, 5*time.Millisecond)
	call.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded}}
	call.Recover <- nil
	call.Close <- nil
	ctx, closeCancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer closeCancel()
	_ = worker.Close(ctx)
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("stalled executor caller did not finish")
	}
	require.Zero(t, gateway.Stats().Active)
	final, err := replacement.store.GetRun("write")
	require.NoError(t, err)
	require.Equal(t, db.RunUnknown, final.State)
	finalChannel, err := replacement.store.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, db.ChannelBlocked, finalChannel.State)
}
