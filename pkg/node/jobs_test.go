package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/jobs"
	"github.com/duravela/djob/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

func genericProposal(t *testing.T, n *durableNode, id string, c db.JobCommand) db.StateResult {
	t.Helper()
	return proposeChannel(t, n, db.StateCommand{RequestID: id, At: time.Now().UTC(), Job: &c})
}

func TestRaftGenericComputeAndHTTPCallbackSurviveSnapshotRebuild(t *testing.T) {
	dir := t.TempDir()
	n := startDurableNode(t, dir, 2)
	var requests atomic.Int32
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("receipt"))
	}))
	defer remote.Close()
	config, err := json.Marshal(jobs.HTTPBinding{Operations: []jobs.HTTPOperation{{Name: "request", Method: "POST", URL: remote.URL}}})
	require.NoError(t, err)
	for i, c := range []db.JobCommand{
		{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "tenant", MaxRunning: 1}}},
		{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "tenant", ID: "hash", Runtime: db.StepRuntime, Handler: "hash", HandlerVersion: "1", Input: []byte("input")}}},
		{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: "hash", DefinitionRevision: 1, RunID: "compute"}},
		{PutBinding: &db.PutJobBinding{Binding: db.JobBinding{TenantID: "tenant", ID: "remote", Driver: "http", DriverVersion: "1", Config: config}}},
		{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "tenant", ID: "report", Runtime: db.StepRuntime, Handler: "report", HandlerVersion: "1", BindingID: "remote"}}},
		{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: "report", DefinitionRevision: 1, RunID: "http"}},
	} {
		genericProposal(t, n, fmt.Sprintf("configure/%d", i), c)
	}
	registry := jobs.Registry{Handlers: map[jobs.Version]jobs.Handler{
		{Name: "hash", Version: "1"}: func(v jobs.View) (jobs.Decision, error) {
			digest := sha256.Sum256(v.Run.Definition.Input)
			return jobs.Decision{Kind: db.JobComplete, Status: db.JobSucceeded, Result: digest[:]}, nil
		},
		{Name: "report", Version: "1"}: func(v jobs.View) (jobs.Decision, error) {
			switch string(v.Run.State) {
			case "":
				return jobs.Decision{Kind: db.JobDoAction, State: []byte("requested"), Operation: "request"}, nil
			case "requested":
				return jobs.Decision{Kind: db.JobWait, State: []byte("waiting"), WaitKey: "report-ready"}, nil
			case "waiting":
				if v.Event != nil {
					return jobs.Decision{Kind: db.JobComplete, Status: db.JobSucceeded, Result: v.Event.Data}, nil
				}
			}
			return jobs.Decision{}, fmt.Errorf("invalid conversation state")
		},
	}, Drivers: map[jobs.Version]jobs.DriverFactory{{Name: "http", Version: "1"}: jobs.HTTPFactory(remote.Client())}}
	w, err := jobs.NewWorker(n.store, jobs.Config{}, registry)
	require.NoError(t, err)
	r, err := w.Execute(context.Background(), "tenant", "compute")
	require.NoError(t, err)
	require.Equal(t, db.JobSucceeded, r.Run.Status)
	require.Empty(t, r.Run.Resources)
	r, err = w.Execute(context.Background(), "tenant", "http")
	require.NoError(t, err)
	require.True(t, r.Yielded)
	require.Equal(t, db.JobWaiting, r.Run.Status)
	target := n.store.LastAppliedIndex()
	command := db.StateCommand{RequestID: "snapshot-barrier", At: time.Now().UTC(), Job: &db.JobCommand{PutTenant: &db.PutJobTenant{ExpectedRevision: 1, Tenant: db.JobTenant{ID: "tenant", MaxRunning: 1}}}}
	for range 3 {
		proposeChannel(t, n, command)
	}
	require.Eventually(t, func() bool {
		snapshot, err := n.raft.Snapshotter.Load()
		return err == nil && snapshot.Metadata.Index >= target
	}, 3*time.Second, time.Millisecond)
	n.close()
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "store")))
	n = startDurableNode(t, dir, 2)
	view, err := n.store.GetJobView("tenant", "http")
	require.NoError(t, err)
	require.Equal(t, []byte("waiting"), view.Run.State)
	callback, err := jobs.CallbackHandler(n.store, func(r *http.Request) (string, error) {
		if r.Header.Get("Authorization") != "Bearer tenant-secret" {
			return "", fmt.Errorf("bad credential")
		}
		return "tenant", nil
	})
	require.NoError(t, err)
	callbackServer := httptest.NewServer(callback)
	defer callbackServer.Close()
	body, err := json.Marshal(jobs.Callback{RunID: "http", EventID: "callback-1", Key: "report-ready", Data: []byte("measurement")})
	require.NoError(t, err)
	request, err := http.NewRequest(http.MethodPost, callbackServer.URL, bytes.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer tenant-secret")
	response, err := callbackServer.Client().Do(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusAccepted, response.StatusCode)
	w, err = jobs.NewWorker(n.store, jobs.Config{}, registry)
	require.NoError(t, err)
	r, err = w.Execute(context.Background(), "tenant", "http")
	require.NoError(t, err)
	require.Equal(t, db.JobSucceeded, r.Run.Status)
	require.Equal(t, []byte("measurement"), r.Run.Result)
	require.EqualValues(t, 1, requests.Load())
	require.EqualValues(t, 2, r.Run.AttemptCount)
	require.EqualValues(t, 1, r.Run.ActionCount)
	action, err := n.store.GetJobAction("tenant", "http", 1)
	require.NoError(t, err)
	require.Equal(t, []byte("receipt"), action.Result)
}

type genericTestDriver struct{ calls *atomic.Int32 }

func (d *genericTestDriver) Execute(context.Context, jobs.Action) jobs.Effect {
	d.calls.Add(1)
	return jobs.Effect{Status: db.EffectSucceeded, Data: []byte("reading")}
}
func (*genericTestDriver) Quiesce(context.Context) error { return nil }
func (*genericTestDriver) Close(context.Context) error   { return nil }

func TestRaftGenericConversationExcludesTenantsAcrossLeaderLossAndExpiresSafely(t *testing.T) {
	nodes := startChannelCluster(t)
	proposeChannel(t, nodes[0], db.StateCommand{RequestID: "channel", At: time.Now().UTC(), Configure: &db.ConfigureChannel{ID: "bus", GatewayID: "bus", Protocol: "fake", Endpoint: "gateway"}})
	genericProposal(t, nodes[0], "resource", db.JobCommand{PutResource: &db.PutJobResource{Spec: db.ResourceSpec{ID: "bus", ChannelID: "bus", Duplex: "half", Capacity: 1}}})
	for _, tenant := range []string{"alpha", "beta"} {
		for i, c := range []db.JobCommand{
			{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: tenant}}},
			{Grant: &db.PutResourceGrant{Grant: db.ResourceGrant{TenantID: tenant, ResourceID: "bus", Allowed: true}}},
			{PutGroup: &db.PutJobGroup{Group: db.JobGroup{TenantID: tenant, ID: "group", Runtime: db.StepRuntime, Resources: []string{"bus"}}}},
			{PutBinding: &db.PutJobBinding{Binding: db.JobBinding{TenantID: tenant, ID: "driver", Driver: "test", DriverVersion: "1"}}},
			{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: tenant, ID: "collect", GroupID: "group", Handler: "collect", HandlerVersion: "1", BindingID: "driver"}}},
			{Launch: &db.LaunchJob{TenantID: tenant, DefinitionID: "collect", DefinitionRevision: 1, RunID: "r"}},
		} {
			genericProposal(t, nodes[0], fmt.Sprintf("%s/%d", tenant, i), c)
		}
	}
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			if _, err := n.store.GetJobRun("beta", "r"); err != nil {
				return false
			}
		}
		return true
	}, 3*time.Second, time.Millisecond)
	leader := -1
	for i, n := range nodes {
		if n.raft.IsLeader() {
			leader = i
			break
		}
	}
	require.NotEqual(t, -1, leader)
	workerNode := nodes[(leader+1)%3]
	var calls atomic.Int32
	registry := jobs.Registry{Handlers: map[jobs.Version]jobs.Handler{{Name: "collect", Version: "1"}: func(v jobs.View) (jobs.Decision, error) {
		switch string(v.Run.State) {
		case "":
			return jobs.Decision{Kind: db.JobDoAction, State: []byte("selected"), Operation: "select"}, nil
		case "selected":
			return jobs.Decision{Kind: db.JobWait, State: []byte("waiting"), WaitKey: "continue"}, nil
		case "waiting":
			return jobs.Decision{Kind: db.JobDoAction, State: []byte("read"), Operation: "read", Input: v.Event.Data}, nil
		case "read":
			return jobs.Decision{Kind: db.JobComplete, Status: db.JobSucceeded, Result: v.Run.PendingAction.Result}, nil
		}
		return jobs.Decision{}, fmt.Errorf("invalid state")
	}}, Drivers: map[jobs.Version]jobs.DriverFactory{{Name: "test", Version: "1"}: func(db.JobRun) (jobs.Driver, error) { return &genericTestDriver{calls: &calls}, nil }}}
	w, err := jobs.NewWorker(workerNode.store, jobs.Config{}, registry)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		r, err := w.Execute(ctx, "alpha", "r")
		if err == nil && r.Run.Status != db.JobSucceeded {
			err = fmt.Errorf("unexpected result: %s", r.Run.Status)
		}
		done <- err
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("generic worker did not stop")
		}
	})
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			v, err := n.store.GetJobView("alpha", "r")
			if err != nil || v.Run.Status != db.JobWaiting {
				return false
			}
		}
		return true
	}, 3*time.Second, time.Millisecond)
	nodes[leader].close()
	var replacement *durableNode
	require.Eventually(t, func() bool {
		for i, n := range nodes {
			if i != leader && n.raft.IsLeader() {
				replacement = n
				return true
			}
		}
		return false
	}, 3*time.Second, time.Millisecond)
	other, err := jobs.NewWorker(replacement.store, jobs.Config{}, registry)
	require.NoError(t, err)
	_, err = other.Execute(context.Background(), "beta", "r")
	require.ErrorContains(t, err, "resource_busy")
	channel, err := replacement.store.GetChannel("bus")
	require.NoError(t, err)
	_, err = replacement.store.ProposeStateCommand(context.Background(), db.StateCommand{RequestID: "legacy-contender", At: time.Now().UTC(), Acquire: &db.AcquireChannel{ChannelID: "bus", ExpectedEpoch: channel.Epoch, OwnerID: "legacy", LeaseUntil: time.Now().Add(time.Minute)}})
	require.ErrorContains(t, err, "channel_busy")
	genericProposal(t, replacement, "continue", db.JobCommand{Signal: &db.SignalJob{TenantID: "alpha", RunID: "r", EventID: "next", Key: "continue", Data: []byte("read-next")}})
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("conversation failed after election")
	}
	require.EqualValues(t, 2, calls.Load())
	// A new tenant can now acquire the bus. Simulate its crash after committed
	// authorization, then run both legacy and generic leader maintenance paths.
	until := time.Now().UTC().Add(time.Minute)
	owner := db.JobOwner{TenantID: "beta", RunID: "r", AttemptID: "crashed-attempt", OwnerID: "crashed-worker"}
	genericProposal(t, replacement, "new-owner", db.JobCommand{Claim: &db.ClaimJob{JobOwner: owner, LeaseUntil: until}})
	authorized := db.StateCommand{RequestID: "authorized", At: time.Now().UTC(), Job: &db.JobCommand{Advance: &db.AdvanceJob{JobOwner: owner, Kind: db.JobDoAction, Operation: "write"}}}
	first := proposeChannel(t, replacement, authorized)
	legacySweep, err := scheduler.NewOwnershipSweeper(scheduler.OwnershipSweeperConfig{}, replacement.store, replacement.raft.IsLeader)
	require.NoError(t, err)
	count, err := legacySweep.Tick(context.Background(), until)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	sweep, err := scheduler.NewJobMaintenance(scheduler.JobMaintenanceConfig{}, replacement.store, replacement.raft.IsLeader)
	require.NoError(t, err)
	count, err = sweep.Tick(context.Background(), until)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, first, proposeChannel(t, replacement, authorized), "historical receipts must not authorize a new exchange")
	require.Eventually(t, func() bool {
		for i, n := range nodes {
			if i == leader {
				continue
			}
			r, err := n.store.GetJobResource("bus")
			if err != nil || r.State != db.ResourceBlocked {
				return false
			}
			v, err := n.store.GetJobView("beta", "r")
			if err != nil || v.Run.Status != db.JobUnknown || v.Run.PendingAction.Status != db.EffectUnknown {
				return false
			}
		}
		return true
	}, 3*time.Second, time.Millisecond)
}
