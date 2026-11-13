package node

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node/config"
	"github.com/duravela/djob/pkg/node/util/testutil"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestDynamicMembershipLearnerReplacementSurvivesSnapshots(t *testing.T) {
	ca := testutil.NewPeerCA(t)
	peers := make([]string, 5)
	for i := range peers {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		peers[i] = "https://" + listener.Addr().String()
		require.NoError(t, listener.Close())
	}
	nodes := make([]*durableNode, 5)
	configs := make([]config.Config, 5)
	for i := range configs {
		cfg := crashTestConfig(t.TempDir())
		cfg.ID, cfg.Peers, cfg.InitialMembers, cfg.DynamicMembership = uint64(i+1), peers, []uint64{1, 2, 3}, true
		cfg.PeerTLS = ca.Member(t, []string{"127.0.0.1"})
		cfg.Join = i >= 3
		cfg.Bootstrap = i < 3
		cfg.SnapshotCount, cfg.CatchUpEntriesN = 2, 1
		cfg.ProposeC, cfg.ConfChangeC = make(chan []byte), make(chan raftpb.ConfChange)
		configs[i] = cfg
		if i < 3 {
			nodes[i] = startRaftApplication(t, cfg)
		}
	}
	leader := func() *durableNode {
		var found *durableNode
		require.Eventually(t, func() bool {
			for _, n := range nodes {
				if n != nil && n.raft.IsLeader() {
					found = n
					return true
				}
			}
			return false
		}, 5*time.Second, 10*time.Millisecond)
		return found
	}
	change := func(action string, id uint64) {
		require.Eventually(t, func() bool {
			n := leader()
			m := n.raft.Membership()
			c := MembershipChange{RequestID: fmt.Sprintf("test/%s/%d", action, id), Reason: "isolated replacement test", ExpectedConfiguration: m.Configuration, Action: action, MemberID: id}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			return n.raft.ProposeMembership(ctx, c) == nil
		}, 5*time.Second, 20*time.Millisecond)
		require.Eventually(t, func() bool {
			return membershipSatisfied(leader().raft.Membership(), MembershipChange{Action: action, MemberID: id})
		}, 5*time.Second, 10*time.Millisecond)
	}
	initial := leader()
	genericProposal(t, initial, "before-replacement", db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "durable"}}})
	genericProposal(t, initial, "resource", db.JobCommand{PutResource: &db.PutJobResource{Spec: db.ResourceSpec{ID: "physical-bus", Duplex: "half", Capacity: 1}}})
	genericProposal(t, initial, "grant", db.JobCommand{Grant: &db.PutResourceGrant{Grant: db.ResourceGrant{TenantID: "durable", ResourceID: "physical-bus", Allowed: true}}})
	genericProposal(t, initial, "definition", db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "durable", ID: "work", Runtime: db.StepRuntime, Handler: "test", HandlerVersion: "1", Resources: []string{"physical-bus"}}}})
	genericProposal(t, initial, "launch", db.JobCommand{Launch: &db.LaunchJob{TenantID: "durable", DefinitionID: "work", DefinitionRevision: 1, RunID: "uncertain"}})
	owner := db.JobOwner{TenantID: "durable", RunID: "uncertain", AttemptID: "attempt", OwnerID: "lost-controller"}
	until := time.Now().UTC().Add(time.Second)
	genericProposal(t, initial, "claim", db.JobCommand{Claim: &db.ClaimJob{JobOwner: owner, LeaseUntil: until}})
	proposeChannel(t, initial, db.StateCommand{RequestID: "expire", At: until, Job: &db.JobCommand{Expire: &db.ExpireJob{JobOwner: owner, LeaseUntil: until}}})
	minimum := initial.store.LastAppliedIndex()
	old := 0
	if leader() == nodes[old] {
		old = 1
	}
	nodes[old].close()
	nodes[old] = nil
	require.NoError(t, os.RemoveAll(configs[old].DataDir))
	// A fresh voter cannot be manufactured, and an offline learner cannot vote.
	m := initial.raft.Membership()
	require.Error(t, initial.raft.ProposeMembership(context.Background(), MembershipChange{RequestID: "bad", Reason: "test", ExpectedConfiguration: m.Configuration, Action: "promote", MemberID: 4}))
	change("add_learner", 4)
	m = leader().raft.Membership()
	require.Error(t, leader().raft.ProposeMembership(context.Background(), MembershipChange{RequestID: "bad", Reason: "offline", ExpectedConfiguration: m.Configuration, Action: "promote", MemberID: 4}))
	// Reject a locally valid but incompatible bootstrap list before the learner
	// can catch up via a snapshot and become eligible for promotion.
	incompatible := configs[3]
	incompatible.DataDir = t.TempDir()
	incompatible.InitialMembers = []uint64{1, 2}
	badApp, joinErr := OpenApplication(context.Background(), incompatible)
	require.Nil(t, badApp)
	require.ErrorContains(t, joinErr, "matching membership scope")
	nodes[3] = startRaftApplication(t, configs[3])
	require.Eventually(t, func() bool { return nodes[3].store.LastAppliedIndex() >= minimum }, 5*time.Second, time.Millisecond)
	_, err := nodes[3].store.GetJobRecord(db.JobRecordQuery{Kind: db.JobTenantRecord, TenantID: "durable"})
	require.NoError(t, err)
	change("promote", 4)
	requireReadBarrier(t, nodes[3], minimum)
	// Simulate complete loss only after stopping the old member. Its ID will
	// remain unavailable even though its directory no longer exists.
	change("remove", uint64(old+1))
	m = leader().raft.Membership()
	require.False(t, slices.Contains(m.Voters, uint64(old+1)))
	require.True(t, leader().raft.IsIDRemoved(uint64(old+1)))
	require.Error(t, leader().raft.ProposeMembership(context.Background(), MembershipChange{RequestID: "reuse", Reason: "must fail", ExpectedConfiguration: m.Configuration, Action: "add_learner", MemberID: uint64(old + 1)}))
	// More writes compact membership history out of the WAL. The active highest
	// ID still preserves the allocation boundary through snapshot and restart.
	for i := range 8 {
		genericProposal(t, leader(), fmt.Sprintf("after-%d", i), db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: fmt.Sprintf("after-%d", i)}}})
	}
	for i, n := range nodes {
		if n != nil {
			n.close()
			nodes[i] = nil
		}
	}
	for i := range configs {
		if i != old && i < 4 {
			cfg := configs[i]
			cfg.ProposeC, cfg.ConfChangeC = make(chan []byte), make(chan raftpb.ConfChange)
			nodes[i] = startRaftApplication(t, cfg)
		}
	}
	m = leader().raft.Membership()
	require.EqualValues(t, 4, m.MaxID)
	require.Eventually(t, func() bool { return leader().raft.IsIDRemoved(uint64(old + 1)) }, 5*time.Second, time.Millisecond)
	_, err = leader().store.GetJobRecord(db.JobRecordQuery{Kind: db.JobTenantRecord, TenantID: "durable"})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.True(t, leader().raft.PeerClockWithin(ctx, time.Second), "clock majority must use current voters, not unused catalog slots")
	nodes[3].close()
	nodes[3] = nil
	require.NoError(t, os.RemoveAll(configs[3].DataDir))
	joinCtx, stopJoin := context.WithTimeout(context.Background(), 5*time.Second)
	app, joinErr := OpenApplication(joinCtx, configs[3])
	stopJoin()
	require.Nil(t, app)
	require.ErrorContains(t, joinErr, "quorum-confirmed learner", "a promoted member that loses its volume must not recreate its voting identity")
	change("add_learner", 5)
	nodes[4] = startRaftApplication(t, configs[4])
	require.Eventually(t, func() bool { return nodes[4].store.LastAppliedIndex() >= minimum }, 5*time.Second, time.Millisecond)
	change("promote", 5)
	change("remove", 4)
	require.True(t, leader().raft.IsIDRemoved(4))
	require.EqualValues(t, 5, leader().raft.Membership().MaxID)
	run, err := nodes[4].store.GetJobRun("durable", "uncertain")
	require.NoError(t, err)
	require.Equal(t, db.JobUnknown, run.Status, "membership repair must not replay uncertain physical work")
	resource, err := nodes[4].store.GetJobRecord(db.JobRecordQuery{Kind: db.JobResourceRecord, ID: "physical-bus"})
	require.NoError(t, err)
	require.Equal(t, db.ResourceBlocked, resource.Resource.State)
	nodes[4].close()
	artifact := t.TempDir() + "/dynamic-backup"
	backup, err := BackupMember(context.Background(), configs[4].DataDir, artifact, 5)
	require.NoError(t, err)
	require.Equal(t, 2, backup.Identity.Version)
	_, err = VerifyBackup(context.Background(), artifact)
	require.NoError(t, err)
}

func TestMembershipRejectsStaleChangesAndKeepsSnapshotConfiguration(t *testing.T) {
	cfg := crashTestConfig(t.TempDir())
	cfg.DynamicMembership = true
	cfg.Peers = []string{"http://127.0.0.1:1", "http://127.0.0.1:2", "http://127.0.0.1:3", "http://127.0.0.1:4"}
	n := &RaftNode{raftCfg: &cfg.RaftConfig, confState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}
	m := n.membershipStatus()
	c := MembershipChange{RequestID: "request", Reason: "review", ExpectedConfiguration: m.Configuration, Action: "add_learner", MemberID: 4}
	require.NoError(t, validateMembershipChange(m, c, 4))
	c.ExpectedConfiguration = "old"
	require.Error(t, validateMembershipChange(m, c, 4))
	c.ExpectedConfiguration = m.Configuration
	c.Action = "remove"
	c.MemberID = 3
	require.Error(t, validateMembershipChange(m, c, 4), "highest ID preserves the durable allocation watermark")
	message := raftpb.Message{Type: raftpb.MsgSnap, Snapshot: &raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}}}}
	require.Equal(t, []uint64{1, 2}, n.processMessages([]raftpb.Message{message})[0].Snapshot.Metadata.ConfState.Voters)
}

func TestInitialMemberProvisioningIsSeparateFromNormalStartup(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	peer := "http://" + l.Addr().String()
	require.NoError(t, l.Close())
	directory := t.TempDir() + "/new-member"
	cfg := crashTestConfig(directory)
	cfg.DynamicMembership = true
	cfg.InitialMembers = []uint64{1}
	cfg.Peers = []string{peer, "http://127.0.0.1:12345"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	app, err := OpenApplication(ctx, cfg)
	require.Nil(t, app)
	require.ErrorContains(t, err, "no durable Raft state")
	// Preserve the failed attempt as evidence. Provision into a separate new path.
	cfg.DataDir = t.TempDir() + "/initialized"
	require.NoError(t, InitializeMember(ctx, cfg))
	require.Error(t, InitializeMember(ctx, cfg), "initialization must never overwrite a member")
	app, err = OpenApplication(ctx, cfg)
	require.NoError(t, err)
	app.Close()
	require.NoError(t, os.RemoveAll(cfg.DataDir))
	app, err = OpenApplication(ctx, cfg)
	require.Nil(t, app)
	require.ErrorContains(t, err, "no durable Raft state", "normal service restart must refuse a wiped original voter")
}

func TestCommittedMembershipScopeFailsClosed(t *testing.T) {
	cfg := crashTestConfig(t.TempDir())
	cfg.DynamicMembership = true
	cfg.InitialMembers = []uint64{1, 2, 3}
	cfg.Peers = []string{"http://127.0.0.1:1", "http://127.0.0.1:2", "http://127.0.0.1:3", "http://127.0.0.1:4"}
	n := &RaftNode{raftCfg: &cfg.RaftConfig, confState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}
	change := MembershipChange{RequestID: "request", Reason: "review", ExpectedConfiguration: n.membershipStatus().Configuration, Action: "add_learner", MemberID: 4}
	envelope := membershipEnvelope{change, n.membershipScope()}
	encode := func(value any, prefix string, id uint64, typ raftpb.ConfChangeType) raftpb.ConfChange {
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		return raftpb.ConfChange{NodeID: id, Type: typ, Context: append([]byte(prefix), raw...)}
	}
	cc := encode(envelope, membershipPrefix, 4, raftpb.ConfChangeAddLearnerNode)
	allowed, err := n.membershipEntryAllowed(cc)
	require.NoError(t, err)
	require.True(t, allowed)
	stale := envelope
	stale.ExpectedConfiguration = "stale"
	allowed, err = n.membershipEntryAllowed(encode(stale, membershipPrefix, 4, cc.Type))
	require.NoError(t, err)
	require.False(t, allowed, "stale reviews must deterministically become no-ops")
	bootstrap := encode(n.membershipScope(), bootstrapPrefix, 1, raftpb.ConfChangeAddNode)
	n.confState = raftpb.ConfState{}
	allowed, err = n.membershipEntryAllowed(bootstrap)
	require.NoError(t, err)
	require.True(t, allowed)
	for _, tc := range []struct {
		name   string
		mutate func()
	}{
		{"mode", func() { cfg.DynamicMembership = false }},
		{"cluster", func() { cfg.ClusterID++ }},
		{"catalog", func() { cfg.Peers = append([]string(nil), cfg.Peers...); cfg.Peers[3] = "http://127.0.0.1:5" }},
		{"bootstrap", func() { cfg.InitialMembers = []uint64{1, 2} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := cfg.RaftConfig
			tc.mutate()
			for _, entry := range []raftpb.ConfChange{cc, bootstrap} {
				allowed, err := n.membershipEntryAllowed(entry)
				require.ErrorContains(t, err, "scope disagrees")
				require.False(t, allowed)
			}
			cfg.RaftConfig = original
		})
	}
	_, err = n.membershipEntryAllowed(raftpb.ConfChange{NodeID: 1, Type: raftpb.ConfChangeAddNode})
	require.ErrorContains(t, err, "scoped committed")
}
