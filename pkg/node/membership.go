package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/duravela/djob/pkg/base"
	"github.com/duravela/djob/pkg/node/config"
	"go.etcd.io/raft/v3/raftpb"
)

const membershipPrefix = "djob-membership/v1:"
const bootstrapPrefix = "djob-bootstrap/v1:"

// Replicated scope prevents local configuration differences from producing
// different membership decisions for the same committed log entry.
type membershipScope struct {
	ClusterID      uint64   `json:"cluster_id,string"`
	Catalog        string   `json:"catalog"`
	InitialMembers []uint64 `json:"initial_members"`
}

type membershipEnvelope struct {
	MembershipChange
	Scope membershipScope `json:"scope"`
}

func (rc *RaftNode) membershipScope() membershipScope {
	return membershipScope{rc.raftCfg.ClusterID, membershipDigest(rc.raftCfg.Peers), bootstrapVoters(*rc.raftCfg)}
}

func (rc *RaftNode) validateMembershipScope(scope membershipScope) error {
	local := rc.membershipScope()
	if !rc.raftCfg.DynamicMembership || scope.ClusterID != local.ClusterID || scope.Catalog != local.Catalog || !slices.Equal(scope.InitialMembers, local.InitialMembers) {
		return fmt.Errorf("committed membership scope disagrees with local mode, cluster, catalog or initial members")
	}
	return nil
}

// MembershipStatus is copied from the application goroutine. A public caller
// must perform a read barrier before using it to review a configuration change.
type MemberProgress struct {
	MemberID     uint64 `json:"member_id,string"`
	Match        uint64 `json:"match,string"`
	RecentActive bool   `json:"recent_active"`
}

type MembershipStatus struct {
	Commit        uint64           `json:"commit,string"`
	Progress      []MemberProgress `json:"progress,omitempty"`
	Enabled       bool             `json:"enabled"`
	ClusterID     uint64           `json:"cluster_id,string"`
	MemberID      uint64           `json:"member_id,string"`
	LeaderID      uint64           `json:"leader_id,string"`
	Configuration string           `json:"configuration"`
	Catalog       string           `json:"catalog"`
	Scope         string           `json:"scope"`
	CatalogSize   int              `json:"catalog_size"`
	Voters        []uint64         `json:"voters"`
	Learners      []uint64         `json:"learners"`
	MaxID         uint64           `json:"max_id,string"`
}

// ExpectedConfiguration binds each change to its reviewed starting point.
// Retrying an already satisfied action is harmless and never changes another ID.
type MembershipChange struct {
	RequestID             string `json:"request_id"`
	Reason                string `json:"reason"`
	Actor                 string `json:"actor,omitempty"`
	ExpectedConfiguration string `json:"expected_configuration"`
	Action                string `json:"action"`
	MemberID              uint64 `json:"member_id,string"`
}

type membershipRequest struct {
	ctx    context.Context
	change MembershipChange
	done   chan error
}

func bootstrapVoters(cfg base.RaftConfig) []uint64 {
	if len(cfg.InitialMembers) > 0 {
		return append([]uint64(nil), cfg.InitialMembers...)
	}
	ids := make([]uint64, len(cfg.Peers))
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	return ids
}

func validateMembershipConfig(cfg base.RaftConfig) error {
	if cfg.Bootstrap && (!cfg.DynamicMembership || cfg.Join) {
		return fmt.Errorf("bootstrap provisioning requires a dynamic initial member")
	}
	if !cfg.DynamicMembership {
		if len(cfg.InitialMembers) > 0 {
			return fmt.Errorf("initial members require dynamic membership mode")
		}
		return nil
	}
	if len(cfg.Peers) > 128 {
		return fmt.Errorf("dynamic peer catalog exceeds 128 slots")
	}
	ids := bootstrapVoters(cfg)
	if !slices.IsSorted(ids) {
		return fmt.Errorf("bootstrap voter IDs must be ordered")
	}
	seen := map[uint64]bool{}
	for _, id := range ids {
		if id == 0 || id > uint64(len(cfg.Peers)) || seen[id] {
			return fmt.Errorf("bootstrap voter IDs must be unique catalog entries")
		}
		seen[id] = true
	}
	if cfg.Join == seen[cfg.ID] {
		return fmt.Errorf("bootstrap voters must start normally; only a fresh learner slot may join")
	}
	return nil
}

// InitializeMember provisions initial WAL and application state without waiting
// for an election or starting public APIs. Ordinary service startup cannot
// recreate an initial voter's missing history after a volume loss.
func InitializeMember(ctx context.Context, cfg config.Config) error {
	if !cfg.DynamicMembership || cfg.Join {
		return fmt.Errorf("initialize only a new dynamic bootstrap voter")
	}
	cfg.Bootstrap = true
	cfg.SetDefaults()
	if err := ValidateApplicationConfig(cfg); err != nil {
		return err
	}
	if err := os.Mkdir(cfg.DataDir, 0750); err != nil {
		return fmt.Errorf("initial member destination must be new: %w", err)
	}
	app, err := OpenApplication(ctx, cfg)
	if err != nil {
		return err
	}
	defer app.Close()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for app.Store.LastAppliedIndex() < uint64(len(bootstrapVoters(cfg.RaftConfig))) {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		case err := <-app.Raft.ErrorC:
			return fmt.Errorf("initialize member: %v", err)
		}
	}
	return nil
}

func membershipDigest(value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (rc *RaftNode) membershipStatus() MembershipStatus {
	voters := append([]uint64{}, rc.confState.Voters...)
	learners := append([]uint64{}, rc.confState.Learners...)
	slices.Sort(voters)
	slices.Sort(learners)
	m := MembershipStatus{Enabled: rc.raftCfg.DynamicMembership, ClusterID: rc.raftCfg.ClusterID, MemberID: rc.raftCfg.ID,
		Voters: voters, Learners: learners, Catalog: membershipDigest(rc.raftCfg.Peers), Scope: membershipDigest(rc.membershipScope()), CatalogSize: len(rc.raftCfg.Peers)}
	for _, id := range append(append([]uint64(nil), voters...), learners...) {
		m.MaxID = max(m.MaxID, id)
	}
	m.Configuration = membershipDigest(struct {
		Cluster          uint64
		Catalog          string
		Voters, Learners []uint64
	}{m.ClusterID, m.Catalog, voters, learners})
	return m
}

func (rc *RaftNode) publishMembership() {
	if !rc.raftCfg.DynamicMembership {
		return
	}
	m := rc.membershipStatus()
	status := rc.node.Status()
	m.LeaderID = status.Lead
	m.Commit = status.Commit
	for id, progress := range status.Progress {
		m.Progress = append(m.Progress, MemberProgress{id, progress.Match, progress.RecentActive})
	}
	slices.SortFunc(m.Progress, func(a, b MemberProgress) int {
		if a.MemberID < b.MemberID {
			return -1
		}
		if a.MemberID > b.MemberID {
			return 1
		}
		return 0
	})
	rc.membership.Store(&m)
}

func (rc *RaftNode) Membership() MembershipStatus {
	if m := rc.membership.Load(); m != nil {
		copy := *m
		copy.Voters = slices.Clone(m.Voters)
		copy.Learners = slices.Clone(m.Learners)
		copy.Progress = slices.Clone(m.Progress)
		return copy
	}
	return MembershipStatus{}
}

func isMembershipContext(raw []byte) bool { return bytes.HasPrefix(raw, []byte(membershipPrefix)) }
func isBootstrapContext(raw []byte) bool  { return bytes.HasPrefix(raw, []byte(bootstrapPrefix)) }

func changeType(action string) (raftpb.ConfChangeType, error) {
	switch action {
	case "add_learner":
		return raftpb.ConfChangeAddLearnerNode, nil
	case "promote":
		return raftpb.ConfChangeAddNode, nil
	case "remove":
		return raftpb.ConfChangeRemoveNode, nil
	default:
		return 0, fmt.Errorf("action must be add_learner, promote or remove")
	}
}

func membershipSatisfied(m MembershipStatus, c MembershipChange) bool {
	switch c.Action {
	case "add_learner":
		return slices.Contains(m.Learners, c.MemberID) || slices.Contains(m.Voters, c.MemberID)
	case "promote":
		return slices.Contains(m.Voters, c.MemberID)
	case "remove":
		return c.MemberID > 0 && c.MemberID < m.MaxID && !slices.Contains(m.Voters, c.MemberID) && !slices.Contains(m.Learners, c.MemberID)
	}
	return false
}

func validateMembershipChange(m MembershipStatus, c MembershipChange, catalogSize int) error {
	if strings.TrimSpace(c.RequestID) == "" || len(c.RequestID) > 256 || strings.TrimSpace(c.Reason) == "" || len(c.Reason) > 4096 {
		return fmt.Errorf("membership changes require a bounded request ID and audited reason")
	}
	if !m.Enabled {
		return fmt.Errorf("dynamic membership is disabled")
	}
	if c.MemberID == 0 || c.MemberID > uint64(catalogSize) {
		return fmt.Errorf("member must select a preauthorized peer catalog slot")
	}
	if c.ExpectedConfiguration == "" || c.ExpectedConfiguration != m.Configuration {
		return fmt.Errorf("membership changed; review its current configuration")
	}
	switch c.Action {
	case "add_learner":
		if c.MemberID <= m.MaxID {
			return fmt.Errorf("member IDs cannot be reused; select a fresh higher catalog slot")
		}
		if len(m.Learners) >= 2 {
			return fmt.Errorf("at most two learners may catch up at once")
		}
	case "promote":
		if !slices.Contains(m.Learners, c.MemberID) {
			return fmt.Errorf("promotion requires an existing learner")
		}
	case "remove":
		// Keeping the greatest allocated ID in ConfState durably preserves the
		// allocation high-water mark across snapshots without a separate journal.
		if c.MemberID == m.MaxID {
			return fmt.Errorf("add a higher fresh member before removing the highest allocated ID")
		}
		if !slices.Contains(m.Voters, c.MemberID) && !slices.Contains(m.Learners, c.MemberID) {
			return fmt.Errorf("member does not exist")
		}
		if slices.Contains(m.Voters, c.MemberID) && len(m.Voters) <= 1 {
			return fmt.Errorf("cannot remove the last voter")
		}
	default:
		return fmt.Errorf("unsupported membership action")
	}
	return nil
}

func (rc *RaftNode) ProposeMembership(ctx context.Context, c MembershipChange) error {
	request := membershipRequest{ctx: ctx, change: c, done: make(chan error, 1)}
	select {
	case rc.membershipRequests <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-rc.donec:
		return fmt.Errorf("Raft stopped")
	}
	select {
	case err := <-request.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-rc.donec:
		return fmt.Errorf("Raft stopped")
	}
}

func (rc *RaftNode) proposeMembership(request membershipRequest) {
	c := request.change
	m := rc.membershipStatus()
	if request.ctx.Err() != nil {
		request.done <- request.ctx.Err()
		return
	}
	status := rc.node.Status()
	if status.Lead != rc.raftCfg.ID {
		request.done <- fmt.Errorf("membership changes require the current leader")
		return
	}
	if !m.Enabled {
		request.done <- fmt.Errorf("dynamic membership is disabled")
		return
	}
	if c.ExpectedConfiguration != "" && c.RequestID != "" && c.Reason != "" && membershipSatisfied(m, c) {
		request.done <- nil
		return
	}
	if err := validateMembershipChange(m, c, len(rc.raftCfg.Peers)); err != nil {
		request.done <- err
		return
	}
	if time.Now().Before(rc.membershipPendingUntil) {
		request.done <- fmt.Errorf("a membership proposal is pending; inspect before retrying")
		return
	}
	if c.Action == "promote" {
		progress, ok := status.Progress[c.MemberID]
		if !ok || !progress.RecentActive || progress.Match < status.Commit {
			request.done <- fmt.Errorf("learner must be connected and caught up before promotion")
			return
		}
	}
	if c.Action == "remove" && c.MemberID == status.Lead {
		request.done <- fmt.Errorf("transfer leadership before removing its current member")
		return
	}
	typ, _ := changeType(c.Action)
	raw, _ := json.Marshal(membershipEnvelope{c, rc.membershipScope()})
	err := rc.node.ProposeConfChange(request.ctx, raftpb.ConfChange{Type: typ, NodeID: c.MemberID, Context: append([]byte(membershipPrefix), raw...)})
	if err == nil {
		rc.membershipPendingUntil = time.Now().Add(5 * time.Second)
	}
	request.done <- err
}

// Recheck the reviewed configuration at application, since a prior leader's
// delayed proposal can appear after another change. Rejected changes become
// Raft no-ops, preserving log/application ordering without altering membership.
// Incompatible local configuration is fatal: it must never become a local no-op.
func (rc *RaftNode) membershipEntryAllowed(cc raftpb.ConfChange) (bool, error) {
	if isBootstrapContext(cc.Context) {
		var scope membershipScope
		if err := json.Unmarshal(cc.Context[len(bootstrapPrefix):], &scope); err != nil {
			return false, fmt.Errorf("decode committed bootstrap scope: %w", err)
		}
		if err := rc.validateMembershipScope(scope); err != nil {
			return false, err
		}
		return cc.Type == raftpb.ConfChangeAddNode && slices.Contains(scope.InitialMembers, cc.NodeID) && cc.NodeID > rc.membershipStatus().MaxID, nil
	}
	if !isMembershipContext(cc.Context) {
		if rc.raftCfg.DynamicMembership {
			return false, fmt.Errorf("dynamic membership requires a scoped committed configuration entry")
		}
		return true, nil
	}
	var envelope membershipEnvelope
	if err := json.Unmarshal(cc.Context[len(membershipPrefix):], &envelope); err != nil {
		return false, fmt.Errorf("decode committed membership envelope: %w", err)
	}
	if err := rc.validateMembershipScope(envelope.Scope); err != nil {
		return false, err
	}
	c := envelope.MembershipChange
	if c.MemberID != cc.NodeID {
		return false, nil
	}
	typ, err := changeType(c.Action)
	return err == nil && typ == cc.Type && validateMembershipChange(rc.membershipStatus(), c, len(rc.raftCfg.Peers)) == nil, nil
}

func (rc *RaftNode) membershipHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || r.URL.RawQuery != "" {
		http.Error(w, "invalid membership query", 400)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if _, err := rc.ReadBarrier(ctx); err != nil {
		http.Error(w, "membership quorum unavailable", 503)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(rc.Membership())
}

// A fresh directory may join only an already committed learner slot. Requiring
// matching quorum observations prevents an empty former voter from rejoining
// under its old identity after losing its durable term and vote.
func (rc *RaftNode) validateFreshJoin() error {
	ctx, cancel := context.WithTimeout(rc.ctx, 5*time.Second)
	defer cancel()
	type reply struct {
		id uint64
		m  MembershipStatus
	}
	results := make(chan reply, len(rc.raftCfg.Peers))
	for i, peer := range rc.raftCfg.Peers {
		if uint64(i+1) == rc.raftCfg.ID {
			continue
		}
		go func(id uint64, peer string) {
			result := reply{id: id}
			defer func() { results <- result }()
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(peer, "/")+"/djob/membership", nil)
			if err != nil {
				return
			}
			response, err := rc.peerClockClient.Do(request)
			if err != nil {
				return
			}
			defer response.Body.Close()
			if response.StatusCode == 200 {
				_ = json.NewDecoder(io.LimitReader(response.Body, 16384)).Decode(&result.m)
			}
		}(uint64(i+1), peer)
	}
	observed := map[string][]reply{}
	for range len(rc.raftCfg.Peers) - 1 {
		r := <-results
		m := r.m
		if !m.Enabled || m.ClusterID != rc.raftCfg.ClusterID || m.MemberID != r.id || m.Catalog != membershipDigest(rc.raftCfg.Peers) || m.Scope != membershipDigest(rc.membershipScope()) || !slices.Contains(m.Learners, rc.raftCfg.ID) || !slices.Contains(m.Voters, r.id) {
			continue
		}
		observed[m.Configuration] = append(observed[m.Configuration], r)
	}
	for _, group := range observed {
		if len(group) > len(group[0].m.Voters)/2 {
			return nil
		}
	}
	return fmt.Errorf("fresh joining identity is not a quorum-confirmed learner with matching membership scope; never recreate a lost voter directory")
}
