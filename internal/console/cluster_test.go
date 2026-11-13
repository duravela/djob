package console

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/duravela/djob/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestConsoleReplacementReconcilesLostRepliesAndPinsTopology(t *testing.T) {
	current := node.MembershipStatus{Enabled: true, ClusterID: 4096, MemberID: 1, LeaderID: 1, Configuration: "a", Catalog: "catalog", Scope: "scope", CatalogSize: 8, Voters: []uint64{1, 2, 3}, Learners: []uint64{}, MaxID: 3}
	actions := []string{}
	b := newBrowserAs(t, Identity{Subject: "platform", Role: "operator"}, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/jobs/v1/cluster/members", r.URL.Path)
		if r.Method == http.MethodGet {
			writeJSON(w, 200, current)
			return
		}
		var change node.MembershipChange
		require.NoError(t, json.NewDecoder(r.Body).Decode(&change))
		require.Equal(t, current.Configuration, change.ExpectedConfiguration)
		actions = append(actions, change.Action)
		switch change.Action {
		case "add_learner":
			current.Learners = []uint64{4}
			current.MaxID = 4
		case "promote":
			current.Voters = []uint64{1, 2, 3, 4}
			current.Learners = []uint64{}
		case "remove":
			require.Equal(t, uint64(2), change.MemberID)
			current.Voters = []uint64{1, 3, 4}
		}
		current.Configuration += "x"
		// The write commits but its acknowledgement is lost.
		problem(w, 503, "lost reply")
	})
	response := b.call(http.MethodPost, "cluster/plan", map[string]string{"old": "2", "new": "4", "reason": "confirmed volume loss"})
	require.Equal(t, 200, response.Code, response.Body.String())
	var review struct {
		Plan replacementPlan `json:"plan"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &review))
	require.NotEmpty(t, review.Plan.Digest)
	require.Equal(t, 409, b.call(http.MethodPost, "cluster/advance", map[string]string{"approval": "wrong"}).Code)
	for range 3 {
		response = b.call(http.MethodPost, "cluster/advance", map[string]string{"approval": review.Plan.Digest})
		require.Equal(t, 409, response.Code, response.Body.String())
	}
	response = b.call(http.MethodPost, "cluster/advance", map[string]string{"approval": review.Plan.Digest})
	require.Equal(t, 200, response.Code, response.Body.String())
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &review))
	require.True(t, review.Plan.Complete)
	require.Equal(t, []string{"add_learner", "promote", "remove"}, actions)
	// The replacement review is part of encrypted shared-session continuity.
	sess := &session{replacement: &review.Plan, shared: &sharedSession{}}
	raw, err := reviewBytes(sess)
	require.NoError(t, err)
	var retained sharedReview
	require.NoError(t, json.Unmarshal(raw, &retained))
	require.Equal(t, review.Plan.Digest, retained.Replacement.Digest)
}

func TestReplacementStageRejectsConcurrentTopologyAndScope(t *testing.T) {
	before := node.MembershipStatus{ClusterID: 1, Catalog: "c", Scope: "s", Voters: []uint64{1, 2, 3}, Learners: []uint64{}}
	p := &replacementPlan{ClusterID: 1, Catalog: "c", Scope: "s", Old: 2, New: 4, Before: before}
	current := before
	current.Learners = []uint64{5}
	_, _, err := replacementStage(p, current)
	require.ErrorContains(t, err, "outside this plan")
	current = before
	current.Scope = "other"
	_, _, err = replacementStage(p, current)
	require.ErrorContains(t, err, "scope")
}
func TestConsoleClusterRejectsTenantAndReader(t *testing.T) {
	for _, identity := range []Identity{{Subject: "reader", Role: "reader", TenantID: "alpha"}, {Subject: "tenant", Role: "operator", TenantID: "alpha"}} {
		b := newBrowserAs(t, identity, func(http.ResponseWriter, *http.Request) { t.Error("unauthorized cluster access") })
		require.Equal(t, 403, b.call(http.MethodPost, "cluster/status", map[string]any{}).Code)
	}
}
