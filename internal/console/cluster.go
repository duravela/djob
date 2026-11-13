package console

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/duravela/djob/pkg/node"
)

type replacementPlan struct {
	ID        string                 `json:"id"`
	Digest    string                 `json:"digest"`
	ClusterID uint64                 `json:"cluster_id,string"`
	Catalog   string                 `json:"catalog"`
	Scope     string                 `json:"scope"`
	Old       uint64                 `json:"old,string"`
	New       uint64                 `json:"new,string"`
	Reason    string                 `json:"reason"`
	Before    node.MembershipStatus  `json:"before"`
	Intent    *node.MembershipChange `json:"intent,omitempty"`
	Complete  bool                   `json:"complete"`
}

func (s *Server) clusterRead(ctx context.Context, sess *session) (node.MembershipStatus, int, error) {
	// Prefer the leader so catch-up observations are available. Every response is
	// quorum-read by the controller; no local hint establishes authority.
	var fallback node.MembershipStatus
	fallbackIndex := 0
	for i := range s.controllers {
		response, _, err := s.request(ctx, sess.credential(), i, "cluster/members", http.MethodGet, nil, false)
		if err != nil || response.status != 200 {
			continue
		}
		var m node.MembershipStatus
		if json.Unmarshal(response.body, &m) != nil || !m.Enabled || m.Configuration == "" {
			continue
		}
		fallback = m
		fallbackIndex = i
		if m.MemberID == m.LeaderID {
			return m, i, nil
		}
	}
	if fallback.Enabled {
		return fallback, fallbackIndex, nil
	}
	return fallback, 0, fmt.Errorf("dynamic membership quorum unavailable")
}

func replacementStage(p *replacementPlan, m node.MembershipStatus) (string, uint64, error) {
	if m.ClusterID != p.ClusterID || m.Catalog != p.Catalog || m.Scope != p.Scope {
		return "", 0, fmt.Errorf("replacement scope changed")
	}
	voters := slices.Clone(p.Before.Voters)
	learners := []uint64{}
	stage := "add_learner"
	id := p.New
	if slices.Contains(m.Learners, p.New) {
		learners = append(learners, p.New)
		stage = "promote"
	}
	if slices.Contains(m.Voters, p.New) {
		voters = append(voters, p.New)
		stage = "remove"
		id = p.Old
	}
	if !slices.Contains(m.Voters, p.Old) {
		voters = slices.DeleteFunc(voters, func(id uint64) bool { return id == p.Old })
		if !slices.Contains(m.Voters, p.New) {
			return "", 0, fmt.Errorf("old voter disappeared before replacement promotion")
		}
		stage = "complete"
	}
	slices.Sort(voters)
	if !slices.Equal(voters, m.Voters) || !slices.Equal(learners, m.Learners) {
		return "", 0, fmt.Errorf("membership changed outside this plan; review a new plan")
	}
	return stage, id, nil
}

func (s *Server) cluster(w http.ResponseWriter, r *http.Request, sess *session) {
	if !platformSession(sess) {
		problem(w, 403, "cluster administration requires a platform operator")
		return
	}
	sess.releaseMu.Lock()
	defer sess.releaseMu.Unlock()
	var q struct {
		Old      uint64 `json:"old,string"`
		New      uint64 `json:"new,string"`
		Reason   string `json:"reason"`
		Approval string `json:"approval"`
	}
	if decode(w, r, 8192, &q) != nil {
		problem(w, 400, "invalid replacement request")
		return
	}
	m, leaderIndex, err := s.clusterRead(r.Context(), sess)
	if err != nil {
		problem(w, 503, err.Error())
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	switch r.PathValue("method") {
	case "status":
		writeJSON(w, 200, map[string]any{"membership": m, "plan": sess.replacement})
		return
	case "plan":
		if sess.replacement != nil && !sess.replacement.Complete {
			problem(w, 409, "finish the retained replacement plan first")
			return
		}
		if q.Old == m.LeaderID || !slices.Contains(m.Voters, q.Old) || q.New <= m.MaxID || q.New > uint64(m.CatalogSize) || len(m.Learners) != 0 || strings.TrimSpace(q.Reason) == "" || len(q.Reason) > 4096 {
			problem(w, 400, "select a nonleader voter, fresh higher catalog slot, no existing learners and an audited reason")
			return
		}
		id, err := randomID()
		if err != nil {
			problem(w, 500, "cannot allocate plan identity")
			return
		}
		p := &replacementPlan{ID: id, ClusterID: m.ClusterID, Catalog: m.Catalog, Scope: m.Scope, Old: q.Old, New: q.New, Reason: q.Reason, Before: m}
		raw, _ := json.Marshal(p)
		sum := sha256.Sum256(raw)
		p.Digest = hex.EncodeToString(sum[:])
		sess.replacement = p
		writeJSON(w, 200, map[string]any{"membership": m, "plan": p})
		return
	case "advance":
		p := sess.replacement
		if p == nil || q.Approval != p.Digest {
			problem(w, 409, "approve the exact retained replacement digest")
			return
		}
		stage, id, err := replacementStage(p, m)
		if err != nil {
			problem(w, 409, err.Error())
			return
		}
		if stage == "complete" {
			p.Complete = true
			p.Intent = nil
			writeJSON(w, 200, map[string]any{"membership": m, "plan": p})
			return
		}
		if p.Intent == nil || p.Intent.Action != stage {
			p.Intent = &node.MembershipChange{RequestID: "console/replacement/" + p.ID + "/" + stage, Action: stage, MemberID: id, Reason: p.Reason, ExpectedConfiguration: m.Configuration}
		}
		// Persist exact intent before sending. Lost replies are reconciled against
		// current topology by the next advance; HTTP 202 never means completion.
		if sess.shared != nil {
			if err := s.saveShared(r.Context(), sess); err != nil {
				problem(w, 503, "replacement intent could not be retained")
				return
			}
		}
		raw, _ := json.Marshal(p.Intent)
		response, _, err := s.request(r.Context(), sess.credential(), leaderIndex, "cluster/members", http.MethodPost, raw, false)
		if err != nil {
			problem(w, 503, "submission uncertain; refresh and resume the retained plan")
			return
		}
		if response.status != http.StatusAccepted {
			problem(w, 409, "stage not accepted; refresh membership and learner catch-up before resuming")
			return
		}
		writeJSON(w, 200, map[string]any{"membership": m, "plan": p, "submitted": stage})
		return
	default:
		problem(w, 404, "unknown cluster operation")
	}
}
