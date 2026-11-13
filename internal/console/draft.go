package console

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
)

// Drafts stay inside the authenticated session and its encrypted shared review.
// A separate logical revision prevents a stale browser tab replacing newer edits.
func (s *Server) saveDraft(w http.ResponseWriter, r *http.Request, sess *session) {
	var q struct {
		Expected uint64          `json:"expected_revision,string"`
		Draft    json.RawMessage `json:"draft"`
	}
	if decode(w, r, 600<<10, &q) != nil || len(q.Draft) > 512<<10 {
		problem(w, 400, "draft must be valid JSON within 512 KiB")
		return
	}
	if bytes.Equal(q.Draft, []byte("null")) {
		q.Draft = nil
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if q.Expected != sess.draftRevision || sess.draftRevision == math.MaxUint64 {
		problem(w, 409, "editor draft changed in another tab; reload before replacing it")
		return
	}
	if sess.pending != nil && len(q.Draft) != 0 {
		problem(w, 409, "retain the current command review before editing another draft")
		return
	}
	sess.draft = bytes.Clone(q.Draft)
	sess.draftRevision++
	writeJSON(w, 200, struct {
		Revision uint64 `json:"revision,string"`
	}{sess.draftRevision})
}

func clearDraftLocked(sess *session) {
	sess.draft = nil
	// Fence tabs that still hold unsaved fields even if no draft was retained.
	if sess.draftRevision < math.MaxUint64 {
		sess.draftRevision++
	}
}
