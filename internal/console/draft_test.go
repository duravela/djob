package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDraftRevisionPreventsStaleTabAndRetiredDraftOverwrite(t *testing.T) {
	b := newBrowser(t, func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]bool{"ok": true}) })
	saved := b.call("PUT", "draft", map[string]any{"expected_revision": "0", "draft": map[string]any{"fields": map[string]string{"input": "private draft"}}})
	require.Equal(t, 200, saved.Code, saved.Body.String())
	require.Contains(t, b.call("GET", "session", nil).Body.String(), "private draft")
	require.Equal(t, 409, b.call("PUT", "draft", map[string]any{"expected_revision": "0", "draft": map[string]string{"stale": "tab"}}).Code)
	p := b.prepared()
	info := b.call("GET", "session", nil)
	var result struct {
		Draft    json.RawMessage `json:"draft"`
		Revision string          `json:"draft_revision"`
	}
	require.NoError(t, json.Unmarshal(info.Body.Bytes(), &result))
	require.Empty(t, result.Draft)
	require.Equal(t, "2", result.Revision)
	require.Equal(t, 200, b.call("DELETE", "mutations/current", map[string]string{"request_id": p.RequestID}).Code)
	require.Equal(t, 409, b.call("PUT", "draft", map[string]any{"expected_revision": "1", "draft": map[string]string{"stale": "tab"}}).Code)
}

func TestConsoleReadinessDropsWhileExistingSessionRemainsServiceable(t *testing.T) {
	b := newBrowser(t, func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]bool{"ok": true}) })
	ready := func(path string) int {
		w := httptest.NewRecorder()
		b.server.ServeHTTP(w, httptest.NewRequest("GET", testOrigin+path, nil))
		return w.Code
	}
	require.Equal(t, 200, ready("/readyz"))
	b.server.BeginDrain()
	require.Equal(t, 503, ready("/readyz"))
	require.Equal(t, 200, ready("/healthz"))
	require.Equal(t, 200, b.call("GET", "session", nil).Code)
}

func TestPrepareFencesTabWithAnUnsavedDraft(t *testing.T) {
	b := newBrowser(t, func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]bool{"ok": true}) })
	p := b.prepared()
	require.Equal(t, 200, b.call("DELETE", "mutations/current", map[string]string{"request_id": p.RequestID}).Code)
	require.Equal(t, 409, b.call("PUT", "draft", map[string]any{"expected_revision": "0", "draft": map[string]string{"stale": "unsaved tab"}}).Code)
}
