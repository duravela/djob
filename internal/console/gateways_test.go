package console

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"testing"
)

func TestGatewayRecoveryRetainsExactEpochAndRequiresPlatformOperator(t *testing.T) {
	input := map[string]any{"operation": "resolve", "id": "bus", "expected_epoch": "9007199254740993", "reason": "independently excluded old owner and verified idle", "quiescent": true}
	for _, identity := range []Identity{{Subject: "tenant", Role: "operator", TenantID: "alpha"}, {Subject: "writer", Role: "writer", TenantID: "alpha"}} {
		b := newBrowserAs(t, identity, func(http.ResponseWriter, *http.Request) { t.Error("unauthorized gateway request") })
		require.Equal(t, 403, b.call(http.MethodPost, "mutations/gateway", input).Code)
	}
	var sent [][]byte
	b := newBrowserAs(t, Identity{Subject: "platform", Role: "operator"}, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/scheduler.Scheduler/ResolveChannel", r.URL.Path)
		raw, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		sent = append(sent, raw)
		if len(sent) == 1 {
			problem(w, 503, "receipt unavailable")
			return
		}
		writeJSON(w, 200, map[string]any{"channel": map[string]any{"id": "bus", "epoch": "9007199254740993", "state": "available"}})
	})
	prepared := b.call(http.MethodPost, "mutations/gateway", input)
	require.Equal(t, 200, prepared.Code)
	var pending pendingMutation
	require.NoError(t, json.Unmarshal(prepared.Body.Bytes(), &pending))
	require.Equal(t, "uncertain", b.commit(pending.RequestID).State)
	require.Equal(t, "confirmed", b.commit(pending.RequestID).State)
	require.Equal(t, sent[0], sent[1])
	require.Contains(t, string(sent[0]), `"9007199254740993"`)
}
