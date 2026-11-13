package console

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestConsoleLimitsArePlatformOnlyAndRetainExactRetry(t *testing.T) {
	for operation, kind := range map[string]string{"put_limits": "LIMITS", "put_configuration_limits": "CONFIGURATION_LIMITS"} {
		t.Run(operation, func(t *testing.T) {
			field := "max_pending_runs"
			if operation == "put_configuration_limits" {
				field = "max_definitions"
			}
			input := map[string]any{"tenant_id": "alpha", operation: map[string]any{"expected_revision": "9007199254740993", "limits": map[string]string{field: "18446744073709551615"}}}
			for _, identity := range []Identity{{Subject: "reader", TenantID: "alpha", Role: "reader"}, {Subject: "writer", TenantID: "alpha", Role: "writer"}, {Subject: "operator", TenantID: "alpha", Role: "operator"}} {
				b := newBrowserAs(t, identity, func(http.ResponseWriter, *http.Request) { t.Error("unauthorized policy reached controller") })
				require.Equal(t, 403, b.call(http.MethodPost, "mutations", input).Code)
			}
			var envelopes [][]byte
			b := newBrowserAs(t, Identity{Subject: "platform", Role: "operator"}, func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/api/jobs/v1/mutate", r.URL.Path)
				data, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				envelopes = append(envelopes, data)
				if len(envelopes) == 1 {
					problem(w, 503, "acknowledgement unavailable")
					return
				}
				var q jp.MutationRequest
				require.NoError(t, protojson.Unmarshal(data, &q))
				writeJSON(w, 200, map[string]any{"request_id": q.Command.RequestId, "reference": map[string]string{"kind": kind, "tenant_id": "alpha"}, "revision": "9007199254740994"})
			})
			response := b.call(http.MethodPost, "mutations", input)
			require.Equal(t, 200, response.Code, response.Body.String())
			var pending pendingMutation
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &pending))
			require.Contains(t, string(pending.Request), `"expected_revision":"9007199254740993"`)
			require.Contains(t, string(pending.Request), `"`+field+`":"18446744073709551615"`)
			require.Equal(t, "uncertain", b.commit(pending.RequestID).State)
			require.Equal(t, "confirmed", b.commit(pending.RequestID).State)
			require.Equal(t, "confirmed", b.commit(pending.RequestID).State)
			require.Len(t, envelopes, 2)
			require.Equal(t, envelopes[0], envelopes[1])
			// The public JSON wrapper may compact RawMessage whitespace. The
			// retained controller bytes themselves must match on every retry.
			require.JSONEq(t, string(pending.Request), string(envelopes[0]))
		})
	}
}
