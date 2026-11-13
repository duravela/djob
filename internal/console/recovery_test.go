package console

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
)

func recoveryInput(resource bool) map[string]any {
	if resource {
		return map[string]any{"resolve_resource": map[string]any{"resource_id": "bus", "expected_epoch": "9007199254740993", "quiescent": true, "reason": "verified physical stop"}}
	}
	return map[string]any{"tenant_id": "alpha", "resolve_run": map[string]any{"run_id": "job", "expected_step": "9007199254740993", "status": "failed", "reason": "independently reconciled", "result": "AP8="}}
}

func TestRecoveryPreparationAuthorization(t *testing.T) {
	for _, identity := range []Identity{{Role: "reader", TenantID: "alpha"}, {Role: "writer", TenantID: "alpha"}, {Role: "operator", TenantID: "alpha"}, {Role: "operator"}} {
		for _, resource := range []bool{false, true} {
			t.Run(identity.Role+"/"+identity.TenantID+"/"+map[bool]string{true: "resource", false: "run"}[resource], func(t *testing.T) {
				identity.Subject = "recovery/operator"
				b := newBrowserAs(t, identity, func(http.ResponseWriter, *http.Request) { t.Error("preparation submitted recovery") })
				want := 403
				if identity.Role == "operator" && (!resource || identity.TenantID == "") {
					want = 200
				}
				reply := b.call(http.MethodPost, "mutations", recoveryInput(resource))
				require.Equal(t, want, reply.Code, reply.Body.String())
			})
		}
	}
	b := newBrowserAs(t, Identity{Subject: "recovery/operator", Role: "operator", TenantID: "alpha"}, func(http.ResponseWriter, *http.Request) { t.Error("preparation submitted recovery") })
	q := recoveryInput(false)
	q["tenant_id"] = "beta"
	require.Equal(t, 403, b.call(http.MethodPost, "mutations", q).Code)
	delete(q, "tenant_id")
	reply := b.call(http.MethodPost, "mutations", q)
	require.Equal(t, 200, reply.Code, reply.Body.String())
	var p pendingMutation
	require.NoError(t, json.Unmarshal(reply.Body.Bytes(), &p))
	var wire jp.MutationRequest
	require.NoError(t, protojson.Unmarshal(p.Request, &wire))
	require.Equal(t, "alpha", wire.TenantId)
}

func TestRecoveryPreparationRejectsInvalidAttestations(t *testing.T) {
	for _, tc := range []struct {
		resource bool
		key      string
		value    any
	}{
		{false, "status", "unknown"}, {false, "expected_step", "18446744073709551615"}, {false, "reason", " "}, {false, "reason", strings.Repeat("界", 1366)}, {false, "run_id", ""},
		{true, "expected_epoch", "0"}, {true, "quiescent", false}, {true, "reason", ""}, {true, "resource_id", ""},
	} {
		t.Run(tc.key+"/"+map[bool]string{true: "resource", false: "run"}[tc.resource], func(t *testing.T) {
			b := newBrowserAs(t, Identity{Subject: "recovery/operator", Role: "operator"}, func(http.ResponseWriter, *http.Request) { t.Error("invalid review submitted") })
			q := recoveryInput(tc.resource)
			op := "resolve_run"
			if tc.resource {
				op = "resolve_resource"
			}
			q[op].(map[string]any)[tc.key] = tc.value
			reply := b.call(http.MethodPost, "mutations", q)
			require.Equal(t, 400, reply.Code, reply.Body.String())
		})
	}
}

func TestRecoveryReceiptAndRetryRetainExactCheckpoint(t *testing.T) {
	for _, resource := range []bool{false, true} {
		t.Run(map[bool]string{true: "resource", false: "run"}[resource], func(t *testing.T) {
			var submitted [][]byte
			b := newBrowserAs(t, Identity{Subject: "recovery/operator", Role: "operator"}, func(w http.ResponseWriter, r *http.Request) {
				data, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				submitted = append(submitted, data)
				if len(submitted) == 1 {
					problem(w, 503, "receipt lost")
					return
				}
				var q jp.MutationRequest
				require.NoError(t, protojson.Unmarshal(data, &q))
				ack := &jp.MutationResponse{RequestId: q.Command.RequestId}
				if resource {
					ack.Reference = &jp.Reference{Kind: jp.Kind_RESOURCE, Id: "bus"}
					ack.Epoch = q.GetResolveResource().ExpectedEpoch
					ack.Status = "available"
				} else {
					ack.Reference = &jp.Reference{Kind: jp.Kind_RUN, TenantId: "alpha", Id: "job"}
					ack.Step = q.GetResolveRun().ExpectedStep + 1
					ack.Status = "failed"
				}
				if len(submitted) == 2 {
					ack.Reference.Id = "other"
				}
				data, err = protojson.Marshal(ack)
				require.NoError(t, err)
				w.Write(data)
			})
			reply := b.call(http.MethodPost, "mutations", recoveryInput(resource))
			require.Equal(t, 200, reply.Code, reply.Body.String())
			var p pendingMutation
			require.NoError(t, json.Unmarshal(reply.Body.Bytes(), &p))
			require.Equal(t, "uncertain", b.commit(p.RequestID).State)
			require.Equal(t, "uncertain", b.commit(p.RequestID).State)
			require.Equal(t, "confirmed", b.commit(p.RequestID).State)
			require.Equal(t, "confirmed", b.commit(p.RequestID).State)
			require.Len(t, submitted, 3)
			// The public review JSON can compact RawMessage whitespace. The
			// retained controller envelope must still be identical on every send.
			require.JSONEq(t, string(p.Request), string(submitted[0]))
			for _, data := range submitted {
				require.Equal(t, submitted[0], data)
			}
		})
	}
}

func TestRecoveryAcknowledgementChecksScopeAndPrecondition(t *testing.T) {
	for _, resource := range []bool{false, true} {
		raw, err := json.Marshal(recoveryInput(resource))
		require.NoError(t, err)
		var q jp.MutationRequest
		require.NoError(t, protojson.Unmarshal(raw, &q))
		ack := &jp.MutationResponse{Reference: &jp.Reference{Kind: jp.Kind_RUN, TenantId: "alpha", Id: "job"}, Step: 9007199254740994, Status: "failed"}
		if resource {
			ack = &jp.MutationResponse{Reference: &jp.Reference{Kind: jp.Kind_RESOURCE, Id: "bus"}, Epoch: 9007199254740993, Status: "available"}
		}
		require.True(t, recoveryAcknowledgementMatches(&q, ack))
		ack.Reference.TenantId = "beta"
		require.False(t, recoveryAcknowledgementMatches(&q, ack))
		ack.Reference.TenantId = q.TenantId
		ack.Status = "unknown"
		require.False(t, recoveryAcknowledgementMatches(&q, ack))
		if resource {
			ack.Status = "available"
			ack.Epoch--
		} else {
			ack.Status = "failed"
			ack.Step--
		}
		require.False(t, recoveryAcknowledgementMatches(&q, ack))
	}
}
