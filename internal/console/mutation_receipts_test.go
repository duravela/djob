package console

import (
	"encoding/json"
	"io"
	"math"
	"net/http"
	"testing"

	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestOrdinaryMutationReceiptMustMatchReviewedRecord(t *testing.T) {
	for _, tc := range []struct {
		name, input, tenant, id string
		kind                    jp.Kind
	}{
		{"tenant", `{"put_tenant":{"expected_revision":"9007199254740993"}}`, "alpha", "", jp.Kind_TENANT},
		{"group", `{"put_group":{"expected_revision":"9007199254740993","group":{"id":"g"}}}`, "alpha", "g", jp.Kind_GROUP},
		{"definition", `{"put_definition":{"expected_revision":"9007199254740993","definition":{"id":"d"}}}`, "alpha", "d", jp.Kind_DEFINITION},
		{"binding", `{"put_binding":{"expected_revision":"9007199254740993","binding":{"id":"b"}}}`, "alpha", "b", jp.Kind_BINDING},
		{"resource", `{"put_resource":{"expected_revision":"9007199254740993","spec":{"id":"bus"}}}`, "", "bus", jp.Kind_RESOURCE},
		{"grant", `{"put_grant":{"expected_revision":"9007199254740993","resource_id":"bus"}}`, "alpha", "bus", jp.Kind_GRANT},
		{"trigger", `{"put_trigger":{"expected_revision":"9007199254740993","definition":{"id":"t"}}}`, "alpha", "t", jp.Kind_TRIGGER},
		{"pause", `{"pause_trigger":{"expected_revision":"9007199254740993","id":"t"}}`, "alpha", "t", jp.Kind_TRIGGER},
		{"limits", `{"put_limits":{"expected_revision":"9007199254740993","limits":{}}}`, "alpha", "", jp.Kind_LIMITS},
		{"configuration_limits", `{"put_configuration_limits":{"expected_revision":"9007199254740993","limits":{}}}`, "alpha", "", jp.Kind_CONFIGURATION_LIMITS},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var q jp.MutationRequest
			require.NoError(t, protojson.Unmarshal([]byte(tc.input), &q))
			identity := Identity{TenantID: "alpha"}
			ack := &jp.MutationResponse{Reference: &jp.Reference{Kind: tc.kind, TenantId: tc.tenant, Id: tc.id}, Revision: 9007199254740994}
			require.True(t, mutationAcknowledgementMatches(&q, ack, identity))
			for _, change := range []func(*jp.MutationResponse){
				func(a *jp.MutationResponse) { a.Reference.TenantId = "other" },
				func(a *jp.MutationResponse) { a.Reference.Id = "other" },
				func(a *jp.MutationResponse) { a.Reference.Kind = jp.Kind_RUN },
				func(a *jp.MutationResponse) { a.Revision-- },
				func(a *jp.MutationResponse) { a.Revision++ },
			} {
				bad := proto.Clone(ack).(*jp.MutationResponse)
				change(bad)
				require.False(t, mutationAcknowledgementMatches(&q, bad, identity))
			}
			q.TenantId = "explicit"
			if tc.kind != jp.Kind_RESOURCE {
				require.False(t, mutationAcknowledgementMatches(&q, ack, identity))
				ack.Reference.TenantId = "explicit"
			}
			require.True(t, mutationAcknowledgementMatches(&q, ack, identity))
		})
	}
}

func TestMutationReceiptRejectsRevisionOverflow(t *testing.T) {
	q := &jp.MutationRequest{Operation: &jp.MutationRequest_PutTenant{PutTenant: &jp.PutTenant{ExpectedRevision: math.MaxUint64}}}
	ack := &jp.MutationResponse{Reference: &jp.Reference{Kind: jp.Kind_TENANT, TenantId: "alpha"}}
	require.False(t, mutationAcknowledgementMatches(q, ack, Identity{TenantID: "alpha"}))
}

func TestOrdinaryMutationMismatchedReceiptRetainsExactRequest(t *testing.T) {
	for _, input := range []string{
		`{"launch":{"definition_id":"job","definition_revision":"1"}}`,
		`{"cancel":{"run_id":"job"}}`,
		`{"put_definition":{"expected_revision":"9007199254740993","definition":{"id":"job"}}}`,
	} {
		t.Run(input, func(t *testing.T) {
			var original []byte
			calls := 0
			b := newBrowser(t, func(w http.ResponseWriter, r *http.Request) {
				data, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				calls++
				if original == nil {
					original = data
				} else {
					require.Equal(t, original, data)
				}
				var q jp.MutationRequest
				require.NoError(t, protojson.Unmarshal(data, &q))
				ack := &jp.MutationResponse{RequestId: q.Command.RequestId, Reference: &jp.Reference{Kind: jp.Kind_RUN, TenantId: "alpha", Id: "job"}}
				if q.GetLaunch() != nil {
					ack.Reference.Id = q.GetLaunch().RunId
				}
				if q.GetPutDefinition() != nil {
					ack.Reference.Kind = jp.Kind_DEFINITION
					ack.Revision = 9007199254740994
				}
				switch calls {
				case 1:
					ack.Reference.TenantId = "beta"
				case 2:
					ack.Reference.Id = "different"
				case 3:
					ack.Reference.Kind = jp.Kind_TRIGGER
				}
				data, err = protojson.Marshal(ack)
				require.NoError(t, err)
				w.Header().Set("Content-Type", "application/json")
				_, err = w.Write(data)
				require.NoError(t, err)
			})
			reply := b.call(http.MethodPost, "mutations", json.RawMessage(input))
			require.Equal(t, 200, reply.Code, reply.Body.String())
			var pending pendingMutation
			require.NoError(t, json.Unmarshal(reply.Body.Bytes(), &pending))
			for i := 0; i < 3; i++ {
				require.Equal(t, "uncertain", b.commit(pending.RequestID).State)
			}
			require.Equal(t, "confirmed", b.commit(pending.RequestID).State)
			require.Equal(t, "confirmed", b.commit(pending.RequestID).State)
			require.Equal(t, 4, calls)
		})
	}
}
