package e2e_test

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

const soakMutationAttempts = 3
const soakMutationRetryDelay = 100 * time.Millisecond
const uncertainCommitMessage = "commit acknowledgement timed out; retry the identical request"

// Retry only the documented uncertain acknowledgement, using the same bytes,
// identity and timestamp. This resubmits an API command, not an external action.
// Other failures, including admission holds and transport errors, remain fatal.
func (r *soakReport) callMutation(member int, actor string, body any) (int, object, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return 0, nil, err
	}
	ctx, cancel := context.WithTimeout(r.h.ctx, r.Config.MaxLatency)
	defer cancel()
	var code int
	var result object
	for attempt := 0; attempt < soakMutationAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return code, result, err
		}
		if attempt > 0 {
			timer := time.NewTimer(soakMutationRetryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return code, result, ctx.Err()
			case <-timer.C:
			}
			if err := ctx.Err(); err != nil {
				return code, result, err
			}
			r.MutationRetries++
			if attempt == 1 {
				r.RetriedMutationCalls++
			}
		}
		code, result, err = r.h.callBytes(ctx, member, "POST", "/api/jobs/v1/mutate", actor, data)
		if err != nil || code != http.StatusGatewayTimeout || result["code"] != float64(4) || result["message"] != uncertainCommitMessage {
			return code, result, err
		}
	}
	return code, result, err
}

func (r *soakReport) requestMutation(member int, actor string, body any) object {
	r.h.t.Helper()
	code, result, err := r.callMutation(member, actor, body)
	if err != nil || code != http.StatusOK {
		r.h.t.Fatalf("member %d POST /api/jobs/v1/mutate: status %d, error %v, result %v", member+1, code, err, result)
	}
	return result
}

func (r *soakReport) mutate(member int, actor, tenant, operation string, value any) object {
	r.h.t.Helper()
	body := object{"command": r.h.meta(), operation: value}
	if tenant != "" {
		body["tenant_id"] = tenant
	}
	return r.requestMutation(member, actor, body)
}
