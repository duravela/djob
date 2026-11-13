package console

import (
	"math"
	"net/http"
	"strings"
	"unicode/utf8"

	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
)

// Recovery review binds a scoped operator, a specific checkpoint/epoch and an
// explicit attestation. The controller repeats authorization and state checks.
func validateRecoveryPreparation(q *jp.MutationRequest, identity Identity) (int, string) {
	validID := func(id string) bool { return strings.TrimSpace(id) != "" && len(id) <= 256 && utf8.ValidString(id) }
	validReason := func(reason string) bool {
		return strings.TrimSpace(reason) != "" && len(reason) <= 4096 && utf8.ValidString(reason)
	}
	if purge := q.GetPurge(); purge != nil {
		if identity.Role != "operator" || identity.TenantID != "" && q.TenantId != "" && identity.TenantID != q.TenantId {
			return http.StatusForbidden, "history deletion requires an operator for this tenant"
		}
		if identity.TenantID != "" {
			q.TenantId = identity.TenantID
		}
		if !validID(q.TenantId) || !validID(purge.RunId) || !validReason(purge.Reason) || purge.BatchSize < 1 || purge.BatchSize > 128 || purge.FinishedBefore == nil || purge.FinishedBefore.CheckValid() != nil {
			return http.StatusBadRequest, "provide the tenant, run checkpoint, bounded deletion batch, cutoff and audited reason"
		}
	}
	if run := q.GetResolveRun(); run != nil {
		if identity.Role != "operator" {
			return http.StatusForbidden, "resolving an unknown outcome requires an operator"
		}
		if identity.TenantID != "" {
			if q.TenantId != "" && q.TenantId != identity.TenantID {
				return http.StatusForbidden, "operator does not permit this tenant"
			}
			q.TenantId = identity.TenantID
		}
		if !validID(q.TenantId) || !validID(run.RunId) || !validReason(run.Reason) || run.ExpectedStep == math.MaxUint64 || (run.Status != "succeeded" && run.Status != "failed" && run.Status != "cancelled") {
			return http.StatusBadRequest, "provide the run, tenant, exact checkpoint, terminal outcome and a reason within 4096 UTF-8 bytes"
		}
	}
	if resource := q.GetResolveResource(); resource != nil {
		if identity.Role != "operator" || identity.TenantID != "" {
			return http.StatusForbidden, "resource recovery requires a platform operator"
		}
		if q.TenantId != "" || !validID(resource.ResourceId) || resource.ExpectedEpoch == 0 || !resource.Quiescent || !validReason(resource.Reason) {
			return http.StatusBadRequest, "provide a global resource, exact positive epoch, confirmed quiescence and a reason within 4096 UTF-8 bytes"
		}
	}
	return 0, ""
}

func recoveryAcknowledgementMatches(q *jp.MutationRequest, ack *jp.MutationResponse) bool {
	if purge := q.GetPurge(); purge != nil {
		return ack.Reference != nil && ack.Reference.Kind == jp.Kind_DELETION && ack.Reference.TenantId == q.TenantId && ack.Reference.Id == purge.RunId && ack.Step == purge.ExpectedStep && (ack.Status == "succeeded" || ack.Status == "failed" || ack.Status == "cancelled")
	}

	if run := q.GetResolveRun(); run != nil {
		return ack.Reference != nil && ack.Reference.Kind == jp.Kind_RUN && ack.Reference.TenantId == q.TenantId && ack.Reference.Id == run.RunId && run.ExpectedStep < math.MaxUint64 && ack.Step == run.ExpectedStep+1 && ack.Status == run.Status
	}
	if resource := q.GetResolveResource(); resource != nil {
		return ack.Reference != nil && ack.Reference.Kind == jp.Kind_RESOURCE && ack.Reference.TenantId == "" && ack.Reference.Id == resource.ResourceId && ack.Epoch == resource.ExpectedEpoch && ack.Status == "available"
	}
	return true
}
