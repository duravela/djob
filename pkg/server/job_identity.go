package server

import (
	"encoding/json"
	"net/http"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// identityHTTP describes only the actual Bearer credential. It does not inspect
// replicated state or accept forwarded subject, role, or tenant assertions.
func (s *jobsServer) identityHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Vary", "Authorization")
	if r.URL.RawQuery != "" {
		writeAPIError(w, invalidHTTP("identity does not accept query parameters"))
		return
	}
	p, err := s.authenticate(r.Context())
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if p.role != APIReader && p.role != APIWriter && p.role != APIOperator || p.tenantID == "" && p.role != APIOperator {
		writeAPIError(w, status.Error(codes.PermissionDenied, "job inspection requires a tenant reader, writer, operator, or a platform operator"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Subject   string    `json:"subject"`
		Role      APIRole   `json:"role"`
		TenantID  string    `json:"tenant_id"`
		ExpiresAt time.Time `json:"expires_at,omitzero"`
	}{p.subject, p.role, p.tenantID, p.oidcExpires})
}
