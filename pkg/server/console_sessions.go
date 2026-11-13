package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/duravela/djob/pkg/consolesession"
	"github.com/duravela/djob/pkg/db"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type consoleSessions struct {
	metrics                 *consoleSessionMetrics
	server                  *topLevelServer
	probes, writes, revokes *rate.Limiter
	byteBudget              *rate.Limiter
	slots                   chan struct{}
	probeSlots              chan struct{}
}

func newConsoleSessions(s *topLevelServer) *consoleSessions {
	var metrics *consoleSessionMetrics
	if s != nil && s.metrics != nil {
		metrics = s.metrics.consoleSessions
	}
	return &consoleSessions{metrics: metrics, server: s, probes: rate.NewLimiter(32, 64), byteBudget: rate.NewLimiter(rate.Limit(16<<20), 32<<20), writes: rate.NewLimiter(16, 32), revokes: rate.NewLimiter(4, 8), slots: make(chan struct{}, 4), probeSlots: make(chan struct{}, 4)}
}

func bearer(ctx context.Context) string {
	values, httpRequest := ctx.Value(httpAuthorizationKey{}).([]string)
	if !httpRequest {
		md, _ := metadata.FromIncomingContext(ctx)
		values = md.Get("authorization")
	}
	if len(values) != 1 {
		return ""
	}
	scheme, token, ok := strings.Cut(values[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	return token
}

func (h *consoleSessions) barrier(ctx context.Context) error {
	if !h.probes.Allow() {
		if h.metrics != nil {
			h.metrics.probeRejected.Inc()
		}
		return status.Error(codes.ResourceExhausted, "console session probe budget exhausted")
	}
	select {
	case h.probeSlots <- struct{}{}:
		defer func() { <-h.probeSlots }()
	default:
		if h.metrics != nil {
			h.metrics.probeRejected.Inc()
		}
		return status.Error(codes.ResourceExhausted, "console session probes busy")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if h.server.admission.reason() == "clock_untrusted" {
		return status.Error(codes.Unavailable, "console session clock untrusted")
	}
	if err := h.server.validateCommandTime(time.Now().UTC()); err != nil {
		return err
	}
	_, err := h.server.application.Raft.ReadBarrier(ctx)
	if err != nil {
		return status.Error(codes.Unavailable, "console session quorum unavailable")
	}
	return nil
}

func eligibleConsole(p *apiCredential) bool {
	return p != nil && (p.role == APIReader || p.role == APIWriter || p.role == APIOperator) && (p.tenantID != "" || p.role == APIOperator)
}

func (h *consoleSessions) authenticate(ctx context.Context, revoked bool) (*apiCredential, *consolesession.Record, error) {
	denied := func() (*apiCredential, *consolesession.Record, error) {
		return nil, nil, status.Error(codes.Unauthenticated, "console session unavailable or expired")
	}
	token := bearer(ctx)
	id, _, err := consolesession.ParseCredential(token)
	if err != nil {
		return denied()
	}
	if err := h.barrier(ctx); err != nil {
		return nil, nil, err
	}
	r, err := h.server.application.Store.GetConsoleSession(id)
	if err != nil || r.Binding.ClusterID != h.server.application.Raft.ClusterID() || !time.Now().Before(r.ExpiresAt) || r.Revoked && !revoked {
		return denied()
	}
	hash := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare(hash[:], r.Binding.CredentialHash[:]) != 1 {
		return denied()
	}
	if r.Binding.OIDCSubject != "" {
		auth := h.server.jobs.auth
		p, ok := auth.oidcBindings[r.Binding.OIDCSubject]
		if auth.oidcRevoked != nil {
			revoked, err := auth.oidcRevoked(ctx, r.Binding.OIDCIssuer, r.Binding.OIDCSubject, r.Binding.OIDCSID, r.Binding.OIDCIssuedAt)
			if err != nil || revoked {
				return denied()
			}
		}
		if ok && auth.oidcIssuer == r.Binding.OIDCIssuer && time.Now().Before(r.Binding.OIDCExpiresAt) && !r.ExpiresAt.After(r.Binding.OIDCExpiresAt) && eligibleConsole(&p) && p.subject == r.Binding.Subject && string(p.role) == r.Binding.Role && p.tenantID == r.Binding.TenantID {
			p.hash = r.Binding.SourceHash
			return &p, r, nil
		}
		return denied()
	}
	for _, p := range h.server.jobs.auth.credentials {
		if subtle.ConstantTimeCompare(p.hash[:], r.Binding.SourceHash[:]) == 1 && eligibleConsole(&p) && p.subject == r.Binding.Subject && string(p.role) == r.Binding.Role && p.tenantID == r.Binding.TenantID {
			return &p, r, nil
		}
	}
	return denied()
}

func (h *consoleSessions) http(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	fail := func(err error) {
		if h.metrics != nil {
			h.metrics.rejected.WithLabelValues(status.Code(apiError(err)).String()).Inc()
		}
		writeAPIError(w, apiError(err))
	}
	if r.URL.RawQuery != "" {
		fail(status.Error(codes.InvalidArgument, "query parameters unsupported"))
		return
	}
	action := r.PathValue("action")
	if action != "read" && action != "policy" {
		budget := h.writes
		if action == "revoke" || action == "cleanup" {
			budget = h.revokes
		}
		if !budget.Allow() {
			fail(status.Error(codes.ResourceExhausted, "console session write budget exhausted"))
			return
		}
	}
	// Admission happens before body allocation and Raft probes.
	select {
	case h.slots <- struct{}{}:
	default:
		fail(status.Error(codes.ResourceExhausted, "console session requests busy"))
		return
	}
	defer func() { <-h.slots }()
	var c db.ConsoleSessionCommand
	if r.ContentLength < 0 || r.ContentLength > 6<<20 {
		fail(status.Error(codes.ResourceExhausted, "session requests require a content length of at most 6 MiB"))
		return
	}
	if !h.byteBudget.AllowN(time.Now(), int(r.ContentLength)) {
		fail(status.Error(codes.ResourceExhausted, "session byte budget exhausted"))
		return
	}
	body, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, 6<<20))
	if readErr != nil {
		fail(status.Error(codes.ResourceExhausted, "session request exceeds 6 MiB"))
		return
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&c) != nil || d.Decode(new(any)) != io.EOF {
		fail(status.Error(codes.InvalidArgument, "invalid console session command"))
		return
	}
	if c.Action != "" && c.Action != action {
		fail(status.Error(codes.InvalidArgument, "session action mismatch"))
		return
	}
	c.Action, c.At = action, time.Now().UTC()

	var p *apiCredential
	var record *consolesession.Record
	var err error
	if action == "activate" || action == "configure" || action == "recover" || action == "create" || action == "policy" || action == "cleanup" {
		p, err = h.server.jobs.auth.authenticate(r.Context())
		if err == nil && !eligibleConsole(p) {
			err = status.Error(codes.PermissionDenied, "eligible original API credential required")
		}
		if err == nil {
			err = h.barrier(r.Context())
		}
	} else {
		p, record, err = h.authenticate(r.Context(), action == "revoke")
	}
	if err != nil {
		fail(err)
		return
	}
	switch action {
	case "activate", "configure", "recover", "cleanup":
		if p.role != APIOperator || p.tenantID != "" {
			fail(status.Error(codes.PermissionDenied, "platform operator required"))
			return
		}
		if (action == "activate" || action == "configure" || action == "recover") && c.Policy != nil && c.Policy.Limits.MaxCiphertextBytes > consolesession.MaxReviewBytes {
			fail(status.Error(codes.InvalidArgument, "shared review limit exceeds 4 MiB"))
			return
		}
		if (action == "activate" || action == "configure" || action == "recover") && c.Policy != nil && c.Policy.ClusterID == 0 {
			c.Policy.ClusterID = h.server.application.Raft.ClusterID()
		}
		if (action == "activate" || action == "configure" || action == "recover") && (c.Policy == nil || c.Policy.ClusterID != h.server.application.Raft.ClusterID()) {
			fail(status.Error(codes.InvalidArgument, "activation requires current cluster identity"))
			return
		}
	case "policy":
		policy, err := h.server.application.Store.ConsoleSessionPolicy()
		if err != nil {
			fail(status.Error(codes.FailedPrecondition, "console session storage not activated"))
			return
		}
		if policy.ClusterID != h.server.application.Raft.ClusterID() {
			fail(status.Error(codes.FailedPrecondition, "restored sessions require recovery invalidation"))
			return
		}
		writeConsoleJSON(w, policy)
		return
	case "create":
		if c.Creation == nil {
			fail(status.Error(codes.InvalidArgument, "session creation required"))
			return
		}
		b := &c.Creation.Binding
		if err := validatePublicCommandTime(c.Creation.CreatedAt, h.server.cfg.Admission.MaxClockSkew); err != nil {
			fail(err)
			return
		}
		if c.Creation.CreatedAt.After(c.At) {
			c.At = c.Creation.CreatedAt
		}
		b.SourceHash, b.Subject, b.Role, b.TenantID = p.hash, p.subject, string(p.role), p.tenantID
		b.OIDCSubject, b.OIDCIssuer, b.OIDCExpiresAt = p.oidcSubject, p.oidcIssuer, p.oidcExpires
		b.OIDCSID, b.OIDCIssuedAt = p.oidcSID, p.oidcIssuedAt
		if p.oidcSubject != "" && c.Creation.ExpiresAt.After(p.oidcExpires) {
			writeAPIError(w, status.Error(codes.InvalidArgument, "console session must expire before its OIDC credential"))
			return
		}
		if b.ClusterID != h.server.application.Raft.ClusterID() {
			fail(status.Error(codes.InvalidArgument, "session cluster mismatch"))
			return
		}
	case "read":
		writeConsoleJSON(w, record)
		return
	case "renew":
		if c.ID != record.Binding.ID || c.Renewal == nil {
			fail(status.Error(codes.InvalidArgument, "renewal required"))
			return
		}
		fresh := h.server.jobs.auth.oidcPrincipal(r.Context(), r.Header.Get("X-Djob-OIDC-Token"))
		b := record.Binding
		if fresh == nil || fresh.oidcSubject != b.OIDCSubject || fresh.oidcIssuer != b.OIDCIssuer || fresh.oidcSID != b.OIDCSID || fresh.subject != b.Subject || string(fresh.role) != b.Role || fresh.tenantID != b.TenantID {
			fail(status.Error(codes.Unauthenticated, "renewal identity changed"))
			return
		}
		b.SourceHash = fresh.hash
		b.OIDCExpiresAt = fresh.oidcExpires
		// Retain the original login cutoff across token refresh.
		// A provider logout must also end sessions whose source tokens rotate.
		c.Renewal.Binding = b
	case "replace", "revoke":
		if c.ID != record.Binding.ID {
			fail(status.Error(codes.PermissionDenied, "session identity mismatch"))
			return
		}
	default:
		fail(status.Error(codes.InvalidArgument, "unknown console session action"))
		return
	}
	if action != "cleanup" && action != "revoke" && action != "recover" && h.server.admission.reason() != "" {
		fail(status.Error(codes.Unavailable, "console session write admission held"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	result, err := h.server.application.Store.ProposeConsoleSession(ctx, c)
	if err != nil {
		if result.Rejection != nil {
			code := codes.FailedPrecondition
			switch result.Rejection.Code {
			case "conflict":
				code = codes.Aborted
			case "capacity":
				code = codes.ResourceExhausted
			case "expired", "revoked":
				code = codes.Unauthenticated
			case "invalid":
				code = codes.InvalidArgument
			}
			fail(status.Error(code, result.Rejection.Message))
			return
		}
		fail(status.Error(codes.Unavailable, "console session write outcome unavailable"))
		return
	}
	writeConsoleJSON(w, result)
}

func writeConsoleJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (h *consoleSessions) cleanup(ctx context.Context) {
	h.sampleMetrics()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.sampleMetrics()
			if !h.server.application.Raft.IsLeader() {
				continue
			}
			needed, checkErr := h.server.application.Store.ConsoleSessionCleanupNeeded(time.Now().UTC())
			if checkErr != nil {
				h.metrics.cleanupFailures.Inc()
				continue
			}
			if !needed {
				continue
			}
			probe, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := h.barrier(probe)
			if err == nil {
				_, err = h.server.application.Store.ProposeConsoleSession(probe, db.ConsoleSessionCommand{Action: "cleanup", At: time.Now().UTC()})
			}
			if err != nil {
				h.metrics.cleanupFailures.Inc()
			} else {
				h.metrics.cleanupLastSuccess.Set(float64(time.Now().UnixNano()) / 1e9)
			}
			cancel()
		}
	}
}

func (h *consoleSessions) sampleMetrics() {
	if h.metrics == nil {
		return
	}
	stats, err := h.server.application.Store.ConsoleSessionStats()
	if err != nil {
		h.metrics.sampleFailures.Inc()
		return
	}
	h.metrics.observe(stats)
}
