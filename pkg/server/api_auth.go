package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"github.com/coreos/go-oidc/v3/oidc"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type APIRole string

const (
	APIReader   APIRole = "reader"
	APIWriter   APIRole = "writer"
	APIOperator APIRole = "operator"
	APICallback APIRole = "callback"
	APIProducer APIRole = "producer"
	APIWorker   APIRole = "worker"
	APIMonitor  APIRole = "monitor" // Global metrics only; grants no job or recovery access.
)

// APICredential binds a secret to a stable audit identity. Multiple tokens may
// share a subject during rotation. Credentials never enter Raft or snapshots.
type APICredential struct {
	Subject  string  `json:"subject"`
	Role     APIRole `json:"role"`
	Token    string  `json:"token"`
	TenantID string  `json:"tenant_id,omitempty"`
	SourceID string  `json:"source_id,omitempty"`
}

// A nil APIAuth config retains anonymous ordinary API access. Operator recovery
// always requires a configured operator credential, including on embedded APIs.
type APIAuthConfig struct {
	Credentials []APICredential `json:"credentials"`
	OIDC        *APIOIDCConfig  `json:"oidc,omitempty"`
}

type apiCredential struct {
	oidcSubject, oidcIssuer, oidcSID string
	oidcIssuedAt                     time.Time
	oidcExpires                      time.Time
	workerID                         string // Set only for dynamically enrolled workers.
	subject                          string
	role                             APIRole
	tenantID                         string
	sourceID                         string
	hash                             [32]byte
}

type apiAuthorizer struct {
	credentials        []apiCredential
	oidcIssuer         string
	oidcVerifier       *oidc.IDTokenVerifier
	oidcLogoutVerifier *oidc.IDTokenVerifier
	oidcRevoked        func(context.Context, string, string, string, time.Time) (bool, error)
	oidcBindings       map[string]apiCredential
}

func roleLevel(role APIRole) int {
	switch role {
	case APIReader:
		return 1
	case APICallback, APIProducer, APIWorker, APIMonitor:
		return 1 // Restricted roles are checked explicitly by the relevant service.
	case APIWriter:
		return 2
	case APIOperator:
		return 3
	}
	return 0
}

func newAPIAuthorizer(cfg *APIAuthConfig) (*apiAuthorizer, error) {
	if cfg == nil {
		return nil, nil
	}
	if len(cfg.Credentials) == 0 && cfg.OIDC == nil || len(cfg.Credentials) > 128 {
		return nil, fmt.Errorf("API auth requires 1 to 128 credentials")
	}
	a := &apiAuthorizer{}
	seen := make(map[[32]byte]bool)
	for i, c := range cfg.Credentials {
		if len(c.Subject) == 0 || len(c.Subject) > 128 || strings.ContainsAny(c.Subject, "\r\n\t ") {
			return nil, fmt.Errorf("API credential %d requires a subject of 1 to 128 visible ASCII characters", i)
		}
		for _, ch := range c.Subject {
			if ch < '!' || ch > '~' {
				return nil, fmt.Errorf("API credential %d has an invalid subject", i)
			}
		}
		if roleLevel(c.Role) == 0 {
			return nil, fmt.Errorf("API credential %d requires role reader, writer, operator, callback, producer, worker, or monitor", i)
		}
		if c.TenantID != "" && (len(c.TenantID) > 256 || strings.TrimSpace(c.TenantID) == "" || !utf8.ValidString(c.TenantID)) {
			return nil, fmt.Errorf("API credential %d has an invalid tenant_id", i)
		}
		if c.Role == APICallback && c.TenantID == "" {
			return nil, fmt.Errorf("API callback credential %d requires a tenant_id", i)
		}
		if c.Role == APIWorker && c.TenantID == "" {
			return nil, fmt.Errorf("API worker credential %d requires a tenant_id", i)
		}
		if c.Role == APIMonitor && c.TenantID != "" {
			return nil, fmt.Errorf("API monitor credential %d must be unscoped", i)
		}
		if c.Role == APIProducer {
			if c.TenantID == "" || len(c.SourceID) == 0 || len(c.SourceID) > 256 || strings.TrimSpace(c.SourceID) == "" || !utf8.ValidString(c.SourceID) {
				return nil, fmt.Errorf("API producer credential %d requires tenant_id and a valid source_id", i)
			}
		} else if c.SourceID != "" {
			return nil, fmt.Errorf("API credential %d permits source_id only for producer role", i)
		}
		if !validAPIToken(c.Token) {
			return nil, fmt.Errorf("API credential %d requires a token of 32 to 256 letters, digits, or -._~+/=", i)
		}
		hash := sha256.Sum256([]byte(c.Token))
		if seen[hash] {
			return nil, fmt.Errorf("API credential %d duplicates a token", i)
		}
		seen[hash] = true
		a.credentials = append(a.credentials, apiCredential{subject: c.Subject, role: c.Role, tenantID: c.TenantID, sourceID: c.SourceID, hash: hash})
	}
	if err := a.configureOIDC(cfg.OIDC); err != nil {
		return nil, err
	}
	return a, nil
}

func validAPIToken(token string) bool {
	if len(token) < 32 || len(token) > 256 {
		return false
	}
	for _, ch := range token {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || strings.ContainsRune("-._~+/=", ch) {
			continue
		}
		return false
	}
	return true
}

// LoadAPIAuthConfig reads a bounded, strict JSON file. Parse and validation
// errors identify the field or credential index without echoing secret values.
func LoadAPIAuthConfig(path string) (*APIAuthConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open API auth file: %w", err)
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (64<<10)+1))
	if err != nil || len(data) > 64<<10 {
		return nil, fmt.Errorf("cannot read API auth file (maximum 64 KiB)")
	}
	var cfg APIAuthConfig
	d := json.NewDecoder(strings.NewReader(string(data)))
	d.DisallowUnknownFields()
	if d.Decode(&cfg) != nil || d.Decode(new(any)) != io.EOF {
		return nil, fmt.Errorf("API auth file must contain one JSON object with credentials")
	}
	if _, err := newAPIAuthorizer(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

type httpAuthorizationKey struct{}

// Preserve the actual HTTP Authorization header before grpc-gateway constructs
// metadata. Grpc-Metadata-* and forwarded identity headers cannot supply it.
func captureHTTPAuthorization(r *http.Request) *http.Request {
	values := append([]string(nil), r.Header.Values("Authorization")...)
	return r.WithContext(context.WithValue(r.Context(), httpAuthorizationKey{}, values))
}

func (s *schedulerServer) authorize(ctx context.Context, required APIRole) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", apiError(err)
	}
	if s.auth == nil {
		if required != APIOperator && required != APIMonitor {
			return "", nil
		}
		if required == APIMonitor {
			return "", status.Error(codes.Unauthenticated, "monitor or operator credentials are required")
		}
		return "", status.Error(codes.Unauthenticated, "operator credentials are required")
	}
	principal, err := s.auth.authenticate(ctx)
	if err != nil {
		return "", err
	}
	// Monitoring is a separate capability, outside the ordinary role hierarchy.
	if required == APIMonitor || principal.role == APIMonitor {
		if required == APIMonitor && principal.tenantID == "" && (principal.role == APIMonitor || principal.role == APIOperator) {
			return principal.subject, nil
		}
		return "", status.Error(codes.PermissionDenied, "credential does not permit this operation")
	}
	if principal.tenantID != "" || principal.role == APICallback {
		return "", status.Error(codes.PermissionDenied, "tenant credentials cannot access legacy global APIs")
	}
	if roleLevel(principal.role) < roleLevel(required) {
		return "", status.Error(codes.PermissionDenied, "credential does not permit this operation")
	}
	return principal.subject, nil
}

func (a *apiAuthorizer) authenticate(ctx context.Context) (*apiCredential, error) {
	if err := ctx.Err(); err != nil {
		return nil, apiError(err)
	}
	if a == nil {
		return nil, status.Error(codes.Unauthenticated, "credentials are required for generic jobs")
	}
	values, httpRequest := ctx.Value(httpAuthorizationKey{}).([]string)
	if !httpRequest {
		md, _ := metadata.FromIncomingContext(ctx)
		values = md.Get("authorization")
	}
	if len(values) != 1 {
		return nil, status.Error(codes.Unauthenticated, "one valid Bearer credential is required")
	}
	scheme, token, ok := strings.Cut(values[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || !validBearerToken(token) {
		return nil, status.Error(codes.Unauthenticated, "one valid Bearer credential is required")
	}
	hash := sha256.Sum256([]byte(token))
	var principal *apiCredential
	for i := range a.credentials {
		c := &a.credentials[i]
		if subtle.ConstantTimeCompare(hash[:], c.hash[:]) == 1 {
			principal = c
		}
	}
	if principal == nil {
		principal = a.oidcPrincipal(ctx, token)
	}
	if principal == nil {
		return nil, status.Error(codes.Unauthenticated, "one valid Bearer credential is required")
	}
	return principal, nil
}
