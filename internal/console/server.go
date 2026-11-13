package console

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//go:embed ui/index.html ui/app.js ui/model.js ui/history.js ui/routing.js ui/usage.js ui/enrollments.js ui/deployments.js ui/recovery.js ui/references.js ui/forms.js ui/style.css
var assets embed.FS

const api = "/api/console/v1/"
const maxRequest = 128 << 10
const maxReply = 8 << 20

type Identity struct {
	Subject   string    `json:"subject"`
	Role      string    `json:"role"`
	TenantID  string    `json:"tenant_id"`
	ExpiresAt time.Time `json:"expires_at,omitzero"`
}

type session struct {
	releaseMu     sync.Mutex
	replacement   *replacementPlan
	releasePlan   *releasePlan
	renewedToken  atomic.Pointer[string]
	oidc          *oidcSession
	draft         json.RawMessage
	draftRevision uint64
	shared        *sharedSession
	mu            sync.Mutex
	token         string
	csrf          string
	identity      Identity
	expires       time.Time
	expiry        *time.Timer // Guarded by Server.mu, together with sessions.
	controller    int
	pending       *pendingMutation
}

type Server struct {
	forms       []CompanyForm
	oidc        *oidcLogin
	draining    bool // Guarded by mu; readiness only, existing traffic may finish.
	shared      bool
	origin      *url.URL
	controllers []*url.URL
	client      *http.Client
	ttl         time.Duration
	maxSessions int
	cookieName  string
	mu          sync.Mutex
	sessions    map[string]*session
	closed      bool
	discovery   chan struct{}
	mux         *http.ServeMux
}

func New(cfg Config) (*Server, error) {
	origin, err := endpoint(cfg.Origin)
	if err != nil {
		return nil, fmt.Errorf("public origin: %w", err)
	}
	if len(cfg.Controllers) == 0 || len(cfg.Controllers) > 16 {
		return nil, fmt.Errorf("configure 1 to 16 controller origins")
	}
	s := &Server{origin: origin, ttl: cfg.SessionTTL, maxSessions: cfg.MaxSessions, sessions: make(map[string]*session), cookieName: "djob-console", discovery: make(chan struct{}, 4)}
	s.shared = cfg.SharedSessions
	s.forms = cfg.CompanyForms
	if s.ttl == 0 {
		s.ttl = 8 * time.Hour
	}
	if s.ttl < time.Minute || s.ttl > 24*time.Hour {
		return nil, fmt.Errorf("session lifetime must be in 1m..24h")
	}
	if s.maxSessions == 0 {
		s.maxSessions = 128
	}
	if s.maxSessions < 1 || s.maxSessions > 1024 {
		return nil, fmt.Errorf("session capacity must be in 1..1024")
	}
	for _, raw := range cfg.Controllers {
		u, err := endpoint(raw)
		if err != nil {
			return nil, fmt.Errorf("controller origin: %w", err)
		}
		s.controllers = append(s.controllers, u)
	}
	trust := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.TLS != nil {
		trust = cfg.TLS.Clone()
		if trust.InsecureSkipVerify {
			return nil, fmt.Errorf("controller certificate verification cannot be disabled")
		}
		trust.MinVersion = max(trust.MinVersion, tls.VersionTLS12)
	}
	s.client = &http.Client{
		Timeout:       10 * time.Second,
		Transport:     &http.Transport{TLSClientConfig: trust, MaxIdleConns: 32, MaxIdleConnsPerHost: 8, IdleConnTimeout: 30 * time.Second, ResponseHeaderTimeout: 8 * time.Second},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	if origin.Scheme == "https" {
		s.cookieName = "__Host-djob-console"
	}
	if err := s.configureOIDC(cfg.OIDC); err != nil {
		s.client.CloseIdleConnections()
		return nil, err
	}
	s.mux = http.NewServeMux()
	s.mux.HandleFunc("GET /{$}", s.asset("index.html", "text/html; charset=utf-8"))
	s.mux.HandleFunc("GET /app.js", s.asset("app.js", "text/javascript; charset=utf-8"))
	s.mux.HandleFunc("GET /forms.js", s.asset("forms.js", "text/javascript; charset=utf-8"))
	s.mux.HandleFunc("GET /model.js", s.asset("model.js", "text/javascript; charset=utf-8"))
	s.mux.HandleFunc("GET /references.js", s.asset("references.js", "text/javascript; charset=utf-8"))
	s.mux.HandleFunc("GET /recovery.js", s.asset("recovery.js", "text/javascript; charset=utf-8"))
	s.mux.HandleFunc("GET /history.js", s.asset("history.js", "text/javascript; charset=utf-8"))
	s.mux.HandleFunc("GET /routing.js", s.asset("routing.js", "text/javascript; charset=utf-8"))
	s.mux.HandleFunc("GET /usage.js", s.asset("usage.js", "text/javascript; charset=utf-8"))
	s.mux.HandleFunc("GET /enrollments.js", s.asset("enrollments.js", "text/javascript; charset=utf-8"))
	s.mux.HandleFunc("GET /deployments.js", s.asset("deployments.js", "text/javascript; charset=utf-8"))
	s.mux.HandleFunc("GET /style.css", s.asset("style.css", "text/css; charset=utf-8"))
	s.mux.HandleFunc("GET "+api+"oidc/config", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]bool{"enabled": s.oidc != nil})
	})
	s.mux.HandleFunc("GET "+api+"oidc/start", s.oidcStart)
	s.mux.HandleFunc("GET "+api+"oidc/callback", s.oidcCallback)
	s.mux.HandleFunc("POST "+api+"oidc/refresh", s.auth(s.oidcRefresh))
	s.mux.HandleFunc("POST "+api+"oidc/backchannel", s.oidcBackchannel)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	s.mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		ready := !s.closed && !s.draining
		s.mu.Unlock()
		if !ready {
			problem(w, 503, "console draining")
			return
		}
		writeJSON(w, 200, map[string]bool{"ready": true})
	})
	s.mux.HandleFunc("PUT "+api+"draft", s.auth(s.saveDraft))
	s.mux.HandleFunc("POST "+api+"session", s.login)
	s.mux.HandleFunc("GET "+api+"session", s.auth(s.sessionInfo))
	s.mux.HandleFunc("DELETE "+api+"session", s.auth(s.logout))
	s.mux.HandleFunc("POST "+api+"query/{method}", s.auth(s.query))
	s.mux.HandleFunc("POST "+api+"workers", s.auth(s.workers))
	s.mux.HandleFunc("POST "+api+"worker-enrollments/{method}", s.auth(s.enrollmentQuery))
	s.mux.HandleFunc("POST "+api+"worker-deployments/{method}", s.auth(s.deploymentQuery))
	s.mux.HandleFunc("POST "+api+"cluster/{method}", s.auth(s.cluster))
	s.mux.HandleFunc("POST "+api+"forms", s.auth(s.companyForms))
	s.mux.HandleFunc("POST "+api+"release-plans/{method}", s.auth(s.releasePlans))
	s.mux.HandleFunc("POST "+api+"gateways/{method}", s.auth(s.gatewayQuery))
	s.mux.HandleFunc("POST "+api+"mutations/gateway", s.auth(s.prepareGateway))
	s.mux.HandleFunc("POST "+api+"mutations/worker-deployment", s.auth(s.prepareDeployment))
	s.mux.HandleFunc("POST "+api+"mutations/worker-enrollment", s.auth(s.prepareEnrollment))
	s.mux.HandleFunc("POST "+api+"mutations/worker-config", s.auth(s.downloadWorkerConfig))
	s.mux.HandleFunc("POST "+api+"mutations", s.auth(s.prepare))
	s.mux.HandleFunc("POST "+api+"mutations/commit", s.auth(s.commit))
	s.mux.HandleFunc("DELETE "+api+"mutations/current", s.auth(s.discard))
	return s, nil
}

func (s *Server) Close() {
	s.client.CloseIdleConnections()
	s.mu.Lock()
	s.closed = true
	for id := range s.sessions {
		s.removeSessionLocked(id)
	}
	s.mu.Unlock()
}

// BeginDrain removes readiness before the listener shuts down. Traffic already
// routed here remains serviceable while load balancers update their endpoints.
func (s *Server) BeginDrain() { s.mu.Lock(); s.draining = true; s.mu.Unlock() }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'")
	if !strings.EqualFold(r.Host, s.origin.Host) {
		problem(w, 421, "request Host does not match the configured public origin")
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead && r.URL.Path != api+"oidc/backchannel" {
		if r.Header.Get("Origin") != s.origin.String() || r.Header.Get("Sec-Fetch-Site") == "cross-site" {
			problem(w, 403, "same-origin requests are required")
			return
		}
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) asset(name, contentType string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		data, err := assets.ReadFile("ui/" + name)
		if err != nil {
			problem(w, 500, "console asset unavailable")
			return
		}
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(data)
	}
}

func randomID() (string, error) {
	var data [32]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(data[:]), nil
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func problem(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}

func readBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return nil, fmt.Errorf("Content-Type must be application/json")
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		return nil, fmt.Errorf("request exceeds its size limit or cannot be read")
	}
	return data, nil
}

func decode(w http.ResponseWriter, r *http.Request, limit int64, value any) error {
	data, err := readBody(w, r, limit)
	if err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return fmt.Errorf("invalid JSON request")
	}
	if d.Decode(new(any)) != io.EOF {
		return fmt.Errorf("request must contain one JSON value")
	}
	return nil
}

type reply struct {
	status int
	body   []byte
}

// Reads may fail over. A mutation is sent once per explicit commit/retry, always
// with the exact prepared envelope. Redirects never receive credentials.
func (s *Server) request(ctx context.Context, token string, start int, path, method string, body []byte, retryReads bool, params ...url.Values) (reply, int, error) {
	tries := 1
	if retryReads {
		tries = len(s.controllers)
	}
	for n := 0; n < tries; n++ {
		index := (start + n) % len(s.controllers)
		u := *s.controllers[index]
		u.Path = "/api/jobs/v1/" + path
		switch path {
		case "legacy/GetChannel", "legacy/GetResolution", "legacy/ResolveChannel", "legacy/ExpireChannel":
			u.Path = "/scheduler.Scheduler/" + strings.TrimPrefix(path, "legacy/")
		}
		if len(params) > 0 {
			u.RawQuery = params[0].Encode()
		}
		r, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
		if err != nil {
			return reply{}, index, fmt.Errorf("cannot construct controller request")
		}
		r.Header.Set("Authorization", "Bearer "+token)
		if method == http.MethodPost {
			r.Header.Set("Content-Type", "application/json")
		}
		if path == "console-sessions/renew" {
			if token, ok := ctx.Value(oidcRenewKey{}).(string); ok {
				r.Header.Set("X-Djob-OIDC-Token", token)
			}
		}
		if path == "oidc/logout" {
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
		response, err := s.client.Do(r)
		if err != nil {
			if n+1 < tries {
				continue
			}
			return reply{}, (index + 1) % len(s.controllers), fmt.Errorf("controller response unavailable; the outcome of a submitted mutation may be uncertain")
		}
		data, readErr := io.ReadAll(io.LimitReader(response.Body, maxReply+1))
		_ = response.Body.Close()
		if readErr != nil || len(data) > maxReply || !json.Valid(data) {
			return reply{}, index, fmt.Errorf("controller response is incomplete, oversized, or invalid")
		}
		if response.StatusCode >= 500 && n+1 < tries {
			continue
		}
		if response.StatusCode >= 300 && response.StatusCode < 400 {
			return reply{}, index, fmt.Errorf("controller redirects are refused")
		}
		return reply{response.StatusCode, data}, index, nil
	}
	panic("controller request without an attempt")
}

func (s *Server) auth(next func(http.ResponseWriter, *http.Request, *session)) http.HandlerFunc {
	if s.shared {
		return s.sharedAuth(next)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie(s.cookieName)
		if err != nil {
			problem(w, 401, "sign in to the console")
			return
		}
		s.mu.Lock()
		sess := s.sessions[cookie.Value]
		if sess != nil && !time.Now().Before(sess.expires) {
			s.removeSessionLocked(cookie.Value)
			sess = nil
		}
		s.mu.Unlock()
		if sess == nil {
			problem(w, 401, "console session expired; sign in again")
			return
		}
		if r.Method != http.MethodGet && subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Djob-CSRF")), []byte(sess.csrf)) != 1 {
			problem(w, 403, "invalid console CSRF token")
			return
		}
		next(w, r, sess)
	}
}
