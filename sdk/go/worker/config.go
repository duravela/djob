// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

// Package worker runs company handlers and drivers outside a djob Raft member.
// Workers initiate authenticated gRPC streams, execute one conversation at a time and keep
// its live driver local. Connection loss cancels I/O and ends the session; the
// supervisor can establish a fresh session after safe local cleanup. External
// operations and previous sessions are never replayed or resumed.
package worker

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/duravela/djob/sdk/go"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
)

type Transport string

const (
	TransportGRPC Transport = "grpc"
	// TransportHTTPSV1 is an explicit upgrade bridge for older controllers.
	TransportHTTPSV1 Transport = "https-v1"
)

type Config struct {
	DeploymentID         string
	DeploymentGeneration uint64

	Transport      Transport // Defaults to gRPC. There is no automatic transport downgrade.
	TLSConfig      *tls.Config
	RequestTimeout time.Duration // Bounds registration and individual stream requests; defaults to 5s.
	ServerURL      string
	// FallbackURLs are explicit trusted controller origins for Serve. Run uses
	// only ServerURL. All members must accept the same tenant credential.
	FallbackURLs []string
	WorkerID     string
	TenantID     string
	Token        string
	Registry     djob.Registry
	// Client configures HTTPS v1. For source compatibility, its standard HTTP
	// transport's TLS settings are also used by gRPC when TLSConfig is nil.
	Client *http.Client
	// CleanupTimeout bounds each cooperative recovery/close callback when a
	// session ends with a local driver still retained.
	// Run still waits for callbacks to return; it never detaches live driver I/O.
	CleanupTimeout time.Duration
	DrainTimeout   time.Duration // Grace after cancellation to finish the current conversation; defaults to 30s.
}

func validToken(s string) bool {
	if len(s) < 32 || len(s) > 256 {
		return false
	}
	for _, ch := range s {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || strings.ContainsRune("-._~+/=", ch)) {
			return false
		}
	}
	return true
}

func strict(data []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(target) != nil || d.Decode(new(any)) != io.EOF {
		return fmt.Errorf("invalid worker message")
	}
	return nil
}

// LoadConfig loads process-local credentials and verified TLS material. Relative
// certificate paths are resolved against the private configuration file.
func LoadConfig(path string) (Config, error) {
	environment := Config{}
	projectedRoot, err := managedEnvironment(&environment)
	if err != nil {
		return Config{}, err
	}
	var file struct {
		Transport    Transport `json:"transport,omitempty"`
		ServerURL    string    `json:"server_url"`
		FallbackURLs []string  `json:"fallback_urls,omitempty"`
		WorkerID     string    `json:"worker_id"`
		TenantID     string    `json:"tenant_id"`
		Token        string    `json:"token"`
		CAFile       string    `json:"ca_file,omitempty"`
		CertFile     string    `json:"cert_file,omitempty"`
		KeyFile      string    `json:"key_file,omitempty"`
	}
	privatePath, info, err := privateWorkerPath(path, projectedRoot)
	if err != nil || info.Size() > 64<<10 {
		return Config{}, fmt.Errorf("worker configuration must be a private regular file of at most 64 KiB")
	}
	data, err := os.ReadFile(privatePath)
	if err != nil || strict(data, &file) != nil {
		return Config{}, fmt.Errorf("cannot read worker configuration")
	}
	resolve := func(p string) string {
		if filepath.IsAbs(p) {
			return p
		}
		return filepath.Join(filepath.Dir(path), p)
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if file.CAFile != "" {
		pem, err := os.ReadFile(resolve(file.CAFile))
		pool := x509.NewCertPool()
		if err != nil || !pool.AppendCertsFromPEM(pem) {
			return Config{}, fmt.Errorf("invalid worker server CA")
		}
		tlsConfig.RootCAs = pool
	}
	if file.CertFile != "" || file.KeyFile != "" {
		keyPath, _, err := privateWorkerPath(resolve(file.KeyFile), projectedRoot)
		if file.CertFile == "" || file.KeyFile == "" || err != nil {
			return Config{}, fmt.Errorf("worker client TLS requires a certificate and private regular key file")
		}
		cert, err := tls.LoadX509KeyPair(resolve(file.CertFile), keyPath)
		if err != nil {
			return Config{}, fmt.Errorf("invalid worker client TLS certificate or key")
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	return Config{DeploymentID: environment.DeploymentID, DeploymentGeneration: environment.DeploymentGeneration, Transport: file.Transport, TLSConfig: tlsConfig, ServerURL: file.ServerURL, FallbackURLs: file.FallbackURLs, WorkerID: file.WorkerID, TenantID: file.TenantID, Token: file.Token, Client: &http.Client{Transport: transport, Timeout: 5 * time.Second}}, nil
}

func serverOrigin(value string) (string, error) {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", fmt.Errorf("worker server requires an HTTPS origin without credentials or parameters")
	}
	if u.Port() != "" {
		port, err := strconv.Atoi(u.Port())
		if err != nil || port < 1 || port > 65535 {
			return "", fmt.Errorf("invalid worker server port")
		}
	}
	u.Host = strings.ToLower(u.Host)
	u.Path, u.RawPath = "", ""
	return u.String(), nil
}

func (c *Config) validate() error {
	if _, err := managedEnvironment(c); err != nil {
		return err
	}
	if c.Transport == "" {
		c.Transport = TransportGRPC
	}
	if c.Transport != TransportGRPC && c.Transport != TransportHTTPSV1 {
		return fmt.Errorf("unsupported worker transport")
	}
	if c.DeploymentID != "" && c.Transport != TransportGRPC {
		return fmt.Errorf("managed workers require the gRPC transport")
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 5 * time.Second
	}
	if c.RequestTimeout < 10*time.Millisecond || c.RequestTimeout > 10*time.Second {
		return fmt.Errorf("worker request timeout must be in 10ms..10s")
	}
	if len(c.FallbackURLs) > 7 {
		return fmt.Errorf("worker supports at most eight controller origins")
	}
	origins := append([]string{c.ServerURL}, c.FallbackURLs...)
	seen := make(map[string]bool)
	for i, origin := range origins {
		normalized, err := serverOrigin(origin)
		if err != nil {
			return err
		}
		if seen[normalized] {
			return fmt.Errorf("worker controller origins must be distinct")
		}
		seen[normalized] = true
		origins[i] = normalized
	}
	c.ServerURL, c.FallbackURLs = origins[0], origins[1:]
	if !wire.ValidWorkerID(c.WorkerID) || strings.TrimSpace(c.TenantID) == "" || len(c.TenantID) > 256 || !utf8.ValidString(c.TenantID) || !validToken(c.Token) {
		return fmt.Errorf("worker requires an ID, expected tenant and valid bearer credential")
	}
	if len(c.Registry.Handlers) == 0 || len(c.Registry.Handlers) > 64 || len(c.Registry.Drivers) > 64 {
		return fmt.Errorf("worker requires 1..64 handlers and at most 64 drivers")
	}
	versionOK := func(v djob.Version) bool {
		return strings.TrimSpace(v.Name) != "" && strings.TrimSpace(v.Version) != "" && len(v.Name) <= 256 && len(v.Version) <= 256 && utf8.ValidString(v.Name) && utf8.ValidString(v.Version)
	}
	for v, handler := range c.Registry.Handlers {
		if !versionOK(v) || handler == nil {
			return fmt.Errorf("invalid worker handler registration")
		}
	}
	for v, factory := range c.Registry.Drivers {
		if !versionOK(v) || factory == nil {
			return fmt.Errorf("invalid worker driver registration")
		}
	}
	if c.CleanupTimeout == 0 {
		c.CleanupTimeout = 3 * time.Second
	}
	if c.CleanupTimeout < time.Millisecond || c.CleanupTimeout > time.Minute {
		return fmt.Errorf("worker cleanup timeout must be in 1ms..1m")
	}
	if c.DrainTimeout == 0 {
		c.DrainTimeout = 30 * time.Second
	}
	if c.DrainTimeout < time.Millisecond || c.DrainTimeout > 5*time.Minute {
		return fmt.Errorf("worker drain timeout must be in 1ms..5m")
	}
	base := c.Client
	if base == nil {
		base = &http.Client{Timeout: 5 * time.Second}
	}
	copy := *base
	if copy.Transport == nil {
		copy.Transport = http.DefaultTransport
	}
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if copy.Timeout == 0 || copy.Timeout > 10*time.Second {
		copy.Timeout = 5 * time.Second
	}
	if t, ok := copy.Transport.(*http.Transport); ok && t.TLSClientConfig != nil && t.TLSClientConfig.InsecureSkipVerify {
		return fmt.Errorf("worker TLS must verify server identity")
	}
	c.Client = &copy
	if c.Transport == TransportGRPC {
		if c.TLSConfig == nil {
			t, ok := copy.Transport.(*http.Transport)
			if !ok {
				return fmt.Errorf("worker gRPC requires TLSConfig or a standard HTTP transport")
			}
			c.TLSConfig = t.TLSClientConfig
		}
		if c.TLSConfig != nil {
			if c.TLSConfig.InsecureSkipVerify {
				return fmt.Errorf("worker TLS must verify server identity")
			}
			c.TLSConfig = c.TLSConfig.Clone()
			c.TLSConfig.MinVersion = max(tls.VersionTLS12, c.TLSConfig.MinVersion)
		}
	}
	return nil
}
