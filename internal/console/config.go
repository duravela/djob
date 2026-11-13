// Package console serves the management application as an ordinary authenticated
// client of controller HTTP APIs. It never opens a controller store or joins Raft.
package console

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

type Config struct {
	CompanyForms   []CompanyForm
	OIDC           *OIDCConfig
	Controllers    []string
	Origin         string
	TLS            *tls.Config // Controller trust; certificate verification is mandatory.
	SessionTTL     time.Duration
	MaxSessions    int
	SharedSessions bool // Store browser sessions in the controller cluster.
}

func endpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u == nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" || u.Opaque != "" {
		return nil, fmt.Errorf("use an http(s) origin without credentials, path, query, or fragment")
	}
	if u.Scheme != "https" && (u.Scheme != "http" || !loopback(u.Hostname())) {
		return nil, fmt.Errorf("HTTPS is required except for literal loopback addresses")
	}
	if strings.ContainsAny(u.Host, "\\\r\n\t ") {
		return nil, fmt.Errorf("invalid origin host")
	}
	u.Path = ""
	return u, nil
}

func loopback(host string) bool {
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// ValidateListener prevents accidental cleartext exposure of login credentials.
// A TLS reverse proxy may connect to a literal loopback listener while preserving
// the configured public Host and Origin; forwarded headers are never trusted.
func ValidateListener(address, origin string, tlsEnabled bool) error {
	u, err := endpoint(origin)
	if err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("listen address requires host:port")
	}
	if tlsEnabled && u.Scheme != "https" {
		return fmt.Errorf("a TLS listener requires an HTTPS public origin")
	}
	if !tlsEnabled && !loopback(host) {
		return fmt.Errorf("non-loopback console listeners require TLS")
	}
	return nil
}
