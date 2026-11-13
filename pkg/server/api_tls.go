package server

import (
	"crypto/tls"
	"fmt"
	"net"
)

// APITLSConfig encrypts the HTTP and gRPC APIs using the same certificate. Raft
// peer traffic remains a separate transport and trust boundary.
type APITLSConfig struct {
	CertFile string
	KeyFile  string
}

func loadAPITLS(cfg *APITLSConfig) (*tls.Config, error) {
	if cfg == nil {
		return nil, nil
	}
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return nil, fmt.Errorf("API TLS requires both a certificate and a private key")
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load API TLS certificate: %w", err)
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}, nil
}

func validateAPIAuthTransport(cfg Config, auth *apiAuthorizer, transport *tls.Config) error {
	if auth == nil || transport != nil {
		return nil
	}
	for _, address := range []string{cfg.Addr, cfg.HTTPAddr} {
		host, _, err := net.SplitHostPort(address)
		ip := net.ParseIP(host)
		if err != nil || ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("authenticated APIs require TLS or literal loopback listen addresses")
		}
	}
	return nil
}
