package node

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"github.com/duravela/djob/pkg/base"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/raft/v3/raftpb"
)

func validateRaftPeerURLs(cfg base.RaftConfig) error {
	scheme := "http"
	if cfg.PeerTLS != nil {
		scheme = "https"
	}
	seen := make(map[string]bool)
	for _, peer := range cfg.Peers {
		u, err := url.Parse(peer)
		if err != nil || u.Scheme != scheme || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
			return fmt.Errorf("Raft peers must all use %s://host:port URLs; HTTPS requires peer TLS credentials", scheme)
		}
		host, port, err := net.SplitHostPort(u.Host)
		number, numberErr := strconv.Atoi(port)
		if err != nil || numberErr != nil || host == "" || number < 0 || number > 65535 || (number == 0 && len(cfg.Peers) != 1) || seen[u.Host] {
			return fmt.Errorf("invalid or duplicate peer address %q", u.Host)
		}
		seen[u.Host] = true
	}
	return nil
}

// ValidateRaftPeerTLSConfig validates credentials without opening listeners or storage.
func ValidateRaftPeerTLSConfig(cfg base.RaftConfig) error {
	_, _, err := loadRaftPeerTLS(cfg)
	return err
}

// Peer TLS has a separate cluster trust root from the public API. Every member
// authenticates as both a TLS server and a client. The CA authorizes peer access;
// this does not turn Raft into a protocol that tolerates malicious members.
func loadRaftPeerTLS(cfg base.RaftConfig) (*tls.Config, transport.TLSInfo, error) {
	var info transport.TLSInfo
	if err := validateRaftPeerURLs(cfg); err != nil {
		return nil, info, err
	}
	if cfg.PeerTLS == nil {
		return nil, info, nil
	}
	c := *cfg.PeerTLS
	if cfg.Config == nil || cfg.ID == 0 || cfg.ID > uint64(len(cfg.Peers)) {
		return nil, info, fmt.Errorf("peer TLS requires a node ID in the ordered peer list")
	}
	if cfg.Join && !cfg.DynamicMembership {
		return nil, info, fmt.Errorf("dynamic joining is not supported with Raft peer TLS")
	}
	if c.CAFile == "" || c.CertFile == "" || c.KeyFile == "" {
		return nil, info, fmt.Errorf("Raft peer TLS requires a CA, certificate and private key")
	}
	keyInfo, err := os.Stat(c.KeyFile)
	if err != nil {
		return nil, info, fmt.Errorf("inspect Raft peer TLS key: %w", err)
	}
	if !keyInfo.Mode().IsRegular() || keyInfo.Mode().Perm()&0077 != 0 {
		return nil, info, fmt.Errorf("Raft peer TLS private key must be a regular file with owner-only permissions")
	}
	pair, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, info, fmt.Errorf("load Raft peer TLS certificate and key: %w", err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, info, fmt.Errorf("parse Raft peer TLS certificate: %w", err)
	}
	if leaf.IsCA {
		return nil, info, fmt.Errorf("Raft peer TLS requires a leaf certificate, not a CA certificate")
	}
	rootsPEM, err := os.ReadFile(c.CAFile)
	if err != nil {
		return nil, info, fmt.Errorf("read Raft peer TLS CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(rootsPEM) {
		return nil, info, fmt.Errorf("Raft peer TLS CA contains no certificates")
	}
	intermediates := x509.NewCertPool()
	for _, der := range pair.Certificate[1:] {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, info, fmt.Errorf("parse Raft peer TLS chain: %w", err)
		}
		intermediates.AddCert(cert)
	}
	localURL, _ := url.Parse(cfg.Peers[cfg.ID-1])
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, DNSName: localURL.Hostname(), KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
			return nil, info, fmt.Errorf("verify local Raft peer TLS certificate: %w", err)
		}
	}
	info = transport.TLSInfo{CertFile: c.CertFile, KeyFile: c.KeyFile, TrustedCAFile: c.CAFile, ClientCertAuth: true, MinVersion: tls.VersionTLS13}
	// Keep inbound credentials fixed for the member's lifetime. Restart all peer
	// connections after certificate/trust changes; no insecure reload fallback.
	server := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots, NextProtos: []string{"http/1.1"}}
	return server, info, nil
}

func secureRaftPeerHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
			http.Error(w, "verified Raft peer certificate required", http.StatusForbidden)
			return
		}
		// The application has a static ordered peer list. Do not allow the etcd
		// handler to discover arbitrary URLs from request headers, including HTTP.
		r.Header.Del("X-PeerURLs")
		next.ServeHTTP(w, r)
	})
}

func validateRaftTLSConfChange(cfg base.RaftConfig, cc raftpb.ConfChange) error {
	if cfg.PeerTLS == nil {
		return nil
	}
	if cc.NodeID == 0 || cc.NodeID > uint64(len(cfg.Peers)) {
		return fmt.Errorf("Raft peer TLS configuration change must select a configured member")
	}
	if len(cc.Context) > 0 && string(cc.Context) != cfg.Peers[cc.NodeID-1] && !isMembershipContext(cc.Context) && !isBootstrapContext(cc.Context) {
		return fmt.Errorf("Raft peer TLS configuration change must retain the configured HTTPS peer URL")
	}
	return nil
}
