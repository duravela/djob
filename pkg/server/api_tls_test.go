package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func apiTestCertificate(t *testing.T) (*APITLSConfig, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	dir := t.TempDir()
	cfg := &APITLSConfig{CertFile: filepath.Join(dir, "cert.pem"), KeyFile: filepath.Join(dir, "key.pem")}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	require.NoError(t, os.WriteFile(cfg.CertFile, certPEM, 0600))
	require.NoError(t, os.WriteFile(cfg.KeyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0600))
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(certPEM))
	return cfg, roots
}

func TestTLSProtectsBothAPIsAndVerifiesServerIdentity(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = apiCredentials()
	var roots *x509.CertPool
	cfg.APITLS, roots = apiTestCertificate(t)
	s := startAPI(t, cfg)
	require.Equal(t, uint16(tls.VersionTLS12), s.apiTLS.MinVersion)
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	endpoint := "https://" + s.HTTPAddr()
	authenticatedHTTP(t, client, endpoint, http.MethodGet, "/api/v1/channels/", "", nil, nil, 401, nil)
	authenticatedHTTP(t, client, endpoint, http.MethodGet, "/api/v1/channels/", apiCredentials().Credentials[0].Token, nil, &pb.ListChannelsResponse{}, 200, nil)
	conn, err := grpc.NewClient(s.RPCAddr(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12})))
	require.NoError(t, err)
	defer conn.Close()
	c := pb.NewSchedulerClient(conn)
	_, err = c.ListChannels(apiAuthContext(apiCredentials().Credentials[0].Token), &pb.ListChannelsRequest{})
	require.NoError(t, err)
	_, err = c.ListChannels(context.Background(), &pb.ListChannelsRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	badTransport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: x509.NewCertPool(), MinVersion: tls.VersionTLS12}}
	defer badTransport.CloseIdleConnections()
	badClient := &http.Client{Transport: badTransport, Timeout: time.Second}
	_, err = badClient.Get(endpoint + "/healthz")
	require.Error(t, err, "unknown server certificates must fail verification")
	badConn, err := grpc.NewClient(s.RPCAddr(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: "wrong.example", MinVersion: tls.VersionTLS12})))
	require.NoError(t, err)
	defer badConn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = pb.NewSchedulerClient(badConn).ListChannels(ctx, &pb.ListChannelsRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err), "gRPC must verify the server name")
}

func TestInvalidSecurityConfigurationFailsBeforeOpeningStorage(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*Config)
	}{
		{"empty auth", func(c *Config) { c.APIAuth = &APIAuthConfig{} }},
		{"public HTTP without TLS", func(c *Config) { c.APIAuth = apiCredentials(); c.HTTPAddr = "0.0.0.0:0" }},
		{"public gRPC without TLS", func(c *Config) { c.APIAuth = apiCredentials(); c.Addr = ":0" }},
		{"certificate only", func(c *Config) { c.APITLS = &APITLSConfig{CertFile: "missing"} }},
		{"missing key pair", func(c *Config) { c.APITLS = &APITLSConfig{CertFile: "missing", KeyFile: "missing"} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := apiTestConfig(filepath.Join(t.TempDir(), "unopened"))
			test.edit(&cfg)
			s, err := Start(context.Background(), cfg)
			require.Error(t, err)
			require.Nil(t, s)
			_, err = os.Stat(cfg.Raft.DataDir)
			require.True(t, os.IsNotExist(err), "invalid credentials or TLS must fail before storage initialization")
		})
	}
}
