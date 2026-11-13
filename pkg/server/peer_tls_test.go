package server

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/duravela/djob/pkg/base"
	"github.com/duravela/djob/pkg/node/util/testutil"
	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/stretchr/testify/require"
)

func TestPeerTLSKeepsAPIAndClusterTrustIndependent(t *testing.T) {
	ca := testutil.NewPeerCA(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	peerURL := "https://" + listener.Addr().String()
	require.NoError(t, listener.Close())
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = apiCredentials()
	cfg.Raft.Peers = []string{peerURL}
	cfg.Raft.PeerTLS = ca.Member(t, []string{"127.0.0.1"})
	apiTLS, apiRoots := apiTestCertificate(t)
	cfg.APITLS = apiTLS
	s := startAPI(t, cfg)
	apiClient := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: apiRoots, MinVersion: tls.VersionTLS12}}}
	defer apiClient.CloseIdleConnections()
	authenticatedHTTP(t, apiClient, "https://"+s.HTTPAddr(), http.MethodGet, "/api/v1/channels/", apiCredentials().Credentials[0].Token, nil, &pb.ListChannelsResponse{}, http.StatusOK, nil)
	// A public API Bearer credential cannot establish a peer TLS connection.
	peerClient := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: ca.Roots, MinVersion: tls.VersionTLS13}}}
	defer peerClient.CloseIdleConnections()
	req, err := http.NewRequest(http.MethodGet, peerURL+"/raft/probing", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+apiCredentials().Credentials[0].Token)
	response, err := peerClient.Do(req)
	if response != nil {
		response.Body.Close()
	}
	require.Error(t, err)
	// The peer CA cannot substitute for the public API's distinct server trust.
	_, err = peerClient.Get("https://" + s.HTTPAddr() + "/healthz")
	require.Error(t, err)
	memberPair, err := tls.LoadX509KeyPair(cfg.Raft.PeerTLS.CertFile, cfg.Raft.PeerTLS.KeyFile)
	require.NoError(t, err)
	clientWithPeerIdentity := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: apiRoots, Certificates: []tls.Certificate{memberPair}, MinVersion: tls.VersionTLS12}}}
	defer clientWithPeerIdentity.CloseIdleConnections()
	authenticatedHTTP(t, clientWithPeerIdentity, "https://"+s.HTTPAddr(), http.MethodGet, "/api/v1/channels/", "", nil, nil, http.StatusUnauthorized, nil)
}

func TestInvalidPeerTLSFailsBeforeAPIsOrRaftStorageStart(t *testing.T) {
	for _, tlsConfig := range []*base.RaftTLSConfig{{}, {CAFile: "missing", CertFile: "missing", KeyFile: "missing"}} {
		cfg := apiTestConfig(filepath.Join(t.TempDir(), "unopened"))
		cfg.Raft.Peers = []string{"https://127.0.0.1:0"}
		cfg.Raft.PeerTLS = tlsConfig
		s, err := Start(context.Background(), cfg)
		require.Error(t, err)
		require.Nil(t, s)
		_, err = os.Stat(cfg.Raft.DataDir)
		require.True(t, os.IsNotExist(err))
	}
}
