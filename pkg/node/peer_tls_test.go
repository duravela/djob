package node

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/base"
	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node/config"
	"github.com/duravela/djob/pkg/node/util/testutil"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

func TestRaftPeerTLSValidationFailsBeforeStorage(t *testing.T) {
	ca := testutil.NewPeerCA(t)
	cases := []struct {
		name string
		edit func(*config.Config)
	}{
		{"missing key", func(c *config.Config) { c.PeerTLS.KeyFile = "" }},
		{"missing CA", func(c *config.Config) { c.PeerTLS.CAFile = "" }},
		{"missing certificate", func(c *config.Config) { c.PeerTLS.CertFile = "" }},
		{"missing file", func(c *config.Config) { c.PeerTLS.KeyFile = filepath.Join(t.TempDir(), "missing") }},
		{"unprotected key", func(c *config.Config) { require.NoError(t, os.Chmod(c.PeerTLS.KeyFile, 0644)) }},
		{"HTTP with TLS", func(c *config.Config) { c.Peers = []string{"http://127.0.0.1:0"} }},
		{"HTTPS without TLS", func(c *config.Config) { c.PeerTLS = nil }},
		{"mixed peer schemes", func(c *config.Config) { c.Peers = []string{"https://127.0.0.1:12001", "http://127.0.0.1:12002"} }},
		{"unknown CA", func(c *config.Config) { c.PeerTLS.CAFile = testutil.NewPeerCA(t).File }},
		{"wrong hostname", func(c *config.Config) { c.PeerTLS = ca.Member(t, []string{"other.example"}) }},
		{"server-only certificate", func(c *config.Config) {
			c.PeerTLS = ca.Member(t, []string{"127.0.0.1"}, func(v *x509.Certificate) { v.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth} })
		}},
		{"client-only certificate", func(c *config.Config) {
			c.PeerTLS = ca.Member(t, []string{"127.0.0.1"}, func(v *x509.Certificate) { v.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth} })
		}},
		{"expired certificate", func(c *config.Config) {
			c.PeerTLS = ca.Member(t, []string{"127.0.0.1"}, func(v *x509.Certificate) { v.NotAfter = time.Now().Add(-time.Minute) })
		}},
		{"future certificate", func(c *config.Config) {
			c.PeerTLS = ca.Member(t, []string{"127.0.0.1"}, func(v *x509.Certificate) { v.NotBefore = time.Now().Add(time.Hour) })
		}},
		{"CA as member", func(c *config.Config) {
			c.PeerTLS = ca.Member(t, []string{"127.0.0.1"}, func(v *x509.Certificate) { v.IsCA = true; v.KeyUsage |= x509.KeyUsageCertSign })
		}},
		{"mismatched key", func(c *config.Config) { c.PeerTLS.KeyFile = ca.Member(t, []string{"127.0.0.1"}).KeyFile }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := crashTestConfig(filepath.Join(t.TempDir(), "unopened"))
			cfg.Peers = []string{"https://127.0.0.1:0"}
			cfg.PeerTLS = ca.Member(t, []string{"127.0.0.1"})
			tc.edit(&cfg)
			app, err := OpenApplication(context.Background(), cfg)
			require.Error(t, err)
			require.Nil(t, app)
			_, err = os.Stat(cfg.DataDir)
			require.True(t, os.IsNotExist(err))
		})
	}
}

func TestRaftPeerTLSRequiresClientTrustAndVerifiesServerHostname(t *testing.T) {
	ca := testutil.NewPeerCA(t)
	member := ca.Member(t, []string{"127.0.0.1"})
	cfg := crashTestConfig(t.TempDir())
	cfg.Peers = []string{"https://127.0.0.1:0"}
	cfg.PeerTLS = member
	serverTLS, info, err := loadRaftPeerTLS(cfg.RaftConfig)
	require.NoError(t, err)
	var calls atomic.Int64
	server := httptest.NewUnstartedServer(secureRaftPeerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		require.Empty(t, r.Header.Get("X-PeerURLs"))
		w.WriteHeader(http.StatusNoContent)
	})))
	server.TLS = serverTLS
	server.StartTLS()
	defer server.Close()
	roundTripper, err := rafthttp.NewRoundTripper(info, time.Second)
	require.NoError(t, err)

	client := &http.Client{Transport: roundTripper, Timeout: time.Second}
	defer client.CloseIdleConnections()
	request, err := http.NewRequest(http.MethodGet, server.URL+"/raft/probing", nil)
	require.NoError(t, err)
	request.Header.Set("X-PeerURLs", "http://untrusted.example:8080")
	response, err := client.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.Equal(t, uint16(tls.VersionTLS13), response.TLS.Version)
	response.Body.Close()
	trustedPair, err := tls.LoadX509KeyPair(member.CertFile, member.KeyFile)
	require.NoError(t, err)
	foreign := testutil.NewPeerCA(t).Member(t, []string{"127.0.0.1"})
	foreignPair, err := tls.LoadX509KeyPair(foreign.CertFile, foreign.KeyFile)
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		cfg  *tls.Config
	}{
		{"missing client certificate", &tls.Config{RootCAs: ca.Roots, MinVersion: tls.VersionTLS13}},
		{"unknown client CA", &tls.Config{RootCAs: ca.Roots, Certificates: []tls.Certificate{foreignPair}, MinVersion: tls.VersionTLS13}},
		{"unknown server CA", &tls.Config{RootCAs: x509.NewCertPool(), Certificates: []tls.Certificate{trustedPair}, MinVersion: tls.VersionTLS13}},
		{"wrong server name", &tls.Config{RootCAs: ca.Roots, Certificates: []tls.Certificate{trustedPair}, ServerName: "wrong.example", MinVersion: tls.VersionTLS13}},
		{"TLS 1.2", &tls.Config{RootCAs: ca.Roots, Certificates: []tls.Certificate{trustedPair}, MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := &http.Transport{TLSClientConfig: tc.cfg}
			defer tr.CloseIdleConnections()
			c := &http.Client{Transport: tr, Timeout: time.Second}
			response, err := c.Get(server.URL + "/raft")
			if response != nil {
				response.Body.Close()
			}
			require.Error(t, err)
		})
	}
	require.Equal(t, int64(1), calls.Load(), "rejected connections never reach the Raft handler")
	plain, err := http.Post(strings.Replace(server.URL, "https:", "http:", 1)+"/raft", "application/octet-stream", strings.NewReader("plaintext"))
	require.NoError(t, err)
	defer plain.Body.Close()
	require.NotEqual(t, http.StatusNoContent, plain.StatusCode)
	require.Equal(t, int64(1), calls.Load())
	// Test the actual outgoing Raft transport against a CA-trusted wrong-host leaf.
	wrong := ca.Member(t, []string{"wrong.example"})
	wrongPair, err := tls.LoadX509KeyPair(wrong.CertFile, wrong.KeyFile)
	require.NoError(t, err)
	wrongServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("wrong-host server received a Raft request") }))
	wrongServer.TLS = serverTLS.Clone()
	wrongServer.TLS.Certificates = []tls.Certificate{wrongPair}
	wrongServer.StartTLS()
	defer wrongServer.Close()
	_, err = client.Get(wrongServer.URL)
	require.Error(t, err)
}

func TestRaftPeerTLSRejectsDiscoveryAndMembershipDowngrades(t *testing.T) {
	cfg := base.RaftConfig{Peers: []string{"https://one.example:1234", "https://two.example:1234"}, PeerTLS: &base.RaftTLSConfig{}}
	for _, cc := range []raftpb.ConfChange{{NodeID: 0}, {NodeID: 3}, {NodeID: 2, Context: []byte("http://two.example:1234")}, {NodeID: 2, Context: []byte("https://other.example:1234")}} {
		require.Error(t, validateRaftTLSConfChange(cfg, cc))
	}
	require.NoError(t, validateRaftTLSConfChange(cfg, raftpb.ConfChange{NodeID: 2, Context: []byte(cfg.Peers[1])}))
	called := false
	h := secureRaftPeerHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "http://peer/raft", nil))
	require.Equal(t, http.StatusForbidden, w.Code)
	require.False(t, called)
}

func TestRaftPeerTLSReplicatesFailsOverAndRebuildsFromSnapshot(t *testing.T) {
	ca := testutil.NewPeerCA(t)
	peers := make([]string, 3)
	configs := make([]config.Config, 3)
	nodes := make([]*durableNode, 3)
	for i := range peers {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		peers[i] = "https://" + listener.Addr().String()
		require.NoError(t, listener.Close())
	}
	for i := range nodes {
		cfg := crashTestConfig(t.TempDir())
		cfg.ID = uint64(i + 1)
		cfg.Peers = peers
		cfg.PeerTLS = ca.Member(t, []string{"127.0.0.1"})
		cfg.ProposeC = make(chan []byte)
		cfg.ConfChangeC = make(chan raftpb.ConfChange)
		cfg.SnapshotCount, cfg.CatchUpEntriesN = 2, 1
		cfg.Logger = zap.NewNop()
		configs[i] = cfg
		nodes[i] = startRaftApplication(t, cfg)
	}
	for _, n := range nodes {
		_, err := n.raft.WaitForLeader(5 * time.Second)
		require.NoError(t, err)
	}
	genericProposal(t, nodes[0], "tls-tenant", db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "tenant"}}})
	genericProposal(t, nodes[1], "tls-definition", db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "tenant", ID: "hash", Runtime: db.StepRuntime, Handler: "sha256", HandlerVersion: "1", Input: []byte("private application payload")}}})
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			_, err := n.store.GetJobRecord(db.JobRecordQuery{Kind: db.JobDefinitionRecord, TenantID: "tenant", ID: "hash"})
			if err != nil {
				return false
			}
		}
		return true
	}, 5*time.Second, 5*time.Millisecond)
	leader := -1
	for i, n := range nodes {
		if n.raft.IsLeader() {
			leader = i
		}
	}
	require.NotEqual(t, -1, leader)
	nodes[leader].close()
	next := -1
	require.Eventually(t, func() bool {
		for i, n := range nodes {
			if i != leader && n.raft.IsLeader() {
				next = i
				return true
			}
		}
		return false
	}, 5*time.Second, 5*time.Millisecond)
	genericProposal(t, nodes[next], "tls-launch", db.JobCommand{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: "hash", DefinitionRevision: 1, RunID: "run"}})
	target := nodes[next].store.LastAppliedIndex()
	barrier := db.StateCommand{RequestID: "tls-barrier", At: time.Now().UTC(), Job: &db.JobCommand{PutTenant: &db.PutJobTenant{ExpectedRevision: 1, Tenant: db.JobTenant{ID: "tenant"}}}}
	for range 3 {
		proposeChannel(t, nodes[next], barrier)
	}
	require.Eventually(t, func() bool {
		s, err := nodes[next].raft.Snapshotter.Load()
		return err == nil && s.Metadata.Index >= target
	}, 5*time.Second, 5*time.Millisecond)
	// Restart every member. One must recover its store solely from snapshot/WAL.
	for i, n := range nodes {
		if i != leader {
			n.close()
		}
	}
	require.NoError(t, os.RemoveAll(filepath.Join(configs[next].DataDir, "store")))
	for i := range nodes {
		nodes[i] = startRaftApplication(t, configs[i])
	}
	for _, n := range nodes {
		_, err := n.raft.WaitForLeader(5 * time.Second)
		require.NoError(t, err)
	}
	for _, n := range nodes {
		require.Eventually(t, func() bool { _, err := n.store.GetJobRun("tenant", "run"); return err == nil }, 5*time.Second, 5*time.Millisecond)
		run, err := n.store.GetJobRun("tenant", "run")
		require.NoError(t, err)
		require.Equal(t, "private application payload", string(run.Definition.Input))
	}
	// A valid TLS client can reach peer probing without public API credentials.
	cfg := configs[0]
	_, info, err := loadRaftPeerTLS(cfg.RaftConfig)
	require.NoError(t, err)
	rt, err := rafthttp.NewRoundTripper(info, time.Second)
	require.NoError(t, err)

	c := &http.Client{Transport: rt, Timeout: time.Second}
	defer c.CloseIdleConnections()
	r, err := c.Get(peers[1] + "/raft/probing")
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, r.Body)
	require.NoError(t, err)
	r.Body.Close()
	require.Equal(t, http.StatusOK, r.StatusCode)
}

func TestRawRaftNodeRejectsHTTPSWithoutCredentialsBeforeStorage(t *testing.T) {
	cfg := crashTestConfig(filepath.Join(t.TempDir(), "unopened"))
	cfg.Peers = []string{"https://127.0.0.1:0"}
	cfg.ProposeC = make(chan []byte)
	cfg.ConfChangeC = make(chan raftpb.ConfChange)
	n := NewRaftNode(cfg)
	t.Cleanup(n.Stop)
	select {
	case err := <-n.ErrorC:
		require.ErrorContains(t, err, "HTTPS requires peer TLS credentials")
	case <-time.After(3 * time.Second):
		t.Fatal("raw Raft node did not reject missing peer TLS")
	}
	_, err := os.Stat(cfg.DataDir)
	require.True(t, os.IsNotExist(err))
}
