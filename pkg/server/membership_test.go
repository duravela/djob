package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node"
	"github.com/duravela/djob/pkg/node/util/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestMembershipAPIRequiresPlatformOperatorAndConfirmedLearner(t *testing.T) {
	s, client := workerAPI(t, func(cfg *Config) {
		cfg.Raft.DynamicMembership = true
		cfg.Raft.Bootstrap = true
		cfg.Raft.InitialMembers = []uint64{1}
		cfg.Raft.Peers = []string{"http://127.0.0.1:0", "http://127.0.0.1:12345", "http://127.0.0.1:12346"}
		// Port zero is permitted only for single-entry catalogs, so reserve a
		// real ephemeral listener address for the bootstrap member.
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		cfg.Raft.Peers[0] = "http://" + l.Addr().String()
		require.NoError(t, l.Close())
	})
	m := s.application.Raft.Membership()
	q := node.MembershipChange{RequestID: "api/add", Reason: "test replacement", ExpectedConfiguration: m.Configuration, Action: "add_learner", MemberID: 2}
	for _, actor := range []string{"alpha/reader", "alpha/writer", "alpha/operator", "alpha/callback"} {
		workerPost(t, client, s, genericToken(actor), "/api/jobs/v1/cluster/members", q, nil, 403)
	}
	workerPost(t, client, s, genericToken("ops/alice"), "/api/jobs/v1/cluster/members", q, nil, 202)
	require.Eventually(t, func() bool { return slices.Contains(s.application.Raft.Membership().Learners, uint64(2)) }, 3*time.Second, time.Millisecond)
	policy := &admissionPolicy{s: s, peerOK: true, peerAt: time.Now(), peerConfiguration: m.Configuration}
	require.False(t, policy.observation().peerFresh, "clock agreement from the previous membership must be invalidated")
	policy.peerConfiguration = s.application.Raft.Membership().Configuration
	require.True(t, policy.observation().peerFresh)
	collected, err := s.metrics.registry.Gather()
	require.NoError(t, err)
	families := make(map[string]*dto.MetricFamily, len(collected))
	for _, family := range collected {
		families[family.GetName()] = family
	}
	require.EqualValues(t, 1, metricValue(t, families, "djob_raft_dynamic_membership_enabled"))
	require.EqualValues(t, 1, metricValue(t, families, "djob_raft_membership_learners"))
	require.EqualValues(t, 1, metricValue(t, families, "djob_raft_membership_fresh_slots"))
	q.ExpectedConfiguration = s.application.Raft.Membership().Configuration
	q.Action = "promote"
	workerPost(t, client, s, genericToken("ops/alice"), "/api/jobs/v1/cluster/members", q, nil, 409)
}

func TestReplacementCLIRecoversEmptyVolumeWithFreshIdentity(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("Python CLI integration requires python3")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	ca := testutil.NewPeerCA(t)
	apiTLS, roots := apiTestCertificate(t)
	peers := make([]string, 4)
	for i := range peers {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		peers[i] = "https://" + l.Addr().String()
		require.NoError(t, l.Close())
	}
	configs := make([]Config, 4)
	for i := range configs {
		cfg := apiTestConfig(t.TempDir())
		id := uint64(i + 1)
		cfg.NodeID = &id
		cfg.Raft.ID = id
		cfg.Raft.Peers = peers
		cfg.Raft.DynamicMembership = true
		cfg.Raft.InitialMembers = []uint64{1, 2, 3}
		cfg.Raft.Join = i == 3
		cfg.Raft.Bootstrap = i < 3
		cfg.Raft.PeerTLS = ca.Member(t, []string{"127.0.0.1"})
		cfg.APIAuth = genericCredentials()
		cfg.APITLS = apiTLS
		configs[i] = cfg
	}
	type started struct {
		i   int
		s   *Server
		err error
	}
	results := make(chan started, 4)
	servers := make([]*Server, 4)
	for i := range 3 {
		go func(i int) { s, err := Start(ctx, configs[i]); results <- started{i, s, err} }(i)
	}
	t.Cleanup(func() {
		for _, s := range servers {
			if s != nil {
				closeCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
				_ = s.Close(closeCtx)
				stop()
			}
		}
	})
	for range 3 {
		r := <-results
		servers[r.i] = r.s
		require.NoError(t, r.err)
	}
	old := 0
	if servers[old].application.Raft.IsLeader() {
		old = 1
	}
	survivor := (old + 1) % 3
	command := db.StateCommand{RequestID: "replacement/durable", At: time.Now().UTC(), Job: &db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "retained"}}}}
	_, err = servers[survivor].application.Store.ProposeStateCommand(ctx, command)
	require.NoError(t, err)
	require.NoError(t, servers[old].Close(ctx))
	servers[old] = nil
	require.NoError(t, os.RemoveAll(configs[old].Raft.DataDir))
	var origins []string
	for _, s := range servers {
		if s != nil {
			origins = append(origins, "https://"+s.HTTPAddr())
		}
	}
	dir := t.TempDir()
	credential := filepath.Join(dir, "operator.header")
	path := filepath.Join(dir, "replacement.json")
	require.NoError(t, os.WriteFile(credential, []byte("Authorization: Bearer "+genericToken("ops/alice")), 0600))
	script := filepath.Join("..", "..", "scripts", "replace-member.py")
	common := []string{"--controllers", strings.Join(origins, ","), "--credential-file", credential, "--ca-file", apiTLS.CertFile, "--plan-file", path}
	args := append([]string{script, "plan"}, common...)
	args = append(args, "--old-member", fmt.Sprint(old+1), "--new-member", "4", "--reason", "isolated complete volume loss")
	output, err := exec.CommandContext(ctx, python, args...).CombinedOutput()
	require.NoError(t, err, string(output))
	fields := strings.Fields(string(output))
	approval := fields[len(fields)-1]
	// Model an orchestrator restarting a preprovisioned fresh learner as soon
	// as the quorum authorizes its slot. The CLI owns all membership sequencing.
	go func() {
		for !slices.Contains(servers[survivor].application.Raft.Membership().Learners, uint64(4)) {
			select {
			case <-ctx.Done():
				results <- started{3, nil, ctx.Err()}
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
		s, err := Start(ctx, configs[3])
		results <- started{3, s, err}
	}()
	args = append([]string{script, "apply"}, common...)
	args = append(args, "--approve", approval, "--timeout", "25")
	output, err = exec.CommandContext(ctx, python, args...).CombinedOutput()
	if err != nil {
		t.Logf("replacement apply: %s", output)
		cancel()
	}
	r := <-results
	servers[3] = r.s
	require.NoError(t, r.err)
	require.NoError(t, err, string(output))
	m := servers[3].application.Raft.Membership()
	require.Contains(t, m.Voters, uint64(4))
	require.NotContains(t, m.Voters, uint64(old+1))
	_, err = servers[3].application.Store.GetJobRecord(db.JobRecordQuery{Kind: db.JobTenantRecord, TenantID: "retained"})
	require.NoError(t, err)
	// Verify public observations over API TLS after promotion.
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}}
	defer client.CloseIdleConnections()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+servers[3].HTTPAddr()+"/api/jobs/v1/cluster/members", nil)
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+genericToken("ops/alice"))
	response, err := client.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, 200, response.StatusCode)
	var observed node.MembershipStatus
	require.NoError(t, json.NewDecoder(response.Body).Decode(&observed))
	require.Equal(t, m.Configuration, observed.Configuration)
}
