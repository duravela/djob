package server

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/duravela/djob/sdk/go/worker"
	"github.com/stretchr/testify/require"
)

func TestDeploymentSessionsSeparateGenerationsStatesAndTenantIdentities(t *testing.T) {
	at := time.Now()
	hub := &remoteWorkers{cfg: RemoteWorkerConfig{IdleTimeout: time.Minute}, sessions: make(map[string]*workerSession)}
	s := &topLevelServer{nodeID: 9007199254740993, workerInstance: "first-process", remoteWorkers: hub}
	v := db.WorkerDeployment{TenantID: "alpha", ID: "line", WorkerID: "company-worker", Generation: 3}
	add := func(edit func(*workerSession)) {
		session := &workerSession{hub: hub, tenant: "alpha", started: true, lastSeen: at, info: jp.WorkerInfo{TenantID: "alpha", WorkerID: v.WorkerID, DeploymentID: v.ID, DeploymentGeneration: 3}}
		if edit != nil {
			edit(session)
		}
		hub.sessions[fmt.Sprint(len(hub.sessions))] = session
	}
	add(nil)
	add(func(s *workerSession) { s.started = false })
	add(func(s *workerSession) { s.draining = true })
	add(func(s *workerSession) { s.relocating = true })
	add(func(s *workerSession) { s.lastSeen = at.Add(-time.Minute) })
	add(func(s *workerSession) { s.info.DeploymentGeneration = 2; s.draining = true })
	add(func(s *workerSession) { s.info.DeploymentGeneration = 4 }) // A lagging replica can read an older desired generation.
	add(func(s *workerSession) { s.tenant = "beta" })
	add(func(s *workerSession) { s.info.DeploymentID = "another-deployment" })
	add(func(s *workerSession) { s.info.WorkerID = "another-worker" })
	add(func(s *workerSession) { s.info.DeploymentID = "" })
	add(func(s *workerSession) { s.closed = true })
	observed := s.workerDeploymentSessions(v, at)
	require.Equal(t, "9007199254740993", observed.ControllerID)
	require.Equal(t, "first-process", observed.InstanceID)
	require.Equal(t, at.UTC(), observed.ObservedAt)
	require.EqualValues(t, 3, observed.Generation)
	require.True(t, observed.Enabled)
	require.EqualValues(t, 1, observed.Connected)
	require.EqualValues(t, 1, observed.Registered)
	require.EqualValues(t, 1, observed.Draining)
	require.EqualValues(t, 1, observed.Relocating)
	require.EqualValues(t, 1, observed.Stale)
	require.Zero(t, observed.Unavailable)
	require.EqualValues(t, 2, observed.OtherGenerations)
	hub.quorumUnavailable = true
	observed = s.workerDeploymentSessions(v, at)
	require.Zero(t, observed.Connected)
	require.Zero(t, observed.Registered)
	require.EqualValues(t, 2, observed.Unavailable)
	require.EqualValues(t, 1, observed.Draining)
	s.remoteWorkers = nil
	observed = s.workerDeploymentSessions(v, at)
	require.False(t, observed.Enabled)
	require.Zero(t, observed.OtherGenerations)
}

func TestDeploymentStatusReportsRealGRPCConnectionsWithoutARuntime(t *testing.T) {
	s, client := workerAPI(t)
	remotePrepare(t, s)
	token := "djobw1." + strings.Repeat("f", 64)
	enrollmentAPI(t, s, client, "company-bus", "enroll", 0, token)
	managedDeploymentAPI(t, s, client, "put", 0, 1)
	var reply struct {
		Configured bool                        `json:"configured"`
		Sessions   jp.WorkerDeploymentSessions `json:"worker_sessions"`
	}
	query := func(connected uint32) {
		t.Helper()
		require.Eventually(t, func() bool {
			workerPost(t, client, s, genericToken("alpha/reader"), "/api/jobs/v1/worker-deployments/status", jp.WorkerDeploymentQuery{TenantID: "alpha", ID: "line-one"}, &reply, 200)
			return reply.Sessions.Connected == connected
		}, 4*time.Second, 10*time.Millisecond)
		require.False(t, reply.Configured)
		require.True(t, reply.Sessions.Enabled)
		require.NotEmpty(t, reply.Sessions.InstanceID)
		require.False(t, reply.Sessions.ObservedAt.IsZero())
		require.EqualValues(t, 1, reply.Sessions.Generation)
	}
	query(0)
	cancel, done := runRemoteWorker(t, s, client, "alpha", remoteRegistry(&remoteDevice{}), func(cfg *worker.Config) {
		cfg.WorkerID, cfg.Token, cfg.DeploymentID, cfg.DeploymentGeneration = "company-bus", token, "line-one", 1
	})
	query(1)
	workerPost(t, client, s, genericToken("beta/reader"), "/api/jobs/v1/worker-deployments/status", jp.WorkerDeploymentQuery{TenantID: "alpha", ID: "line-one"}, nil, 403)
	cancel()
	require.NoError(t, <-done)
	query(0)
}
