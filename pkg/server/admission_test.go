package server

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func healthyAdmissionFixture(t *testing.T) (*admissionPolicy, *atomic.Int64) {
	t.Helper()
	directory := t.TempDir()
	cfg := apiTestConfig(directory)
	cfg.Admission = AdmissionConfig{MinAvailableBytes: 1024, MaxClockSkew: 5 * time.Second}
	cfg.StorageSampleInterval = time.Second
	s := &topLevelServer{cfg: cfg, storage: &storageMonitor{interval: time.Second}}
	var offset atomic.Int64
	start := time.Now()
	s.clock = newClockMonitor(ClockMonitorConfig{SampleInterval: time.Second, JumpThreshold: time.Second}, func() clockReading {
		now := time.Now()
		return clockReading{wall: now.Round(0).Add(time.Duration(offset.Load())), elapsed: now.Sub(start)}
	})
	for i := storageStore; i <= storageSnapshots; i++ {
		s.storage.state.components[i] = storageComponentObservation{haveSample: true, lastOK: true, sampledAt: time.Now(), value: storageValue{space: filesystemSpace{available: 4096}, fileSize: 1024}}
	}
	p, err := newAdmissionPolicy(s)
	require.NoError(t, err)
	s.admission = p
	p.peerOK = true
	p.peerAt = time.Now()
	return p, &offset
}

func TestAdmissionStorageBudgetsPreserveCleanupUntilCriticalReserve(t *testing.T) {
	p, _ := healthyAdmissionFixture(t)
	require.Empty(t, p.reason())
	growth := db.StateCommand{At: time.Now().UTC(), Job: &db.JobCommand{Launch: &db.LaunchJob{}}}
	cleanup := db.StateCommand{At: time.Now().UTC(), CancelRun: &db.CancelRun{RunID: "existing"}}
	p.s.storage.state.components[storageStore].value.space.available = 512
	require.Equal(t, "storage_pressure", p.reason())
	require.ErrorIs(t, p.admit(growth), db.ErrAdmissionHeld)
	require.NoError(t, p.admit(cleanup))
	p.s.storage.state.components[storageStore].value.space.available = 0
	require.Equal(t, "storage_exhausted", p.reason())
	require.ErrorIs(t, p.admit(cleanup), db.ErrAdmissionHeld)
	p.s.storage.state.components[storageStore].value.space.available = 4096
	p.s.cfg.Admission.MaxDatabaseBytes = 512
	require.Equal(t, "storage_pressure", p.reason())
	require.NoError(t, p.admit(cleanup))
	p.s.storage.state.components[storageStore].value.fileSize = 512 + (64 << 20)
	require.Equal(t, "storage_exhausted", p.reason())
	p.s.cfg.Admission.MaxDatabaseBytes = 0
	p.s.storage.state.components[storageStore].value.fileSize = 1024
	p.s.storage.state.components[storageWAL].lastOK = false
	require.Equal(t, "storage_pressure", p.reason())
	p.recoveryHeld = true
	p.s.storage.state.components[storageSnapshots].value.space.available = 0
	require.Equal(t, "storage_exhausted", p.reason(), "recovery holds cannot bypass the critical disk reserve")
}

func TestServerClockHoldAffectsReadinessAndRequiresStoppedReset(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.Admission = AdmissionConfig{MinAvailableBytes: 1}
	cfg.StorageSampleInterval = 100 * time.Millisecond
	s := startAPI(t, cfg)
	require.Eventually(t, func() bool { return s.admission.reason() == "" }, 3*time.Second, time.Millisecond)
	client := apiClient(t, s)
	future := meta("future-legacy-command")
	future.At = timestamppb.New(time.Now().Add(time.Minute))
	_, err := client.ConfigureChannel(context.Background(), &pb.ConfigureChannelRequest{Command: future, Id: "future", GatewayId: "future", Protocol: "fake", Endpoint: "future"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	s.clock.mu.Lock()
	read := s.clock.read
	s.clock.read = func() clockReading { value := read(); value.wall = value.wall.Add(2 * time.Second); return value }
	s.clock.mu.Unlock()
	require.Equal(t, "clock_untrusted", s.admission.reason())
	recorder := httptest.NewRecorder()
	s.readiness(recorder, httptest.NewRequest("GET", "/readyz", nil))
	require.Equal(t, 503, recorder.Code)
	var reply readinessResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &reply))
	require.Equal(t, "clock_untrusted", reply.Reason)
	require.False(t, reply.Ready)
	require.NotZero(t, reply.ConfirmedIndex, "a policy hold still reports the successful quorum observation")
	require.ErrorContains(t, node.ResetClockHold(cfg.Raft.DataDir, "verified UTC", time.Now().UTC(), time.Second), "lock node data directory")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, s.Close(ctx))
	require.NoError(t, node.ResetClockHold(cfg.Raft.DataDir, "verified UTC after stopping the member", time.Now().UTC(), time.Second))
	s = startAPI(t, cfg)
	require.Eventually(t, func() bool { return s.admission.reason() == "" }, 3*time.Second, time.Millisecond)
}

func TestClockJumpLatchesAcrossRepairAndRequiresStoppedReset(t *testing.T) {
	p, offset := healthyAdmissionFixture(t)
	offset.Store(int64(2 * time.Second))
	require.Equal(t, "clock_untrusted", p.reason())
	hold := filepath.Join(p.s.cfg.Raft.DataDir, ".djob-clock-hold")
	_, err := os.Stat(hold)
	require.NoError(t, err)
	offset.Store(0)
	require.Equal(t, "clock_untrusted", p.reason(), "restoring the wall clock does not silently clear the latch")
	cleanup := db.StateCommand{At: time.Now().UTC(), CancelRun: &db.CancelRun{RunID: "existing"}}
	require.ErrorIs(t, p.admit(cleanup), db.ErrAdmissionHeld)
	reloaded, err := newAdmissionPolicy(p.s)
	require.NoError(t, err)
	require.True(t, reloaded.clockHeld)
	require.Error(t, node.ResetClockHold(p.s.cfg.Raft.DataDir, "fixed", time.Now().Add(-time.Hour), time.Second))
	require.NoError(t, node.ResetClockHold(p.s.cfg.Raft.DataDir, "verified trusted UTC after repairing synchronization", time.Now().UTC(), time.Second))
	require.Equal(t, "clock_untrusted", p.reason(), "a running instance cannot reset its in-memory latch")
	reloaded, err = newAdmissionPolicy(p.s)
	require.NoError(t, err)
	require.False(t, reloaded.clockHeld)
}

func TestAdmissionRequiresFreshClockQuorumAndRejectsFutureCommands(t *testing.T) {
	p, _ := healthyAdmissionFixture(t)
	p.peerOK = false
	require.Equal(t, "clock_untrusted", p.reason())
	p.peerOK = true
	p.peerAt = time.Now().Add(-4 * time.Second)
	require.Equal(t, "clock_untrusted", p.reason())
	p.peerAt = time.Now()
	require.Empty(t, p.reason())
	c := db.StateCommand{At: time.Now().Add(time.Minute), CancelRun: &db.CancelRun{RunID: "existing"}}
	require.ErrorIs(t, p.admit(c), db.ErrAdmissionHeld)
	require.Equal(t, codes.InvalidArgument, status.Code(validatePublicCommandTime(c.At, 5*time.Second)))
	require.NoError(t, validatePublicCommandTime(time.Now().Add(-time.Hour), 5*time.Second), "old timestamps remain usable for exact retries")
}

func TestAdmissionLoadsHoldsRecordedBetweenConstructionAndStartup(t *testing.T) {
	p, _ := healthyAdmissionFixture(t)
	require.Empty(t, p.reason())
	path := filepath.Join(p.s.cfg.Raft.DataDir, ".djob-clock-hold")
	require.NoError(t, os.WriteFile(path, []byte("previous process detected a jump"), 0600))
	require.NoError(t, p.loadHolds())
	require.Equal(t, "clock_untrusted", p.reason())
	require.NoError(t, os.Remove(path))
	require.NoError(t, p.loadHolds())
	require.Equal(t, "clock_untrusted", p.reason(), "reloading cannot clear an in-memory hold")
}

func TestServerStorageHoldIsTransientAndProducesNoNewWorkReceipt(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.Admission = AdmissionConfig{MinAvailableBytes: 1, MaxDatabaseBytes: 1}
	cfg.StorageSampleInterval = 100 * time.Millisecond
	s := startAPI(t, cfg)
	require.Eventually(t, func() bool { return s.admission.reason() == "storage_pressure" }, 3*time.Second, time.Millisecond)
	client := genericClient(t, s)
	request := &jp.MutationRequest{Command: genericMeta("held-tenant"), TenantId: "alpha", Operation: &jp.MutationRequest_PutTenant{PutTenant: &jp.PutTenant{}}}
	_, err := client.Mutate(genericContext("ops/alice"), request)
	require.Equal(t, codes.Unavailable, status.Code(err))
	before := s.application.Store.LastAppliedIndex()
	_, err = client.Mutate(genericContext("ops/alice"), request)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, before, s.application.Store.LastAppliedIndex())
	_, err = s.application.Store.ProposeStateCommand(context.Background(), db.StateCommand{RequestID: "cleanup-under-pressure", At: time.Now().UTC(), CancelRun: &db.CancelRun{RunID: "absent"}})
	var rejection *db.CommandRejection
	require.ErrorAs(t, err, &rejection)
	require.Equal(t, "not_found", rejection.Code)
	request.Command.At = timestamppb.New(time.Now().Add(time.Minute))
	_, err = client.Mutate(genericContext("ops/alice"), request)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
