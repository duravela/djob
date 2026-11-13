// Portions Copyright 2014 The Cockroach Authors.
// Older configuration/server interfaces derive from CockroachDB 23.1.0.
// The applicable Apache-2.0 grant and original conversion terms are recorded in
// licenses/Apache-2.0.txt and licenses/CockroachDB-23.1-BSL.txt.
// Modified for djob; new engine implementation is governed by the root LICENSE.

package server

import (
	"context"
	"crypto/tls"
	"reflect"
	"sync"
	"time"

	"github.com/duravela/djob/pkg/jobs"
	raftnode "github.com/duravela/djob/pkg/node"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/duravela/djob/pkg/rpc"
	scheduling "github.com/duravela/djob/pkg/scheduler"
	"github.com/duravela/djob/pkg/server/serverctl"
	"github.com/duravela/djob/pkg/util/stop"
	"github.com/duravela/djob/pkg/workercontrol"
	"github.com/pkg/errors"
	"net/http"
)

type topLevelServer struct {
	nodeID uint64

	cfg        Config
	rpcContext *rpc.Context

	http      *httpServer
	scheduler *schedulerServer
	jobs      *jobsServer
	metrics   *runtimeMetrics
	clock     *clockMonitor
	storage   *storageMonitor
	queue     *queueMonitor
	states    *stateMonitor
	admission *admissionPolicy

	stopper              *stop.Stopper
	stopTrigger          *stopTrigger
	grpc                 *grpcServer
	application          *raftnode.Application
	materializer         *scheduling.Materializer
	deadlineSweeper      *scheduling.DeadlineSweeper
	retryPromoter        *scheduling.RetryPromoter
	ownershipSweeper     *scheduling.OwnershipSweeper
	jobMaintenance       *scheduling.JobMaintenance
	jobTriggers          *scheduling.JobTriggerMaterializer
	jobRetention         *scheduling.JobRetention
	dispatcher           *scheduling.Dispatcher
	jobDispatcher        *jobs.Dispatcher
	remoteWorkers        *remoteWorkers
	workerReconciler     *workercontrol.Reconciler
	stopWorkerReconciler context.CancelFunc
	workerInstance       string
	embeddedInfo         *jp.WorkerInfo
	apiHTTP              *http.Server
	apiTLS               *tls.Config
	startMu              sync.Mutex
	started              bool
	life                 context.Context
	cancel               context.CancelFunc
	startupContext       context.Context
	cancelStartup        context.CancelFunc
	workers              sync.WaitGroup
	closeOnce            sync.Once
	done                 chan struct{}
	closeErr             error
}

// Server owns the public APIs, materializer, optional dispatcher, and Raft store.
type Server = topLevelServer

func NewServer(cfg Config, stopper *stop.Stopper) (serverctl.ServerStartupInterface, error) {
	if cfg.WorkerRuntime != nil && !cfg.RemoteWorkers.Enabled {
		return nil, errors.New("managed worker runtime requires remote workers")
	}
	if cfg.Admission.MinAvailableBytes == 0 {
		cfg.Admission.MinAvailableBytes = 256 << 20
	}
	if cfg.Admission.MaxClockSkew == 0 {
		cfg.Admission.MaxClockSkew = 5 * time.Second
	}
	if err := ValidateAdmissionConfig(cfg.Admission); err != nil {
		return nil, err
	}
	if cfg.Config == nil {
		cfg.Config = MakeBaseConfig().Config
	}
	if cfg.NodeID == nil {
		id := uint64(1)
		cfg.NodeID = &id
	}
	if *cfg.NodeID == 0 {
		return nil, errors.New("node ID must be positive")
	}
	idCopy := *cfg.NodeID
	cfg.NodeID = &idCopy
	baseCopy := *cfg.Config
	cfg.Config = &baseCopy
	if cfg.Raft.Config != nil {
		v := *cfg.Raft.Config
		cfg.Raft.Config = &v
	}
	if cfg.Raft.SnapshotConfig != nil {
		v := *cfg.Raft.SnapshotConfig
		cfg.Raft.SnapshotConfig = &v
	}
	cfg.Raft.Peers = append([]string(nil), cfg.Raft.Peers...)
	if cfg.Raft.PeerTLS != nil {
		v := *cfg.Raft.PeerTLS
		cfg.Raft.PeerTLS = &v
	}
	if cfg.StartupTimeout == 0 {
		cfg.StartupTimeout = 10 * time.Second
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 10 * time.Second
	}
	if cfg.ReadinessTimeout == 0 {
		cfg.ReadinessTimeout = DefaultReadinessTimeout
	}
	if cfg.ReadinessTimeout < 0 || cfg.ReadinessTimeout > MaxReadinessTimeout {
		return nil, errors.New("readiness timeout must be positive and at most 5s")
	}
	if cfg.StartupTimeout <= 0 || cfg.ShutdownTimeout <= 0 {
		return nil, errors.New("server timeouts must be positive")
	}
	if cfg.ClockMonitor.SampleInterval == 0 {
		cfg.ClockMonitor.SampleInterval = DefaultClockSampleInterval
	}
	if cfg.ClockMonitor.JumpThreshold == 0 {
		cfg.ClockMonitor.JumpThreshold = DefaultClockJumpThreshold
	}
	if err := ValidateClockMonitorConfig(cfg.ClockMonitor); err != nil {
		return nil, err
	}
	if cfg.StorageSampleInterval == 0 {
		cfg.StorageSampleInterval = DefaultStorageSampleInterval
	}
	if err := ValidateStorageSampleInterval(cfg.StorageSampleInterval); err != nil {
		return nil, err
	}
	if cfg.QueueMonitor.SampleInterval == 0 {
		cfg.QueueMonitor.SampleInterval = DefaultQueueSampleInterval
	}
	if cfg.QueueMonitor.ScanLimit == 0 {
		cfg.QueueMonitor.ScanLimit = DefaultQueueSampleLimit
	}
	if err := ValidateQueueMonitorConfig(cfg.QueueMonitor); err != nil {
		return nil, err
	}
	if cfg.StateMonitor.SampleInterval == 0 {
		cfg.StateMonitor.SampleInterval = DefaultStateSampleInterval
	}
	if cfg.StateMonitor.ScanLimit == 0 {
		cfg.StateMonitor.ScanLimit = DefaultStateSampleLimit
	}
	if err := ValidateStateMonitorConfig(cfg.StateMonitor); err != nil {
		return nil, err
	}
	if stopper == nil {
		return nil, errors.New("stopper is required")
	}
	auth, err := newAPIAuthorizer(cfg.APIAuth)
	if err != nil {
		return nil, err
	}
	if cfg.ConsoleSessions && auth == nil {
		return nil, errors.New("shared console sessions require API credentials")
	}
	transport, err := loadAPITLS(cfg.APITLS)
	if err != nil {
		return nil, err
	}
	if err := validateAPIAuthTransport(cfg, auth, transport); err != nil {
		return nil, err
	}
	if cfg.RemoteWorkers.Enabled {
		if auth == nil || transport == nil {
			return nil, errors.New("remote workers require API credentials and TLS")
		}
		if err := cfg.RemoteWorkers.defaults(); err != nil {
			return nil, err
		}
		if cfg.JobWorker.OperationTimeout < 0 || cfg.JobWorker.OperationTimeout > time.Hour {
			return nil, errors.New("remote worker operation timeout must be in 0..1h")
		}
	}
	if cfg.Raft.PeerTLS != nil {
		peerCfg := cfg.Raft.RaftConfig
		peerCfg.SetDefaults()
		peerCfg.ID = *cfg.NodeID
		if err := raftnode.ValidateRaftPeerTLSConfig(peerCfg); err != nil {
			return nil, err
		}
		cfg.Raft.RaftConfig = peerCfg
	}
	// Retain only token hashes. Caller-owned credentials cannot change a live
	// service, and raw secrets are absent from the server's configuration copy.
	cfg.APIAuth = nil
	cfg.APITLS = nil
	ctx := cfg.AmbientCtx.AnnotateCtx(context.Background())

	nodeID := cfg.NodeID

	rpcCtxOpts := rpc.ContextOptions{}
	rpcCtxOpts.NodeID = nodeID
	rpcCtxOpts.Stopper = stopper

	rpcContext := rpc.NewContext(ctx, rpcCtxOpts)
	grpcServer, err := newGRPCServer(ctx, rpcContext, transport)

	if err != nil {
		return nil, err
	}
	lateBoundServer := &topLevelServer{}

	sHTTP := newHTTPServer(cfg.BaseConfig)

	sScheduler := newSchedulerServer(
		cfg.BaseConfig.AmbientCtx,
		rpcContext,
		grpcServer,
		lateBoundServer,
	)
	sScheduler.auth = auth
	sJobs := &jobsServer{auth: auth}
	jp.RegisterJobsServer(grpcServer.Server, sJobs)

	// Connect the various servers to RPC.
	for i, gw := range []grpcGatewayServer{sScheduler} {
		if reflect.ValueOf(gw).IsNil() {
			return nil, errors.Errorf("%d: nil", i)
		}
		gw.RegisterService(grpcServer.Server)
	}

	stopTrigger := newStopTrigger()

	*lateBoundServer = topLevelServer{
		nodeID:      *nodeID,
		cfg:         cfg,
		rpcContext:  rpcContext,
		http:        sHTTP,
		scheduler:   sScheduler,
		jobs:        sJobs,
		stopper:     stopper,
		stopTrigger: stopTrigger,
		grpc:        grpcServer,
		apiTLS:      transport,
		done:        make(chan struct{}),
	}
	lateBoundServer.life, lateBoundServer.cancel = context.WithCancel(context.Background())
	lateBoundServer.startupContext, lateBoundServer.cancelStartup = context.WithCancel(context.Background())
	lateBoundServer.clock = newClockMonitor(cfg.ClockMonitor, nil)
	lateBoundServer.storage = newStorageMonitor(cfg.StorageSampleInterval, cfg.Raft.DataDir, *nodeID)
	lateBoundServer.queue = newQueueMonitor(cfg.QueueMonitor)
	lateBoundServer.states = newStateMonitor(cfg.StateMonitor)
	lateBoundServer.admission, err = newAdmissionPolicy(lateBoundServer)
	if err != nil {
		return nil, err
	}
	sJobs.validateTime = lateBoundServer.validateCommandTime
	lateBoundServer.metrics = newRuntimeMetrics(lateBoundServer)
	lateBoundServer.instrumentBackgroundErrors()
	lateBoundServer.instrumentExecutions()
	lateBoundServer.instrumentScheduling()
	stopper.AddCloser(stop.CloserFn(lateBoundServer.shutdown))

	return lateBoundServer, nil
}

// AnnotateCtx is a convenience wrapper; see AmbientContext.
func (s *topLevelServer) AnnotateCtx(ctx context.Context) context.Context {
	return s.cfg.AmbientCtx.AnnotateCtx(ctx)
}

// NodeID returns the ID of this node within its cluster.
func (s *topLevelServer) NodeID() *uint64 {
	return &s.nodeID
}

func (s *topLevelServer) PreStart(ctx context.Context) error {
	return s.startRuntime(ctx)
}

// ShutdownRequested returns a channel that is signaled when a subsystem wants
// the server to be shut down.
func (s *topLevelServer) ShutdownRequested() <-chan serverctl.ShutdownRequest {
	return s.stopTrigger.C()
}
