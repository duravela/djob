package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/duravela/djob/pkg/jobs"
	"github.com/duravela/djob/pkg/node"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/duravela/djob/pkg/scheduler"
	"github.com/duravela/djob/pkg/server/serverctl"
	"github.com/duravela/djob/pkg/util/stop"
	"github.com/duravela/djob/pkg/workercontrol"
	workerwire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
	workerpb "github.com/duravela/djob/sdk/go/worker/protocol/v2"
	gateway "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// Start waits for recovery, leader discovery, and both API listeners. Cancelling
// ctx bounds startup only. Close the returned server explicitly during shutdown.
func Start(ctx context.Context, cfg Config) (*Server, error) {
	stopper := stop.NewStopper()
	startup, err := NewServer(cfg, stopper)
	if err != nil {
		stopper.Stop(context.Background())
		return nil, err
	}
	s := startup.(*topLevelServer)
	if err = s.PreStart(ctx); err != nil {
		_ = s.Close(context.Background())
		return nil, err
	}
	return s, nil
}

func (s *topLevelServer) startRuntime(parent context.Context) error {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	if s.startupContext.Err() != nil {
		return fmt.Errorf("server is closing")
	}
	if s.started {
		return fmt.Errorf("server has already started")
	}
	s.started = true
	ctx, cancel := context.WithTimeout(parent, s.cfg.StartupTimeout)
	defer cancel()
	stopStartup := context.AfterFunc(s.startupContext, cancel)
	defer stopStartup()
	cfg := s.cfg.Raft
	cfg.SetDefaults()
	cfg.ID = s.nodeID
	if len(cfg.Peers) == 0 {
		return fmt.Errorf("an ordered Raft peer list is required")
	}
	if s.cfg.Raft.DataDir == "" {
		return fmt.Errorf("an explicit Raft data directory is required")
	}
	// Monitor independently of Raft and storage, including startup recovery.
	// Close cancels and joins this worker along with the other runtime loops.
	s.worker(func() { s.clock.run(s.life) })
	cfg.ProposalTimeout = 3 * time.Second
	application, err := node.OpenApplication(ctx, cfg)
	if err != nil {
		return err
	}
	s.application = application
	s.jobs.membership = application.Raft
	s.jobs.membershipGuard = func() error {
		if reason := s.admission.reason(); reason == "clock_untrusted" || reason == "storage_exhausted" {
			return status.Error(codes.Unavailable, "membership admission held")
		}
		return nil
	}
	s.jobs.oidcLogoutGuard = func(ctx context.Context) error {
		if reason := s.admission.reason(); reason == "clock_untrusted" || reason == "storage_exhausted" {
			return status.Error(codes.Unavailable, "logout admission held")
		}
		if err := s.validateCommandTime(time.Now().UTC()); err != nil {
			return err
		}
		_, err := application.Raft.ReadBarrier(ctx)
		return err
	}
	if s.jobs.auth != nil {
		s.jobs.auth.oidcRevoked = func(ctx context.Context, issuer, subject, sid string, issued time.Time) (bool, error) {
			ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			if _, err := application.Raft.ReadBarrier(ctx); err != nil {
				return false, err
			}
			return application.Store.OIDCRevoked(ctx, issuer, subject, sid, issued)
		}
	}
	if err := s.admission.loadHolds(); err != nil {
		return err
	}
	application.Store.SetAdmissionPolicy(s.admission.admit)
	s.worker(func() { s.admission.run(s.life) })
	s.storage.probes[storageDatabase] = func() (storageValue, error) {
		stats, err := application.Store.StorageStats()
		return storageValue{database: stats}, err
	}
	s.worker(func() { s.storage.run(s.life) })
	s.queue.probe = application.Store.QueueStats
	s.worker(func() { s.queue.run(s.life) })
	s.states.probe = application.Store.StateInventory
	s.worker(func() { s.states.run(s.life) })
	if err = application.Store.RegisterMetrics(s.metrics.registry); err != nil {
		return err
	}
	if _, err = application.Raft.WaitForLeaderContext(ctx); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	s.scheduler.backend = application.Store
	s.jobs.backend = application.Store
	if s.cfg.ConsoleSessions {
		s.jobs.sessions = newConsoleSessions(s)
		s.worker(func() { s.jobs.sessions.cleanup(s.life) })
	}
	s.workerInstance, err = workerNonce()
	if err != nil {
		return err
	}
	s.jobs.inventory = s.workerInventory
	s.jobs.deploymentSessions = s.workerDeploymentSessions
	if s.cfg.RemoteWorkers.Enabled {
		s.remoteWorkers = newRemoteWorkers(s)
		s.http.mux.Handle(workerwire.Path, s.remoteWorkers)
		s.worker(func() { s.remoteWorkers.run(s.life) })
	}
	if s.cfg.WorkerRuntime != nil {
		reconcileContext, stopReconcile := context.WithCancel(s.life)
		s.stopWorkerReconciler = stopReconcile
		s.workerReconciler = &workercontrol.Reconciler{Backend: application.Store, Runtime: s.cfg.WorkerRuntime, OnError: s.metrics.onError("worker_deployments", nil), Eligible: func(ctx context.Context) bool {
			if !application.Raft.IsLeader() {
				return false
			}
			_, err := application.Raft.ReadBarrier(ctx)
			return err == nil && application.Raft.IsLeader()
		}}
		s.jobs.deployments = s.workerReconciler
		s.worker(func() { s.workerReconciler.Run(reconcileContext) })
	}
	if s.cfg.JobRegistry != nil {
		registry, registryErr := s.cfg.JobRegistry(application.Store)
		if registryErr != nil {
			return registryErr
		}
		s.jobDispatcher, err = jobs.NewDispatcher(application.Store, s.cfg.JobDispatch, s.cfg.JobWorker, registry)
		if err != nil {
			return err
		}
		info := workerCapabilities(registry)
		info.ID, info.WorkerID, info.Mode = "embedded", "controller-"+fmt.Sprint(s.nodeID), "embedded"
		s.embeddedInfo = &info
	}
	canSchedule := func() bool { return application.Raft.IsLeader() && s.admission.reason() == "" }
	canMaintain := func() bool {
		reason := s.admission.reason()
		return application.Raft.IsLeader() && reason != "clock_untrusted" && reason != "storage_exhausted"
	}
	s.materializer, err = scheduler.NewMaterializer(s.cfg.Materializer, application.Store, canSchedule)
	if err != nil {
		return err
	}
	if s.cfg.TransportFactory != nil {
		s.dispatcher, err = scheduler.NewDispatcher(s.cfg.Dispatcher, application.Store, s.cfg.TransportFactory)
		if err != nil {
			return err
		}
	}
	s.deadlineSweeper, err = scheduler.NewDeadlineSweeper(s.cfg.Deadlines, application.Store, canMaintain)
	if err != nil {
		return err
	}
	s.retryPromoter, err = scheduler.NewRetryPromoter(s.cfg.Retries, application.Store, canSchedule)
	if err != nil {
		return err
	}
	s.ownershipSweeper, err = scheduler.NewOwnershipSweeper(s.cfg.Ownership, application.Store, canMaintain)
	if err != nil {
		return err
	}
	s.jobTriggers, err = scheduler.NewJobTriggerMaterializer(s.cfg.JobTriggers, application.Store, canSchedule)
	if err != nil {
		return err
	}
	s.jobRetention, err = scheduler.NewJobRetention(s.cfg.JobRetention, application.Store, canMaintain)
	if err != nil {
		return err
	}
	s.jobMaintenance, err = scheduler.NewJobMaintenance(s.cfg.JobMaintenance, application.Store, canMaintain)
	if err != nil {
		return err
	}
	jsonMux := gateway.NewServeMux(gateway.WithErrorHandler(gatewayAPIError), gateway.WithMarshalerOption(gateway.MIMEWildcard, &gateway.JSONPb{
		MarshalOptions:   protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true},
		UnmarshalOptions: protojson.UnmarshalOptions{DiscardUnknown: false},
	}))
	if err = pb.RegisterSchedulerHandlerServer(s.life, jsonMux, s.scheduler); err != nil {
		return err
	}
	s.http.mux.Handle("/scheduler.Scheduler/", SchedulerHTTPHandler(jsonMux))
	if err = jp.RegisterJobsHandlerServer(s.life, jsonMux, s.jobs); err != nil {
		return err
	}
	s.http.mux.Handle("/djob.jobs.v1.Jobs/", JobsHTTPHandler(jsonMux))
	s.http.mux.Handle("/api/jobs/v1/", s.jobs.HTTPHandler())
	s.http.mux.HandleFunc("/healthz", s.health)
	s.http.mux.HandleFunc("/readyz", s.readiness)
	s.http.mux.HandleFunc("/metrics", s.metricsHTTP)
	if err = s.http.setupRoutes(ctx, newAPIV1Server(ctx, &apiV1ServerOpts{scheduler: s.scheduler})); err != nil {
		return err
	}
	grpcListener, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen for gRPC: %w", err)
	}
	httpListener, err := net.Listen("tcp", s.cfg.HTTPAddr)
	if err != nil {
		_ = grpcListener.Close()
		return fmt.Errorf("listen for HTTP: %w", err)
	}
	s.cfg.Addr, s.cfg.HTTPAddr = grpcListener.Addr().String(), httpListener.Addr().String()
	s.apiHTTP = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.remoteWorkers != nil && strings.HasPrefix(r.URL.Path, workerpb.ServicePath) {
			s.remoteWorkers.serveGRPC(w, r)
			return
		}
		limit := int64(maxAPIRequestBytes)
		if strings.HasPrefix(r.URL.Path, workerwire.Path) {
			limit = workerwire.MaxResultBytes
		}
		r.Body = http.MaxBytesReader(w, r.Body, limit)
		s.http.baseHandler(w, r)
	}), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: time.Minute, MaxHeaderBytes: 32 << 10}
	if s.apiTLS != nil {
		s.apiHTTP.TLSConfig = s.apiTLS.Clone()
	}
	s.worker(func() {
		if err := s.grpc.Serve(grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) && s.life.Err() == nil {
			s.fatal(err)
		}
	})
	s.worker(func() {
		var err error
		if s.apiTLS != nil {
			err = s.apiHTTP.ServeTLS(httpListener, "", "")
		} else {
			err = s.apiHTTP.Serve(httpListener)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.fatal(err)
		}
	})
	s.worker(func() { _ = s.materializer.Run(s.life) })
	s.worker(func() { _ = s.deadlineSweeper.Run(s.life) })
	s.worker(func() { _ = s.retryPromoter.Run(s.life) })
	s.worker(func() { _ = s.ownershipSweeper.Run(s.life) })
	s.worker(func() { _ = s.jobMaintenance.Run(s.life) })
	s.worker(func() { _ = s.jobTriggers.Run(s.life) })
	s.worker(func() { _ = s.jobRetention.Run(s.life) })
	if s.dispatcher != nil {
		s.worker(func() { _ = s.dispatcher.Run(s.life) })
	}
	if s.jobDispatcher != nil {
		s.worker(func() { _ = s.jobDispatcher.Run(s.life) })
	}
	s.worker(func() {
		select {
		case err := <-application.Raft.ErrorC:
			if err != nil {
				s.fatal(err)
			}
		case <-s.life.Done():
		}
	})
	if s.cfg.ReadyFn != nil {
		s.cfg.ReadyFn(false)
	}
	return nil
}

func (s *topLevelServer) worker(fn func()) {
	s.workers.Add(1)
	go func() { defer s.workers.Done(); fn() }()
}
func (s *topLevelServer) fatal(err error) {
	s.stopTrigger.signalStop(context.Background(), serverctl.ShutdownRequest{Reason: serverctl.ShutdownReasonFatalError, Err: err})
}

func (s *topLevelServer) health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	select {
	case <-s.application.Raft.Done():
		w.WriteHeader(http.StatusServiceUnavailable)
	default:
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"node_id": s.nodeID, "leader": s.application.Raft.IsLeader(), "dispatch_enabled": s.dispatcher != nil, "job_dispatch_enabled": s.jobDispatcher != nil})
}

func (s *topLevelServer) RPCAddr() string       { return s.cfg.Addr }
func (s *topLevelServer) HTTPAddr() string      { return s.cfg.HTTPAddr }
func (s *topLevelServer) Done() <-chan struct{} { return s.done }

func (s *topLevelServer) Close(ctx context.Context) error {
	s.closeOnce.Do(func() { s.cancelStartup(); go s.stopper.Stop(context.Background()) })
	select {
	case <-s.done:
		return s.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *topLevelServer) shutdown() {
	s.cancelStartup()
	s.startMu.Lock()
	defer s.startMu.Unlock()
	defer close(s.done)
	var failures []error
	if s.stopWorkerReconciler != nil {
		s.stopWorkerReconciler()
	}
	if s.remoteWorkers != nil {
		s.remoteWorkers.stop()
	}
	if s.apiHTTP != nil {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
		if err := s.apiHTTP.Shutdown(ctx); err != nil {
			failures = append(failures, err)
			_ = s.apiHTTP.Close()
		}
		cancel()
	}
	stopped := make(chan struct{})
	go func() { s.grpc.GracefulStop(); close(stopped) }()
	timer := time.NewTimer(s.cfg.ShutdownTimeout)
	select {
	case <-stopped:
	case <-timer.C:
		s.grpc.Stop()
		<-stopped
	}
	timer.Stop()
	s.cancel()
	if s.remoteWorkers != nil {
		s.remoteWorkers.close()
	}
	if s.dispatcher != nil {
		// A caller deadline never detaches a still-running transport. Keep Raft
		// available for its final completion and orderly channel release.
		if err := s.dispatcher.Close(context.Background()); err != nil {
			failures = append(failures, err)
		}
	}
	if s.jobDispatcher != nil {
		if err := s.jobDispatcher.Close(context.Background()); err != nil {
			failures = append(failures, err)
		}
	}
	s.workers.Wait()
	if s.application != nil {
		s.application.Close()
	}
	s.closeErr = errors.Join(failures...)
}
