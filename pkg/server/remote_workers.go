package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/jobs"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/duravela/djob/sdk/go"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

type RemoteWorkerConfig struct {
	Enabled             bool
	MaxSessions         int
	MaxPerTenant        int
	IdleTimeout         time.Duration
	PollWait            time.Duration
	Heartbeat           time.Duration
	ReplyGrace          time.Duration // Wait for stopped I/O after cancellation, before losing the session.
	QuorumProbeInterval time.Duration
	QuorumProbeTimeout  time.Duration
	QuorumGrace         time.Duration // Maximum age of successful quorum confirmation before relocation.
}

func (c *RemoteWorkerConfig) defaults() error {
	if c.MaxSessions == 0 {
		c.MaxSessions = 16
	}
	if c.MaxPerTenant == 0 {
		c.MaxPerTenant = min(4, c.MaxSessions)
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 8 * time.Second
	}
	if c.PollWait == 0 {
		c.PollWait = time.Second
	}
	if c.Heartbeat == 0 {
		c.Heartbeat = time.Second
	}
	if c.ReplyGrace == 0 {
		c.ReplyGrace = 8 * time.Second
	}
	if c.QuorumProbeInterval == 0 {
		c.QuorumProbeInterval = time.Second
	}
	if c.QuorumProbeTimeout == 0 {
		c.QuorumProbeTimeout = 2 * time.Second
	}
	if c.QuorumGrace == 0 {
		c.QuorumGrace = 5 * time.Second
	}
	if c.MaxSessions < 1 || c.MaxSessions > 128 || c.MaxPerTenant < 1 || c.MaxPerTenant > c.MaxSessions || c.IdleTimeout < 3*time.Second || c.IdleTimeout > time.Minute || c.PollWait < 10*time.Millisecond || c.PollWait > 2*time.Second || c.Heartbeat < 100*time.Millisecond || c.Heartbeat > 2*time.Second || c.IdleTimeout < 3*c.Heartbeat || c.ReplyGrace < time.Second || c.ReplyGrace > time.Minute {
		return fmt.Errorf("invalid remote worker capacities or timing")
	}
	if c.QuorumProbeInterval < 100*time.Millisecond || c.QuorumProbeInterval > 10*time.Second || c.QuorumProbeTimeout < 100*time.Millisecond || c.QuorumProbeTimeout > MaxReadinessTimeout || c.QuorumGrace < c.QuorumProbeInterval+c.QuorumProbeTimeout || c.QuorumGrace > time.Minute {
		return fmt.Errorf("invalid remote worker quorum timing")
	}
	return nil
}

type workerBackend interface {
	jobs.DispatchBackend
	jobs.GatewayBackend
}
type workerTenantBackend struct {
	workerBackend
	tenant       string
	deploymentID string
	allows       func() bool
}

func (b workerTenantBackend) ListDispatchJobs(after []byte, limit int, excluded []string) (db.JobRecordPage, error) {
	if b.allows != nil && !b.allows() {
		return db.JobRecordPage{}, nil
	}
	page, err := b.workerBackend.ListDispatchJobs(after, limit, excluded)
	if err != nil {
		return page, err
	}
	published := false
	if b.deploymentID != "" {
		backend, ok := b.workerBackend.(interface {
			GetWorkerDeployment(string, string) (*db.WorkerDeployment, error)
		})
		if !ok {
			return db.JobRecordPage{}, fmt.Errorf("worker release storage unavailable")
		}
		deployment, err := backend.GetWorkerDeployment(b.tenant, b.deploymentID)
		if err != nil {
			return db.JobRecordPage{}, err
		}
		published = deployment.Published
	}
	filtered := page.Entries[:0]
	for _, entry := range page.Entries {
		run := entry.Record.Run
		if run != nil && run.TenantID == b.tenant && (run.Definition.WorkerRelease == b.deploymentID || !published && run.Definition.WorkerRelease == "") {
			filtered = append(filtered, entry)
		}
	}
	page.Entries = filtered
	return page, nil
}

type remoteWorkers struct {
	rpc                      *grpc.Server
	server                   *topLevelServer
	cfg                      RemoteWorkerConfig
	backend                  workerBackend
	mu                       sync.Mutex
	sessions                 map[string]*workerSession
	slots                    int // Includes disconnected sessions whose dispatchers are still stopping.
	tenantSlots              map[string]int
	closed                   bool
	runners                  sync.WaitGroup
	probe                    func(context.Context) error
	lastProbe, lastConfirmed time.Time
	quorumUnavailable        bool
	quorumMetrics            *workerQuorumMetrics
	enrollmentProbes         chan struct{}
	streamStarts             chan struct{}
}

type workerCall struct {
	command            wire.Command
	ctx                context.Context
	response           chan wire.Result
	delivered, replied bool
}

type workerSession struct {
	hub                                *remoteWorkers
	id, tenant                         string
	credential                         [32]byte
	life                               context.Context
	cancel                             context.CancelFunc
	dispatcher                         *jobs.Dispatcher
	mu                                 sync.Mutex
	lastSeen                           time.Time
	pending                            *workerCall
	sequence                           uint64
	started, polling, closed, draining bool
	relocating                         bool
	notify                             chan struct{}
	done                               chan struct{}
	info                               jp.WorkerInfo // Immutable, bounded advertised capability metadata.
	enrolled                           bool
	deploymentID                       string
	deploymentGeneration               uint64
}

func workerNonce() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("worker identity unavailable")
	}
	return hex.EncodeToString(value[:]), nil
}

func newRemoteWorkers(s *topLevelServer) *remoteWorkers {
	h := &remoteWorkers{server: s, cfg: s.cfg.RemoteWorkers, backend: s.application.Store, sessions: make(map[string]*workerSession), tenantSlots: make(map[string]int), enrollmentProbes: make(chan struct{}, 4), streamStarts: make(chan struct{}, s.cfg.RemoteWorkers.MaxSessions)}
	h.probe = func(ctx context.Context) error { _, err := s.application.Raft.ReadBarrier(ctx); return err }
	h.rpc = h.newGRPCTransport()
	h.quorumMetrics = newWorkerQuorumMetrics(h, s.metrics.registry)
	s.metrics.registry.MustRegister(
		newWorkerSessionMetricsCollector(h),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "djob_worker_sessions", Help: "Live outbound worker sessions on this member."}, func() float64 { h.mu.Lock(); defer h.mu.Unlock(); return float64(len(h.sessions)) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "djob_worker_session_slots", Help: "Reserved worker slots, including disconnected dispatchers still stopping."}, func() float64 { h.mu.Lock(); defer h.mu.Unlock(); return float64(h.slots) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "djob_worker_sessions_closing", Help: "Disconnected worker sessions whose controller dispatchers have not returned."}, func() float64 { h.mu.Lock(); defer h.mu.Unlock(); return float64(h.slots - len(h.sessions)) }),
	)
	return h
}

func (h *remoteWorkers) run(ctx context.Context) {
	ticker := time.NewTicker(min(h.cfg.IdleTimeout/4, h.cfg.QuorumProbeInterval, time.Second))
	defer ticker.Stop()
	for {
		h.mu.Lock()
		probeDue := h.lastProbe.IsZero() || time.Since(h.lastProbe) >= h.cfg.QuorumProbeInterval
		h.mu.Unlock()
		if probeDue {
			h.checkQuorum(ctx)
		}
		for _, s := range h.snapshot() {
			s.mu.Lock()
			expired := time.Since(s.lastSeen) >= h.cfg.IdleTimeout
			relocating := s.relocating
			s.mu.Unlock()
			if expired || (relocating && s.finished()) {
				s.stop()
			}
		}
		select {
		case <-ctx.Done():
			h.stop()
			return
		case <-ticker.C:
		}
	}
}

func (h *remoteWorkers) snapshot() []*workerSession {
	h.mu.Lock()
	defer h.mu.Unlock()
	var list []*workerSession
	for _, s := range h.sessions {
		list = append(list, s)
	}
	return list
}

func (h *remoteWorkers) stop() {
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
	for _, s := range h.snapshot() {
		s.stop()
	}
	if h.rpc != nil {
		h.rpc.Stop()
	}
}

func (s *workerSession) stop() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.cancel()
	started := s.started
	// A returning dispatcher may call stop concurrently before releasing its
	// slot. Finish removing the routes before another stop can observe closed.
	s.hub.mu.Lock()
	delete(s.hub.sessions, s.id)
	s.hub.mu.Unlock()
	s.mu.Unlock()
	if !started {
		_ = s.dispatcher.Close(context.Background())
		s.release()
	}
}

// release runs once, after a started dispatcher returns or an unstarted one
// closes. Removing its RPC routes does not by itself free execution capacity.
func (s *workerSession) release() {
	s.hub.mu.Lock()
	s.hub.slots--
	s.hub.tenantSlots[s.tenant]--
	if s.hub.tenantSlots[s.tenant] == 0 {
		delete(s.hub.tenantSlots, s.tenant)
	}
	s.hub.mu.Unlock()
	close(s.done)
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *workerSession) start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.started || s.relocating {
		return
	}
	s.hub.mu.Lock()
	if s.hub.closed {
		s.hub.mu.Unlock()
		return
	}
	s.hub.runners.Add(1)
	s.started = true
	s.hub.mu.Unlock()
	go func() {
		defer s.hub.runners.Done()
		defer func() {
			s.stop()
			s.release()
		}()
		_ = s.dispatcher.Run(s.life)
	}()
}

func (h *remoteWorkers) close() {
	h.stop()
	// Keep Raft open until each controller-side execution has recorded its loss
	// of the worker or finished cleanup. Remote device I/O is never assumed idle.
	h.runners.Wait()
}

func workerJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if value != nil {
		_ = json.NewEncoder(w).Encode(value)
	}
}

func workerError(w http.ResponseWriter, code int, text string) {
	workerJSON(w, code, map[string]string{"error": text})
}

func workerDecode(w http.ResponseWriter, r *http.Request, limit int64, value any) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	d.DisallowUnknownFields()
	if d.Decode(value) != nil || d.Decode(new(any)) != io.EOF {
		workerError(w, http.StatusBadRequest, "invalid worker message")
		return false
	}
	return true
}

func (h *remoteWorkers) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		workerError(w, 405, "method not allowed")
		return
	}
	if r.TLS == nil {
		workerError(w, 403, "worker connections require TLS")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, wire.Path)
	if path == "register" {
		p := h.registrationPrincipal(w, r)
		if p == nil {
			return
		}
		h.register(w, r, p)
		return
	}
	parts := strings.Split(path, "/")
	if len(parts) != 2 {
		workerError(w, 404, "unknown worker operation")
		return
	}
	principal, authErr := h.server.jobs.auth.authenticate(captureHTTPAuthorization(r).Context())
	if authErr == nil && (principal.role != APIWorker || principal.tenantID == "") {
		workerError(w, 403, "tenant worker credential required")
		return
	}
	_, token, _ := strings.Cut(r.Header.Get("Authorization"), " ")
	if authErr != nil && !strings.HasPrefix(token, "djobw1.") {
		workerError(w, 401, "worker credentials required")
		return
	}
	h.mu.Lock()
	session := h.sessions[parts[0]]
	h.mu.Unlock()
	hash, valid := workerRequestHash(r)
	if !valid {
		workerError(w, 401, "worker credentials required")
		return
	}
	if session == nil || session.credential != hash {
		workerError(w, 404, "worker session unavailable")
		return
	}
	session.mu.Lock()
	session.lastSeen = time.Now()
	closed := session.closed
	session.mu.Unlock()
	if closed {
		workerError(w, 410, "worker session ended")
		return
	}
	// A rotated/revoked credential can finish only its already authenticated
	// session. It cannot register again or receive another conversation.
	session.enrollmentAllows()
	switch parts[1] {
	case "poll":
		var request struct{}
		if !workerDecode(w, r, 1024, &request) {
			return
		}
		session.poll(w, r)
	case "heartbeat":
		var request wire.Heartbeat
		if !workerDecode(w, r, 1024, &request) {
			return
		}
		session.mu.Lock()
		pending := session.pending
		response := wire.HeartbeatReply{CommandID: request.CommandID, Cancel: request.CommandID != ""}
		if pending != nil && pending.command.ID == request.CommandID {
			response.Budget = pending.budget()
			response.Cancel = response.Budget <= 0
		}
		session.mu.Unlock()
		workerJSON(w, 200, response)
	case "result":
		var result wire.Result
		if !workerDecode(w, r, wire.MaxResultBytes, &result) {
			return
		}
		session.result(w, result)
	case "drain":
		var request struct{}
		if !workerDecode(w, r, 1024, &request) {
			return
		}
		session.drain()
		workerJSON(w, 204, nil)
	case "disconnect":
		session.stop()
		workerJSON(w, 204, nil)
	default:
		workerError(w, 404, "unknown worker operation")
	}
}

type workerFault struct {
	status  int
	message string
}

func writeWorkerFault(w http.ResponseWriter, fault *workerFault) {
	workerError(w, fault.status, fault.message)
}

func (h *remoteWorkers) register(w http.ResponseWriter, r *http.Request, principal *apiCredential) {
	var request wire.Register
	if !workerDecode(w, r, wire.MaxControlBytes, &request) {
		return
	}
	if request.DeploymentID != "" || request.DeploymentGeneration != 0 {
		workerError(w, 426, "managed workers require gRPC")
		return
	}
	_, registered, fault := h.newSession(request, principal)
	if fault != nil {
		writeWorkerFault(w, fault)
		return
	}
	workerJSON(w, 200, registered)
}

func (h *remoteWorkers) newSession(request wire.Register, principal *apiCredential) (*workerSession, wire.Registered, *workerFault) {
	if request.Protocol != wire.Version {
		return nil, wire.Registered{}, &workerFault{426, "unsupported worker protocol version"}
	}
	if request.TenantID != principal.tenantID {
		return nil, wire.Registered{}, &workerFault{403, "worker tenant does not match its credential"}
	}
	if !wire.ValidWorkerID(request.WorkerID) || len(request.Handlers) == 0 || len(request.Handlers) > 64 || len(request.Drivers) > 64 {
		return nil, wire.Registered{}, &workerFault{400, "invalid worker capabilities"}
	}
	if principal.workerID != "" && principal.workerID != request.WorkerID || !h.workerEnrollmentAllows(request.TenantID, request.WorkerID, principal.hash, principal.workerID != "") {
		return nil, wire.Registered{}, &workerFault{403, "worker identity does not match an enabled enrollment"}
	}
	guard := db.WorkerAdmission{WorkerID: request.WorkerID, CredentialHash: principal.hash, Enrolled: principal.workerID != "", DeploymentID: request.DeploymentID, DeploymentGeneration: request.DeploymentGeneration}
	if !h.workerDeploymentAllows(request.TenantID, guard) {
		return nil, wire.Registered{}, &workerFault{403, "worker deployment does not permit this session"}
	}
	if !h.acceptingWorkers() {
		return nil, wire.Registered{}, &workerFault{503, "worker controller quorum unavailable"}
	}
	id, err := workerNonce()
	if err != nil {
		return nil, wire.Registered{}, &workerFault{503, "worker registration unavailable"}
	}
	life, cancel := context.WithCancel(h.server.life)
	s := &workerSession{hub: h, id: id, tenant: principal.tenantID, credential: principal.hash, enrolled: principal.workerID != "", life: life, cancel: cancel, lastSeen: time.Now(), notify: make(chan struct{}, 1), done: make(chan struct{})}
	s.deploymentID, s.deploymentGeneration = request.DeploymentID, request.DeploymentGeneration
	registry := djob.Registry{Handlers: make(map[djob.Version]djob.Handler), Drivers: make(map[djob.Version]djob.DriverFactory)}
	valid := func(v djob.Version) bool { return apiID(v.Name) == nil && apiID(v.Version) == nil }
	for _, v := range request.Handlers {
		if !valid(v) || registry.Handlers[v] != nil {
			cancel()
			return nil, wire.Registered{}, &workerFault{400, "invalid worker handler capabilities"}
		}
		registry.Handlers[v] = func(view djob.View) (djob.Decision, error) {
			timeout := h.server.cfg.JobWorker.OperationTimeout
			if timeout == 0 {
				timeout = 10 * time.Second
			}
			ctx, stop := context.WithTimeout(s.life, timeout)
			defer stop()
			result, err := s.call(ctx, wire.Command{Kind: wire.Evaluate, View: &view})
			if err != nil {
				s.stop()
				return djob.Decision{}, fmt.Errorf("%w: %v", jobs.ErrHandlerUnavailable, err)
			}
			if result.Error != "" {
				return djob.Decision{}, errors.New(result.Error)
			}
			if result.Decision == nil {
				return djob.Decision{}, fmt.Errorf("worker omitted its decision")
			}
			return *result.Decision, nil
		}
	}
	for _, v := range request.Drivers {
		if !valid(v) || registry.Drivers[v] != nil {
			cancel()
			return nil, wire.Registered{}, &workerFault{400, "invalid worker driver capabilities"}
		}
		registry.Drivers[v] = func(run djob.JobRun) (djob.Driver, error) { return &workerDriver{session: s, run: run}, nil }
	}
	s.info = workerCapabilities(registry)
	s.info.ID, s.info.WorkerID, s.info.TenantID, s.info.Subject, s.info.Mode = workerDisplayID(s.id), request.WorkerID, s.tenant, principal.subject, "remote"
	s.info.DeploymentID, s.info.DeploymentGeneration = request.DeploymentID, request.DeploymentGeneration
	dispatch := h.server.cfg.JobDispatch
	dispatch.MaxConcurrent, dispatch.MaxPerTenant = 1, 1
	execution := h.server.cfg.JobWorker
	execution.WorkerAdmission = nil
	if s.enrolled {
		execution.WorkerAdmission = &guard
	}
	execution.OwnerPrefix = fmt.Sprintf("remote-worker/%d/%s/%s", h.server.nodeID, request.WorkerID, s.id)
	s.dispatcher, err = jobs.NewDispatcher(workerTenantBackend{workerBackend: h.backend, tenant: s.tenant, deploymentID: s.deploymentID, allows: s.enrollmentAllows}, dispatch, execution, registry)
	if err != nil {
		cancel()
		return nil, wire.Registered{}, &workerFault{503, "worker execution configuration unavailable"}
	}
	h.mu.Lock()
	if h.quorumUnavailable {
		h.mu.Unlock()
		cancel()
		_ = s.dispatcher.Close(context.Background())
		return nil, wire.Registered{}, &workerFault{503, "worker controller quorum unavailable"}
	}
	if h.closed || h.slots >= h.cfg.MaxSessions || h.tenantSlots[s.tenant] >= h.cfg.MaxPerTenant {
		h.mu.Unlock()
		cancel()
		_ = s.dispatcher.Close(context.Background())
		return nil, wire.Registered{}, &workerFault{429, "worker session capacity reached"}
	}
	h.sessions[id] = s
	h.slots++
	h.tenantSlots[s.tenant]++
	h.mu.Unlock()
	return s, wire.Registered{Protocol: wire.Version, SessionID: id, TenantID: s.tenant, HeartbeatEvery: h.cfg.Heartbeat, DeploymentID: request.DeploymentID, DeploymentGeneration: request.DeploymentGeneration}, nil
}

func (p *workerCall) budget() time.Duration {
	if p.ctx.Err() != nil {
		return 0
	}
	deadline, ok := p.ctx.Deadline()
	if !ok {
		return 0
	}
	if p.command.Kind == wire.Execute {
		authority, valid := jobs.ActionAuthorityDeadline(p.ctx)
		if !valid {
			return 0
		}
		if authority.Before(deadline) {
			deadline = authority
		}
	}
	return max(0, time.Until(deadline))
}

func (s *workerSession) call(ctx context.Context, command wire.Command) (wire.Result, error) {
	id, err := workerNonce()
	if err != nil {
		return wire.Result{}, err
	}
	command.ID = id
	p := &workerCall{command: command, ctx: ctx, response: make(chan wire.Result, 1)}
	s.mu.Lock()
	if s.closed || s.pending != nil {
		s.mu.Unlock()
		return wire.Result{}, fmt.Errorf("worker session unavailable")
	}
	s.sequence++
	p.command.Sequence = s.sequence
	s.pending = p
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.pending == p {
			s.pending = nil
		}
		s.mu.Unlock()
	}()
	select {
	case s.notify <- struct{}{}:
	default:
	}
	// Cancellation requests stopped I/O; it does not establish the outcome. A
	// bounded grace period allows a cooperative driver to return its final report.
	select {
	case result := <-p.response:
		return result, nil
	case <-s.life.Done():
		return wire.Result{}, fmt.Errorf("worker session lost")
	case <-ctx.Done():
	}
	timer := time.NewTimer(s.hub.cfg.ReplyGrace)
	defer timer.Stop()
	select {
	case result := <-p.response:
		return result, nil
	case <-s.life.Done():
		return wire.Result{}, fmt.Errorf("worker session lost")
	case <-timer.C:
		s.stop()
		return wire.Result{}, fmt.Errorf("worker did not confirm stopped I/O")
	}
}

func (s *workerSession) poll(w http.ResponseWriter, r *http.Request) {
	command, fault := s.nextCommand(r.Context())
	if r.Context().Err() != nil {
		return
	}
	if fault != nil {
		writeWorkerFault(w, fault)
		return
	}
	if command == nil {
		workerJSON(w, 204, nil)
		return
	}
	workerJSON(w, 200, command)
}

func (s *workerSession) nextCommand(ctx context.Context) (*wire.Command, *workerFault) {
	s.mu.Lock()
	if s.polling {
		s.mu.Unlock()
		return nil, &workerFault{409, "a worker poll is already active"}
	}
	s.polling = true
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.polling = false; s.mu.Unlock() }()
	s.start()
	timer := time.NewTimer(s.hub.cfg.PollWait)
	defer timer.Stop()
	for {
		s.mu.Lock()
		p := s.pending
		draining := s.draining
		relocating := s.relocating
		if p != nil && !p.delivered {
			budget := p.budget()
			deadline, valid := p.ctx.Deadline()
			if budget <= 0 || !valid {
				s.mu.Unlock()
				s.stop()
				return nil, &workerFault{410, "worker authorization expired"}
			}
			command := p.command
			command.Timeout = time.Until(deadline)
			command.Budget = min(budget, command.Timeout)
			data, err := json.Marshal(command)
			if err != nil || len(data) > wire.MaxCommandBytes {
				s.mu.Unlock()
				s.stop()
				return nil, &workerFault{503, "worker command exceeds its limit"}
			}
			p.delivered = true
			s.mu.Unlock()
			return &command, nil
		}
		s.mu.Unlock()
		if relocating && s.finished() {
			s.stop()
			return nil, &workerFault{503, "worker controller quorum unavailable"}
		}
		if draining && !relocating && s.dispatcher.Active() == 0 {
			s.stop()
			return &wire.Command{Kind: wire.Drained}, nil
		}
		select {
		case <-s.life.Done():
			return nil, &workerFault{410, "worker session ended"}
		case <-ctx.Done():
			return nil, &workerFault{408, "worker request ended"}
		case <-timer.C:
			return nil, nil
		case <-s.notify:
		}
	}
}

func (s *workerSession) result(w http.ResponseWriter, result wire.Result) {
	if fault := s.acceptResult(result); fault != nil {
		writeWorkerFault(w, fault)
		return
	}
	workerJSON(w, 204, nil)
}

func (s *workerSession) acceptResult(result wire.Result) *workerFault {
	s.mu.Lock()
	p := s.pending
	if p == nil || !p.delivered || p.replied || result.ID != p.command.ID {
		s.mu.Unlock()
		return &workerFault{409, "worker result is stale or already accepted"}
	}
	valid := len(result.Error) <= 4096 && utf8.ValidString(result.Error)
	switch p.command.Kind {
	case wire.Evaluate:
		valid = valid && result.Effect == nil && ((result.Decision != nil && result.Error == "") || (result.Decision == nil && result.Error != ""))
		if result.Decision != nil {
			d := result.Decision
			valid = valid && len(d.State)+len(d.Input)+len(d.Result) <= wire.MaxPayloadBytes && len(d.Error) <= 4096
		}
	case wire.Execute:
		valid = valid && result.Decision == nil && result.Error == "" && result.Effect != nil
		if result.Effect != nil {
			e := result.Effect
			valid = valid && len(e.Data) <= wire.MaxPayloadBytes && len(e.Error) <= 4096 && (e.Status == djob.EffectSucceeded || e.Status == djob.EffectFailed || e.Status == djob.EffectUnknown)
		}
	default:
		valid = valid && result.Decision == nil && result.Effect == nil
	}
	if !valid {
		s.mu.Unlock()
		s.stop()
		return &workerFault{400, "invalid worker outcome"}
	}
	if p.command.Kind == wire.Close && result.Error != "" {
		// Retire this worker before its execution returns and discovery can
		// claim another run. Keep the current call alive for ordinary outcome
		// recording; failure to close cannot authorize resource release.
		s.dispatcher.Pause()
		s.draining = true
	}
	p.replied = true
	p.response <- result
	s.mu.Unlock()
	// This acknowledges receipt, not durable completion. The controller must
	// commit the result before authorizing another operation.
	return nil
}

type workerDriver struct {
	session *workerSession
	run     djob.JobRun
}

func (d *workerDriver) Execute(ctx context.Context, action djob.Action) djob.Effect {
	unknown := func() djob.Effect {
		return djob.Effect{Status: djob.EffectUnknown, Error: "worker did not confirm the action outcome", SessionLost: true}
	}
	view, err := d.session.hub.backend.GetJobView(d.run.TenantID, d.run.ID)
	if err != nil || view.Run == nil {
		return unknown()
	}
	run := view.Run
	if run.OwnerID != d.run.OwnerID || run.AttemptID != d.run.AttemptID || run.PendingAction == nil || run.PendingAction.Sequence != action.Sequence || run.PendingAction.Status != djob.EffectAuthorized {
		return unknown()
	}
	var gateways []wire.Gateway
	for _, id := range run.Resources {
		r, err := d.session.hub.backend.GetJobResource(id)
		if err != nil || r.State != db.ResourceOwned || r.TenantID != run.TenantID || r.RunID != run.ID || r.AttemptID != run.AttemptID || r.OwnerID != run.OwnerID {
			return unknown()
		}
		if r.Spec.ChannelID == "" {
			continue
		}
		c, err := d.session.hub.backend.GetChannel(r.Spec.ChannelID)
		if err != nil || c.State != db.ChannelOwned || c.OwnerID != run.OwnerID || c.Epoch != r.ChannelEpoch {
			return unknown()
		}
		gateways = append(gateways, wire.Gateway{ResourceID: id, ResourceRevision: r.Revision, ResourceEpoch: r.Epoch, Duplex: r.Spec.Duplex, Capacity: r.Spec.Capacity, ChannelID: c.ID, GatewayID: c.GatewayID, Protocol: c.Protocol, Endpoint: c.Endpoint, Revision: c.Revision, Epoch: c.Epoch, OwnerID: c.OwnerID, LeaseUntil: c.LeaseUntil, LastTransition: c.LastTransition})
	}
	result, err := d.session.call(ctx, wire.Command{Kind: wire.Execute, Run: run, Action: &action, Gateways: gateways})
	if err != nil || result.Effect == nil {
		return unknown()
	}
	return *result.Effect
}

func (d *workerDriver) cleanup(ctx context.Context, kind wire.Kind) error {
	result, err := d.session.call(ctx, wire.Command{Kind: kind, Run: &d.run})
	if err != nil {
		return err
	}
	if result.Error != "" {
		return errors.New(result.Error)
	}
	return nil
}
func (d *workerDriver) Quiesce(ctx context.Context) error { return d.cleanup(ctx, wire.Quiesce) }
func (d *workerDriver) Close(ctx context.Context) error   { return d.cleanup(ctx, wire.Close) }
