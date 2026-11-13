package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"

	"github.com/duravela/djob/pkg/db"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
)

func workerRequestHash(r *http.Request) ([32]byte, bool) {
	return workerAuthorizationHash(r.Header.Values("Authorization"))
}

func workerAuthorizationHash(values []string) ([32]byte, bool) {
	if len(values) != 1 {
		return [32]byte{}, false
	}
	scheme, token, ok := strings.Cut(values[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || !validAPIToken(token) {
		return [32]byte{}, false
	}
	return sha256.Sum256([]byte(token)), true
}

func enrollmentMissing(err error) bool {
	var rejection *db.CommandRejection
	return errors.As(err, &rejection) && rejection.Code == "not_found"
}

func (h *remoteWorkers) registrationPrincipal(w http.ResponseWriter, r *http.Request) *apiCredential {
	principal, fault := h.authenticateWorker(captureHTTPAuthorization(r).Context(), r.Header.Values("Authorization"))
	if fault != nil {
		writeWorkerFault(w, fault)
	}
	return principal
}

func (h *remoteWorkers) authenticateWorker(ctx context.Context, values []string) (*apiCredential, *workerFault) {
	principal, err := h.server.jobs.auth.authenticate(ctx)
	if err == nil {
		if principal.role != APIWorker || principal.tenantID == "" {
			return nil, &workerFault{403, "tenant worker credential required"}
		}
		return principal, nil
	}
	hash, valid := workerAuthorizationHash(values)
	token := ""
	if len(values) == 1 {
		_, token, _ = strings.Cut(values[0], " ")
	}
	secret, err := hex.DecodeString(strings.TrimPrefix(token, "djobw1."))
	if !valid || !strings.HasPrefix(token, "djobw1.") || len(secret) != 32 || err != nil {
		return nil, &workerFault{401, "worker credentials required"}
	}
	backend, ok := h.backend.(workerEnrollmentBackend)
	if !ok {
		return nil, &workerFault{503, "worker enrollment unavailable"}
	}
	// Registration verifies the latest applied identity, including rotations and
	// revocations. Bound unauthenticated read barriers independently of sessions.
	select {
	case h.enrollmentProbes <- struct{}{}:
		defer func() { <-h.enrollmentProbes }()
	default:
		return nil, &workerFault{429, "worker enrollment checks busy"}
	}
	ctx, cancel := context.WithTimeout(ctx, h.cfg.QuorumProbeTimeout)
	defer cancel()
	if err := h.probe(ctx); err != nil {
		return nil, &workerFault{503, "worker enrollment quorum unavailable"}
	}
	v, err := backend.LookupWorkerCredential(hash)
	if enrollmentMissing(err) {
		return nil, &workerFault{401, "worker credentials required"}
	}
	if err != nil {
		return nil, &workerFault{503, "worker enrollment unavailable"}
	}
	if v.State != "enabled" {
		return nil, &workerFault{403, "worker enrollment does not permit new sessions"}
	}
	return &apiCredential{subject: v.Subject, tenantID: v.TenantID, workerID: v.WorkerID, role: APIWorker, hash: hash}, nil
}

func (h *remoteWorkers) workerEnrollmentAllows(tenant, id string, hash [32]byte, enrolled bool) bool {
	backend, ok := h.backend.(workerEnrollmentBackend)
	if !ok {
		return !enrolled
	}
	v, err := backend.GetWorkerEnrollment(tenant, id)
	if enrollmentMissing(err) {
		return !enrolled
	}
	return err == nil && v.State == "enabled" && subtle.ConstantTimeCompare(v.CredentialHash[:], hash[:]) == 1
}

func (s *workerSession) enrollmentAllows() bool {
	if s.hub.workerEnrollmentAllows(s.tenant, s.info.WorkerID, s.credential, s.enrolled) && s.hub.workerDeploymentAllows(s.tenant, db.WorkerAdmission{WorkerID: s.info.WorkerID, CredentialHash: s.credential, Enrolled: s.enrolled, DeploymentID: s.deploymentID, DeploymentGeneration: s.deploymentGeneration}) {
		return true
	}
	s.drain()
	return false
}

func (h *remoteWorkers) workerDeploymentAllows(tenant string, guard db.WorkerAdmission) bool {
	if (guard.DeploymentID == "") != (guard.DeploymentGeneration == 0) || guard.DeploymentID != "" && (!guard.Enrolled || !wire.ValidWorkerID(guard.DeploymentID)) {
		return false
	}
	backend, ok := h.backend.(interface {
		CheckWorkerDeploymentAdmission(string, db.WorkerAdmission) error
	})
	if !ok {
		return guard.DeploymentID == ""
	}
	return backend.CheckWorkerDeploymentAdmission(tenant, guard) == nil
}

func (s *workerSession) drain() {
	s.dispatcher.Pause()
	s.mu.Lock()
	s.draining = true
	s.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
}
