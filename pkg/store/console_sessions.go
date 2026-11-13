package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/duravela/djob/pkg/consolesession"
	"github.com/duravela/djob/pkg/db"
)

// ProposeConsoleSession waits for durable application of the exact session
// command. Waiters are transient; session retries never use job receipts.
func (s *Store) ProposeConsoleSession(ctx context.Context, c db.ConsoleSessionCommand) (db.ConsoleSessionResult, error) {
	if s.recoveryHeld && c.Action != "cleanup" && c.Action != "revoke" && c.Action != "recover" && c.Action != "oidc_logout" {
		return db.ConsoleSessionResult{}, fmt.Errorf("%w: recovery_hold", db.ErrAdmissionHeld)
	}
	hash, err := c.Fingerprint()
	if err != nil {
		return db.ConsoleSessionResult{}, err
	}
	var data bytes.Buffer
	version := uint32(db.ConsoleSessionCommandVersion)
	if c.Action == "renew" || c.Action == "oidc_logout" || c.Renewal != nil || c.Logout != nil || c.Creation != nil && (!c.Creation.Binding.OIDCIssuedAt.IsZero() || c.Creation.Binding.OIDCSID != "") {
		version = db.OIDCContinuityCommandVersion
	} else if c.Creation != nil && c.Creation.Binding.OIDCSubject != "" {
		version = db.OIDCSessionCommandVersion
	} else if c.Action == "configure" {
		version = db.ConsoleSessionPolicyCommandVersion
	}
	if err := gob.NewEncoder(&data).Encode(db.Command{Version: version, Type: db.CmdConsoleSession, ConsoleSession: &c}); err != nil {
		return db.ConsoleSessionResult{}, err
	}
	release, err := s.reserveProposal(data.Len())
	if err != nil {
		return db.ConsoleSessionResult{}, err
	}
	defer release()
	waiter := make(chan db.ConsoleSessionResult, 1)
	s.mu.Lock()
	if s.pendingSessions == nil {
		s.pendingSessions = make(map[[32]byte]map[chan db.ConsoleSessionResult]struct{})
	}
	if s.pendingSessions[hash] == nil {
		s.pendingSessions[hash] = make(map[chan db.ConsoleSessionResult]struct{})
	}
	s.pendingSessions[hash][waiter] = struct{}{}
	s.mu.Unlock()
	s.metrics.PendingRequests.Inc()
	defer func() {
		s.mu.Lock()
		delete(s.pendingSessions[hash], waiter)
		if len(s.pendingSessions[hash]) == 0 {
			delete(s.pendingSessions, hash)
		}
		s.mu.Unlock()
		s.metrics.PendingRequests.Dec()
	}()
	timeout := s.cfg.ProposalTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case <-s.stopC:
		return db.ConsoleSessionResult{}, s.stopError()
	default:
	}
	select {
	case s.proposeC <- data.Bytes():
		s.metrics.Proposals.Inc()
	case <-ctx.Done():
		return db.ConsoleSessionResult{}, ctx.Err()
	case <-s.stopC:
		return db.ConsoleSessionResult{}, s.stopError()
	}
	select {
	case result := <-waiter:
		if result.Rejection != nil {
			return result, result.Rejection
		}
		return result, nil
	case <-ctx.Done():
		return db.ConsoleSessionResult{}, ctx.Err()
	case <-s.stopC:
		return db.ConsoleSessionResult{}, s.stopError()
	}
}

func (s *Store) notifyConsoleSessions(results []db.ConsoleSessionResult) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, result := range results {
		for waiter := range s.pendingSessions[result.Fingerprint] {
			select {
			case waiter <- result.Clone():
			default:
			}
		}
	}
}

func (s *Store) GetConsoleSession(id [32]byte) (*consolesession.Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetConsoleSession(id)
}

func (s *Store) ConsoleSessionPolicy() (*db.ConsoleSessionPolicy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ConsoleSessionPolicy()
}

func (s *Store) ConsoleSessionCleanupNeeded(at time.Time) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ConsoleSessionCleanupNeeded(at)
}

func (s *Store) ConsoleSessionStats() (db.ConsoleSessionStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ConsoleSessionStats()
}

func (s *Store) OIDCRevoked(ctx context.Context, issuer, subject, sid string, issued time.Time) (bool, error) {
	return s.db.OIDCRevoked(ctx, issuer, subject, sid, issued)
}
