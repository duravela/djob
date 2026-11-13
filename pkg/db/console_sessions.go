package db

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/duravela/djob/pkg/consolesession"
	"go.etcd.io/bbolt"
)

var consoleSessionsBucket = []byte("console_sessions")
var consoleSessionActivatedKey = []byte("console_session_activated")
var consoleSessionPolicyKey = []byte("console_session_policy")

// ConsoleSessionPolicy is activated explicitly after all controllers support
// the new format. Limits include revoked records until their absolute expiry.
type ConsoleSessionPolicy struct {
	Revision                       uint64
	ClusterID                      uint64
	MaxRecords, MaxCiphertextBytes int
	Limits                         consolesession.Limits
	Cutoff                         time.Time
}

// ConsoleSessionCommand carries no plaintext credentials. Its application time
// advances a durable cutoff so stale retries cannot recreate cleaned-up records.
type ConsoleSessionCommand struct {
	Renewal                *consolesession.Renewal `json:",omitempty"`
	Logout                 *OIDCLogout             `json:",omitempty"`
	Action                 string                  // activate, configure, recover, create, replace, revoke, cleanup
	At                     time.Time
	Policy                 *ConsoleSessionPolicy
	Creation               *consolesession.Creation
	ID, Mutation           [32]byte
	PreviousClusterID      uint64
	ExpectedPolicyRevision uint64
	ExpectedVersion        uint64
	Ciphertext             []byte
}

type ConsoleSessionResult struct {
	Fingerprint [32]byte
	Record      *consolesession.Record
	Deleted     int
	Rejection   *CommandRejection
}

func initializeConsoleSessions(tx *bbolt.Tx) error {
	meta := tx.Bucket(metadataBucket)
	if (meta.Get(formatKey)[0] >= 19) != (tx.Bucket(oidcLogoutBucket) != nil) {
		return fmt.Errorf("OIDC logout bucket and database format disagree")
	}
	policy := meta.Get(consoleSessionPolicyKey)
	b := tx.Bucket(consoleSessionsBucket)
	activated := meta.Get(consoleSessionActivatedKey)
	if activated != nil && !bytes.Equal(activated, []byte{1}) {
		return fmt.Errorf("unsupported console session activation marker")
	}
	if meta.Get(formatKey)[0] < 15 {
		if policy != nil || b != nil || activated != nil {
			return fmt.Errorf("console sessions without an activated format")
		}
		return nil
	}
	if policy == nil && b == nil && meta.Get(formatKey)[0] >= 17 && meta.Get(consoleSessionActivatedKey) == nil {
		return nil
	}
	if policy == nil || b == nil {
		return fmt.Errorf("missing console session policy or bucket")
	}
	if err := meta.Put(consoleSessionActivatedKey, []byte{1}); err != nil {
		return err
	}
	p, err := readRecord[ConsoleSessionPolicy](tx, metadataBucket, string(consoleSessionPolicyKey))
	if err != nil {
		return err
	}
	if !validConsoleSessionPolicy(*p) || p.Cutoff.IsZero() {
		return fmt.Errorf("invalid console session policy")
	}
	return nil
}

func validConsoleSessionPolicy(p ConsoleSessionPolicy) bool {
	return p.ClusterID != 0 && p.MaxRecords > 0 && p.MaxRecords <= 1024 &&
		p.MaxCiphertextBytes > 0 && p.MaxCiphertextBytes <= 64<<20 && p.Limits.MaxCiphertextBytes > 0 &&
		p.Limits.MaxCiphertextBytes <= 8<<20 && p.Limits.MaxCiphertextBytes <= p.MaxCiphertextBytes &&
		p.Limits.MaxLifetime > 0 && p.Limits.MaxLifetime <= 24*time.Hour
}

func (c ConsoleSessionCommand) Fingerprint() ([32]byte, error) {
	data, err := json.Marshal(c)
	return sha256.Sum256(data), err
}

func applyConsoleSession(tx *bbolt.Tx, c ConsoleSessionCommand) (ConsoleSessionResult, error) {
	hash, err := c.Fingerprint()
	if err != nil {
		return ConsoleSessionResult{}, err
	}
	r, err := transitionConsoleSession(tx, c)
	r.Fingerprint = hash
	return r, err
}

func transitionConsoleSession(tx *bbolt.Tx, c ConsoleSessionCommand) (ConsoleSessionResult, error) {
	rejected := func(code, message string) (ConsoleSessionResult, error) {
		return ConsoleSessionResult{Rejection: &CommandRejection{Code: code, Message: message}}, nil
	}
	if c.At.IsZero() {
		return rejected("invalid", "session command requires an application cutoff")
	}
	if c.Action == "oidc_logout" {
		return applyOIDCLogout(tx, c)
	}
	if c.Logout != nil || (c.Action == "renew") != (c.Renewal != nil) {
		return rejected("invalid", "unexpected OIDC continuity payload")
	}
	zero := [32]byte{}
	switch c.Action {
	case "activate", "recover", "configure":
		if c.Policy == nil || !validConsoleSessionPolicy(*c.Policy) || !c.Policy.Cutoff.IsZero() || c.Creation != nil || c.ID != zero || c.Mutation != zero || c.ExpectedVersion != 0 || len(c.Ciphertext) != 0 {
			return rejected("invalid", "invalid session activation")
		}
	case "create":
		if c.Policy != nil || c.Creation == nil || c.ID != zero || c.Mutation != zero || c.ExpectedVersion != 0 || len(c.Ciphertext) != 0 {
			return rejected("invalid", "invalid session creation")
		}
	case "replace", "renew":
		if c.Policy != nil || c.Creation != nil || c.ID == zero {
			return rejected("invalid", "invalid session replacement")
		}
	case "revoke", "cleanup":
		if c.Policy != nil || c.Creation != nil || c.Mutation != zero || c.ExpectedVersion != 0 || len(c.Ciphertext) != 0 || (c.Action == "revoke") != (c.ID != zero) {
			return rejected("invalid", "invalid session revocation or cleanup")
		}
	default:
		return rejected("invalid", "unknown session action")
	}
	meta := tx.Bucket(metadataBucket)
	if c.Action == "configure" && c.ExpectedPolicyRevision == 0 || c.Action != "configure" && c.ExpectedPolicyRevision != 0 || c.Policy != nil && c.Policy.Revision != 0 {
		return rejected("invalid", "invalid policy revision")
	}
	if c.Action == "recover" && c.PreviousClusterID == 0 || c.Action != "recover" && c.PreviousClusterID != 0 {
		return rejected("invalid", "invalid recovery cluster precondition")
	}
	if c.Action == "activate" && meta.Get(consoleSessionPolicyKey) == nil {
		if _, err := tx.CreateBucket(consoleSessionsBucket); err != nil {
			return ConsoleSessionResult{}, err
		}
		p := *c.Policy
		p.Revision = 1
		p.Cutoff = c.At.UTC()
		if err := writeRecord(tx, metadataBucket, string(consoleSessionPolicyKey), &p); err != nil {
			return ConsoleSessionResult{}, err
		}
		if err := meta.Put(consoleSessionActivatedKey, []byte{1}); err != nil {
			return ConsoleSessionResult{}, err
		}
		if meta.Get(formatKey)[0] < 15 {
			return ConsoleSessionResult{}, meta.Put(formatKey, []byte{15})
		}
		return ConsoleSessionResult{}, nil
	}
	if meta.Get(consoleSessionPolicyKey) == nil {
		return rejected("not_active", "console session storage is not activated")
	}
	p, err := readRecord[ConsoleSessionPolicy](tx, metadataBucket, string(consoleSessionPolicyKey))
	if err != nil {
		return ConsoleSessionResult{}, err
	}
	if c.Action == "recover" {
		requested := *c.Policy
		requested.Revision = p.Revision
		requested.Cutoff = p.Cutoff
		if requested == *p && c.PreviousClusterID != p.ClusterID {
			return ConsoleSessionResult{}, nil
		}
		if p.ClusterID != c.PreviousClusterID || requested.ClusterID == p.ClusterID {
			return rejected("conflict", "recovery cluster identity changed")
		}
		requested.Cutoff = p.Cutoff
		original := *p
		original.ClusterID = requested.ClusterID
		if requested != original {
			return rejected("invalid", "recovery must preserve session limits")
		}
		*p = requested
		if c.At.After(p.Cutoff) {
			p.Cutoff = c.At.UTC()
		}
		return ConsoleSessionResult{}, writeRecord(tx, metadataBucket, string(consoleSessionPolicyKey), p)
	}
	if c.Action == "activate" {
		requested := *c.Policy
		requested.Revision = p.Revision
		requested.Cutoff = p.Cutoff
		if requested != *p {
			return rejected("conflict", "activation differs from the current policy; use configure to update limits")
		}
		return ConsoleSessionResult{}, nil
	}
	if c.Action == "configure" {
		requested := *c.Policy
		if requested.ClusterID != p.ClusterID {
			return rejected("invalid", "policy updates cannot change cluster identity")
		}
		revision := p.Revision
		if revision == 0 {
			revision = 1
		}
		requested.Cutoff = p.Cutoff
		requested.Revision = revision
		if revision == c.ExpectedPolicyRevision+1 && requested == *p {
			return ConsoleSessionResult{}, nil
		}
		if revision != c.ExpectedPolicyRevision || revision == ^uint64(0) {
			return rejected("conflict", "session policy revision changed")
		}
		stats, err := consoleSessionStats(tx)
		if err != nil {
			return ConsoleSessionResult{}, err
		}
		if stats.Records > requested.MaxRecords || stats.CiphertextBytes > requested.MaxCiphertextBytes || stats.LargestCiphertext > requested.Limits.MaxCiphertextBytes {
			return rejected("capacity", "new limits are below retained session usage")
		}
		requested.Revision = revision + 1
		if c.At.After(requested.Cutoff) {
			requested.Cutoff = c.At.UTC()
		}
		if err := writeRecord(tx, metadataBucket, string(consoleSessionPolicyKey), &requested); err != nil {
			return ConsoleSessionResult{}, err
		}
		if meta.Get(formatKey)[0] < 16 {
			return ConsoleSessionResult{}, meta.Put(formatKey, []byte{16})
		}
		return ConsoleSessionResult{}, nil
	}
	if c.At.After(p.Cutoff) {
		p.Cutoff = c.At.UTC()
	}
	if err := writeRecord(tx, metadataBucket, string(consoleSessionPolicyKey), p); err != nil {
		return ConsoleSessionResult{}, err
	}
	b := tx.Bucket(consoleSessionsBucket)
	if c.Action == "cleanup" {
		deleted := 0
		cursor := b.Cursor()
		for key, _ := cursor.First(); key != nil && deleted < 64; key, _ = cursor.Next() {
			r, err := readRecord[consolesession.Record](tx, consoleSessionsBucket, string(key))
			if err != nil {
				return ConsoleSessionResult{}, err
			}
			if r.Binding.ClusterID != p.ClusterID || !p.Cutoff.Before(r.ExpiresAt) {
				if err := cursor.Delete(); err != nil {
					return ConsoleSessionResult{}, err
				}
				deleted++
			}
		}
		return ConsoleSessionResult{Deleted: deleted}, nil
	}
	id := c.ID
	if c.Creation != nil {
		id = c.Creation.Binding.ID
	}
	var old *consolesession.Record
	if b.Get(id[:]) != nil {
		old, err = readRecord[consolesession.Record](tx, consoleSessionsBucket, string(id[:]))
		if err != nil {
			return ConsoleSessionResult{}, err
		}
	}
	if old != nil && old.Binding.ClusterID != p.ClusterID || c.Creation != nil && c.Creation.Binding.ClusterID != p.ClusterID {
		return rejected("invalid", "session cluster identity mismatch")
	}
	var next *consolesession.Record
	switch c.Action {
	case "create":
		limits := p.Limits
		if old != nil {
			limits.MaxLifetime = 24 * time.Hour
			limits.MaxCiphertextBytes = 8 << 20
		}
		next, err = consolesession.Create(old, *c.Creation, p.Cutoff, limits)
	case "replace":
		next, err = consolesession.Replace(old, c.ExpectedVersion, c.Mutation, c.Ciphertext, p.Cutoff, p.Limits)
	case "renew":
		next, err = consolesession.Renew(old, c.ExpectedVersion, c.Mutation, c.Ciphertext, *c.Renewal, p.Cutoff, p.Limits)
	case "revoke":
		next, err = consolesession.Revoke(old, p.Cutoff)
	}
	if err != nil {
		code := "invalid"
		switch {
		case errors.Is(err, consolesession.ErrConflict):
			code = "conflict"
		case errors.Is(err, consolesession.ErrExpired):
			code = "expired"
		case errors.Is(err, consolesession.ErrRevoked):
			code = "revoked"
		}
		return rejected(code, err.Error())
	}
	if old == nil && b.Stats().KeyN >= p.MaxRecords {
		return rejected("capacity", "console session record capacity reached")
	}
	// Bound aggregate ciphertext separately from metadata, whose size is
	// bounded by the record count and validated identity lengths.
	total := len(next.Ciphertext)
	if err := b.ForEach(func(key, _ []byte) error {
		if bytes.Equal(key, id[:]) {
			return nil
		}
		r, err := readRecord[consolesession.Record](tx, consoleSessionsBucket, string(key))
		if err == nil {
			total += len(r.Ciphertext)
		}
		return err
	}); err != nil {
		return ConsoleSessionResult{}, err
	}
	if total > p.MaxCiphertextBytes {
		return rejected("capacity", "console session byte capacity reached")
	}
	if err := writeRecord(tx, consoleSessionsBucket, string(id[:]), next); err != nil {
		return ConsoleSessionResult{}, err
	}
	return ConsoleSessionResult{Record: next}, nil
}

// GetConsoleSession reads local state only. The controller must first establish
// a fresh quorum barrier, then check credential binding, expiry and cluster ID.
func (q *DB) GetConsoleSession(id [32]byte) (*consolesession.Record, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var record *consolesession.Record
	err := q.db.View(func(tx *bbolt.Tx) error {
		if tx.Bucket(consoleSessionsBucket) == nil {
			return reject("not_active", "console session storage is not activated")
		}
		var err error
		record, err = readRecord[consolesession.Record](tx, consoleSessionsBucket, string(id[:]))
		return err
	})
	return record, err
}

// ConsoleSessionPolicy returns the locally applied activation policy.
func (q *DB) ConsoleSessionPolicy() (*ConsoleSessionPolicy, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var p *ConsoleSessionPolicy
	err := q.db.View(func(tx *bbolt.Tx) error {
		if tx.Bucket(metadataBucket).Get(consoleSessionPolicyKey) == nil {
			return reject("not_active", "console session storage is not activated")
		}
		var err error
		p, err = readRecord[ConsoleSessionPolicy](tx, metadataBucket, string(consoleSessionPolicyKey))
		if p != nil && p.Revision == 0 {
			p.Revision = 1
		}
		return err
	})
	return p, err
}

// ConsoleSessionCleanupNeeded avoids idle cleanup proposals. This local hint
// never grants authority; the leader still needs a barrier before proposing.
func (q *DB) ConsoleSessionCleanupNeeded(at time.Time) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	needed := false
	err := q.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(consoleSessionsBucket)
		if b == nil || b.Stats().KeyN == 0 {
			return nil
		}
		p, err := readRecord[ConsoleSessionPolicy](tx, metadataBucket, string(consoleSessionPolicyKey))
		if err != nil {
			return err
		}
		cursor := b.Cursor()
		for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
			r, err := readRecord[consolesession.Record](tx, consoleSessionsBucket, string(key))
			if err != nil {
				return err
			}
			if r.Binding.ClusterID != p.ClusterID || !at.Before(r.ExpiresAt) {
				needed = true
				break
			}
		}
		return nil
	})
	return needed, err
}

func (r ConsoleSessionResult) Clone() ConsoleSessionResult {
	if r.Record != nil {
		v := *r.Record
		v.Ciphertext = bytes.Clone(v.Ciphertext)
		r.Record = &v
	}
	if r.Rejection != nil {
		v := *r.Rejection
		r.Rejection = &v
	}
	return r
}

type ConsoleSessionStats struct {
	Active                                               bool
	Records, Revoked, CiphertextBytes, LargestCiphertext int
	RecordLimit, ByteLimit                               int
	PolicyRevision                                       uint64
}

func consoleSessionStats(tx *bbolt.Tx) (ConsoleSessionStats, error) {
	var stats ConsoleSessionStats
	if tx.Bucket(consoleSessionsBucket) == nil {
		return stats, nil
	}
	p, err := readRecord[ConsoleSessionPolicy](tx, metadataBucket, string(consoleSessionPolicyKey))
	if err != nil {
		return stats, err
	}
	stats.Active = true
	stats.RecordLimit = p.MaxRecords
	stats.ByteLimit = p.MaxCiphertextBytes
	stats.PolicyRevision = p.Revision
	if stats.PolicyRevision == 0 {
		stats.PolicyRevision = 1
	}
	err = tx.Bucket(consoleSessionsBucket).ForEach(func(key, _ []byte) error {
		r, err := readRecord[consolesession.Record](tx, consoleSessionsBucket, string(key))
		if err != nil {
			return err
		}
		stats.Records++
		if r.Revoked {
			stats.Revoked++
		}
		stats.CiphertextBytes += len(r.Ciphertext)
		stats.LargestCiphertext = max(stats.LargestCiphertext, len(r.Ciphertext))
		return nil
	})
	return stats, err
}
func (q *DB) ConsoleSessionStats() (ConsoleSessionStats, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var stats ConsoleSessionStats
	err := q.db.View(func(tx *bbolt.Tx) error { var err error; stats, err = consoleSessionStats(tx); return err })
	return stats, err
}
