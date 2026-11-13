// Package consolesession defines deterministic session-record transitions for
// controller storage. It does not authenticate HTTP requests or decrypt reviews.
package consolesession

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrInvalid  = errors.New("invalid session record or mutation")
	ErrExpired  = errors.New("session expired")
	ErrRevoked  = errors.New("session revoked")
	ErrConflict = errors.New("session version or mutation conflict")
)

// Binding contains only public identity and credential hashes. Controllers must
// check the source credential and current cluster identity on authentication.
type Binding struct {
	ID, CredentialHash, SourceHash [32]byte
	ClusterID                      uint64
	Subject, Role, TenantID        string
	OIDCSID                        string    `json:",omitempty"`
	OIDCIssuedAt                   time.Time `json:",omitzero"`
	OIDCSubject                    string    `json:",omitempty"`
	OIDCIssuer                     string    `json:",omitempty"`
	OIDCExpiresAt                  time.Time `json:",omitzero"`
}

// Record retains a revocation tombstone until ExpiresAt. Storage must not delete
// it early: an otherwise valid creation retry could resurrect the session.
type Record struct {
	Binding                Binding
	CreatedAt, ExpiresAt   time.Time
	Version                uint64
	Revoked                bool
	Ciphertext             []byte
	CreationCiphertextHash [32]byte
	LastMutation           [32]byte
}

// Creation fields must remain identical across retries, including ciphertext.
// Only hashes of credentials belong here; callers retain the browser secret.
type Creation struct {
	Binding              Binding
	CreatedAt, ExpiresAt time.Time
	Ciphertext           []byte
}

// Limits are supplied by the future storage admission policy. Aggregate record
// and byte budgets must also be checked in the transaction that stores a record.
type Limits struct {
	MaxCiphertextBytes int
	MaxLifetime        time.Duration
}

func (l Limits) valid() bool {
	return l.MaxCiphertextBytes > 0 && l.MaxLifetime > 0 && l.MaxLifetime <= 24*time.Hour
}

func (b Binding) valid() bool {
	zero := [32]byte{}
	for _, ch := range b.Subject {
		if ch < '!' || ch > '~' {
			return false
		}
	}
	if !utf8.ValidString(b.TenantID) || b.TenantID != "" && strings.TrimSpace(b.TenantID) == "" {
		return false
	}
	return b.ID != zero && b.CredentialHash != zero && b.SourceHash != zero && b.ClusterID != 0 &&
		len(b.Subject) > 0 && len(b.Subject) <= 128 && len(b.TenantID) <= 256 &&
		(b.Role == "reader" || b.Role == "writer" || b.Role == "operator") &&
		(b.TenantID != "" || b.Role == "operator")
}

// Create applies a creation or its exact retry at a replicated application
// cutoff. A retry never extends expiry or replaces a newer review.
func Create(old *Record, c Creation, at time.Time, limits Limits) (*Record, error) {
	if c.Binding.OIDCSubject == "" {
		if c.Binding.OIDCIssuer != "" || !c.Binding.OIDCExpiresAt.IsZero() || c.Binding.OIDCSID != "" || !c.Binding.OIDCIssuedAt.IsZero() {
			return nil, ErrInvalid
		}
	} else if len(c.Binding.OIDCSID) > 256 || !c.Binding.OIDCIssuedAt.IsZero() && (!c.Binding.OIDCExpiresAt.After(c.Binding.OIDCIssuedAt) || c.Binding.OIDCExpiresAt.Sub(c.Binding.OIDCIssuedAt) > 24*time.Hour) || len(c.Binding.OIDCSubject) > 256 || c.Binding.OIDCIssuer == "" || len(c.Binding.OIDCIssuer) > 2048 || c.Binding.OIDCExpiresAt.IsZero() || c.ExpiresAt.After(c.Binding.OIDCExpiresAt) {
		return nil, ErrInvalid
	}

	if !limits.valid() || !c.Binding.valid() || at.IsZero() || c.CreatedAt.IsZero() || c.CreatedAt.After(at) ||
		!c.ExpiresAt.After(c.CreatedAt) || c.ExpiresAt.Sub(c.CreatedAt) > limits.MaxLifetime ||
		len(c.Ciphertext) == 0 || len(c.Ciphertext) > limits.MaxCiphertextBytes {
		return nil, ErrInvalid
	}
	if !at.Before(c.ExpiresAt) {
		return nil, ErrExpired
	}
	if old != nil {
		if old.Binding != c.Binding || !old.CreatedAt.Equal(c.CreatedAt) || !old.ExpiresAt.Equal(c.ExpiresAt) || old.CreationCiphertextHash != sha256.Sum256(c.Ciphertext) {
			return nil, ErrConflict
		}
		if old.Revoked {
			return nil, ErrRevoked
		}
		return clone(old), nil
	}
	return &Record{Binding: c.Binding, CreatedAt: c.CreatedAt.UTC(), ExpiresAt: c.ExpiresAt.UTC(), Version: 1,
		Ciphertext: bytes.Clone(c.Ciphertext), CreationCiphertextHash: sha256.Sum256(c.Ciphertext)}, nil
}

// Replace uses a random mutation identity and an expected version. The most
// recent exact retry is acknowledged without retaining permanent receipts.
// Older retries conflict after another write; they cannot overwrite that write.
func Replace(old *Record, expected uint64, mutation [32]byte, ciphertext []byte, at time.Time, limits Limits) (*Record, error) {
	if old == nil || !limits.valid() || at.IsZero() || at.Before(old.CreatedAt) || mutation == ([32]byte{}) || expected == 0 || expected == math.MaxUint64 || len(ciphertext) == 0 || len(ciphertext) > limits.MaxCiphertextBytes {
		return nil, ErrInvalid
	}
	if !at.Before(old.ExpiresAt) {
		return nil, ErrExpired
	}
	if old.Revoked {
		return nil, ErrRevoked
	}
	if old.LastMutation == mutation {
		if old.Version != expected+1 || !bytes.Equal(old.Ciphertext, ciphertext) {
			return nil, ErrConflict
		}
		return clone(old), nil
	}
	if old.Version != expected {
		return nil, ErrConflict
	}
	next := clone(old)
	next.Version++
	next.LastMutation = mutation
	next.Ciphertext = bytes.Clone(ciphertext)
	return next, nil
}

// Revoke is idempotent and clears retained review data. Storage retains the
// binding and creation identity until expiry for creation and logout retries.
// Authentication of a revoked record may only acknowledge its own revocation.
func Revoke(old *Record, at time.Time) (*Record, error) {
	if old == nil || at.IsZero() || at.Before(old.CreatedAt) {
		return nil, ErrInvalid
	}
	if !at.Before(old.ExpiresAt) {
		return nil, ErrExpired
	}
	if old.Revoked {
		return clone(old), nil
	}
	if old.Version == math.MaxUint64 {
		return nil, ErrConflict
	}
	next := clone(old)
	next.Version++
	next.Revoked = true
	next.Ciphertext = nil
	return next, nil
}

func clone(r *Record) *Record {
	next := *r
	next.Ciphertext = bytes.Clone(r.Ciphertext)
	return &next
}

// Renewal preserves the browser capability and absolute session lifetime while
// binding it to a freshly verified identity token. Plaintext tokens stay outside Raft.
type Renewal struct {
	Binding   Binding
	ExpiresAt time.Time
}

func Renew(old *Record, expected uint64, mutation [32]byte, ciphertext []byte, renewal Renewal, at time.Time, limits Limits) (*Record, error) {
	if old == nil {
		return nil, ErrInvalid
	}
	b := renewal.Binding
	o := old.Binding
	if b.ID != o.ID || b.CredentialHash != o.CredentialHash || b.ClusterID != o.ClusterID || b.Subject != o.Subject || b.Role != o.Role || b.TenantID != o.TenantID || b.OIDCSubject == "" || b.OIDCSubject != o.OIDCSubject || b.OIDCIssuer != o.OIDCIssuer || b.OIDCSID != o.OIDCSID || !b.OIDCIssuedAt.Equal(o.OIDCIssuedAt) || !b.valid() || !renewal.ExpiresAt.After(at) || renewal.ExpiresAt.After(b.OIDCExpiresAt) || renewal.ExpiresAt.After(old.CreatedAt.Add(limits.MaxLifetime)) {
		return nil, ErrInvalid
	}
	if old.LastMutation == mutation {
		if old.Binding != b || !old.ExpiresAt.Equal(renewal.ExpiresAt) {
			return nil, ErrConflict
		}
	}
	next, err := Replace(old, expected, mutation, ciphertext, at, limits)
	if err != nil {
		return nil, err
	}
	next.Binding = b
	next.ExpiresAt = renewal.ExpiresAt
	return next, nil
}
