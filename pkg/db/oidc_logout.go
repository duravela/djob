package db

import (
	"context"
	"crypto/sha256"
	"fmt"
	"go.etcd.io/bbolt"
	"time"
)

var oidcLogoutBucket = []byte("oidc_logout")

type OIDCLogout struct {
	Issuer, Subject, SID string
	IssuedAt             time.Time
}

func logoutKey(issuer, subject, sid string) [32]byte {
	scope := "subject\x00" + subject
	if sid != "" {
		scope = "sid\x00" + sid
	}
	return sha256.Sum256([]byte(issuer + "\x00" + scope))
}
func applyOIDCLogout(tx *bbolt.Tx, c ConsoleSessionCommand) (ConsoleSessionResult, error) {
	v := c.Logout
	if c.Creation != nil || c.Renewal != nil || c.Policy != nil || c.ID != ([32]byte{}) || c.Mutation != ([32]byte{}) || len(c.Ciphertext) > 0 || c.ExpectedVersion != 0 {
		return ConsoleSessionResult{Rejection: &CommandRejection{Code: "invalid", Message: "conflicting logout payload"}}, nil
	}
	if v == nil || v.Issuer == "" || len(v.Issuer) > 2048 || v.Subject == "" && v.SID == "" || len(v.Subject) > 256 || len(v.SID) > 256 || v.IssuedAt.IsZero() || v.IssuedAt.After(c.At.Add(5*time.Minute)) || v.IssuedAt.Before(c.At.Add(-5*time.Minute)) {
		return ConsoleSessionResult{Rejection: &CommandRejection{Code: "invalid", Message: "invalid logout cutoff"}}, nil
	}
	b := tx.Bucket(oidcLogoutBucket)
	if b == nil {
		return ConsoleSessionResult{}, fmt.Errorf("missing OIDC logout bucket")
	}
	// Accepted ID tokens live at most 24h. Keep cutoffs longer than that, including
	// clock skew, so pruning cannot revive a previously rejected source token.
	cur := b.Cursor()
	for k, raw := cur.First(); k != nil; k, raw = cur.Next() {
		at, err := time.Parse(time.RFC3339Nano, string(raw))
		if err != nil {
			return ConsoleSessionResult{}, err
		}
		if at.Before(c.At.Add(-25 * time.Hour)) {
			if err := cur.Delete(); err != nil {
				return ConsoleSessionResult{}, err
			}
		}
	}
	key := logoutKey(v.Issuer, v.Subject, v.SID)
	if raw := b.Get(key[:]); raw != nil {
		old, err := time.Parse(time.RFC3339Nano, string(raw))
		if err != nil {
			return ConsoleSessionResult{}, err
		}
		if !v.IssuedAt.After(old) {
			return ConsoleSessionResult{}, nil
		}
	} else if b.Stats().KeyN >= 4096 {
		return ConsoleSessionResult{Rejection: &CommandRejection{Code: "capacity", Message: "logout cutoff capacity reached"}}, nil
	}
	return ConsoleSessionResult{}, b.Put(key[:], []byte(v.IssuedAt.UTC().Format(time.RFC3339Nano)))
}
func (q *DB) OIDCRevoked(ctx context.Context, issuer, subject, sid string, issued time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false, ErrStateInventoryUnavailable
	}
	revoked := false
	err := q.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(oidcLogoutBucket)
		if b == nil {
			return nil
		}
		for _, key := range [][32]byte{logoutKey(issuer, subject, ""), logoutKey(issuer, "", sid)} {
			if raw := b.Get(key[:]); raw != nil {
				at, err := time.Parse(time.RFC3339Nano, string(raw))
				if err != nil {
					return err
				}
				revoked = revoked || !issued.After(at)
			}
		}
		return nil
	})
	return revoked, err
}
