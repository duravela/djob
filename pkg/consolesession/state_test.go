package consolesession

import (
	"bytes"
	"encoding/gob"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func fixture() (Creation, Limits) {
	at := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	return Creation{Binding: Binding{ID: [32]byte{1}, CredentialHash: [32]byte{2}, SourceHash: [32]byte{3},
			ClusterID: 1, Subject: "alice", Role: "writer", TenantID: "alpha"},
			CreatedAt: at, ExpiresAt: at.Add(time.Hour), Ciphertext: []byte("encrypted review")},
		Limits{MaxCiphertextBytes: 1024, MaxLifetime: 24 * time.Hour}
}

func TestReviewSurvivesSerializationAndLostWriteAcknowledgement(t *testing.T) {
	c, limits := fixture()
	r, err := Create(nil, c, c.CreatedAt, limits)
	require.NoError(t, err)
	updated, err := Replace(r, 1, [32]byte{4}, []byte("encrypted submission marker"), c.CreatedAt, limits)
	require.NoError(t, err)
	var data bytes.Buffer
	require.NoError(t, gob.NewEncoder(&data).Encode(updated))
	var restored Record
	require.NoError(t, gob.NewDecoder(&data).Decode(&restored))
	retried, err := Replace(&restored, 1, [32]byte{4}, []byte("encrypted submission marker"), c.CreatedAt.Add(time.Second), limits)
	require.NoError(t, err)
	require.Equal(t, updated, retried)
	// A creation retry must not replace a later submission marker or extend TTL.
	retried, err = Create(&restored, c, c.CreatedAt.Add(time.Minute), limits)
	require.NoError(t, err)
	require.Equal(t, updated, retried)
	// A late response from the first replica cannot overwrite a newer review.
	latest, err := Replace(retried, 2, [32]byte{5}, []byte("encrypted receipt"), c.CreatedAt, limits)
	require.NoError(t, err)
	_, err = Replace(latest, 1, [32]byte{4}, []byte("encrypted submission marker"), c.CreatedAt, limits)
	require.ErrorIs(t, err, ErrConflict)
	_, err = Replace(latest, 3, [32]byte{5}, []byte("different data"), c.CreatedAt, limits)
	require.ErrorIs(t, err, ErrConflict)
}

func TestLogoutCannotBeUndoneByCreationOrReviewRetry(t *testing.T) {
	c, limits := fixture()
	r, err := Create(nil, c, c.CreatedAt, limits)
	require.NoError(t, err)
	revoked, err := Revoke(r, c.CreatedAt.Add(time.Minute))
	require.NoError(t, err)
	require.Empty(t, revoked.Ciphertext)
	require.Equal(t, uint64(2), revoked.Version)
	again, err := Revoke(revoked, c.CreatedAt.Add(2*time.Minute))
	require.NoError(t, err)
	require.Equal(t, revoked, again)
	_, err = Create(revoked, c, c.CreatedAt, limits)
	require.ErrorIs(t, err, ErrRevoked)
	_, err = Replace(revoked, 2, [32]byte{4}, []byte("new review"), c.CreatedAt, limits)
	require.ErrorIs(t, err, ErrRevoked)
	// Once cleanup removes the tombstone, the original creation is expired.
	_, err = Create(nil, c, c.ExpiresAt, limits)
	require.ErrorIs(t, err, ErrExpired)
}

func TestCreationRejectsChangedBindingExpiryAndReview(t *testing.T) {
	c, limits := fixture()
	r, err := Create(nil, c, c.CreatedAt, limits)
	require.NoError(t, err)
	for _, change := range []func(*Creation){
		func(c *Creation) { c.Binding.ClusterID++ },
		func(c *Creation) { c.Binding.SourceHash[0]++ },
		func(c *Creation) { c.Binding.CredentialHash[0]++ },
		func(c *Creation) { c.Binding.TenantID = "beta" },
		func(c *Creation) { c.ExpiresAt = c.ExpiresAt.Add(time.Second) },
		func(c *Creation) { c.Ciphertext = []byte("different") },
	} {
		changed := c
		change(&changed)
		_, err := Create(r, changed, c.CreatedAt, limits)
		require.ErrorIs(t, err, ErrConflict)
	}
}

func TestTransitionsEnforceExpiryBoundsAndOwnership(t *testing.T) {
	c, limits := fixture()
	r, err := Create(nil, c, c.CreatedAt, limits)
	require.NoError(t, err)
	c.Ciphertext[0] = 'X'
	require.Equal(t, "encrypted review", string(r.Ciphertext))
	input := []byte("next review")
	next, err := Replace(r, 1, [32]byte{4}, input, c.CreatedAt, limits)
	require.NoError(t, err)
	input[0] = 'X'
	require.Equal(t, "next review", string(next.Ciphertext))
	require.Equal(t, uint64(1), r.Version)
	_, err = Replace(r, 1, [32]byte{4}, []byte("next"), c.ExpiresAt, limits)
	require.ErrorIs(t, err, ErrExpired)
	_, err = Revoke(r, c.ExpiresAt)
	require.ErrorIs(t, err, ErrExpired)
	_, err = Replace(r, 1, [32]byte{4}, make([]byte, limits.MaxCiphertextBytes+1), c.CreatedAt, limits)
	require.ErrorIs(t, err, ErrInvalid)
	_, err = Replace(r, 1, [32]byte{4}, []byte("next"), c.CreatedAt.Add(-time.Second), limits)
	require.ErrorIs(t, err, ErrInvalid)
	_, err = Revoke(r, c.CreatedAt.Add(-time.Second))
	require.ErrorIs(t, err, ErrInvalid)
	r.Version = math.MaxUint64
	_, err = Revoke(r, c.CreatedAt)
	require.ErrorIs(t, err, ErrConflict)
	_, err = Replace(r, math.MaxUint64, [32]byte{4}, []byte("next"), c.CreatedAt, limits)
	require.ErrorIs(t, err, ErrInvalid)
}

func TestConcurrentReviewsHaveOneVersionWinner(t *testing.T) {
	c, limits := fixture()
	r, err := Create(nil, c, c.CreatedAt, limits)
	require.NoError(t, err)
	winner, err := Replace(r, 1, [32]byte{4}, []byte("review from replica A"), c.CreatedAt, limits)
	require.NoError(t, err)
	_, err = Replace(winner, 1, [32]byte{5}, []byte("review from replica B"), c.CreatedAt, limits)
	require.ErrorIs(t, err, ErrConflict)
	require.Equal(t, "review from replica A", string(winner.Ciphertext))
	_, err = Replace(winner, 1, [32]byte{4}, []byte("changed retry ciphertext"), c.CreatedAt, limits)
	require.ErrorIs(t, err, ErrConflict)
}

func TestCreationRequiresEligibleIdentityAndBoundedLifetime(t *testing.T) {
	c, limits := fixture()
	for _, change := range []func(*Creation){
		func(c *Creation) { c.Binding.Role = "worker" },
		func(c *Creation) { c.Binding.TenantID = "" },
		func(c *Creation) { c.Binding.CredentialHash = [32]byte{} },
		func(c *Creation) { c.CreatedAt = c.CreatedAt.Add(time.Second) },
		func(c *Creation) { c.ExpiresAt = c.CreatedAt.Add(25 * time.Hour) },
		func(c *Creation) { c.Ciphertext = nil },
	} {
		changed := c
		change(&changed)
		_, err := Create(nil, changed, c.CreatedAt, limits)
		require.ErrorIs(t, err, ErrInvalid)
	}
	c.Binding.Role, c.Binding.TenantID = "operator", ""
	_, err := Create(nil, c, c.CreatedAt, limits)
	require.NoError(t, err)
}

func TestRenewalPreservesCapabilityAndAbsoluteLifetime(t *testing.T) {
	c, limits := fixture()
	c.Binding.OIDCSubject = "alice"
	c.Binding.OIDCIssuer = "https://identity.example"
	c.Binding.OIDCExpiresAt = c.ExpiresAt
	c.Binding.OIDCIssuedAt = c.CreatedAt
	old, err := Create(nil, c, c.CreatedAt, limits)
	require.NoError(t, err)
	binding := c.Binding
	binding.SourceHash = [32]byte{8}
	binding.OIDCExpiresAt = c.CreatedAt.Add(3 * time.Hour)
	binding.OIDCIssuedAt = c.CreatedAt
	renewal := Renewal{Binding: binding, ExpiresAt: c.CreatedAt.Add(2 * time.Hour)}
	next, err := Renew(old, 1, [32]byte{9}, []byte("rotated refresh token and retained review"), renewal, c.CreatedAt.Add(time.Minute), limits)
	require.NoError(t, err)
	require.Equal(t, c.Binding.ID, next.Binding.ID)
	require.Equal(t, c.CreatedAt, next.CreatedAt)
	repeated, err := Renew(next, 1, [32]byte{9}, next.Ciphertext, renewal, c.CreatedAt.Add(2*time.Minute), limits)
	require.NoError(t, err)
	require.Equal(t, next, repeated)
	changed := renewal
	changed.Binding.TenantID = "beta"
	_, err = Renew(old, 1, [32]byte{9}, []byte("bad"), changed, c.CreatedAt.Add(time.Minute), limits)
	require.Error(t, err)
	changed = renewal
	changed.ExpiresAt = c.CreatedAt.Add(25 * time.Hour)
	_, err = Renew(old, 1, [32]byte{9}, []byte("bad"), changed, c.CreatedAt.Add(time.Minute), limits)
	require.Error(t, err)
}

func TestFederatedMetadataCannotBeAttachedToStaticSession(t *testing.T) {
	c, limits := fixture()
	c.Binding.OIDCSID = "foreign-session"
	_, err := Create(nil, c, c.CreatedAt, limits)
	require.Error(t, err)
	c, limits = fixture()
	c.Binding.OIDCIssuedAt = c.CreatedAt
	_, err = Create(nil, c, c.CreatedAt, limits)
	require.Error(t, err)
}
