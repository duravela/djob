package consolesession

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReviewEncryptionBindsCredentialClusterVersionAndEnvelope(t *testing.T) {
	token, err := NewCredential()
	require.NoError(t, err)
	plaintext := []byte("private worker credential and exact request bytes")
	sealed, err := SealReview(token, 7, 1, plaintext)
	require.NoError(t, err)
	again, err := SealReview(token, 7, 1, plaintext)
	require.NoError(t, err)
	require.NotEqual(t, sealed, again, "encryptions must use fresh nonces")
	opened, err := OpenReview(token, 7, 1, sealed)
	require.NoError(t, err)
	require.Equal(t, plaintext, opened)
	other, err := NewCredential()
	require.NoError(t, err)
	_, err = OpenReview(other, 7, 1, sealed)
	require.Error(t, err)
	_, err = OpenReview(token, 8, 1, sealed)
	require.Error(t, err)
	_, err = OpenReview(token, 7, 2, sealed)
	require.Error(t, err)
	for _, pos := range []int{0, 1, len(sealed) - 1} {
		changed := append([]byte(nil), sealed...)
		changed[pos] ^= 1
		_, err = OpenReview(token, 7, 1, changed)
		require.Error(t, err)
	}
	_, err = OpenReview(token, 7, 1, sealed[:12])
	require.Error(t, err)
	_, _, err = ParseCredential("cs1.invalid")
	require.Error(t, err)
}

func TestEditorEnvelopeHasSeparateAuthenticatedFormat(t *testing.T) {
	token, err := NewCredential()
	require.NoError(t, err)
	ciphertext, err := SealEditorReview(token, 7, 1, []byte("editor draft revision"))
	require.NoError(t, err)
	require.Equal(t, byte(2), ciphertext[0])
	data, err := OpenReview(token, 7, 1, ciphertext)
	require.NoError(t, err)
	require.Equal(t, "editor draft revision", string(data))
	ciphertext[0] = 1
	_, err = OpenReview(token, 7, 1, ciphertext)
	require.Error(t, err, "the format marker must be bound to encryption context")
}
