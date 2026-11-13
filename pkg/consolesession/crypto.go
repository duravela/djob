package consolesession

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"
)

const MaxReviewBytes = 4 << 20

// NewCredential creates independent random session identity and browser secret.
func NewCredential() (string, error) {
	var id, secret [32]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	if _, err := rand.Read(secret[:]); err != nil {
		return "", err
	}
	return "cs1." + hex.EncodeToString(id[:]) + "." + hex.EncodeToString(secret[:]), nil
}

func ParseCredential(token string) (id, secret [32]byte, err error) {
	if len(token) != 133 || !strings.HasPrefix(token, "cs1.") || token[68] != '.' {
		return id, secret, ErrInvalid
	}
	a, e1 := hex.DecodeString(token[4:68])
	b, e2 := hex.DecodeString(token[69:])
	if e1 != nil || e2 != nil || token != strings.ToLower(token) {
		return id, secret, ErrInvalid
	}
	copy(id[:], a)
	copy(secret[:], b)
	if id == ([32]byte{}) || secret == ([32]byte{}) {
		return id, secret, ErrInvalid
	}
	return id, secret, nil
}

func reviewCipher(token string, cluster, version uint64, format byte) (cipher.AEAD, []byte, error) {
	id, secret, err := ParseCredential(token)
	if err != nil || cluster == 0 || version == 0 || (format != 1 && format != 2 && format != 3) {
		return nil, nil, ErrInvalid
	}
	context := binary.BigEndian.AppendUint64(id[:], cluster)
	context = binary.BigEndian.AppendUint64(context, version)
	domain := "djob/console-review/v" + strconv.Itoa(int(format))
	key, err := hkdf.Key(sha256.New, secret[:], context, domain, 32)
	if err != nil {
		return nil, nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, err
	}
	aead, err := cipher.NewGCMWithRandomNonce(block)
	aad := append([]byte(domain), id[:]...)
	aad = binary.BigEndian.AppendUint64(aad, cluster)
	aad = binary.BigEndian.AppendUint64(aad, version)
	return aead, aad, err
}

// SealReview returns a versioned envelope with a random nonce. Retries must
// retain the returned bytes. Each session/version has its own derived key.
func SealReview(token string, cluster, version uint64, plaintext []byte) ([]byte, error) {
	return sealReview(token, cluster, version, 1, plaintext)
}

// SealEditorReview marks payloads carrying editor revision state. Older console
// binaries reject format 2 instead of dropping unknown editor fields on a write.
func SealEditorReview(token string, cluster, version uint64, plaintext []byte) ([]byte, error) {
	return sealReview(token, cluster, version, 2, plaintext)
}

func sealReview(token string, cluster, version uint64, format byte, plaintext []byte) ([]byte, error) {
	if len(plaintext) > MaxReviewBytes-29 {
		return nil, ErrInvalid
	}
	aead, aad, err := reviewCipher(token, cluster, version, format)
	if err != nil {
		return nil, err
	}
	return aead.Seal([]byte{format}, nil, plaintext, aad), nil
}

func OpenReview(token string, cluster, version uint64, ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < 29 || len(ciphertext) > MaxReviewBytes || (ciphertext[0] != 1 && ciphertext[0] != 2 && ciphertext[0] != 3) {
		return nil, ErrInvalid
	}
	aead, aad, err := reviewCipher(token, cluster, version, ciphertext[0])
	if err != nil {
		return nil, err
	}
	plaintext, err := aead.Open(nil, nil, ciphertext[1:], aad)
	if err != nil {
		return nil, ErrInvalid
	}
	return plaintext, nil
}

// SealContinuityReview protects refresh and release-plan fields from older writers.
func SealContinuityReview(token string, cluster, version uint64, plaintext []byte) ([]byte, error) {
	return sealReview(token, cluster, version, 3, plaintext)
}
