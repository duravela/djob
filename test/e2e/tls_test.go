package e2e_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"
)

type authority struct {
	key   *ecdsa.PrivateKey
	cert  *x509.Certificate
	pem   []byte
	roots *x509.CertPool
}

func newAuthority(t *testing.T, lifetime time.Duration) authority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "isolated E2E CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(lifetime), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	data := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		t.Fatal("invalid test CA")
	}
	return authority{key, cert, data, roots}
}

func (a authority) leaf(t *testing.T, names ...string) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: serial, DNSNames: names, NotBefore: a.cert.NotBefore, NotAfter: a.cert.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, a.cert, &key.PublicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	encodedKey, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encodedKey})
}

func TestSoakCertificatesCoverRunBudget(t *testing.T) {
	ca := newAuthority(t, 25*time.Hour)
	data, _ := ca.leaf(t, "long-soak")
	block, _ := pem.Decode(data)
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	options := x509.VerifyOptions{DNSName: "long-soak", Roots: ca.roots, CurrentTime: time.Now().Add(24 * time.Hour)}
	if _, err := cert.Verify(options); err != nil {
		t.Fatalf("certificate expires during supported soak: %v", err)
	}
	options.CurrentTime = ca.cert.NotAfter.Add(time.Second)
	if _, err := cert.Verify(options); err == nil {
		t.Fatal("expired soak certificate was accepted")
	}
}
