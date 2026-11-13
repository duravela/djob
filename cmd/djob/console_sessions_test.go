package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCLIRequiresCredentialsAndTLSBeforeOpeningSharedSessionStorage(t *testing.T) {
	auth := filepath.Join(t.TempDir(), "auth.json")
	require.NoError(t, os.WriteFile(auth, []byte(`{"credentials":[{"subject":"platform","role":"operator","token":"abcdefghijklmnopqrstuvwxyz0123456789"}]}`), 0600))
	for _, flags := range [][]string{nil, {"--api-auth-file", auth}, {"--api-tls-cert", "/private/cert.pem", "--api-tls-key", "/private/key.pem"}} {
		dir := filepath.Join(t.TempDir(), "unopened")
		args := append([]string{"--data-dir", dir, "--console-sessions"}, flags...)
		require.ErrorContains(t, run(context.Background(), args, io.Discard), "requires API credentials and TLS")
		_, err := os.Stat(dir)
		require.True(t, os.IsNotExist(err))
	}
}
