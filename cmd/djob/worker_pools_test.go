package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/duravela/djob/pkg/workercontrol"
	"github.com/stretchr/testify/require"
)

func TestCLIRejectsInvalidWorkerPoolConfigurationBeforeOpeningStorage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.json")
	data, err := json.Marshal(workercontrol.KubernetesConfig{ServerURL: "https://kubernetes.example", ClusterID: "plant", Namespace: "workers", TokenFile: "/private/token"})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0600))
	auth := filepath.Join(t.TempDir(), "auth.json")
	require.NoError(t, os.WriteFile(auth, []byte(`{"credentials":[{"subject":"platform","role":"operator","token":"`+strings.Repeat("t", 40)+`"}]}`), 0600))
	for _, test := range []struct {
		name, message string
		flags         []string
	}{
		{"requires workers", "requires --remote-workers", []string{"--worker-kubernetes-config", path}},
		{"empty path", "nonempty", []string{"--worker-kubernetes-config", ""}},
		{"duplicate default", "duplicate Kubernetes worker pool", []string{"--remote-workers", "--api-auth-file", auth, "--api-tls-cert", "/private/cert.pem", "--api-tls-key", "/private/key.pem", "--worker-kubernetes-config", path, "--worker-kubernetes-config", path}},
		{"too many pools", "nonempty", strings.Fields(strings.Repeat("--worker-kubernetes-config /private/pool.json ", workercontrol.MaxKubernetesPools+1))},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "unopened")
			err := run(context.Background(), append([]string{"--data-dir", dir}, test.flags...), io.Discard)
			require.ErrorContains(t, err, test.message)
			_, err = os.Stat(dir)
			require.True(t, os.IsNotExist(err))
		})
	}
}
