package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/server"
	"github.com/stretchr/testify/require"
)

func TestCLIRequiresExplicitStorageAndValidArguments(t *testing.T) {
	for _, args := range [][]string{nil, {"--data-dir", t.TempDir(), "--id", "0"}, {"--unexpected"}, {"--data-dir", t.TempDir(), "extra"}, {"--data-dir", t.TempDir(), "--demo", "--modbus-tcp"}, {"--data-dir", t.TempDir(), "--gateway-recovery-timeout", "0"}, {"--data-dir", t.TempDir(), "--readiness-timeout", "0"}, {"--data-dir", t.TempDir(), "--readiness-timeout", "6s"}} {
		require.Error(t, run(context.Background(), args, io.Discard))
	}
	require.NoError(t, run(context.Background(), []string{"--help"}, io.Discard))
}

func TestInitMemberCommandProvisionsOnceWithoutQuorum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var output strings.Builder
	dir := filepath.Join(t.TempDir(), "initial-member")
	args := []string{"init-member", "--dynamic-membership", "--data-dir", dir, "--peers", "http://127.0.0.1:0"}
	require.NoError(t, run(ctx, args, &output))
	require.Contains(t, output.String(), `"initialized":true`)
	require.FileExists(t, filepath.Join(dir, "store", "jobs.snap"))
	require.Error(t, run(ctx, args, io.Discard), "initialization cannot overwrite member history")
}

func TestCLIRejectsClockMonitorBoundsBeforeOpeningStorage(t *testing.T) {
	for flag, values := range map[string][]time.Duration{
		"--clock-sample-interval": {0, -time.Second, server.MinClockSampleInterval - 1, server.MaxClockSampleInterval + 1},
		"--clock-jump-threshold":  {0, -time.Second, server.MinClockJumpThreshold - 1, server.MaxClockJumpThreshold + 1},
	} {
		for _, value := range values {
			dir := filepath.Join(t.TempDir(), "unopened")
			err := run(context.Background(), []string{"--data-dir", dir, flag, value.String()}, io.Discard)
			require.Error(t, err)
			_, err = os.Stat(dir)
			require.True(t, os.IsNotExist(err))
		}
	}
}

func TestCLIRejectsStorageSamplingBoundsBeforeOpeningStorage(t *testing.T) {
	for _, value := range []time.Duration{0, -time.Second, server.MinStorageSampleInterval - 1, server.MaxStorageSampleInterval + 1} {
		dir := filepath.Join(t.TempDir(), "unopened")
		err := run(context.Background(), []string{"--data-dir", dir, "--storage-sample-interval", value.String()}, io.Discard)
		require.Error(t, err)
		_, err = os.Stat(dir)
		require.True(t, os.IsNotExist(err))
	}
}

func TestCLIRejectsQueueSamplingBoundsBeforeOpeningStorage(t *testing.T) {
	for _, args := range [][]string{
		{"--queue-sample-interval", "0"}, {"--queue-sample-interval", "-1s"}, {"--queue-sample-interval", "99ms"}, {"--queue-sample-interval", "6m"},
		{"--queue-sample-limit", "0"}, {"--queue-sample-limit", "-1"}, {"--queue-sample-limit", "65537"},
	} {
		dir := filepath.Join(t.TempDir(), "unopened")
		err := run(context.Background(), append([]string{"--data-dir", dir}, args...), io.Discard)
		require.Error(t, err)
		_, err = os.Stat(dir)
		require.True(t, os.IsNotExist(err))
	}
}

func TestCLIRejectsStateSamplingBoundsBeforeOpeningStorage(t *testing.T) {
	for _, args := range [][]string{
		{"--state-sample-interval", "0"}, {"--state-sample-interval", "-1s"}, {"--state-sample-interval", "99ms"}, {"--state-sample-interval", "6m"},
		{"--state-sample-limit", "0"}, {"--state-sample-limit", "-1"}, {"--state-sample-limit", "65537"},
	} {
		dir := filepath.Join(t.TempDir(), "unopened")
		err := run(context.Background(), append([]string{"--data-dir", dir}, args...), io.Discard)
		require.Error(t, err)
		_, err = os.Stat(dir)
		require.True(t, os.IsNotExist(err))
	}
}

func TestDemoCannotExecuteGatewayProtocolsOrOtherExecutors(t *testing.T) {
	transport, err := demoFactory(db.CommunicationChannel{Protocol: "modbus"})
	require.Error(t, err)
	require.Nil(t, transport)
	transport, err = demoFactory(db.CommunicationChannel{Protocol: "demo"})
	require.NoError(t, err)
	result, err := transport.Exchange(context.Background(), executor.Request{Executor: "modbus-write"})
	require.Error(t, err)
	require.Equal(t, db.AttemptFailed, result.Status)
	result, err = transport.Exchange(context.Background(), executor.Request{Executor: "echo", Payload: []byte{0, 0xff}})
	require.NoError(t, err)
	require.Equal(t, db.AttemptSucceeded, result.Status)
	require.Equal(t, []byte{0, 0xff}, result.Data)
	require.NoError(t, transport.Recover(context.Background()))
	require.NoError(t, transport.Close(context.Background()))
}

func TestCLISecurityErrorsDoNotStartRaftOrExposeCredentials(t *testing.T) {
	dir := t.TempDir()
	secret := strings.Repeat("s", 40)
	path := filepath.Join(dir, "auth.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"credentials":[{"subject":"ops","role":"`+secret+`","token":"`+secret+`"}]}`), 0600))
	for _, extra := range [][]string{{"--api-tls-cert", "cert.pem"}, {"--raft-tls-ca", "ca.pem"}, {"--raft-tls-cert", "cert.pem"}, {"--raft-tls-key", "key.pem"}, {"--raft-tls-ca", "ca.pem", "--raft-tls-cert", "cert.pem"}, {"--peers", "https://127.0.0.1:0", "--raft-tls-ca", "ca.pem", "--raft-tls-cert", "cert.pem", "--raft-tls-key", "key.pem"}, {"--api-tls-key", "key.pem"}, {"--api-auth-file", path}, {"--api-auth-file", filepath.Join(dir, "missing")}} {
		storage := filepath.Join(t.TempDir(), "unopened")
		args := append([]string{"--data-dir", storage}, extra...)
		err := run(context.Background(), args, io.Discard)
		require.Error(t, err)
		require.NotContains(t, err.Error(), secret)
		_, err = os.Stat(storage)
		require.True(t, os.IsNotExist(err))
	}
}
