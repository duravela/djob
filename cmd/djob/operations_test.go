//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node"
	"github.com/duravela/djob/pkg/node/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCLIOperationalCommandsValidateArgumentsAndHelp(t *testing.T) {
	for _, name := range []string{"backup", "verify-backup", "member-transfer", "restore", "recover", "raft-retain", "recovery-release", "clock-reset"} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, run(context.Background(), []string{name}, io.Discard))
			require.Error(t, run(context.Background(), []string{name, "--unexpected"}, io.Discard))
			require.Error(t, run(context.Background(), []string{name, "--data-dir", t.TempDir(), "--timeout", "0"}, io.Discard))
			require.Error(t, run(context.Background(), []string{name, "--data-dir", t.TempDir(), "extra"}, io.Discard))
			require.NoError(t, run(context.Background(), []string{name, "--help"}, io.Discard))
		})
	}
	for _, args := range [][]string{{"--retain-snapshots", "0"}, {"--retain-snapshots", "101"}, {"--min-storage-available-bytes", "0"}, {"--max-database-bytes", "-1"}, {"--max-clock-skew", "0"}, {"--max-clock-skew", "2m"}} {
		directory := filepath.Join(t.TempDir(), "unopened")
		require.Error(t, run(context.Background(), append([]string{"--data-dir", directory}, args...), io.Discard))
		_, err := os.Stat(directory)
		require.True(t, os.IsNotExist(err))
	}
}

func TestCLIOperationalBackupTransferRecoveryRetentionAndClockReset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	directory := t.TempDir()
	cfg := config.Config{}
	cfg.SetDefaults()
	cfg.DataDir, cfg.Peers, cfg.Logger = directory, []string{"http://127.0.0.1:0"}, zap.NewNop()
	cfg.BatchTimeout, cfg.RaftEventLoopHeartbeatInterval = time.Millisecond, 10*time.Millisecond
	app, err := node.OpenApplication(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(app.Close)
	_, err = app.Raft.WaitForLeaderContext(ctx)
	require.NoError(t, err)
	_, err = app.Store.ProposeStateCommand(ctx, db.StateCommand{RequestID: "cli-checkpoint", At: time.Now().UTC(), Job: &db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "tenant"}}}})
	require.NoError(t, err)
	backup := filepath.Join(t.TempDir(), "backup")
	var output bytes.Buffer
	require.ErrorContains(t, run(ctx, []string{"backup", "--data-dir", directory, "--out", backup}, &output), "lock node data directory")
	require.Empty(t, output.String())
	app.Close()
	command := func(args ...string) []byte {
		t.Helper()
		output.Reset()
		require.NoError(t, run(ctx, args, &output))
		require.True(t, json.Valid(output.Bytes()), output.String())
		return append([]byte(nil), output.Bytes()...)
	}
	var manifest node.MemberBackup
	require.NoError(t, json.Unmarshal(command("backup", "--data-dir", directory, "--out", backup), &manifest))
	require.NotZero(t, manifest.CommittedIndex)
	var verification node.BackupVerification
	require.NoError(t, json.Unmarshal(command("verify-backup", "--backup", backup), &verification))
	require.Equal(t, manifest.AppliedIndex, verification.AppliedIndex)
	require.Equal(t, manifest.CommittedIndex, verification.CommittedIndex)
	require.Equal(t, uint64(len(manifest.Files)), verification.FilesVerified)
	_, err = os.Lstat(filepath.Join(backup, ".djob.lock"))
	require.True(t, os.IsNotExist(err), "verification must not create an artifact lock")
	transfer := filepath.Join(t.TempDir(), "transfer")
	command("member-transfer", "--data-dir", directory, "--out", transfer)
	require.NoError(t, json.Unmarshal(command("verify-backup", "--backup", transfer), &verification))
	require.True(t, verification.Transferred)
	require.False(t, verification.TransferConsumed)
	replacement := filepath.Join(t.TempDir(), "replacement")
	command("restore", "--backup", transfer, "--data-dir", replacement, "--old-access-fenced", "--transfer-never-started")
	require.NoError(t, json.Unmarshal(command("verify-backup", "--backup", transfer), &verification))
	require.True(t, verification.TransferConsumed)
	command("raft-retain", "--data-dir", replacement, "--keep-snapshots", "3")
	require.NoError(t, os.WriteFile(filepath.Join(replacement, ".djob-clock-hold"), []byte("test hold"), 0600))
	command("clock-reset", "--data-dir", replacement, "--reference-time", time.Now().UTC().Format(time.RFC3339Nano), "--reason", "trusted test time checked while stopped")
	recovered := filepath.Join(t.TempDir(), "recovered")
	command("recover", "--backup", backup, "--data-dir", recovered, "--peers", "http://127.0.0.1:0", "--old-access-fenced")
	command("recovery-release", "--data-dir", recovered, "--reason", "no executable work or ownership; old cluster fenced")
	cfg.DataDir = recovered
	app, err = node.OpenApplication(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(app.Close)
	_, err = app.Raft.WaitForLeaderContext(ctx)
	require.NoError(t, err)
	_, err = app.Store.GetJobRecord(db.JobRecordQuery{Kind: db.JobTenantRecord, TenantID: "tenant"})
	require.NoError(t, err)
}

func TestCLIVerifyBackupRefusesInvalidArgumentsAndArtifactsWithoutJSON(t *testing.T) {
	for _, flags := range [][]string{
		{"--unexpected"},
		{"--backup"},
		{"--backup", "missing", "--timeout", "0"},
		{"--backup", "missing", "--timeout", "-1s"},
		{"--backup", "missing", "extra"},
		{"--backup", "missing"},
		{"--backup", t.TempDir()},
	} {
		var output bytes.Buffer
		require.Error(t, run(context.Background(), append([]string{"verify-backup"}, flags...), &output))
		require.Empty(t, output.String())
	}
	var help bytes.Buffer
	require.NoError(t, run(context.Background(), []string{"verify-backup", "--help"}, &help))
	require.Contains(t, help.String(), "Usage of djob verify-backup")
	require.Contains(t, help.String(), "-backup")
}
