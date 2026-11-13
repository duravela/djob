//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func TestCLIStorageReportRequiresExistingStorageAndValidFlags(t *testing.T) {
	for _, args := range [][]string{
		{"storage-report"}, {"storage-report", "--unexpected"},
		{"storage-report", "--data-dir", t.TempDir(), "--timeout", "0"},
		{"storage-report", "--data-dir", t.TempDir(), "extra"},
		{"storage-report", "--data-dir", t.TempDir(), "--http", "127.0.0.1:0"},
	} {
		require.Error(t, run(context.Background(), args, io.Discard))
	}
	missing := filepath.Join(t.TempDir(), "never-created")
	require.Error(t, run(context.Background(), []string{"storage-report", "--data-dir", missing}, io.Discard))
	_, err := os.Stat(missing)
	require.True(t, os.IsNotExist(err))
	require.NoError(t, run(context.Background(), []string{"storage-report", "--help"}, io.Discard))
}

type storageReportErrorWriter struct{ err error }

func (w storageReportErrorWriter) Write([]byte) (int, error) { return 0, w.err }

func TestCLIStorageReportEmitsJSONWithoutStartingRuntime(t *testing.T) {
	directory := t.TempDir()
	store := filepath.Join(directory, "store")
	require.NoError(t, os.Mkdir(store, 0750))
	q, err := db.NewDB(filepath.Join(store, "jobs.snap"))
	require.NoError(t, err)
	require.NoError(t, q.Close())
	args := []string{"storage-report", "--data-dir", directory}
	var output bytes.Buffer
	require.NoError(t, run(context.Background(), args, &output))
	var result db.FileStorageReport
	require.NoError(t, json.Unmarshal(output.Bytes(), &result))
	require.Equal(t, uint64(0), result.AppliedIndex)
	require.Equal(t, byte(1), result.Format)
	require.Positive(t, result.Buckets)
	require.Positive(t, result.Records)
	require.Positive(t, result.FileBytes)
	require.NotEmpty(t, result.Categories)
	for _, path := range []string{"raft", "auth.json"} {
		_, err = os.Stat(filepath.Join(directory, path))
		require.True(t, os.IsNotExist(err), "the report cannot start the runtime")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	output.Reset()
	require.ErrorIs(t, run(ctx, args, &output), context.Canceled)
	require.Empty(t, output.Bytes(), "a failed report emits no partial JSON")
	injected := errors.New("report output unavailable")
	require.ErrorIs(t, run(context.Background(), args, storageReportErrorWriter{injected}), injected)
}
