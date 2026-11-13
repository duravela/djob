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

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func TestCLICompactRequiresExplicitExistingStorageAndValidFlags(t *testing.T) {
	for _, args := range [][]string{
		{"compact"}, {"compact", "--timeout", "0"}, {"compact", "--unexpected"},
		{"compact", "--data-dir", t.TempDir(), "extra"}, {"compact", "--data-dir", t.TempDir(), "--http", "127.0.0.1:0"},
	} {
		require.Error(t, run(context.Background(), args, io.Discard))
	}
	missing := filepath.Join(t.TempDir(), "never-created")
	require.Error(t, run(context.Background(), []string{"compact", "--data-dir", missing}, io.Discard))
	_, err := os.Stat(missing)
	require.True(t, os.IsNotExist(err))
	require.NoError(t, run(context.Background(), []string{"compact", "--help"}, io.Discard))
}

func TestCLICompactReportsStateWithoutStartingRaftOrAPI(t *testing.T) {
	directory := t.TempDir()
	store := filepath.Join(directory, "store")
	require.NoError(t, os.Mkdir(store, 0750))
	q, err := db.NewDB(filepath.Join(store, "jobs.snap"))
	require.NoError(t, err)
	require.NoError(t, q.Close())
	var output bytes.Buffer
	require.NoError(t, run(context.Background(), []string{"compact", "--data-dir", directory}, &output))
	var result db.FileCompactionResult
	require.NoError(t, json.Unmarshal(output.Bytes(), &result))
	require.Equal(t, uint64(0), result.AppliedIndex)
	require.Equal(t, byte(1), result.Format)
	require.Len(t, result.SHA256, 64)
	require.Greater(t, result.Buckets, uint64(0))
	_, err = os.Stat(filepath.Join(directory, "raft"))
	require.True(t, os.IsNotExist(err), "the maintenance command cannot start Raft")
	_, err = os.Stat(filepath.Join(directory, "auth.json"))
	require.True(t, os.IsNotExist(err))
}
