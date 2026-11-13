package store

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotSpoolStreamsToPrivateFileAndCleansUp(t *testing.T) {
	directory := t.TempDir()
	block := bytes.Repeat([]byte("snapshot-data"), 1<<14)
	var name string
	data, err := spoolSnapshot(directory, func(w io.Writer) error {
		file, ok := w.(*os.File)
		require.True(t, ok, "pinned view must encode to disk, not a payload-sized heap buffer")
		name = file.Name()
		info, err := file.Stat()
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0600), info.Mode().Perm())
		for range 64 {
			if _, err := w.Write(block); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat(block, 64), data)
	_, err = os.Stat(name)
	require.True(t, os.IsNotExist(err))
	files, err := filepath.Glob(filepath.Join(directory, ".snapshot-encode-*"))
	require.NoError(t, err)
	require.Empty(t, files)
}
func TestSnapshotSpoolConsumesViewAndCleansUpOnFailures(t *testing.T) {
	injected := errors.New("encode failed")
	directory := t.TempDir()
	_, err := spoolSnapshot(directory, func(w io.Writer) error {
		_, err := w.Write([]byte("partial"))
		require.NoError(t, err)
		return injected
	})
	require.ErrorIs(t, err, injected)
	files, err := filepath.Glob(filepath.Join(directory, ".snapshot-encode-*"))
	require.NoError(t, err)
	require.Empty(t, files)
	consumed := false
	_, err = spoolSnapshot(filepath.Join(directory, "missing"), func(w io.Writer) error {
		require.False(t, consumed)
		consumed = true
		_, err := w.Write([]byte("must release captured view"))
		require.Error(t, err)
		return err
	})
	require.Error(t, err)
	require.True(t, consumed)
}

func TestPreparedSnapshotSpoolFailureReleasesActualDatabaseView(t *testing.T) {
	s, _, _ := newTestJobQueue(t)
	encode, err := s.PrepareSnapshotAt(s.LastAppliedIndex())
	require.NoError(t, err)
	stats, err := s.db.StorageStats()
	require.NoError(t, err)
	require.Equal(t, 1, stats.OpenReadTransactions)
	original := s.storedir
	s.storedir = filepath.Join(original, "missing-directory")
	_, err = encode()
	require.Error(t, err)
	s.storedir = original
	stats, err = s.db.StorageStats()
	require.NoError(t, err)
	require.Zero(t, stats.OpenReadTransactions)
}
