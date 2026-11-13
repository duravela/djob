//go:build linux

package server

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestLinuxStorageSpaceUsesFragmentSizeAndFallback(t *testing.T) {
	stat := unix.Statfs_t{Bsize: 4096, Frsize: 1024, Blocks: 100, Bfree: 20, Bavail: 10}
	space, err := linuxFilesystemSpace(stat)
	require.NoError(t, err)
	require.Equal(t, filesystemSpace{102400, 20480, 10240}, space)
	stat.Frsize = 0
	space, err = linuxFilesystemSpace(stat)
	require.NoError(t, err)
	require.Equal(t, filesystemSpace{409600, 81920, 40960}, space)
	stat.Bsize = -1
	_, err = linuxFilesystemSpace(stat)
	require.Error(t, err)
}
