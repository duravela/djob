//go:build linux

package server

import (
	"errors"

	"golang.org/x/sys/unix"
)

func readFilesystemSpace(path string) (filesystemSpace, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return filesystemSpace{}, err
	}
	return linuxFilesystemSpace(stat)
}

func linuxFilesystemSpace(stat unix.Statfs_t) (filesystemSpace, error) {
	// Match Linux statvfs: block counters use the fragment size, falling back
	// to the block size on filesystems that leave Frsize unspecified.
	size := stat.Frsize
	if size == 0 {
		size = stat.Bsize
	}
	if size <= 0 {
		return filesystemSpace{}, errors.New("invalid filesystem block size")
	}
	return filesystemSpaceFromBlocks(uint64(size), stat.Blocks, stat.Bfree, stat.Bavail)
}
