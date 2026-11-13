//go:build darwin

package server

import "golang.org/x/sys/unix"

func readFilesystemSpace(path string) (filesystemSpace, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return filesystemSpace{}, err
	}
	return filesystemSpaceFromBlocks(uint64(stat.Bsize), stat.Blocks, stat.Bfree, stat.Bavail)
}
