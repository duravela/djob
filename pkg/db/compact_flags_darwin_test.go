//go:build darwin

package db

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func setCompactionTestNoDump(path string) (func(), error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	flags := info.Sys().(*syscall.Stat_t).Flags
	if err := unix.Chflags(path, int(flags)|unix.UF_NODUMP); err != nil {
		return nil, err
	}
	return func() { _ = unix.Chflags(path, int(flags)) }, nil
}
