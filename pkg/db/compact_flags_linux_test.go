//go:build linux

package db

import (
	"os"

	"golang.org/x/sys/unix"
)

func setCompactionTestNoDump(path string) (func(), error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	flags, err := unix.IoctlGetInt(int(f.Fd()), unix.FS_IOC_GETFLAGS)
	if err != nil {
		return nil, err
	}
	const noDump = 0x00000040
	if err := unix.IoctlSetPointerInt(int(f.Fd()), unix.FS_IOC_SETFLAGS, flags|noDump); err != nil {
		return nil, err
	}
	return func() {
		original, err := os.Open(path)
		if err == nil {
			defer original.Close()
			_ = unix.IoctlSetPointerInt(int(original.Fd()), unix.FS_IOC_SETFLAGS, flags)
		}
	}, nil
}
