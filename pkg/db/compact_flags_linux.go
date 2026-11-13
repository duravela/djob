//go:build linux

package db

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func validateCompactionFlags(path string, _ os.FileInfo) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	flags, err := unix.IoctlGetInt(int(f.Fd()), unix.FS_IOC_GETFLAGS)
	if errors.Is(err, unix.ENOTTY) || errors.Is(err, unix.EOPNOTSUPP) {
		return nil // This filesystem does not expose Linux inode flags.
	}
	if err != nil {
		return err
	}
	// FS_EXTENT_FL is the kernel's normal extent-layout marker. Other flags,
	// such as no-COW, no-dump, immutable or compression, need explicit handling
	// rather than being silently lost when a new inode replaces the original.
	const extentLayout = 0x00080000
	if flags & ^extentLayout != 0 {
		return fmt.Errorf("database has filesystem flags that require explicit administrative handling before compaction")
	}
	return nil
}
