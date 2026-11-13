//go:build darwin

package db

import (
	"fmt"
	"os"
	"syscall"
)

func validateCompactionFlags(_ string, info os.FileInfo) error {
	if info.Sys().(*syscall.Stat_t).Flags != 0 {
		return fmt.Errorf("database has filesystem flags that require explicit administrative handling before compaction")
	}
	return nil
}
