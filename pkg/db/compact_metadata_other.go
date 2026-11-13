//go:build !darwin && !linux

package db

import (
	"fmt"
	"os"
)

func validateCompactionFile(string, os.FileInfo) error {
	return fmt.Errorf("offline database compaction currently supports Linux and macOS")
}

func copyCompactionMetadata(string, string, os.FileInfo) error {
	return fmt.Errorf("offline database compaction currently supports Linux and macOS")
}
