//go:build !darwin && !linux

package node

import "fmt"

func publishMaintenanceDirectory(source, target string) error {
	return fmt.Errorf("atomic maintenance publication is supported on Linux and macOS")
}
