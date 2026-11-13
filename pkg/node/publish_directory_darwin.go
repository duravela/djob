package node

import "golang.org/x/sys/unix"

func publishMaintenanceDirectory(source, target string) error {
	return unix.RenamexNp(source, target, unix.RENAME_EXCL)
}
