package node

import (
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
)

func requireMaintenanceMember(directory string) error {
	for _, name := range []string{backupManifestName, ".djob-manifest.sha256", ".djob-restored", ".djob-transferred", ".djob-restore-pending"} {
		if _, err := os.Lstat(filepath.Join(directory, name)); err == nil {
			return fmt.Errorf("member maintenance refuses backup artifacts, retired sources and inactive restores")
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// Inspect before locking so even a damaged artifact never gains a lock file.
// Check again under the stable member lock in case a concurrent transfer retired
// the source between the initial inspection and lock acquisition.
func lockMaintenanceMember(directory string) (*fileutil.LockedFile, error) {
	if err := requireMaintenanceMember(directory); err != nil {
		return nil, err
	}
	lock, err := lockNodeDataDir(directory)
	if err != nil {
		return nil, err
	}
	if err := requireMaintenanceMember(directory); err != nil {
		_ = lock.Close()
		return nil, err
	}
	return lock, nil
}
