//go:build darwin || linux

package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func validateCompactionFile(path string, info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("cannot inspect database file ownership")
	}
	if stat.Nlink != 1 {
		return fmt.Errorf("database has multiple hard links; inspect storage aliases or interrupted compaction artifacts before continuing")
	}
	return validateCompactionFlags(path, info)
}

func copyCompactionMetadata(source, target string, info os.FileInfo) error {
	stat := info.Sys().(*syscall.Stat_t)
	if err := os.Chown(target, int(stat.Uid), int(stat.Gid)); err != nil {
		return err
	}
	if err := os.Chmod(target, info.Mode()); err != nil {
		return err
	}
	attributes, err := readCompactionAttributes(source)
	if err != nil {
		return err
	}
	existing, err := readCompactionAttributes(target)
	if err != nil {
		return err
	}
	for name := range existing {
		if _, ok := attributes[name]; !ok {
			if err := unix.Removexattr(target, name); err != nil {
				return fmt.Errorf("remove inherited database attribute %q: %w", name, err)
			}
		}
	}
	for name, value := range attributes {
		if previous, ok := existing[name]; ok && bytes.Equal(previous, value) {
			continue
		}
		if err := unix.Setxattr(target, name, value, 0); err != nil {
			return fmt.Errorf("copy database attribute %q: %w", name, err)
		}
	}
	actual, err := readCompactionAttributes(target)
	if err != nil {
		return err
	}
	if len(actual) != len(attributes) {
		return fmt.Errorf("database extended attributes differ after copying")
	}
	for name, value := range attributes {
		if copied, ok := actual[name]; !ok || !bytes.Equal(copied, value) {
			return fmt.Errorf("database extended attribute %q differs after copying", name)
		}
	}
	targetInfo, err := os.Stat(target)
	if err != nil {
		return err
	}
	targetStat := targetInfo.Sys().(*syscall.Stat_t)
	if targetStat.Uid != stat.Uid || targetStat.Gid != stat.Gid || targetInfo.Mode() != info.Mode() {
		return fmt.Errorf("database ownership or permissions differ after copying")
	}
	return nil
}

func readCompactionAttributes(path string) (map[string][]byte, error) {
	const maxAttributeBytes = 32 << 20
	attributes := make(map[string][]byte)
	size, err := unix.Listxattr(path, nil)
	if errors.Is(err, unix.ENOTSUP) {
		return attributes, nil
	}
	if err != nil {
		return nil, err
	}
	if size > maxAttributeBytes {
		return nil, fmt.Errorf("database attribute list exceeds maintenance memory budget")
	}
	names := make([]byte, size)
	n, err := unix.Listxattr(path, names)
	if err != nil {
		return nil, err
	}
	if n > len(names) {
		return nil, fmt.Errorf("database attribute list changed during maintenance")
	}
	var total int
	for _, name := range bytes.Split(names[:n], []byte{0}) {
		if len(name) == 0 {
			continue
		}
		size, err := unix.Getxattr(path, string(name), nil)
		if err != nil {
			return nil, err
		}
		if size > maxAttributeBytes-total {
			return nil, fmt.Errorf("database attributes exceed maintenance memory budget")
		}
		value := make([]byte, size)
		n, err := unix.Getxattr(path, string(name), value)
		if err != nil {
			return nil, err
		}
		if n > len(value) {
			return nil, fmt.Errorf("database attribute changed during maintenance")
		}
		attributes[string(name)] = value[:n]
		total += n
	}
	return attributes, nil
}
