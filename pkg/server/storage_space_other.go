//go:build !darwin && !linux

package server

import "errors"

func readFilesystemSpace(string) (filesystemSpace, error) {
	return filesystemSpace{}, errors.New("filesystem storage metrics currently support Linux and macOS")
}
