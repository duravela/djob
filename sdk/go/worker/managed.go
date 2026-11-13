// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
)

const (
	DeploymentIDEnvironment         = "DJOB_WORKER_DEPLOYMENT"
	DeploymentGenerationEnvironment = "DJOB_WORKER_GENERATION"
	CredentialDirectoryEnvironment  = "DJOB_WORKER_CREDENTIAL_DIRECTORY"
)

// ResolveCredentialPath applies the same private-file policy as LoadConfig to
// optional protocol profiles and client keys. Ordinary files remain owner-only;
// an explicitly managed projected volume permits read-only 0400/0440 targets.
// Callers must still bound how much data they read and avoid exposing secrets.
func ResolveCredentialPath(path string) (string, error) {
	root, err := managedEnvironment(&Config{})
	if err != nil {
		return "", err
	}
	resolved, _, err := privateWorkerPath(path, root)
	return resolved, err
}

// The runtime supplies identity metadata separately from the credential Secret.
// A worker cannot claim under a different deployment or an earlier generation.
func managedEnvironment(c *Config) (string, error) {
	id, generation := os.Getenv(DeploymentIDEnvironment), os.Getenv(DeploymentGenerationEnvironment)
	if id != "" || generation != "" {
		g, err := strconv.ParseUint(generation, 10, 64)
		if !wire.ValidWorkerID(id) || err != nil || g == 0 || strconv.FormatUint(g, 10) != generation {
			return "", fmt.Errorf("invalid managed worker environment")
		}
		if c.DeploymentID != "" && c.DeploymentID != id || c.DeploymentGeneration != 0 && c.DeploymentGeneration != g {
			return "", fmt.Errorf("managed worker configuration does not match its environment")
		}
		c.DeploymentID, c.DeploymentGeneration = id, g
	}
	if (c.DeploymentID == "") != (c.DeploymentGeneration == 0) || c.DeploymentID != "" && !wire.ValidWorkerID(c.DeploymentID) {
		return "", fmt.Errorf("provide a complete managed worker identity")
	}
	root := os.Getenv(CredentialDirectoryEnvironment)
	if root != "" && (id == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator)) {
		return "", fmt.Errorf("managed credential directory requires a managed worker environment and an absolute directory")
	}
	return root, nil
}

// Projected Kubernetes Secrets use atomic symlinks and may be group-readable
// through a dedicated fsGroup. This explicit managed directory permits only
// read-only 0400/0440 regular targets inside that volume. Ordinary files retain
// the existing no-symlink, owner-only configuration/key requirements.
func privateWorkerPath(path, root string) (string, os.FileInfo, error) {
	resolved := path
	if root != "" {
		volume, err := filepath.EvalSymlinks(root)
		if err != nil {
			return "", nil, fmt.Errorf("managed credential volume unavailable")
		}
		resolved, err = filepath.EvalSymlinks(path)
		if err != nil {
			return "", nil, fmt.Errorf("managed credential file unavailable")
		}
		relative, err := filepath.Rel(volume, resolved)
		if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return "", nil, fmt.Errorf("managed private files must remain inside their credential volume")
		}
	}
	info, err := os.Lstat(resolved)
	if err != nil || !info.Mode().IsRegular() {
		return "", nil, fmt.Errorf("worker private file must be regular")
	}
	if root == "" {
		if info.Mode().Perm()&0077 != 0 {
			return "", nil, fmt.Errorf("worker private file must be owner-only")
		}
	} else if info.Mode().Perm()&^os.FileMode(0440) != 0 || info.Mode().Perm()&0400 == 0 {
		return "", nil, fmt.Errorf("managed private file must be read-only with mode 0400 or 0440")
	}
	return resolved, info, nil
}
