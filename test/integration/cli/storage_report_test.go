package cli_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

func (c *cluster) checkStorageReport(i int) {
	c.t.Helper()
	c.t.Log("the offline report inspects a stopped member without rewriting its checkpoint")
	path := filepath.Join(c.dataDir(i), "store", "jobs.snap")
	digest := func() []byte {
		f, err := os.Open(path)
		if err != nil {
			c.t.Fatal(err)
		}
		defer f.Close()
		hash := sha256.New()
		if _, err := io.Copy(hash, f); err != nil {
			c.t.Fatal(err)
		}
		return hash.Sum(nil)
	}
	before := digest()
	command := exec.CommandContext(c.ctx, c.binary, "storage-report", "--data-dir", c.dataDir(i), "--timeout", "15s")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		c.t.Fatalf("offline report: %v\n%s", err, stderr.String())
	}
	var report struct {
		AppliedIndex uint64 `json:"applied_index,string"`
		Format       byte   `json:"database_format"`
		Records      uint64 `json:"records,string"`
		Categories   []struct {
			Category string `json:"category"`
			Records  uint64 `json:"records,string"`
		} `json:"categories"`
	}
	if err := json.Unmarshal(output, &report); err != nil {
		c.t.Fatal(err)
	}
	if report.Format != 11 || report.AppliedIndex == 0 || report.Records == 0 {
		c.t.Fatalf("missing checkpoint metadata: %+v", report)
	}
	counts := make(map[string]uint64)
	var sum uint64
	for _, category := range report.Categories {
		counts[category.Category] = category.Records
		sum += category.Records
	}
	for _, category := range []string{"audit_records", "request_receipts", "job_runs", "gateway_configuration", "indexes"} {
		if counts[category] == 0 {
			c.t.Fatalf("missing durable category %q", category)
		}
	}
	if sum != report.Records || !bytes.Equal(before, digest()) {
		c.t.Fatal("the offline report omitted records or changed the database")
	}
	for _, private := range []string{"private application input", "alpha-writer", "device-sessions"} {
		if strings.Contains(string(output), private) {
			c.t.Fatalf("report exposed private record data %q", private)
		}
	}
}

func (c *cluster) checkBackupVerification(i int) {
	c.t.Helper()
	c.t.Log("the real CLI verifies a read-only artifact and rejects corruption without a partial JSON report")
	artifact := filepath.Join(c.root, "verified-backup")
	var stderr bytes.Buffer
	command := exec.CommandContext(c.ctx, c.binary, "backup", "--data-dir", c.dataDir(i), "--id", strconv.Itoa(i+1), "--out", artifact)
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		c.t.Fatalf("backup: %v\n%s", err, stderr.String())
	}
	var manifest struct {
		AppliedIndex   uint64 `json:"applied_index,string"`
		CommittedIndex uint64 `json:"committed_index,string"`
		Files          []struct {
			Size uint64 `json:"size,string"`
		} `json:"files"`
	}
	if err := json.Unmarshal(output, &manifest); err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() {
		_ = filepath.WalkDir(artifact, func(path string, entry os.DirEntry, _ error) error {
			if entry != nil && entry.IsDir() {
				return os.Chmod(path, 0700)
			}
			return nil
		})
	})
	if err := filepath.WalkDir(artifact, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		mode := os.FileMode(0400)
		if entry.IsDir() {
			mode = 0500
		}
		return os.Chmod(path, mode)
	}); err != nil {
		c.t.Fatal(err)
	}
	stderr.Reset()
	command = exec.CommandContext(c.ctx, c.binary, "verify-backup", "--backup", artifact, "--timeout", "15s")
	command.Stderr = &stderr
	output, err = command.Output()
	if err != nil {
		c.t.Fatalf("verify-backup: %v\n%s", err, stderr.String())
	}
	var report struct {
		MemberID       uint64 `json:"member_id,string"`
		AppliedIndex   uint64 `json:"applied_index,string"`
		CommittedIndex uint64 `json:"committed_index,string"`
		FilesVerified  uint64 `json:"files_verified,string"`
		BytesVerified  uint64 `json:"bytes_verified,string"`
	}
	if err := json.Unmarshal(output, &report); err != nil {
		c.t.Fatal(err)
	}
	var total uint64
	for _, file := range manifest.Files {
		total += file.Size
	}
	if report.MemberID != uint64(i+1) || report.AppliedIndex != manifest.AppliedIndex || report.CommittedIndex != manifest.CommittedIndex || report.FilesVerified != uint64(len(manifest.Files)) || report.BytesVerified != total {
		c.t.Fatalf("verification metadata differs from the captured checkpoint: %+v", report)
	}
	if _, err := os.Lstat(filepath.Join(artifact, ".djob.lock")); !os.IsNotExist(err) {
		c.t.Fatalf("read-only verification created a lock: %v", err)
	}
	for _, private := range []string{"private application input", "alpha-writer", "device-sessions"} {
		if strings.Contains(string(output), private) {
			c.t.Fatalf("verification exposed private record data %q", private)
		}
	}
	seal := filepath.Join(artifact, ".djob-manifest.sha256")
	if err := os.Chmod(seal, 0600); err != nil {
		c.t.Fatal(err)
	}
	if err := os.WriteFile(seal, bytes.Repeat([]byte("0"), 64), 0600); err != nil {
		c.t.Fatal(err)
	}
	stderr.Reset()
	command = exec.CommandContext(c.ctx, c.binary, "verify-backup", "--backup", artifact)
	command.Stderr = &stderr
	output, err = command.Output()
	if err == nil || len(output) != 0 || !strings.Contains(stderr.String(), "checksum mismatch") {
		c.t.Fatalf("corrupt artifact did not fail cleanly: %v stdout=%q stderr=%q", err, output, stderr.String())
	}
}
