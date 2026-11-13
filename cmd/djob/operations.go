package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/duravela/djob/pkg/node"
)

func isOperationalCommand(name string) bool {
	switch name {
	case "backup", "verify-backup", "member-transfer", "restore", "recover", "raft-retain", "recovery-release", "clock-reset":
		return true
	}
	return false
}

func runOperations(ctx context.Context, name string, args []string, output io.Writer) error {
	if name == "verify-backup" {
		return runVerifyBackup(ctx, args, output)
	}
	f := flag.NewFlagSet("djob "+name, flag.ContinueOnError)
	f.SetOutput(output)
	directory := f.String("data-dir", "", "Existing source, or new restore/recovery destination")
	id := f.Uint64("id", 1, "Member ID in the ordered peer list")
	backup := f.String("backup", "", "Verified backup directory")
	out := f.String("out", "", "New backup directory (must not exist)")
	peers := f.String("peers", "", "Ordered new peer URLs for disaster recovery")
	keep := f.Int("keep-snapshots", 3, "Complete checkpoints retained, including their WAL recovery tail (1..100)")
	fenced := f.Bool("old-access-fenced", false, "Confirm previous member/cluster access has been externally excluded")
	unstarted := f.Bool("transfer-never-started", false, "Confirm no copy of this exact transfer has ever run as a replacement voter")
	reason := f.String("reason", "", "Operator explanation for releasing a hold")
	reference := f.String("reference-time", "", "Independently trusted current UTC time (RFC3339Nano)")
	skew := f.Duration("max-clock-skew", 5*time.Second, "Maximum difference from the trusted UTC reference")
	timeout := f.Duration("timeout", 10*time.Minute, "Cooperative maintenance timeout")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if f.NArg() != 0 || *directory == "" || *id == 0 || *timeout <= 0 {
		return fmt.Errorf("%s requires --data-dir, a positive member ID/timeout and no positional arguments", name)
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	var result any
	var err error
	switch name {
	case "backup":
		result, err = node.BackupMember(ctx, *directory, *out, *id)
	case "member-transfer":
		result, err = node.TransferMember(ctx, *directory, *out, *id)
	case "restore":
		result, err = node.RestoreMember(ctx, *backup, *directory, node.RestoreOptions{OldAccessFenced: *fenced, TransferNeverStarted: *unstarted})
	case "recover":
		var ordered []string
		for _, peer := range strings.Split(*peers, ",") {
			if strings.TrimSpace(peer) != "" {
				ordered = append(ordered, strings.TrimSpace(peer))
			}
		}
		result, err = node.RecoverCluster(ctx, *backup, *directory, *id, ordered, *fenced)
	case "raft-retain":
		result, err = node.RetainRaftStorage(ctx, *directory, *id, *keep)
	case "recovery-release":
		err = node.ReleaseRecovery(ctx, *directory, *id, *reason)
		result = map[string]bool{"released": err == nil}
	case "clock-reset":
		var at time.Time
		at, err = time.Parse(time.RFC3339Nano, *reference)
		if err == nil {
			err = node.ResetClockHold(*directory, *reason, at, *skew)
		}
		result = map[string]bool{"reset": err == nil}
	default:
		return fmt.Errorf("unsupported maintenance command")
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(result)
}

func runVerifyBackup(ctx context.Context, args []string, output io.Writer) error {
	f := flag.NewFlagSet("djob verify-backup", flag.ContinueOnError)
	f.SetOutput(io.Discard) // Parse errors are returned to main's stderr path.
	backup := f.String("backup", "", "Existing immutable backup or transfer directory (required)")
	timeout := f.Duration("timeout", 10*time.Minute, "Cooperative verification timeout")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			f.SetOutput(output)
			f.Usage()
			return nil
		}
		return err
	}
	if f.NArg() != 0 || *backup == "" || *timeout <= 0 {
		return fmt.Errorf("verify-backup requires --backup, a positive --timeout and no positional arguments")
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	result, err := node.VerifyBackup(ctx, *backup)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(result)
}
