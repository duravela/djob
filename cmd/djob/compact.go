package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node"
)

func runCompact(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("djob compact", flag.ContinueOnError)
	flags.SetOutput(output)
	directory := flags.String("data-dir", "", "Existing data directory of the stopped member (required)")
	backup := flags.Bool("keep-backup", false, "Retain the original database after verified replacement")
	timeout := flags.Duration("timeout", 15*time.Minute, "Maintenance timeout; cancellation is checked between records and verification phases")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *directory == "" || *timeout <= 0 {
		return fmt.Errorf("compact requires --data-dir, a positive --timeout and no positional arguments")
	}
	maintenance, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	result, err := node.CompactStore(maintenance, *directory, db.FileCompactionOptions{KeepBackup: *backup})
	if err == nil || result.Installed {
		if outputErr := json.NewEncoder(output).Encode(result); outputErr != nil {
			err = errors.Join(err, fmt.Errorf("write compaction result (installed=%t): %w", result.Installed, outputErr))
		}
	}
	return err
}
