package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/duravela/djob/pkg/node"
)

func runStorageReport(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("djob storage-report", flag.ContinueOnError)
	flags.SetOutput(output)
	directory := flags.String("data-dir", "", "Existing data directory of the stopped member (required)")
	timeout := flags.Duration("timeout", 2*time.Minute, "Report timeout; cancellation is checked between records")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *directory == "" || *timeout <= 0 {
		return fmt.Errorf("storage-report requires --data-dir, a positive --timeout and no positional arguments")
	}
	inspection, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	result, err := node.ReportStoreStorage(inspection, *directory)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(output).Encode(result); err != nil {
		return fmt.Errorf("write storage report: %w", err)
	}
	return nil
}
