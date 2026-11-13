// Company-owned composition root. No Raft member or local database is started.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	companyworker "example.com/acme/djob-worker"
	"github.com/duravela/djob/sdk/go/worker"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := serve(ctx, os.Args[1:]); err != nil && (errors.Is(err, worker.ErrCleanupFailed) || !errors.Is(err, context.Canceled)) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func serve(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("company-worker", flag.ContinueOnError)
	path := flags.String("worker-config", "", "Private worker credential/TLS configuration (required)")
	recovery := flags.Duration("recovery-timeout", 3*time.Second, "Driver cleanup timeout")
	drain := flags.Duration("drain-timeout", 30*time.Second, "Grace to finish the current conversation on shutdown")
	reconnect := flags.Bool("reconnect", true, "Establish fresh sessions after transient failures and safe cleanup")
	minBackoff := flags.Duration("reconnect-min-backoff", 500*time.Millisecond, "Initial reconnect backoff (at least 100ms)")
	maxBackoff := flags.Duration("reconnect-max-backoff", 30*time.Second, "Maximum reconnect backoff (at most 1m)")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *path == "" || flags.NArg() != 0 {
		return fmt.Errorf("--worker-config is required")
	}
	cfg, err := worker.LoadConfig(*path)
	if err != nil {
		return err
	}
	cfg.CleanupTimeout, cfg.DrainTimeout = *recovery, *drain
	cfg.Registry = companyworker.Registry(*recovery)
	if !*reconnect {
		return worker.Run(ctx, cfg)
	}
	return worker.Serve(ctx, cfg, worker.ReconnectConfig{MinBackoff: *minBackoff, MaxBackoff: *maxBackoff, OnRetry: func(o worker.RetryObservation) {
		fmt.Fprintf(os.Stderr, "worker reconnect attempt %d in %s (controller %d)\n", o.Attempt, o.Delay, o.ControllerIndex)
	}})
}
