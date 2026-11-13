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

	"github.com/duravela/djob/internal/buildinfo"
	httpdriver "github.com/duravela/djob/pkg/integrations/http"
	modbusdriver "github.com/duravela/djob/pkg/integrations/modbus"
	mqttdriver "github.com/duravela/djob/pkg/integrations/mqtt"
	"github.com/duravela/djob/sdk/go"
	"github.com/duravela/djob/sdk/go/handlers"
	"github.com/duravela/djob/sdk/go/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := serve(ctx, os.Args[1:]); err != nil && (errors.Is(err, worker.ErrCleanupFailed) || !errors.Is(err, context.Canceled)) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func serve(ctx context.Context, args []string) error {
	if len(args) == 1 && (args[0] == "version" || args[0] == "--version") {
		return buildinfo.Write(os.Stdout)
	}
	flags := flag.NewFlagSet("djob-worker", flag.ContinueOnError)
	config := flags.String("worker-config", "", "Private worker credential/TLS configuration (required)")
	httpEnabled := flags.Bool("jobs-http", false, "Enable resource-free HTTP bindings")
	mqttProfiles := flags.String("mqtt-profiles", "", "Enable MQTT using private local connection profiles")
	tcp := flags.Bool("modbus-tcp", false, "Enable controller-authorized Modbus TCP bindings")
	rtu := flags.Bool("modbus-rtu-tcp", false, "Enable controller-authorized transparent RTU-over-TCP bindings")
	recovery := flags.Duration("recovery-timeout", 3*time.Second, "Cooperative driver recovery/close timeout")
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
	if *config == "" || flags.NArg() != 0 || *recovery <= 0 || *drain <= 0 {
		return fmt.Errorf("a worker configuration and positive recovery/drain timeouts are required")
	}
	cfg, err := worker.LoadConfig(*config)
	if err != nil {
		return err
	}
	cfg.CleanupTimeout, cfg.DrainTimeout = *recovery, *drain
	cfg.Registry = handlers.Builtins()
	if *httpEnabled {
		cfg.Registry.Drivers[djob.Version{Name: "http", Version: "1"}] = httpdriver.NewFactory(nil)
	}
	if *mqttProfiles != "" {
		profiles, err := mqttdriver.LoadProfiles(*mqttProfiles)
		if err != nil {
			return err
		}
		factory, err := mqttdriver.NewFactory(profiles, mqttdriver.Options{})
		if err != nil {
			return err
		}
		cfg.Registry.Drivers[djob.Version{Name: mqttdriver.Name, Version: mqttdriver.Version}] = factory
	}
	for protocol, enabled := range map[string]bool{"modbus-tcp": *tcp, "modbus-rtu-tcp": *rtu} {
		if !enabled {
			continue
		}
		factory, err := modbusdriver.NewFactory(protocol, *recovery)
		if err != nil {
			return err
		}
		cfg.Registry.Drivers[djob.Version{Name: protocol, Version: modbusdriver.Version}] = factory
	}
	if !*reconnect {
		return worker.Run(ctx, cfg)
	}
	return worker.Serve(ctx, cfg, worker.ReconnectConfig{MinBackoff: *minBackoff, MaxBackoff: *maxBackoff, OnRetry: func(o worker.RetryObservation) {
		fmt.Fprintf(os.Stderr, "worker reconnect attempt %d in %s (controller %d)\n", o.Attempt, o.Delay, o.ControllerIndex)
	}})
}
