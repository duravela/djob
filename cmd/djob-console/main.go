package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/duravela/djob/internal/buildinfo"
	"github.com/duravela/djob/internal/console"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	if len(args) == 1 && (args[0] == "version" || args[0] == "--version") {
		return buildinfo.Write(output)
	}
	flags := flag.NewFlagSet("djob-console", flag.ContinueOnError)
	flags.SetOutput(output)
	listen := flags.String("listen", "127.0.0.1:8090", "Console listen address; non-loopback listeners require TLS")
	origin := flags.String("public-url", "http://127.0.0.1:8090", "Exact browser origin; reverse proxies must preserve Host")
	controllers := flags.String("controllers", "http://127.0.0.1:8080", "Comma-separated trusted controller HTTP API origins, in failover order")
	caFile := flags.String("controller-tls-ca", "", "Additional PEM CA certificates for controller APIs")
	serverName := flags.String("controller-tls-server-name", "", "Verified controller TLS server name; defaults to each controller URL hostname")
	certFile := flags.String("tls-cert", "", "Console listener certificate")
	keyFile := flags.String("tls-key", "", "Console listener private key")
	formsFile := flags.String("forms-file", "", "Operator-controlled tenant company form profiles")
	oidcFile := flags.String("oidc-config", "", "Owner-only confidential OIDC client configuration file")
	shared := flags.Bool("shared-sessions", false, "Keep encrypted browser sessions in explicitly activated controller storage")
	drainDelay := flags.Duration("drain-delay", 5*time.Second, "Time between readiness removal and listener shutdown (0..30s)")
	shutdownTimeout := flags.Duration("shutdown-timeout", 30*time.Second, "Grace for active requests after draining (1s..1m)")
	ttl := flags.Duration("session-ttl", 8*time.Hour, "Absolute browser session lifetime, 1m..24h")
	capacity := flags.Int("max-sessions", 128, "Maximum in-memory browser sessions, 1..1024")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	if *drainDelay < 0 || *drainDelay > 30*time.Second || *shutdownTimeout < time.Second || *shutdownTimeout > time.Minute {
		return fmt.Errorf("drain delay must be 0..30s and shutdown timeout 1s..1m")
	}
	if (*certFile == "") != (*keyFile == "") {
		return fmt.Errorf("--tls-cert and --tls-key must be supplied together")
	}
	if err := console.ValidateListener(*listen, *origin, *certFile != ""); err != nil {
		return err
	}
	trust := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: *serverName}
	if *caFile != "" {
		pem, err := os.ReadFile(*caFile)
		if err != nil {
			return fmt.Errorf("read controller CA: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return fmt.Errorf("controller CA file contains no certificates")
		}
		trust.RootCAs = roots
	}
	var origins []string
	for _, raw := range strings.Split(*controllers, ",") {
		origins = append(origins, strings.TrimSpace(raw))
	}
	var forms []console.CompanyForm
	if *formsFile != "" {
		var err error
		forms, err = console.LoadCompanyForms(*formsFile)
		if err != nil {
			return err
		}
	}
	var oidcConfig *console.OIDCConfig
	if *oidcFile != "" {
		var err error
		oidcConfig, err = console.LoadOIDCConfig(*oidcFile)
		if err != nil {
			return err
		}
	}
	app, err := console.New(console.Config{CompanyForms: forms, OIDC: oidcConfig, Origin: *origin, Controllers: origins, TLS: trust, SessionTTL: *ttl, MaxSessions: *capacity, SharedSessions: *shared})
	if err != nil {
		return err
	}
	defer app.Close()
	server := &http.Server{Handler: app, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: time.Minute, MaxHeaderBytes: 16 << 10, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	if *certFile != "" {
		cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			return fmt.Errorf("load console TLS certificate: %w", err)
		}
		server.TLSConfig.Certificates = []tls.Certificate{cert}
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	done := make(chan error, 1)
	go func() {
		if *certFile != "" {
			done <- server.ServeTLS(listener, "", "")
		} else {
			done <- server.Serve(listener)
		}
	}()
	fmt.Fprintf(output, "djob console: %s (%d controller origins)\n", *origin, len(origins))
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		app.BeginDrain()
		if *drainDelay > 0 {
			timer := time.NewTimer(*drainDelay)
			<-timer.C
		}
		shutdown, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return err
		}
		if err := <-done; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}
