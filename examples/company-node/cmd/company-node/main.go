// Company-owned composition root: the reusable engine is imported, not forked.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	companynode "example.com/acme/djob-node"
	"github.com/duravela/djob/pkg/base"
	"github.com/duravela/djob/pkg/server"
	"go.uber.org/zap"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := serve(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func serve(ctx context.Context) error {
	id := flag.Uint64("id", 1, "One-based member ID in the ordered peer list")
	dir := flag.String("data-dir", "", "Persistent member directory (required)")
	peers := flag.String("peers", "http://127.0.0.1:9021", "Ordered peer URLs")
	httpAddr := flag.String("http", "127.0.0.1:8080", "HTTP API address")
	grpcAddr := flag.String("grpc", "127.0.0.1:10987", "gRPC API address")
	auth := flag.String("api-auth-file", "", "Company API credentials (required)")
	cert := flag.String("api-tls-cert", "", "API certificate")
	key := flag.String("api-tls-key", "", "API private key")
	ca := flag.String("raft-tls-ca", "", "Raft CA")
	peerCert := flag.String("raft-tls-cert", "", "Raft certificate")
	peerKey := flag.String("raft-tls-key", "", "Raft private key")
	flag.Parse()
	if *dir == "" || *auth == "" || *id == 0 || flag.NArg() != 0 {
		return fmt.Errorf("--data-dir, --api-auth-file and a positive --id are required")
	}
	credentials, err := server.LoadAPIAuthConfig(*auth)
	if err != nil {
		return err
	}
	logger, err := zap.NewProduction()
	if err != nil {
		return err
	}
	defer func() { _ = logger.Sync() }()
	cfg := server.Config{BaseConfig: server.MakeBaseConfig(), APIAuth: credentials, JobRegistry: companynode.Registry, StartupTimeout: time.Minute, ShutdownTimeout: 10 * time.Second}
	cfg.NodeID, cfg.Addr, cfg.HTTPAddr = id, *grpcAddr, *httpAddr
	cfg.Raft.SetDefaults()
	cfg.Raft.ID, cfg.Raft.DataDir, cfg.Raft.Logger = *id, *dir, logger
	cfg.Raft.Peers = strings.Split(*peers, ",")
	if *cert != "" || *key != "" {
		cfg.APITLS = &server.APITLSConfig{CertFile: *cert, KeyFile: *key}
	}
	if *ca != "" || *peerCert != "" || *peerKey != "" {
		cfg.Raft.PeerTLS = &base.RaftTLSConfig{CAFile: *ca, CertFile: *peerCert, KeyFile: *peerKey}
	}
	cfg.JobDispatch.MaxConcurrent, cfg.JobDispatch.MaxPerTenant = 4, 2
	s, err := server.Start(ctx, cfg)
	if err != nil {
		return err
	}
	var cause error
	select {
	case <-ctx.Done():
	case request := <-s.ShutdownRequested():
		cause = request.Err
	}
	closing, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return errors.Join(cause, s.Close(closing))
}
