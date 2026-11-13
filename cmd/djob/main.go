package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/duravela/djob/internal/buildinfo"
	"github.com/duravela/djob/pkg/base"
	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	mqttdriver "github.com/duravela/djob/pkg/integrations/mqtt"
	"github.com/duravela/djob/pkg/jobs"
	"github.com/duravela/djob/pkg/node"
	"github.com/duravela/djob/pkg/server"
	"github.com/duravela/djob/pkg/workercontrol"
	"go.uber.org/zap"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	output := os.Stderr
	if len(os.Args) > 1 && (os.Args[1] == "init-member" || os.Args[1] == "version" || os.Args[1] == "--version" || os.Args[1] == "compact" || os.Args[1] == "storage-report" || isOperationalCommand(os.Args[1])) {
		output = os.Stdout
	}
	if err := run(ctx, os.Args[1:], output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	if len(args) == 1 && (args[0] == "version" || args[0] == "--version") {
		return buildinfo.Write(output)
	}
	if len(args) > 0 && isOperationalCommand(args[0]) {
		return runOperations(ctx, args[0], args[1:], output)
	}
	if len(args) > 0 && args[0] == "compact" {
		return runCompact(ctx, args[1:], output)
	}
	if len(args) > 0 && args[0] == "storage-report" {
		return runStorageReport(ctx, args[1:], output)
	}
	initializing := len(args) > 0 && args[0] == "init-member"
	if initializing {
		args = args[1:]
	}
	flags := flag.NewFlagSet("djob", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.Usage = func() {
		fmt.Fprintln(output, "Usage: djob [server flags]\n       djob version\n       djob compact --data-dir PATH [--keep-backup] [--timeout 15m]\n       djob storage-report --data-dir PATH [--timeout 2m]\n       djob backup --data-dir PATH --id N --out NEW_PATH\n       djob verify-backup --backup PATH [--timeout 10m]\n       djob member-transfer --data-dir PATH --id N --out NEW_PATH\n       djob restore --backup PATH --data-dir NEW_PATH --old-access-fenced --transfer-never-started\n       djob recover --backup PATH --data-dir NEW_PATH --id N --peers URLS --old-access-fenced\n       djob raft-retain --data-dir PATH --id N [--keep-snapshots 3]\n       djob recovery-release --data-dir PATH --id N --reason TEXT\n       djob clock-reset --data-dir PATH --reason TEXT --reference-time RFC3339")
		flags.PrintDefaults()
		fmt.Fprintln(output, "Use djob init-member with dynamic membership Raft flags to provision a new initial voter before ordinary service startup.")
	}
	id := flags.Uint64("id", 1, "Node ID: one-based index in the ordered peer list")
	dataDir := flags.String("data-dir", "", "Persistent data directory (required; one directory per member)")
	peers := flags.String("peers", "http://127.0.0.1:9021", "Comma-separated ordered Raft peer URLs")
	dynamicMembership := flags.Bool("dynamic-membership", false, "Enable guarded learner-first membership using the peer catalog (new clusters only)")
	initialMembers := flags.String("initial-members", "", "Comma-separated ordered bootstrap voter IDs; remaining peer slots may join later")
	joinMember := flags.Bool("join", false, "Join a quorum-confirmed fresh learner slot; never recreate a lost voter ID")
	clusterID := flags.Uint64("cluster-id", 0x1000, "Raft transport cluster identity; fresh learners must match the existing cluster")
	httpAddr := flags.String("http", "127.0.0.1:8080", "HTTP API listen address")
	grpcAddr := flags.String("grpc", "127.0.0.1:10987", "gRPC API listen address")
	authFile := flags.String("api-auth-file", "", "JSON file binding API bearer tokens to subjects and roles")
	tlsCert := flags.String("api-tls-cert", "", "PEM certificate chain for the HTTP and gRPC APIs")
	tlsKey := flags.String("api-tls-key", "", "PEM private key for the HTTP and gRPC APIs")
	raftCA := flags.String("raft-tls-ca", "", "PEM cluster CA for mutually authenticated Raft peers")
	raftCert := flags.String("raft-tls-cert", "", "PEM member certificate chain for Raft peer TLS")
	raftKey := flags.String("raft-tls-key", "", "PEM owner-only private key for Raft peer TLS")
	demo := flags.Bool("demo", false, "Execute only demo protocol / echo executor jobs without gateway I/O")
	modbusTCP := flags.Bool("modbus-tcp", false, "Enable Modbus TCP gateway I/O (protocol modbus-tcp / executor modbus)")
	modbusRTUTCP := flags.Bool("modbus-rtu-tcp", false, "Enable raw Modbus RTU frames through transparent TCP/serial gateways (protocol modbus-rtu-tcp / executor modbus)")
	genericJobs := flags.Bool("jobs", false, "Execute registered protocol-agnostic sha256, invoke, and request-reply jobs")
	consoleSessions := flags.Bool("console-sessions", false, "Enable shared console session APIs after upgrading all controllers")
	remoteWorkers := flags.Bool("remote-workers", false, "Accept tenant-scoped external workers (requires API credentials and TLS)")
	var workerKubernetes []string
	flags.Func("worker-kubernetes-config", "Private Kubernetes worker-pool configuration; repeat for multiple pools (requires --remote-workers)", func(path string) error {
		if strings.TrimSpace(path) == "" || len(workerKubernetes) >= workercontrol.MaxKubernetesPools {
			return fmt.Errorf("provide 1 to %d nonempty Kubernetes worker-pool configuration paths", workercontrol.MaxKubernetesPools)
		}
		workerKubernetes = append(workerKubernetes, path)
		return nil
	})
	jobsHTTP := flags.Bool("jobs-http", false, "Enable HTTP bindings for generic jobs (requires --jobs)")
	jobsMQTT := flags.Bool("jobs-mqtt", false, "Enable MQTT 5 publish/request-reply bindings (requires --jobs and --mqtt-profiles)")
	mqttProfiles := flags.String("mqtt-profiles", "", "Private JSON file with local MQTT connection profiles and tenant permissions")
	jobOperationTimeout := flags.Duration("jobs-operation-timeout", 10*time.Second, "Maximum duration of a generic job action (1ms..1h)")
	jobConcurrency := flags.Int("jobs-concurrency", 16, "Maximum generic job attempts executing on this process")
	jobsPerTenant := flags.Int("jobs-per-tenant", 1, "Maximum generic job attempts per tenant on this process")
	recoveryTimeout := flags.Duration("gateway-recovery-timeout", 3*time.Second, "Protocol recovery budget, including the gateway turnaround period")
	startupTimeout := flags.Duration("startup-timeout", 15*time.Second, "Startup and leader discovery timeout")
	readinessTimeout := flags.Duration("readiness-timeout", server.DefaultReadinessTimeout, "Quorum readiness timeout (positive, at most 5s)")
	clockSampleInterval := flags.Duration("clock-sample-interval", server.DefaultClockSampleInterval, "Local clock diagnostic sample interval (10ms to 1m)")
	clockJumpThreshold := flags.Duration("clock-jump-threshold", server.DefaultClockJumpThreshold, "Per-sample wall/monotonic discrepancy counted as a clock jump (1ms to 1h)")
	storageSampleInterval := flags.Duration("storage-sample-interval", server.DefaultStorageSampleInterval, "Background storage metric sample interval (100ms to 5m)")
	queueSampleInterval := flags.Duration("queue-sample-interval", server.DefaultQueueSampleInterval, "Background ready-inventory sample interval (100ms to 5m)")
	queueSampleLimit := flags.Int("queue-sample-limit", server.DefaultQueueSampleLimit, "Maximum ready records decoded per execution profile per sample (1 to 65536; also capped at 8 MiB)")
	stateSampleInterval := flags.Duration("state-sample-interval", server.DefaultStateSampleInterval, "Background durable-state inventory sample interval (100ms to 5m)")
	stateSampleLimit := flags.Int("state-sample-limit", server.DefaultStateSampleLimit, "Maximum primary records decoded per state-inventory kind per sample (1 to 65536; also capped at 8 MiB)")
	snapshotBytes := flags.Uint64("raft-snapshot-bytes", 32<<20, "Serialized applied log bytes that trigger a checkpoint")
	catchUpBytes := flags.Uint64("raft-catch-up-bytes", 32<<20, "Maximum serialized checkpointed log bytes retained for peer catch-up")
	uncommittedBytes := flags.Uint64("raft-uncommitted-bytes", 32<<20, "Leader uncommitted proposal byte budget")
	retainedSnapshots := flags.Int("retain-snapshots", 3, "Recoverable disk checkpoints retained with their WAL tail (1..100)")
	minAvailable := flags.Uint64("min-storage-available-bytes", 256<<20, "Hold new work below this filesystem headroom on any member storage component")
	maxDatabase := flags.Int64("max-database-bytes", 0, "Hold new work at this database file length; zero disables the separate ceiling")
	maxSkew := flags.Duration("max-clock-skew", 5*time.Second, "Maximum relative peer-clock uncertainty and future command timestamp tolerance (1ms..1m)")
	shutdownTimeout := flags.Duration("shutdown-timeout", 10*time.Second, "Caller wait for graceful shutdown")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	if *dataDir == "" || *id == 0 {
		return fmt.Errorf("--data-dir and a positive --id are required")
	}
	if *demo && (*modbusTCP || *modbusRTUTCP) {
		return fmt.Errorf("--demo is mutually exclusive with --modbus-tcp and --modbus-rtu-tcp")
	}
	if *recoveryTimeout <= 0 {
		return fmt.Errorf("--gateway-recovery-timeout must be positive")
	}
	if *jobsHTTP && !*genericJobs {
		return fmt.Errorf("--jobs-http requires --jobs")
	}
	if (*jobsMQTT && (!*genericJobs || *mqttProfiles == "")) || (!*jobsMQTT && *mqttProfiles != "") {
		return fmt.Errorf("--jobs-mqtt requires --jobs and --mqtt-profiles; profiles require MQTT execution")
	}
	if *jobOperationTimeout < time.Millisecond || *jobOperationTimeout > time.Hour {
		return fmt.Errorf("--jobs-operation-timeout must be in 1ms..1h")
	}
	if *jobConcurrency < 1 || *jobConcurrency > 1024 || *jobsPerTenant < 1 || *jobsPerTenant > *jobConcurrency {
		return fmt.Errorf("generic job concurrency must be in 1..1024 and per-tenant concurrency must fit within it")
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		return fmt.Errorf("--api-tls-cert and --api-tls-key must be supplied together")
	}
	if *remoteWorkers && (*authFile == "" || *tlsCert == "" || *tlsKey == "") {
		return fmt.Errorf("--remote-workers requires --api-auth-file and API TLS")
	}
	if (*raftCA != "" || *raftCert != "" || *raftKey != "") && (*raftCA == "" || *raftCert == "" || *raftKey == "") {
		return fmt.Errorf("--raft-tls-ca, --raft-tls-cert and --raft-tls-key must be supplied together")
	}
	if *readinessTimeout <= 0 || *readinessTimeout > server.MaxReadinessTimeout {
		return fmt.Errorf("--readiness-timeout must be positive and at most 5s")
	}
	clockConfig := server.ClockMonitorConfig{SampleInterval: *clockSampleInterval, JumpThreshold: *clockJumpThreshold}
	if err := server.ValidateClockMonitorConfig(clockConfig); err != nil {
		return err
	}
	if err := server.ValidateStorageSampleInterval(*storageSampleInterval); err != nil {
		return err
	}
	queueConfig := server.QueueMonitorConfig{SampleInterval: *queueSampleInterval, ScanLimit: *queueSampleLimit}
	if err := server.ValidateQueueMonitorConfig(queueConfig); err != nil {
		return err
	}
	stateConfig := server.StateMonitorConfig{SampleInterval: *stateSampleInterval, ScanLimit: *stateSampleLimit}
	if err := server.ValidateStateMonitorConfig(stateConfig); err != nil {
		return err
	}
	var auth *server.APIAuthConfig
	if *authFile != "" {
		var err error
		auth, err = server.LoadAPIAuthConfig(*authFile)
		if err != nil {
			return err
		}
	}
	var mqttFactory jobs.DriverFactory
	if *jobsMQTT {
		profiles, err := mqttdriver.LoadProfiles(*mqttProfiles)
		if err != nil {
			return err
		}
		mqttFactory, err = mqttdriver.NewFactory(profiles, mqttdriver.Options{OperationTimeout: *jobOperationTimeout})
		if err != nil {
			return err
		}
	}
	logger, err := zap.NewProduction()
	if err != nil {
		return err
	}
	defer func() { _ = logger.Sync() }()
	cfg := server.Config{BaseConfig: server.MakeBaseConfig(), StartupTimeout: *startupTimeout, ShutdownTimeout: *shutdownTimeout, ReadinessTimeout: *readinessTimeout}
	cfg.ClockMonitor = clockConfig
	cfg.StorageSampleInterval = *storageSampleInterval
	cfg.QueueMonitor = queueConfig
	cfg.StateMonitor = stateConfig
	cfg.Admission = server.AdmissionConfig{MinAvailableBytes: *minAvailable, MaxDatabaseBytes: *maxDatabase, MaxClockSkew: *maxSkew}
	if *minAvailable == 0 || *retainedSnapshots < 1 || *retainedSnapshots > 100 {
		return fmt.Errorf("positive storage reserve and 1..100 retained snapshots are required")
	}
	if err := server.ValidateAdmissionConfig(cfg.Admission); err != nil {
		return err
	}
	cfg.ConsoleSessions = *consoleSessions
	if *consoleSessions && (auth == nil || *tlsCert == "") {
		return fmt.Errorf("--console-sessions requires API credentials and TLS")
	}
	cfg.APIAuth = auth
	cfg.RemoteWorkers.Enabled = *remoteWorkers
	if len(workerKubernetes) != 0 {
		if !*remoteWorkers {
			return fmt.Errorf("--worker-kubernetes-config requires --remote-workers")
		}
		configs := make([]workercontrol.KubernetesConfig, 0, len(workerKubernetes))
		for _, path := range workerKubernetes {
			config, err := workercontrol.LoadKubernetesConfig(path)
			if err != nil {
				return err
			}
			configs = append(configs, config)
		}
		runtime, err := workercontrol.NewKubernetesPools(configs)
		if err != nil {
			return err
		}
		defer runtime.Close()
		cfg.WorkerRuntime = runtime
	}
	apiScheme := "http"
	if *tlsCert != "" {
		cfg.APITLS = &server.APITLSConfig{CertFile: *tlsCert, KeyFile: *tlsKey}
		apiScheme = "https"
	}
	cfg.NodeID = id
	cfg.Addr, cfg.HTTPAddr = *grpcAddr, *httpAddr
	cfg.Raft.SetDefaults()
	cfg.Raft.ID = *id
	cfg.Raft.ClusterID = *clusterID
	cfg.Raft.RetainSnapshots = *retainedSnapshots
	cfg.Raft.SnapshotBytes = *snapshotBytes
	cfg.Raft.CatchUpBytes = *catchUpBytes
	cfg.Raft.MaxUncommittedEntriesSize = *uncommittedBytes
	if *snapshotBytes == 0 || *catchUpBytes == 0 || *uncommittedBytes == 0 {
		return fmt.Errorf("Raft byte budgets must be positive")
	}
	if *raftCA != "" {
		cfg.Raft.PeerTLS = &base.RaftTLSConfig{CAFile: *raftCA, CertFile: *raftCert, KeyFile: *raftKey}
	}
	cfg.Raft.DataDir = *dataDir
	cfg.Raft.Logger = logger
	cfg.Raft.Peers = strings.Split(*peers, ",")
	cfg.Raft.DynamicMembership, cfg.Raft.Join = *dynamicMembership, *joinMember
	if *initialMembers != "" {
		for _, raw := range strings.Split(*initialMembers, ",") {
			member, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
			if err != nil || member == 0 {
				return fmt.Errorf("invalid initial member ID")
			}
			cfg.Raft.InitialMembers = append(cfg.Raft.InitialMembers, member)
		}
	}
	for i := range cfg.Raft.Peers {
		cfg.Raft.Peers[i] = strings.TrimSpace(cfg.Raft.Peers[i])
	}
	cfg.Raft.BatchTimeout = 10 * time.Millisecond
	if initializing {
		initCtx, stopInit := context.WithTimeout(ctx, 30*time.Second)
		defer stopInit()
		if err := node.InitializeMember(initCtx, cfg.Raft); err != nil {
			return err
		}
		_, err := fmt.Fprintf(output, "{\"initialized\":true,\"member_id\":\"%d\"}\n", *id)
		return err
	}
	cfg.Materializer.OnError = func(err error) { logger.Error("materialization failed", zap.Error(err)) }
	cfg.Deadlines.OnError = func(err error) { logger.Error("run expiry failed", zap.Error(err)) }
	cfg.Retries.OnError = func(err error) { logger.Error("retry promotion failed", zap.Error(err)) }
	cfg.Ownership.OnError = func(err error) { logger.Error("ownership expiry failed", zap.Error(err)) }
	cfg.JobRetention.OnError = func(err error) { logger.Error("job retention failed", zap.Error(err)) }
	cfg.JobTriggers.OnError = func(err error) { logger.Error("generic trigger materialization failed", zap.Error(err)) }
	cfg.JobMaintenance.OnError = func(err error) { logger.Error("generic job maintenance failed", zap.Error(err)) }
	cfg.JobDispatch.OnError = func(err error) { logger.Error("generic job dispatch failed", zap.Error(err)) }
	cfg.JobDispatch.MaxConcurrent, cfg.JobDispatch.MaxPerTenant = *jobConcurrency, *jobsPerTenant
	cfg.JobWorker.OperationTimeout = *jobOperationTimeout
	cfg.Dispatcher.OnError = func(err error) { logger.Error("dispatch failed", zap.Error(err)) }
	cfg.Dispatcher.Executor.RecoveryTimeout = *recoveryTimeout
	if *demo {
		cfg.TransportFactory = demoFactory
	} else if *modbusTCP || *modbusRTUTCP {
		cfg.TransportFactory = modbusFactory(*modbusTCP, *modbusRTUTCP)
	}
	if *genericJobs {
		cfg.JobRegistry = jobRegistry(*jobsHTTP, *modbusTCP, *modbusRTUTCP, *recoveryTimeout, mqttFactory)
	}
	s, err := server.Start(ctx, cfg)
	if err != nil {
		return err
	}
	fmt.Fprintf(output, "djob node %d ready: HTTP %s://%s, gRPC %s, auth=%t, demo=%t, modbus-tcp=%t, modbus-rtu-tcp=%t, jobs=%t, jobs-http=%t, jobs-mqtt=%t\n", *id, apiScheme, s.HTTPAddr(), s.RPCAddr(), auth != nil, *demo, *modbusTCP, *modbusRTUTCP, *genericJobs, *jobsHTTP, *jobsMQTT)
	var runErr error
	select {
	case <-ctx.Done():
	case request := <-s.ShutdownRequested():
		runErr = request.Err
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	return errors.Join(runErr, s.Close(shutdownCtx))
}

func demoFactory(channel db.CommunicationChannel) (executor.Transport, error) {
	if channel.Protocol != "demo" {
		return nil, fmt.Errorf("demo mode only supports protocol demo")
	}
	return demoTransport{}, nil
}

type demoTransport struct{}

func (demoTransport) Exchange(ctx context.Context, request executor.Request) (executor.Outcome, error) {
	if err := ctx.Err(); err != nil {
		return executor.Outcome{Status: db.AttemptFailed}, err
	}
	if request.Executor != "echo" {
		return executor.Outcome{Status: db.AttemptFailed}, fmt.Errorf("demo mode only supports executor echo")
	}
	return executor.Outcome{Status: db.AttemptSucceeded, Data: bytes.Clone(request.Payload)}, nil
}
func (demoTransport) Recover(ctx context.Context) error { return ctx.Err() }
func (demoTransport) Close(ctx context.Context) error   { return ctx.Err() }
