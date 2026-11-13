// Portions Copyright 2015 The Cockroach Authors.
// Older configuration/server interfaces derive from CockroachDB 23.1.0.
// The applicable Apache-2.0 grant and original conversion terms are recorded in
// licenses/Apache-2.0.txt and licenses/CockroachDB-23.1-BSL.txt.
// Modified for djob; new engine implementation is governed by the root LICENSE.

package server

import (
	"context"
	"time"

	"github.com/duravela/djob/pkg/base"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/jobs"
	nodeconfig "github.com/duravela/djob/pkg/node/config"
	"github.com/duravela/djob/pkg/scheduler"
	"github.com/duravela/djob/pkg/util/log"
	"github.com/duravela/djob/pkg/workercontrol"
)

type BaseConfig struct {
	*base.Config

	NodeID *uint64

	AmbientCtx log.AmbientContext
	ReadyFn    func(waitForInit bool) // Called after listeners start.

}

type Config struct {
	BaseConfig
	Raft             nodeconfig.Config
	TransportFactory executor.Factory // Nil runs a controller without gateway I/O.
	Materializer     scheduler.MaterializerConfig
	Deadlines        scheduler.DeadlineSweeperConfig
	Retries          scheduler.RetryPromoterConfig
	Ownership        scheduler.OwnershipSweeperConfig
	JobMaintenance   scheduler.JobMaintenanceConfig
	JobTriggers      scheduler.MaterializerConfig
	JobRetention     scheduler.JobRetentionConfig
	// JobRegistry explicitly selects the company handlers and protocol drivers
	// available to this member. Supply company code here rather than adding it
	// to the replicated state machine. Nil disables generic job execution.
	JobRegistry           func(JobBackend) (jobs.Registry, error)
	JobWorker             jobs.Config
	JobDispatch           jobs.DispatchConfig
	ConsoleSessions       bool // Enables explicitly activated shared console session APIs.
	RemoteWorkers         RemoteWorkerConfig
	WorkerRuntime         workercontrol.Runtime // Optional process deployment backend; jobs remain protocol-agnostic.
	Dispatcher            scheduler.DispatcherConfig
	StartupTimeout        time.Duration
	ShutdownTimeout       time.Duration
	ReadinessTimeout      time.Duration // Zero defaults to 2s; maximum 5s.
	ClockMonitor          ClockMonitorConfig
	Admission             AdmissionConfig
	QueueMonitor          QueueMonitorConfig
	StateMonitor          StateMonitorConfig
	StorageSampleInterval time.Duration  // Zero defaults to 10s; accepts 100ms through 5m.
	APIAuth               *APIAuthConfig // Nil permits anonymous ordinary APIs; recovery still requires an operator.
	APITLS                *APITLSConfig
}

type JobBackend interface {
	jobs.Backend
	jobs.GatewayBackend
}

// InitNode is retained for callers that initialize configuration before NewServer.
func (cfg *Config) InitNode(ctx context.Context) error { return nil }

func MakeBaseConfig() BaseConfig {
	baseCfg := BaseConfig{Config: new(base.Config)}
	baseCfg.SetDefaults()

	return baseCfg
}

func (cfg *BaseConfig) SetDefaults() {
	c := cfg.Config
	if c == nil {
		c = new(base.Config)
	}
	c.InitDefaults()
	*cfg = BaseConfig{Config: c}
}
