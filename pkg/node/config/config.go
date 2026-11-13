package config

import (
	"fmt"
	"os"
	"time"

	"github.com/duravela/djob/pkg/base"
	"go.uber.org/zap"
)

const (
	defaultBaseDir = ".djob"
)

var DefaultBaseDirectory = os.ExpandEnv(fmt.Sprintf("%s/%s", os.Getenv("HOME"), defaultBaseDir))

// Config holds the parameters needed to set up a node.
type Config struct {
	BaseConfig
	NodeConfig
	StoreConfig
}

type BaseConfig struct {
	Version string // Version of the application
	Logger  *zap.Logger
}

// NodeConfig holds the parameters that (together with a BaseConfig) allow setting
// up a node.
type NodeConfig struct {
	base.RaftConfig
	Server struct {
		Port int
	}

	DataDir string // Directory for node data
}

func (cfg *BaseConfig) SetDefaults() {
	if cfg.Version == "" {
		cfg.Version = "1.0.0"
	}

	if cfg.Logger == nil {
		cfg.Logger = zap.NewExample()
	}
}

func (cfg *Config) WithDefaults() *Config {
	cfg.SetDefaults()
	return cfg
}

func (cfg *Config) Customize(customizeFn func(c *Config)) *Config {
	customizeFn(cfg)
	return cfg
}

func (cfg *Config) SetDefaults() {
	cfg.BaseConfig.SetDefaults()
	cfg.NodeConfig.SetDefaults()
}

func (cfg *NodeConfig) SetDefaults() {
	cfg.RaftConfig.SetDefaults()
	cfg.SnapshotConfig.SetDefaults()

	if cfg.DataDir == "" {
		cfg.DataDir = DefaultBaseDirectory
	}

	if cfg.Server.Port == 0 {
		cfg.Server.Port = 8080
	}
}

type StoreConfig struct {

	// ProposalTimeout specifies the maximum duration to wait for a Raft proposal
	// (such as adding or updating a job) to be committed and acknowledged.
	// If the proposal is not completed within this timeout, it is considered failed,
	// and an error is returned to the caller. This helps prevent indefinite blocking
	// when Raft is unavailable or overloaded.
	ProposalTimeout time.Duration
}

func (cfg *StoreConfig) SetDefaults() {
	cfg.ProposalTimeout = 30 * time.Second
}
