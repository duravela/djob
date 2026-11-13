package testutil

import (
	"testing"

	"github.com/duravela/djob/pkg/node/config"
)

type Config struct {
	config.Config

	SnapshotTriggeredC <-chan struct{}
}

func NewTestConfig(t *testing.T, customizerFns ...func(c *Config)) *Config {
	snapshotFn, snapshotTriggeredC := (func() (func(uint64) ([]byte, error), <-chan struct{}) {
		snapshotTriggeredC := make(chan struct{})
		return func(uint64) ([]byte, error) {
			select {
			case snapshotTriggeredC <- struct{}{}:
			default:
			}
			return nil, nil
		}, snapshotTriggeredC
	})()

	cfg := Config{}
	cfg.DataDir = t.TempDir()
	cfg.SetDefaults()
	cfg.SnapshotConfig.GetSnapshot = snapshotFn
	cfg.SnapshotTriggeredC = snapshotTriggeredC
	for i := range customizerFns {
		customizerFns[i](&cfg)
	}

	return &cfg
}
