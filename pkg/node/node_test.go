package node

import (
	"testing"

	"github.com/duravela/djob/pkg/base"
	"github.com/duravela/djob/pkg/node/config"
	"go.uber.org/zap/zaptest"
)

func newTestConfig(t *testing.T) *config.Config {
	cfg := config.Config{
		BaseConfig: config.BaseConfig{
			Logger: zaptest.NewLogger(t),
		},
		NodeConfig: config.NodeConfig{
			RaftConfig: base.RaftConfig{
				Peers: []string{"http://127.0.0.1:0"},
			},
			DataDir: t.TempDir(),
		},
	}

	cfg.WithDefaults()
	cfg.Server.Port = 0
	return &cfg
}

func Test_NewWorkerNode(t *testing.T) {
	workerNode, err := NewNode(*newTestConfig(t))
	if err != nil {
		t.Fatalf("Failed to create worker node: %v", err)
	}
	t.Cleanup(workerNode.Close)

	// Ensure the worker node is initialized correctly
	if workerNode == nil {
		t.Fatal("Worker node is nil")
	}
}

func Test_WorkerNode(t *testing.T) {
	node, err := NewNode(*newTestConfig(t))
	if err != nil {
		t.Fatalf("Failed to create worker node: %v", err)
	}
	t.Cleanup(node.Close)

	if err = node.service.Open(); err != nil {
		t.Fatalf("Failed to start the service: %v", err)
	}
	defer node.service.Close()
}
