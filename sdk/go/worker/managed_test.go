// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package worker_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/sdk/go/worker"
	pb "github.com/duravela/djob/sdk/go/worker/protocol/v2"
	"google.golang.org/grpc"
)

func TestManagedProjectedConfigurationIsReadOnlyAndConfinedToItsVolume(t *testing.T) {
	root := t.TempDir()
	version := filepath.Join(root, "..generation-7")
	if err := os.Mkdir(version, 0700); err != nil {
		t.Fatal(err)
	}
	data := []byte(`{"server_url":"https://controller.example","worker_id":"company","tenant_id":"alpha","token":"` + strings.Repeat("s", 32) + `"}`)
	actual := filepath.Join(version, "worker.json")
	if err := os.WriteFile(actual, data, 0440); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(version, filepath.Join(root, "..data")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "worker.json")
	if err := os.Symlink("..data/worker.json", path); err != nil {
		t.Fatal(err)
	}
	t.Setenv(worker.DeploymentIDEnvironment, "line-one")
	t.Setenv(worker.DeploymentGenerationEnvironment, "7")
	t.Setenv(worker.CredentialDirectoryEnvironment, root)
	cfg, err := worker.LoadConfig(path)
	if err != nil || cfg.DeploymentID != "line-one" || cfg.DeploymentGeneration != 7 {
		t.Fatalf("projected configuration: %#v %v", cfg.DeploymentGeneration, err)
	}
	for _, mode := range []os.FileMode{0640, 0444} {
		if err := os.Chmod(actual, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := worker.LoadConfig(path); err == nil {
			t.Fatalf("unsafe projected mode %o accepted", mode)
		}
	}
	if err := os.Chmod(actual, 0400); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "worker.json")
	if err := os.WriteFile(outside, data, 0400); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.LoadConfig(path); err == nil {
		t.Fatal("projected credential escaped its volume")
	}
	t.Setenv(worker.DeploymentIDEnvironment, "")
	t.Setenv(worker.DeploymentGenerationEnvironment, "")
	t.Setenv(worker.CredentialDirectoryEnvironment, "")
	if _, err := worker.LoadConfig(path); err == nil {
		t.Fatal("ordinary configuration followed a symlink")
	}
}

func TestManagedWorkerRequiresControllerAcknowledgementOfItsGeneration(t *testing.T) {
	var polls atomic.Int32
	s := grpcController(t, func(stream grpc.BidiStreamingServer[pb.ClientFrame, pb.ServerFrame]) error {
		frame, err := stream.Recv()
		if err != nil {
			return err
		}
		if frame.GetRegister().DeploymentId != "line-one" || frame.GetRegister().DeploymentGeneration != 7 {
			t.Error("managed identity missing from protobuf registration")
		}
		if err := stream.Send(&pb.ServerFrame{RequestId: frame.RequestId, Payload: &pb.ServerFrame_Registered{Registered: &pb.Registered{Protocol: pb.ProtocolVersion, SessionId: strings.Repeat("a", 64), TenantId: "alpha", HeartbeatEveryNs: int64(time.Second)}}}); err != nil {
			return err
		}
		if _, err := stream.Recv(); err == nil {
			polls.Add(1)
		}
		return nil
	}, false)
	cfg := grpcConfig(t, s, nil)
	cfg.DeploymentID, cfg.DeploymentGeneration = "line-one", 7
	if err := worker.Run(context.Background(), cfg); err == nil || polls.Load() != 0 {
		t.Fatalf("unsupported managed registration started work: polls=%d err=%v", polls.Load(), err)
	}
	cfg.Transport = worker.TransportHTTPSV1
	if err := worker.Run(context.Background(), cfg); err == nil {
		t.Fatal("managed worker used the legacy transport")
	}
}
