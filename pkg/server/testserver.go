// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"fmt"
	"github.com/duravela/djob/pkg/base"
	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/duravela/djob/pkg/rpc"
	"github.com/duravela/djob/pkg/testutils/serverutils"
	"github.com/duravela/djob/pkg/util/log"
	"github.com/duravela/djob/pkg/util/stop"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"time"
)

type testServerFactoryImpl struct{}

var TestServerFactory = testServerFactoryImpl{}

type testServer struct {
	*topLevelServer
	activate sync.Once
	ready    func(bool)
}

func (testServerFactoryImpl) New(p base.TestServerArgs) (any, error) {
	cfg := Config{BaseConfig: MakeBaseConfig()}
	cfg.Addr = "127.0.0.1:0"
	cfg.HTTPAddr = "127.0.0.1:0"
	if p.Addr != "" {
		cfg.Addr = p.Addr
	}
	if p.HTTPAddr != "" {
		cfg.HTTPAddr = p.HTTPAddr
	}
	s := p.Stopper
	if s == nil {
		s = stop.NewStopper()
	}
	dir, err := os.MkdirTemp("", "djob-test-")
	if err != nil {
		return nil, err
	}
	s.AddCloser(stop.CloserFn(func() { _ = os.RemoveAll(dir) }))
	cfg.Raft.DataDir = dir
	cfg.Raft.SetDefaults()
	cfg.Raft.Peers = []string{"http://127.0.0.1:0"}
	cfg.Raft.BatchTimeout = time.Millisecond
	cfg.Raft.RaftEventLoopHeartbeatInterval = 10 * time.Millisecond
	if err = cfg.InitNode(context.Background()); err != nil {
		s.Stop(context.Background())
		return nil, err
	}
	service, err := NewServer(cfg, s)
	if err != nil {
		s.Stop(context.Background())
		return nil, err
	}
	return &testServer{topLevelServer: service.(*topLevelServer)}, nil
}
func (t *testServer) Stopper() *stop.Stopper    { return t.stopper }
func (t *testServer) AppStopper() *stop.Stopper { return t.stopper }
func (t *testServer) Stop(ctx context.Context)  { t.stopper.Stop(ctx) }
func (t *testServer) Start(ctx context.Context) error {
	if err := t.PreStart(ctx); err != nil {
		t.Stop(ctx)
		return err
	}
	return t.Activate(ctx)
}
func (t *testServer) Activate(ctx context.Context) error {
	var err error
	t.activate.Do(func() {
		err = t.stopper.RunAsyncTask(ctx, "test-server-shutdown", func(context.Context) {
			select {
			case <-t.ShutdownRequested():
				go t.Stop(context.Background())
			case <-t.stopper.ShouldQuiesce():
			}
		})
		if err == nil && t.ready != nil {
			t.ready(false)
		}
	})
	return err
}
func (t *testServer) SetReadyFn(f func(bool)) { t.ready = f }
func (t *testServer) Readiness(ctx context.Context) error {
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil).WithContext(ctx)
	t.readiness(response, request)
	if response.Code != http.StatusOK {
		return fmt.Errorf("server not ready: %s", response.Body.String())
	}
	return nil
}
func (t *testServer) NodeDialer() any          { return nil }
func (t *testServer) RPCContext() *rpc.Context { return t.rpcContext }
func (t *testServer) NewClientRPCContext(ctx context.Context) *rpc.Context {
	c, s := rpc.NewClientContext(ctx, rpc.MakeClientConnConfigFromBaseConfig(*t.cfg.Config))
	t.stopper.AddCloser(stop.CloserFn(func() { s.Stop(context.Background()) }))
	return c
}
func (t *testServer) RPCClientConnE() (serverutils.RPCConn, error) {
	c, err := t.NewClientRPCContext(context.Background()).GRPCDialNode(t.RPCAddr(), t.NodeID()).Connect(context.Background())
	if err != nil {
		return nil, err
	}
	return serverutils.FromGRPCConn(c), nil
}
func (t *testServer) RPCClientConn(test serverutils.TestFataler) serverutils.RPCConn {
	test.Helper()
	c, err := t.RPCClientConnE()
	if err != nil {
		test.Fatal(err)
	}
	return c
}
func (t *testServer) GetSchedulerClient(test serverutils.TestFataler) pb.SchedulerClient {
	return t.RPCClientConn(test).NewSchedulerClient()
}
func (t *testServer) AmbientCtx() log.AmbientContext { return t.cfg.AmbientCtx }
func (t *testServer) SchedulerURL() *serverutils.TestURL {
	return &serverutils.TestURL{URL: t.cfg.SchedulerURL()}
}
func (t *testServer) GetSchedulerHTTPClient() (http.Client, error) {
	return http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone(), Timeout: 30 * time.Second}, nil
}
func (t *testServer) GetUnauthenticatedHTTPClient() (http.Client, error) {
	return t.GetSchedulerHTTPClient()
}
func (t *testServer) GetUnauthenticatedHTTPClientWithTransport() (http.Client, *http.Transport, error) {
	c, err := t.GetSchedulerHTTPClient()
	return c, c.Transport.(*http.Transport), err
}
