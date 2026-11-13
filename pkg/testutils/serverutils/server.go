// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package serverutils

import (
	"context"
	"fmt"
	"github.com/duravela/djob/pkg/base"
	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/duravela/djob/pkg/rpc"
	"github.com/duravela/djob/pkg/util/log"
	"github.com/duravela/djob/pkg/util/stop"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"io"
	"net/http"
	"net/url"
	"sync"
)

type TestLogger interface {
	Helper()
	Log(...any)
	Logf(string, ...any)
}
type TestFataler interface {
	TestLogger
	Fatal(...any)
	Fatalf(string, ...any)
	Errorf(string, ...any)
	FailNow()
}
type TestServerController interface {
	Start(context.Context) error
	PreStart(context.Context) error
	Activate(context.Context) error
	Stop(context.Context)
	SetReadyFn(func(bool))
	Stopper() *stop.Stopper
}
type ApplicationLayerInterface interface {
	Readiness(context.Context) error
	HTTPAddr() string
	RPCAddr() string
	NodeDialer() any
	RPCContext() *rpc.Context
	NewClientRPCContext(context.Context) *rpc.Context
	RPCClientConn(TestFataler) RPCConn
	RPCClientConnE() (RPCConn, error)
	GetSchedulerClient(TestFataler) pb.SchedulerClient
	AmbientCtx() log.AmbientContext
	SchedulerURL() *TestURL
	GetUnauthenticatedHTTPClient() (http.Client, error)
	GetUnauthenticatedHTTPClientWithTransport() (http.Client, *http.Transport, error)
	GetSchedulerHTTPClient() (http.Client, error)
	AppStopper() *stop.Stopper
}
type TestServerInterfaceRaw interface {
	TestServerController
	ApplicationLayerInterface
}
type TestServerInterface interface {
	TestServerController
	ApplicationLayer() ApplicationLayerInterface
}
type TestServerFactory interface {
	New(base.TestServerArgs) (any, error)
}

var factory struct {
	sync.RWMutex
	value TestServerFactory
}

func InitTestServerFactory(f TestServerFactory) {
	factory.Lock()
	defer factory.Unlock()
	factory.value = f
}

type adapter struct{ TestServerInterfaceRaw }

func (a adapter) ApplicationLayer() ApplicationLayerInterface { return a.TestServerInterfaceRaw }
func NewServer(params base.TestServerArgs) (TestServerInterface, error) {
	factory.RLock()
	f := factory.value
	factory.RUnlock()
	if f == nil {
		return nil, fmt.Errorf("test server factory is not configured")
	}
	value, err := f.New(params)
	if err != nil {
		return nil, err
	}
	s, ok := value.(TestServerInterfaceRaw)
	if !ok {
		return nil, fmt.Errorf("test server factory returned incompatible server %T", value)
	}
	return adapter{s}, nil
}
func StartServerOnlyE(t TestLogger, params base.TestServerArgs) (TestServerInterface, error) {
	s, err := NewServer(params)
	if err != nil {
		return nil, err
	}
	if err = s.Start(context.Background()); err != nil {
		s.Stop(context.Background())
		return nil, err
	}
	return s, nil
}
func StartServerOnly(t TestFataler, p base.TestServerArgs) TestServerInterface {
	t.Helper()
	s, err := StartServerOnlyE(t, p)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func StartServer(t TestFataler, p base.TestServerArgs) TestServerInterface {
	return StartServerOnly(t, p)
}

type RPCConn interface{ NewSchedulerClient() pb.SchedulerClient }
type client struct{ connection grpc.ClientConnInterface }

func FromGRPCConn(c *grpc.ClientConn) RPCConn           { return client{c} }
func (c client) NewSchedulerClient() pb.SchedulerClient { return pb.NewSchedulerClient(c.connection) }

type TestURL struct{ *url.URL }

func NewTestURL(value string) TestURL {
	u, err := url.Parse(value)
	if err != nil {
		panic(err)
	}
	return TestURL{u}
}
func (t *TestURL) WithPath(value string) *TestURL {
	ref, err := url.Parse(value)
	if err != nil {
		panic(err)
	}
	copy := *t.URL
	copy.Path = copy.JoinPath(ref.Path).Path
	query := copy.Query()
	for k, v := range ref.Query() {
		query[k] = v
	}
	copy.RawQuery = query.Encode()
	return &TestURL{&copy}
}
func GetJSONProtoWithSchedulerOption(s ApplicationLayerInterface, path string, result proto.Message) error {
	c, err := s.GetUnauthenticatedHTTPClient()
	if err != nil {
		return err
	}
	response, err := c.Get(s.SchedulerURL().WithPath(path).String())
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP response status %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return err
	}
	return protojson.Unmarshal(data, result)
}
