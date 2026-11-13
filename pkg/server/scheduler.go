// Portions Copyright 2014 The Cockroach Authors.
// Service registration scaffolding adapted from CockroachDB 23.1.0 admin.go;
// see licenses/Apache-2.0.txt and licenses/CockroachDB-23.1-BSL.txt.
// Modified for djob; scheduler behavior is governed by the root LICENSE.

package server

import (
	"context"
	"net/http"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/duravela/djob/pkg/rpc"
	"github.com/duravela/djob/pkg/util/log"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
)

type schedulerServer struct {
	scheduler.UnimplementedSchedulerServer
	log.AmbientContext

	rpcContext *rpc.Context
	grpc       *grpcServer

	server  *topLevelServer
	backend SchedulerBackend
	auth    *apiAuthorizer
}

// SchedulerBackend combines committed mutations with local inspection queries.
// Reads do not grant execution authority and may lag another replica.
type SchedulerBackend interface {
	ProposeStateCommand(context.Context, db.StateCommand) (db.StateResult, error)
	GetChannel(string) (*db.CommunicationChannel, error)
	GetSchedule(string) (*db.Schedule, error)
	GetRun(string) (*db.Run, error)
	GetAttempt(string) (*db.Attempt, error)
	GetResolution(string) (*db.Resolution, error)
	ListChannels([]byte, int) (db.ChannelPage, error)
	ListSchedules([]byte, int) (db.SchedulePage, error)
	ListRuns([]byte, int) (db.RunPage, error)
}

// NewSchedulerService can also register this API on an embedding gRPC server.
func NewSchedulerService(backend SchedulerBackend) scheduler.SchedulerServer {
	return &schedulerServer{backend: backend}
}

// NewSchedulerServiceWithAuth enforces credentials inside the service methods,
// including direct calls and grpc-gateway's in-process handlers. The embedding
// application owns TLS and must capture HTTP Authorization with the provided
// SchedulerHTTPHandler when serving an in-process JSON gateway.
func NewSchedulerServiceWithAuth(backend SchedulerBackend, cfg APIAuthConfig) (scheduler.SchedulerServer, error) {
	auth, err := newAPIAuthorizer(&cfg)
	if err != nil {
		return nil, err
	}
	return &schedulerServer{backend: backend, auth: auth}, nil
}

// SchedulerHTTPHandler preserves the HTTP credential source for an embedded
// in-process gateway. Authentication itself remains in the service methods.
func SchedulerHTTPHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, captureHTTPAuthorization(r))
	})
}

func newSchedulerServer(
	ambient log.AmbientContext,
	rpcCtx *rpc.Context,
	grpc *grpcServer,
	s *topLevelServer,
) *schedulerServer {
	server := &schedulerServer{
		AmbientContext: ambient,
		rpcContext:     rpcCtx,
		grpc:           grpc,
		server:         s,
	}
	return server
}

// RegisterService registers the GRPC service.
func (s *schedulerServer) RegisterService(g *grpc.Server) {
	scheduler.RegisterSchedulerServer(g, s)
}

// RegisterGateway starts the gateway (i.e. reverse proxy) that proxies HTTP requests
// to the appropriate gRPC endpoints.
func (s *schedulerServer) RegisterGateway(
	ctx context.Context, mux *gwruntime.ServeMux, conn *grpc.ClientConn,
) error {
	// Register the endpoints defined in the proto.
	return scheduler.RegisterSchedulerHandler(ctx, mux, conn)
}
