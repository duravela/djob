// Portions Copyright 2014 The Cockroach Authors.
// Older configuration/server interfaces derive from CockroachDB 23.1.0.
// The applicable Apache-2.0 grant and original conversion terms are recorded in
// licenses/Apache-2.0.txt and licenses/CockroachDB-23.1-BSL.txt.
// Modified for djob; new engine implementation is governed by the root LICENSE.

package server

import (
	"context"
	"crypto/tls"

	"github.com/duravela/djob/pkg/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type grpcServer struct {
	*grpc.Server
}

func newGRPCServer(
	ctx context.Context, rpcCtx *rpc.Context, transport *tls.Config,
) (*grpcServer, error) {
	s := &grpcServer{}

	opts := []grpc.ServerOption{grpc.MaxRecvMsgSize(maxAPIRequestBytes), grpc.MaxSendMsgSize(4 << 20)}
	if transport != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(transport.Clone())))
	}
	s.Server = grpc.NewServer(opts...)
	return s, nil
}
