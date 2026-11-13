// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
)

// grpcGatewayServer is the registration contract implemented by generated services.
type grpcGatewayServer interface {
	RegisterService(*grpc.Server)
	RegisterGateway(context.Context, *gwruntime.ServeMux, *grpc.ClientConn) error
}

var _ grpcGatewayServer = (*schedulerServer)(nil)
