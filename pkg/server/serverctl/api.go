// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package serverctl

import "context"

type ServerShutdownInterface interface {
	AnnotateCtx(context.Context) context.Context
	ShutdownRequested() <-chan ShutdownRequest
}
type ServerStartupInterface interface{ ServerShutdownInterface }

type ShutdownReason int

const (
	ShutdownReasonDrainRPC ShutdownReason = iota
	ShutdownReasonServerStartupError
	ShutdownReasonFatalError
	ShutdownReasonGracefulStopRequestedByOrchestration
)

type ShutdownRequest struct {
	Reason ShutdownReason
	Err    error
}
