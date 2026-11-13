// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package serverctl

import "errors"

func (r ShutdownRequest) ShutdownCause() error {
	if r.Reason == ShutdownReasonDrainRPC {
		return errors.New("drain requested shutdown")
	}
	return r.Err
}
func (r ShutdownRequest) TerminateUsingGracefulDrain() bool {
	return r.Reason == ShutdownReasonGracefulStopRequestedByOrchestration
}
func MakeShutdownRequest(reason ShutdownReason, cause error) ShutdownRequest {
	if reason == ShutdownReasonDrainRPC && cause != nil {
		panic("drain shutdown cannot carry an error")
	}
	return ShutdownRequest{Reason: reason, Err: cause}
}
