// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package base

import "github.com/duravela/djob/pkg/util/stop"

// TestServerArgs configures isolated fixtures; JoinAddr is retained for source compatibility.
type TestServerArgs struct {
	Addr, HTTPAddr, JoinAddr string
	Stopper                  *stop.Stopper
}
