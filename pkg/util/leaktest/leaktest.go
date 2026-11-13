// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package leaktest

import (
	"go.uber.org/goleak"
	"testing"
)

// AfterTest reports newly leaked goroutines through the maintained goleak library.
func AfterTest(t testing.TB) func() {
	t.Helper()
	baseline := goleak.IgnoreCurrent()
	return func() { t.Helper(); goleak.VerifyNone(t, baseline) }
}
