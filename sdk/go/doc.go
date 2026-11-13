// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

// Package djob defines the protocol-agnostic contracts for company job handlers
// and drivers. This base package uses only the Go standard library; storage, consensus,
// dispatch and protocol implementations belong to the execution engine or to
// optional integrations.
//
// A Handler evaluates a durable View and returns a Decision. Handlers must be
// bounded and replayable, and perform no external I/O. Effects run through a
// Driver only after the engine durably authorizes an Action. Driver sessions are
// local to one attempt; persisted logical state does not restore a lost session.
//
// JobRun and related values are observations of committed state. Constructing or
// changing these values grants no execution or resource-release authority. Only
// the engine validates and commits state transitions.
//
// This is the v0 SDK. The worker subpackage runs independent gRPC workers and
// the handlers subpackage supplies optional built-ins. Embedded applications can
// also register these contracts through the engine's server configuration.
package djob
