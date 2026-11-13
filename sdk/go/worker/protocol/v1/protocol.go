// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

// Package v1 defines the versioned HTTPS worker protocol. Capability and session
// messages are process-local; job authorization and outcomes remain durable in
// the controller. None of these messages grants physical fencing.
package v1

import (
	"time"

	"github.com/duravela/djob/sdk/go"
)

const Version = 1
const Path = "/api/workers/v1/"
const MaxControlBytes = 64 << 10
const MaxResultBytes = 4 << 20
const MaxCommandBytes = 8 << 20
const MaxPayloadBytes = 1 << 20

// ValidWorkerID keeps the deployment identity readable inside durable owner IDs.
func ValidWorkerID(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}

// ValidNonce restricts server-generated identities before they enter URL paths.
func ValidNonce(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

type Register struct {
	DeploymentID         string         `json:"deployment_id,omitempty"`
	DeploymentGeneration uint64         `json:"deployment_generation,omitempty"`
	Protocol             int            `json:"protocol"`
	WorkerID             string         `json:"worker_id"`
	TenantID             string         `json:"tenant_id"`
	Handlers             []djob.Version `json:"handlers"`
	Drivers              []djob.Version `json:"drivers"`
}

type Registered struct {
	DeploymentID         string        `json:"deployment_id,omitempty"`
	DeploymentGeneration uint64        `json:"deployment_generation,omitempty"`
	Protocol             int           `json:"protocol"`
	SessionID            string        `json:"session_id"`
	TenantID             string        `json:"tenant_id"`
	HeartbeatEvery       time.Duration `json:"heartbeat_every_ns"`
}

type Kind string

const (
	Evaluate Kind = "evaluate"
	Execute  Kind = "execute"
	Quiesce  Kind = "quiesce"
	Close    Kind = "close"
	Drained  Kind = "drained"
)

// Command sequence numbers are strictly increasing within a fresh session.
// Budget is measured from before the worker's poll request. Timeout is the
// operation's remaining overall limit. Heartbeats may renew Budget, never Timeout.
type Command struct {
	ID       string        `json:"id"`
	Sequence uint64        `json:"sequence"`
	Kind     Kind          `json:"kind"`
	Budget   time.Duration `json:"budget_ns"`
	Timeout  time.Duration `json:"timeout_ns"`
	View     *djob.View    `json:"view,omitempty"`
	Run      *djob.JobRun  `json:"run,omitempty"`
	Action   *djob.Action  `json:"action,omitempty"`
	Gateways []Gateway     `json:"gateways,omitempty"`
}

// Gateway is the optional legacy gateway execution profile, projected only from
// the currently owned run/resource/channel. Business handlers do not consume it.
type Gateway struct {
	ResourceID       string    `json:"resource_id"`
	ResourceRevision uint64    `json:"resource_revision"`
	ResourceEpoch    uint64    `json:"resource_epoch"`
	Duplex           string    `json:"duplex"`
	Capacity         uint32    `json:"capacity"`
	ChannelID        string    `json:"channel_id"`
	GatewayID        string    `json:"gateway_id"`
	Protocol         string    `json:"protocol"`
	Endpoint         string    `json:"endpoint"`
	Revision         uint64    `json:"revision"`
	Epoch            uint64    `json:"epoch"`
	OwnerID          string    `json:"owner_id"`
	LeaseUntil       time.Time `json:"lease_until"`
	LastTransition   time.Time `json:"last_transition"`
}

type Result struct {
	ID       string         `json:"id"`
	Decision *djob.Decision `json:"decision,omitempty"`
	Effect   *djob.Effect   `json:"effect,omitempty"`
	Error    string         `json:"error,omitempty"`
}

type Heartbeat struct {
	CommandID string `json:"command_id"`
}

type HeartbeatReply struct {
	CommandID string        `json:"command_id"`
	Cancel    bool          `json:"cancel"`
	Budget    time.Duration `json:"budget_ns"`
}
