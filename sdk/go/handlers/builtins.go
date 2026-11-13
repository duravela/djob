// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package handlers

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/duravela/djob/sdk/go"
)

// RequestReplyInput is application input; a binding determines how the semantic
// "request" operation reaches its target. The same handler can use any driver.
type RequestReplyInput struct {
	CorrelationKey string        `json:"correlation_key"`
	Payload        []byte        `json:"payload,omitempty"`
	Timeout        time.Duration `json:"timeout_ns,omitempty"`
}

// Builtins supplies small versioned handlers, with no driver enabled implicitly.
func Builtins() djob.Registry {
	return djob.Registry{Handlers: map[djob.Version]djob.Handler{
		{"invoke", "1"}: func(v djob.View) (djob.Decision, error) {
			if v.Run.Binding == nil {
				return djob.Decision{}, fmt.Errorf("invoke requires a binding")
			}
			if len(v.Run.State) == 0 {
				return djob.Decision{Kind: djob.JobDoAction, State: []byte("executed"), Operation: "execute", Input: v.Run.Definition.Input}, nil
			}
			if string(v.Run.State) != "executed" || v.Run.PendingAction == nil {
				return djob.Decision{}, fmt.Errorf("unrecognized invoke checkpoint")
			}
			status := djob.JobSucceeded
			if v.Run.PendingAction.Status != djob.EffectSucceeded {
				status = djob.JobFailed
			}
			return djob.Decision{Kind: djob.JobComplete, State: v.Run.State, Status: status, Result: v.Run.PendingAction.Result, Error: v.Run.PendingAction.Error}, nil
		},
		{"sha256", "1"}: func(v djob.View) (djob.Decision, error) {
			sum := sha256.Sum256(v.Run.Definition.Input)
			return djob.Decision{Kind: djob.JobComplete, Status: djob.JobSucceeded, Result: sum[:]}, nil
		},
		{"sha256-event", "1"}: func(v djob.View) (djob.Decision, error) {
			if v.Run.Activation == nil {
				return djob.Decision{}, fmt.Errorf("sha256-event requires an event activation")
			}
			sum := sha256.Sum256(v.Run.Activation.Data)
			return djob.Decision{Kind: djob.JobComplete, Status: djob.JobSucceeded, Result: sum[:]}, nil
		},
		{"request-reply", "1"}: requestReply,
	}, Drivers: make(map[djob.Version]djob.DriverFactory)}
}

func requestReply(v djob.View) (djob.Decision, error) {
	var input RequestReplyInput
	decoder := json.NewDecoder(bytes.NewReader(v.Run.Definition.Input))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || strings.TrimSpace(input.CorrelationKey) == "" || len(input.CorrelationKey) > 256 || !utf8.ValidString(input.CorrelationKey) || input.Timeout < 0 || v.Run.Binding == nil {
		return djob.Decision{}, fmt.Errorf("request-reply requires a binding, correlation_key, optional payload and nonnegative timeout_ns")
	}
	switch string(v.Run.State) {
	case "":
		return djob.Decision{Kind: djob.JobDoAction, State: []byte("requested"), Operation: "request", Input: input.Payload}, nil
	case "requested":
		if v.Run.PendingAction == nil || v.Run.PendingAction.Status != djob.EffectSucceeded {
			return djob.Decision{Kind: djob.JobComplete, State: v.Run.State, Status: djob.JobFailed, Error: "request did not succeed"}, nil
		}
		return djob.Decision{Kind: djob.JobWait, State: []byte("awaiting-reply"), WaitKey: input.CorrelationKey, WaitFor: input.Timeout}, nil
	case "awaiting-reply":
		if v.Event != nil {
			return djob.Decision{Kind: djob.JobComplete, State: []byte("complete"), Status: djob.JobSucceeded, Result: v.Event.Data}, nil
		}
		return djob.Decision{Kind: djob.JobComplete, State: v.Run.State, Status: djob.JobFailed, Error: "reply deadline passed"}, nil
	}
	return djob.Decision{}, fmt.Errorf("unrecognized request-reply checkpoint")
}
