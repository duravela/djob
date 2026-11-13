// Package modbusbinding maps semantic job actions to operator-owned Modbus operations.
// Company handlers can use the same action names with another driver or protocol.
package modbusbinding

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor/modbus"
	"github.com/duravela/djob/pkg/jobs"
)

type modbusJobBinding struct {
	ChannelID  string `json:"channel_id"`
	Operations []struct {
		Name      string          `json:"name"`
		Operation json.RawMessage `json:"operation"`
	} `json:"operations"`
}

// Encode translates a semantic action without opening connections.
// The application job names an operation. Only the operator-owned binding
// contains unit IDs, register addresses and protocol payloads. This supplied
// encoder supports static operations; a company driver can translate richer
// application inputs without changing the job/run storage contract.
func Encode(binding db.JobBinding, action jobs.Action) (string, []byte, error) {
	var config modbusJobBinding
	decoder := json.NewDecoder(bytes.NewReader(binding.Config))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&config) != nil || decoder.Decode(new(any)) != io.EOF || config.ChannelID == "" {
		return "", nil, fmt.Errorf("invalid Modbus job binding")
	}
	if len(action.Input) != 0 {
		return "", nil, fmt.Errorf("static Modbus binding does not accept action input")
	}
	seen := make(map[string]bool)
	var payload []byte
	for _, op := range config.Operations {
		if op.Name == "" || seen[op.Name] {
			return "", nil, fmt.Errorf("Modbus operation names must be nonempty and distinct")
		}
		seen[op.Name] = true
		if _, err := modbus.DecodeOperation(op.Operation); err != nil {
			return "", nil, fmt.Errorf("invalid mapped Modbus operation: %w", err)
		}
		if op.Name == action.Operation {
			payload = op.Operation
		}
	}
	if payload == nil {
		return "", nil, fmt.Errorf("semantic operation has no Modbus binding")
	}
	return modbus.ExecutorName, payload, nil
}
