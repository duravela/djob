// Package measurement contains company business rules expressed as pure job
// decisions. It has no dependency on a device integration or server startup.
package measurement

import (
	"encoding/json"
	"fmt"

	djob "github.com/duravela/djob/sdk/go"
)

const Name = "acme.check-measurement"
const Version = "1"
const Operation = "measure"

type limits struct {
	Maximum *int `json:"maximum"`
}

// Reading is the domain result expected from the semantic measure operation.
// Integrations normalize their protocol-specific results into this shape.
type Reading struct {
	Value int `json:"value"`
}

// Handle requests a reading and checks it against the configured company limit.
func Handle(v djob.View) (djob.Decision, error) {
	var input limits
	if err := json.Unmarshal(v.Run.Definition.Input, &input); err != nil {
		return djob.Decision{}, fmt.Errorf("measurement requires a maximum: %w", err)
	}
	if input.Maximum == nil {
		return djob.Decision{}, fmt.Errorf("measurement requires a maximum")
	}
	if len(v.Run.State) == 0 {
		return djob.Decision{Kind: djob.JobDoAction, State: []byte("measured"), Operation: Operation}, nil
	}
	if string(v.Run.State) != "measured" || v.Run.PendingAction == nil {
		return djob.Decision{}, fmt.Errorf("unrecognized company measurement checkpoint")
	}
	action := v.Run.PendingAction
	if action.Status != djob.EffectSucceeded {
		return djob.Decision{Kind: djob.JobComplete, Status: djob.JobFailed, Error: action.Error}, nil
	}
	var reading Reading
	if err := json.Unmarshal(action.Result, &reading); err != nil {
		return djob.Decision{}, err
	}
	status, message := djob.JobSucceeded, ""
	if reading.Value > *input.Maximum {
		status, message = djob.JobFailed, "measurement exceeds company limit"
	}
	return djob.Decision{Kind: djob.JobComplete, Status: status, Result: action.Result, Error: message}, nil
}
