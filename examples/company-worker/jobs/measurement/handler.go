// Package measurement owns company rules. It depends only on the djob SDK and
// knows nothing about Modbus, gateway ownership or the controller connection.
package measurement

import (
	"encoding/json"
	"fmt"

	"github.com/duravela/djob/sdk/go"
)

const Name = "acme.check-measurement"
const TwiceName = "acme.measure-twice"
const Version = "1"
const Operation = "measure"

type Reading struct {
	Value int `json:"value"`
}

func maximum(v djob.View) (int, error) {
	var input struct {
		Maximum *int `json:"maximum"`
	}
	if json.Unmarshal(v.Run.Definition.Input, &input) != nil || input.Maximum == nil {
		return 0, fmt.Errorf("measurement requires a maximum")
	}
	return *input.Maximum, nil
}

func checked(v djob.View, limit int) (djob.Decision, error) {
	action := v.Run.PendingAction
	if action == nil {
		return djob.Decision{}, fmt.Errorf("missing measurement outcome")
	}
	if action.Status != djob.EffectSucceeded {
		return djob.Decision{Kind: djob.JobComplete, Status: djob.JobFailed, Error: action.Error}, nil
	}
	var reading Reading
	if err := json.Unmarshal(action.Result, &reading); err != nil {
		return djob.Decision{}, err
	}
	status, message := djob.JobSucceeded, ""
	if reading.Value > limit {
		status, message = djob.JobFailed, "measurement exceeds company limit"
	}
	return djob.Decision{Kind: djob.JobComplete, Status: status, Result: action.Result, Error: message}, nil
}

func Handle(v djob.View) (djob.Decision, error) {
	limit, err := maximum(v)
	if err != nil {
		return djob.Decision{}, err
	}
	switch string(v.Run.State) {
	case "":
		return djob.Decision{Kind: djob.JobDoAction, State: []byte("measured"), Operation: Operation}, nil
	case "measured":
		return checked(v, limit)
	default:
		return djob.Decision{}, fmt.Errorf("unrecognized measurement checkpoint")
	}
}

// Twice keeps a conversation alive across two measurements and an authenticated
// "continue" event. The engine retains the canonical reservation during the wait.
func Twice(v djob.View) (djob.Decision, error) {
	limit, err := maximum(v)
	if err != nil {
		return djob.Decision{}, err
	}
	switch string(v.Run.State) {
	case "":
		return djob.Decision{Kind: djob.JobDoAction, State: []byte("first"), Operation: Operation}, nil
	case "first":
		decision, err := checked(v, limit)
		if err != nil || decision.Status != djob.JobSucceeded {
			return decision, err
		}
		return djob.Decision{Kind: djob.JobWait, State: []byte("awaiting-continue"), WaitKey: "continue"}, nil
	case "awaiting-continue":
		if v.Event == nil || v.Event.Key != "continue" {
			return djob.Decision{}, fmt.Errorf("measurement requires a continue event")
		}
		return djob.Decision{Kind: djob.JobDoAction, State: []byte("second"), Operation: Operation}, nil
	case "second":
		return checked(v, limit)
	default:
		return djob.Decision{}, fmt.Errorf("unrecognized measurement checkpoint")
	}
}
