package measurement_test

import (
	"testing"

	"example.com/acme/djob-node/jobs/measurement"
	djob "github.com/duravela/djob/sdk/go"
)

func TestCompanyDecisionUsesOnlyDomainValues(t *testing.T) {
	view := djob.View{Run: djob.JobRun{Definition: djob.JobDefinition{Input: []byte(`{"maximum":100}`)}}}
	decision, err := measurement.Handle(view)
	if err != nil || decision.Kind != djob.JobDoAction || decision.Operation != "measure" || len(decision.Input) != 0 {
		t.Fatalf("semantic action: %+v, %v", decision, err)
	}
	view.Run.State = decision.State
	view.Run.PendingAction = &djob.JobAction{Status: djob.EffectSucceeded, Result: []byte(`{"value":42}`)}
	decision, err = measurement.Handle(view)
	if err != nil || decision.Status != djob.JobSucceeded {
		t.Fatalf("accepted company measurement: %+v, %v", decision, err)
	}
	view.Run.Definition.Input = []byte(`{"maximum":10}`)
	decision, err = measurement.Handle(view)
	if err != nil || decision.Status != djob.JobFailed || decision.Error != "measurement exceeds company limit" {
		t.Fatalf("rejected company measurement: %+v, %v", decision, err)
	}
}

func TestCompanyDecisionDoesNotReplayFailedActions(t *testing.T) {
	view := djob.View{Run: djob.JobRun{Definition: djob.JobDefinition{Input: []byte(`{"maximum":100}`)}, State: []byte("measured"), PendingAction: &djob.JobAction{Status: djob.EffectFailed, Error: "device rejected request"}}}
	decision, err := measurement.Handle(view)
	if err != nil || decision.Kind != djob.JobComplete || decision.Status != djob.JobFailed || decision.Error != "device rejected request" {
		t.Fatalf("failed action must finish without another request: %+v, %v", decision, err)
	}
}

func TestCompanyRejectsMissingLimitBeforeRequestingIO(t *testing.T) {
	for _, input := range []string{`{}`, `null`, `{"maximum":null}`, `not json`} {
		decision, err := measurement.Handle(djob.View{Run: djob.JobRun{Definition: djob.JobDefinition{Input: []byte(input)}}})
		if err == nil || decision.Kind == djob.JobDoAction {
			t.Fatalf("invalid company input requested I/O: %s, %+v, %v", input, decision, err)
		}
	}
}
