package measurement_test

import (
	"testing"

	"example.com/acme/djob-worker/jobs/measurement"
	"github.com/duravela/djob/sdk/go"
)

func TestRelatedMeasurementsRequireAContinueEventAndBothPassTheLimit(t *testing.T) {
	v := djob.View{Run: djob.JobRun{Definition: djob.JobDefinition{Input: []byte(`{"maximum":100}`)}}}
	d, err := measurement.Twice(v)
	if err != nil || d.Kind != djob.JobDoAction || d.Operation != "measure" || len(d.Input) != 0 {
		t.Fatalf("first action: %+v, %v", d, err)
	}
	v.Run.State = d.State
	v.Run.PendingAction = &djob.JobAction{Status: djob.EffectSucceeded, Result: []byte(`{"value":42}`)}
	d, err = measurement.Twice(v)
	if err != nil || d.Kind != djob.JobWait || d.WaitKey != "continue" {
		t.Fatalf("wait: %+v, %v", d, err)
	}
	v.Run.State = d.State
	if _, err := measurement.Twice(v); err == nil {
		t.Fatal("missing event authorized a second measurement")
	}
	v.Event = &djob.JobEvent{Key: "continue"}
	d, err = measurement.Twice(v)
	if err != nil || d.Kind != djob.JobDoAction {
		t.Fatalf("second action: %+v, %v", d, err)
	}
	v.Run.State = d.State
	v.Run.PendingAction.Result = []byte(`{"value":101}`)
	d, err = measurement.Twice(v)
	if err != nil || d.Status != djob.JobFailed || d.Error != "measurement exceeds company limit" {
		t.Fatalf("limit: %+v, %v", d, err)
	}
}

func TestInvalidInputAndFailedEffectsNeverRequestAnotherAction(t *testing.T) {
	for _, handle := range []djob.Handler{measurement.Handle, measurement.Twice} {
		for _, input := range []string{`{}`, `null`, `{"maximum":null}`, `invalid`} {
			if d, err := handle(djob.View{Run: djob.JobRun{Definition: djob.JobDefinition{Input: []byte(input)}}}); err == nil || d.Kind == djob.JobDoAction {
				t.Fatalf("invalid input requested I/O: %+v, %v", d, err)
			}
		}
	}
	v := djob.View{Run: djob.JobRun{Definition: djob.JobDefinition{Input: []byte(`{"maximum":100}`)}, State: []byte("first"), PendingAction: &djob.JobAction{Status: djob.EffectFailed, Error: "device rejected request"}}}
	d, err := measurement.Twice(v)
	if err != nil || d.Status != djob.JobFailed || d.Kind != djob.JobComplete || d.Error != "device rejected request" {
		t.Fatalf("failed effect retried: %+v, %v", d, err)
	}
}
