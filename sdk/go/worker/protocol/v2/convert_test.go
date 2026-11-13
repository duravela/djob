// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package workerpb_test

import (
	"math"
	"reflect"
	"testing"
	"time"

	djob "github.com/duravela/djob/sdk/go"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
	pb "github.com/duravela/djob/sdk/go/worker/protocol/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCommandRoundTripPreservesDurableIdentityAndNanoseconds(t *testing.T) {
	at := time.Date(2026, 11, 11, 12, 34, 56, 987654321, time.UTC)
	run := djob.JobRun{
		Activation: &djob.JobActivation{TenantID: "tenant", TriggerID: "trigger", Source: "source", EventID: "event", RunID: "run", TriggerRevision: math.MaxUint64, OccurredAt: at, AcceptedAt: at, Data: []byte{0, 255}, DataDeleted: true, DataSHA256: []byte{1, 2}},
		TriggerID:  "trigger", TriggerRevision: math.MaxUint64, ScheduledAt: &at, TenantID: "tenant", ID: "run",
		Definition: djob.JobDefinition{TenantID: "tenant", ID: "definition", Revision: math.MaxUint64, GroupID: "group", Runtime: djob.StepRuntime, Handler: "handler", HandlerVersion: "1", WorkerRelease: "release-v1", WorkerReleaseWeights: map[string]uint32{"release-v1": 7000, "release-v2": 3000}, Input: []byte{255}, BindingID: "binding", Resources: []string{"bus"}},
		Group:      &djob.JobGroup{TenantID: "tenant", ID: "group", Revision: math.MaxUint64, Runtime: djob.StepRuntime, Resources: []string{"bus"}, MaxRunning: math.MaxUint64},
		Binding:    &djob.JobBinding{TenantID: "tenant", ID: "binding", Revision: math.MaxUint64, Driver: "driver", DriverVersion: "1", Config: []byte{255}, Resources: []string{"bus"}},
		Runtime:    djob.StepRuntime, Resources: []string{"bus"}, Status: djob.JobRunning, Step: math.MaxUint64, State: []byte{0, 255}, Result: []byte{1}, Error: "error", ActionCount: math.MaxUint64,
		PendingAction: &djob.JobAction{TenantID: "tenant", RunID: "run", Sequence: math.MaxUint64, AttemptID: "attempt", Operation: "measure", Input: []byte{255}, Status: djob.EffectSucceeded, Result: []byte{2}, Error: "error", AuthorizedAt: at, FinishedAt: at},
		WaitKey:       "reply", WakeAt: at, OwnerID: "owner", AttemptID: "attempt", LastAttemptID: "previous", AttemptCount: math.MaxUint64, LeaseUntil: at, CancelRequested: true, CancelRequestedAt: at, CancelReason: "reason", CreatedAt: at, LastTransition: at,
	}
	command := wire.Command{ID: "command", Sequence: math.MaxUint64, Kind: wire.Execute, Budget: time.Second + 1, Timeout: time.Minute + 3,
		View: &djob.View{Run: run, Now: at, Event: &djob.JobEvent{TenantID: "tenant", RunID: "run", ID: "event", Key: "reply", Data: []byte{255}, At: at, ConsumedAt: at}}, Run: &run,
		Action:   &djob.Action{TenantID: "tenant", RunID: "run", AttemptID: "attempt", Sequence: math.MaxUint64, Operation: "measure", Input: []byte{255}, IdempotencyKey: "stable"},
		Gateways: []wire.Gateway{{ResourceID: "bus", ResourceRevision: math.MaxUint64, ResourceEpoch: math.MaxUint64, Duplex: "half", Capacity: 1, ChannelID: "channel", GatewayID: "gateway", Protocol: "mqtt", Endpoint: "broker", Revision: math.MaxUint64, Epoch: math.MaxUint64, OwnerID: "owner", LeaseUntil: at, LastTransition: at}},
	}
	encoded, err := proto.Marshal(pb.EncodeCommand(&command))
	if err != nil {
		t.Fatal(err)
	}
	var message pb.Command
	if err := proto.Unmarshal(encoded, &message); err != nil {
		t.Fatal(err)
	}
	got, err := pb.DecodeCommand(&message)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(command, *got) {
		t.Fatalf("worker command changed across protobuf: want %#v, got %#v", command, got)
	}
	message.View.Now = &timestamppb.Timestamp{Seconds: math.MaxInt64}
	if _, err := pb.DecodeCommand(&message); err == nil {
		t.Fatal("invalid timestamp accepted")
	}
	message.View = &pb.View{}
	if _, err := pb.DecodeCommand(&message); err == nil {
		t.Fatal("view without a run accepted")
	}
}
