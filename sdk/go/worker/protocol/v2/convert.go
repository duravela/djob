// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package workerpb

import (
	"errors"
	djob "github.com/duravela/djob/sdk/go"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
	"maps"
	"time"
)

const ProtocolVersion = 2
const ServicePath = "/djob.workers.v2.Workers/"

type decoder struct{ err error }

func (d *decoder) timestamp(v *timestamppb.Timestamp) time.Time {
	if v == nil {
		return time.Time{}
	}
	if v.CheckValid() != nil {
		d.err = errors.New("invalid worker timestamp")
		return time.Time{}
	}
	return v.AsTime()
}
func timestamp(v time.Time) *timestamppb.Timestamp {
	if v.IsZero() {
		return nil
	}
	return timestamppb.New(v)
}
func value[T any](v *T) T {
	if v == nil {
		var zero T
		return zero
	}
	return *v
}

func encodeVersion(v *djob.Version) *Version {
	if v == nil {
		return nil
	}
	out := &Version{}
	out.Name = v.Name
	out.Version = v.Version
	return out
}

func (d *decoder) decodeVersion(v *Version) *djob.Version {
	if v == nil {
		return nil
	}
	out := &djob.Version{}
	out.Name = v.Name
	out.Version = v.Version
	return out
}

func encodeJobGroup(v *djob.JobGroup) *JobGroup {
	if v == nil {
		return nil
	}
	out := &JobGroup{}
	out.TenantId = v.TenantID
	out.Id = v.ID
	out.Revision = v.Revision
	out.Runtime = v.Runtime
	out.Resources = append([]string(nil), v.Resources...)
	out.MaxRunning = v.MaxRunning
	return out
}

func (d *decoder) decodeJobGroup(v *JobGroup) *djob.JobGroup {
	if v == nil {
		return nil
	}
	out := &djob.JobGroup{}
	out.TenantID = v.TenantId
	out.ID = v.Id
	out.Revision = v.Revision
	out.Runtime = v.Runtime
	out.Resources = append([]string(nil), v.Resources...)
	out.MaxRunning = v.MaxRunning
	return out
}

func encodeJobDefinition(v *djob.JobDefinition) *JobDefinition {
	if v == nil {
		return nil
	}
	out := &JobDefinition{}
	out.TenantId = v.TenantID
	out.Id = v.ID
	out.Revision = v.Revision
	out.GroupId = v.GroupID
	out.Runtime = v.Runtime
	out.Handler = v.Handler
	out.HandlerVersion = v.HandlerVersion
	out.WorkerRelease = v.WorkerRelease
	out.WorkerReleaseWeights = maps.Clone(v.WorkerReleaseWeights)
	out.Input = append([]byte(nil), v.Input...)
	out.BindingId = v.BindingID
	out.Resources = append([]string(nil), v.Resources...)
	return out
}

func (d *decoder) decodeJobDefinition(v *JobDefinition) *djob.JobDefinition {
	if v == nil {
		return nil
	}
	out := &djob.JobDefinition{}
	out.TenantID = v.TenantId
	out.ID = v.Id
	out.Revision = v.Revision
	out.GroupID = v.GroupId
	out.Runtime = v.Runtime
	out.Handler = v.Handler
	out.HandlerVersion = v.HandlerVersion
	out.WorkerRelease = v.WorkerRelease
	out.WorkerReleaseWeights = maps.Clone(v.WorkerReleaseWeights)
	out.Input = append([]byte(nil), v.Input...)
	out.BindingID = v.BindingId
	out.Resources = append([]string(nil), v.Resources...)
	return out
}

func encodeJobBinding(v *djob.JobBinding) *JobBinding {
	if v == nil {
		return nil
	}
	out := &JobBinding{}
	out.TenantId = v.TenantID
	out.Id = v.ID
	out.Revision = v.Revision
	out.Driver = v.Driver
	out.DriverVersion = v.DriverVersion
	out.Config = append([]byte(nil), v.Config...)
	out.Resources = append([]string(nil), v.Resources...)
	return out
}

func (d *decoder) decodeJobBinding(v *JobBinding) *djob.JobBinding {
	if v == nil {
		return nil
	}
	out := &djob.JobBinding{}
	out.TenantID = v.TenantId
	out.ID = v.Id
	out.Revision = v.Revision
	out.Driver = v.Driver
	out.DriverVersion = v.DriverVersion
	out.Config = append([]byte(nil), v.Config...)
	out.Resources = append([]string(nil), v.Resources...)
	return out
}

func encodeJobAction(v *djob.JobAction) *JobAction {
	if v == nil {
		return nil
	}
	out := &JobAction{}
	out.TenantId = v.TenantID
	out.RunId = v.RunID
	out.Sequence = v.Sequence
	out.AttemptId = v.AttemptID
	out.Operation = v.Operation
	out.Input = append([]byte(nil), v.Input...)
	out.Status = string(v.Status)
	out.Result = append([]byte(nil), v.Result...)
	out.Error = v.Error
	out.AuthorizedAt = timestamp(v.AuthorizedAt)
	out.FinishedAt = timestamp(v.FinishedAt)
	return out
}

func (d *decoder) decodeJobAction(v *JobAction) *djob.JobAction {
	if v == nil {
		return nil
	}
	out := &djob.JobAction{}
	out.TenantID = v.TenantId
	out.RunID = v.RunId
	out.Sequence = v.Sequence
	out.AttemptID = v.AttemptId
	out.Operation = v.Operation
	out.Input = append([]byte(nil), v.Input...)
	out.Status = djob.EffectStatus(v.Status)
	out.Result = append([]byte(nil), v.Result...)
	out.Error = v.Error
	out.AuthorizedAt = d.timestamp(v.AuthorizedAt)
	out.FinishedAt = d.timestamp(v.FinishedAt)
	return out
}

func encodeJobRun(v *djob.JobRun) *JobRun {
	if v == nil {
		return nil
	}
	out := &JobRun{}
	out.Activation = encodeJobActivation(v.Activation)
	out.TriggerId = v.TriggerID
	out.TriggerRevision = v.TriggerRevision
	if v.ScheduledAt != nil {
		out.ScheduledAt = timestamppb.New(*v.ScheduledAt)
	}
	out.TenantId = v.TenantID
	out.Id = v.ID
	out.Definition = encodeJobDefinition(&v.Definition)
	out.Group = encodeJobGroup(v.Group)
	out.Binding = encodeJobBinding(v.Binding)
	out.Runtime = v.Runtime
	out.Resources = append([]string(nil), v.Resources...)
	out.Status = string(v.Status)
	out.Step = v.Step
	out.State = append([]byte(nil), v.State...)
	out.Result = append([]byte(nil), v.Result...)
	out.Error = v.Error
	out.ActionCount = v.ActionCount
	out.PendingAction = encodeJobAction(v.PendingAction)
	out.WaitKey = v.WaitKey
	out.WakeAt = timestamp(v.WakeAt)
	out.OwnerId = v.OwnerID
	out.AttemptId = v.AttemptID
	out.LastAttemptId = v.LastAttemptID
	out.AttemptCount = v.AttemptCount
	out.LeaseUntil = timestamp(v.LeaseUntil)
	out.CancelRequested = v.CancelRequested
	out.CancelRequestedAt = timestamp(v.CancelRequestedAt)
	out.CancelReason = v.CancelReason
	out.CreatedAt = timestamp(v.CreatedAt)
	out.LastTransition = timestamp(v.LastTransition)
	return out
}

func (d *decoder) decodeJobRun(v *JobRun) *djob.JobRun {
	if v == nil {
		return nil
	}
	out := &djob.JobRun{}
	out.Activation = d.decodeJobActivation(v.Activation)
	out.TriggerID = v.TriggerId
	out.TriggerRevision = v.TriggerRevision
	if v.ScheduledAt != nil {
		t := d.timestamp(v.ScheduledAt)
		out.ScheduledAt = &t
	}
	out.TenantID = v.TenantId
	out.ID = v.Id
	out.Definition = value(d.decodeJobDefinition(v.Definition))
	out.Group = d.decodeJobGroup(v.Group)
	out.Binding = d.decodeJobBinding(v.Binding)
	out.Runtime = v.Runtime
	out.Resources = append([]string(nil), v.Resources...)
	out.Status = djob.JobStatus(v.Status)
	out.Step = v.Step
	out.State = append([]byte(nil), v.State...)
	out.Result = append([]byte(nil), v.Result...)
	out.Error = v.Error
	out.ActionCount = v.ActionCount
	out.PendingAction = d.decodeJobAction(v.PendingAction)
	out.WaitKey = v.WaitKey
	out.WakeAt = d.timestamp(v.WakeAt)
	out.OwnerID = v.OwnerId
	out.AttemptID = v.AttemptId
	out.LastAttemptID = v.LastAttemptId
	out.AttemptCount = v.AttemptCount
	out.LeaseUntil = d.timestamp(v.LeaseUntil)
	out.CancelRequested = v.CancelRequested
	out.CancelRequestedAt = d.timestamp(v.CancelRequestedAt)
	out.CancelReason = v.CancelReason
	out.CreatedAt = d.timestamp(v.CreatedAt)
	out.LastTransition = d.timestamp(v.LastTransition)
	return out
}

func encodeJobEvent(v *djob.JobEvent) *JobEvent {
	if v == nil {
		return nil
	}
	out := &JobEvent{}
	out.TenantId = v.TenantID
	out.RunId = v.RunID
	out.Id = v.ID
	out.Key = v.Key
	out.Data = append([]byte(nil), v.Data...)
	out.At = timestamp(v.At)
	out.ConsumedAt = timestamp(v.ConsumedAt)
	return out
}

func (d *decoder) decodeJobEvent(v *JobEvent) *djob.JobEvent {
	if v == nil {
		return nil
	}
	out := &djob.JobEvent{}
	out.TenantID = v.TenantId
	out.RunID = v.RunId
	out.ID = v.Id
	out.Key = v.Key
	out.Data = append([]byte(nil), v.Data...)
	out.At = d.timestamp(v.At)
	out.ConsumedAt = d.timestamp(v.ConsumedAt)
	return out
}

func encodeJobActivation(v *djob.JobActivation) *JobActivation {
	if v == nil {
		return nil
	}
	out := &JobActivation{}
	out.TenantId = v.TenantID
	out.TriggerId = v.TriggerID
	out.Source = v.Source
	out.EventId = v.EventID
	out.RunId = v.RunID
	out.TriggerRevision = v.TriggerRevision
	out.OccurredAt = timestamp(v.OccurredAt)
	out.AcceptedAt = timestamp(v.AcceptedAt)
	out.Data = append([]byte(nil), v.Data...)
	out.DataDeleted = v.DataDeleted
	out.DataSha256 = append([]byte(nil), v.DataSHA256...)
	return out
}

func (d *decoder) decodeJobActivation(v *JobActivation) *djob.JobActivation {
	if v == nil {
		return nil
	}
	out := &djob.JobActivation{}
	out.TenantID = v.TenantId
	out.TriggerID = v.TriggerId
	out.Source = v.Source
	out.EventID = v.EventId
	out.RunID = v.RunId
	out.TriggerRevision = v.TriggerRevision
	out.OccurredAt = d.timestamp(v.OccurredAt)
	out.AcceptedAt = d.timestamp(v.AcceptedAt)
	out.Data = append([]byte(nil), v.Data...)
	out.DataDeleted = v.DataDeleted
	out.DataSHA256 = append([]byte(nil), v.DataSha256...)
	return out
}

func encodeView(v *djob.View) *View {
	if v == nil {
		return nil
	}
	out := &View{}
	out.Run = encodeJobRun(&v.Run)
	out.Event = encodeJobEvent(v.Event)
	out.Now = timestamp(v.Now)
	return out
}

func (d *decoder) decodeView(v *View) *djob.View {
	if v == nil {
		return nil
	}
	out := &djob.View{}
	if v.Run == nil {
		d.err = errors.New("worker view omits its run")
	}
	out.Run = value(d.decodeJobRun(v.Run))
	out.Event = d.decodeJobEvent(v.Event)
	out.Now = d.timestamp(v.Now)
	return out
}

func encodeDecision(v *djob.Decision) *Decision {
	if v == nil {
		return nil
	}
	out := &Decision{}
	out.Kind = string(v.Kind)
	out.State = append([]byte(nil), v.State...)
	out.Operation = v.Operation
	out.Input = append([]byte(nil), v.Input...)
	out.WaitKey = v.WaitKey
	out.WakeAt = timestamp(v.WakeAt)
	out.WaitForNs = int64(v.WaitFor)
	out.Status = string(v.Status)
	out.Result = append([]byte(nil), v.Result...)
	out.Error = v.Error
	return out
}

func (d *decoder) decodeDecision(v *Decision) *djob.Decision {
	if v == nil {
		return nil
	}
	out := &djob.Decision{}
	out.Kind = djob.JobDecisionKind(v.Kind)
	out.State = append([]byte(nil), v.State...)
	out.Operation = v.Operation
	out.Input = append([]byte(nil), v.Input...)
	out.WaitKey = v.WaitKey
	out.WakeAt = d.timestamp(v.WakeAt)
	out.WaitFor = time.Duration(v.WaitForNs)
	out.Status = djob.JobStatus(v.Status)
	out.Result = append([]byte(nil), v.Result...)
	out.Error = v.Error
	return out
}

func encodeAction(v *djob.Action) *Action {
	if v == nil {
		return nil
	}
	out := &Action{}
	out.TenantId = v.TenantID
	out.RunId = v.RunID
	out.AttemptId = v.AttemptID
	out.Sequence = v.Sequence
	out.Operation = v.Operation
	out.Input = append([]byte(nil), v.Input...)
	out.IdempotencyKey = v.IdempotencyKey
	return out
}

func (d *decoder) decodeAction(v *Action) *djob.Action {
	if v == nil {
		return nil
	}
	out := &djob.Action{}
	out.TenantID = v.TenantId
	out.RunID = v.RunId
	out.AttemptID = v.AttemptId
	out.Sequence = v.Sequence
	out.Operation = v.Operation
	out.Input = append([]byte(nil), v.Input...)
	out.IdempotencyKey = v.IdempotencyKey
	return out
}

func encodeEffect(v *djob.Effect) *Effect {
	if v == nil {
		return nil
	}
	out := &Effect{}
	out.Status = string(v.Status)
	out.Data = append([]byte(nil), v.Data...)
	out.Error = v.Error
	out.SessionLost = v.SessionLost
	return out
}

func (d *decoder) decodeEffect(v *Effect) *djob.Effect {
	if v == nil {
		return nil
	}
	out := &djob.Effect{}
	out.Status = djob.EffectStatus(v.Status)
	out.Data = append([]byte(nil), v.Data...)
	out.Error = v.Error
	out.SessionLost = v.SessionLost
	return out
}

func encodeRegister(v *wire.Register) *Register {
	if v == nil {
		return nil
	}
	out := &Register{}
	out.Protocol = int32(v.Protocol)
	out.WorkerId = v.WorkerID
	out.TenantId = v.TenantID
	for i := range v.Handlers {
		out.Handlers = append(out.Handlers, encodeVersion(&v.Handlers[i]))
	}
	for i := range v.Drivers {
		out.Drivers = append(out.Drivers, encodeVersion(&v.Drivers[i]))
	}
	out.DeploymentId = v.DeploymentID
	out.DeploymentGeneration = v.DeploymentGeneration
	return out
}

func (d *decoder) decodeRegister(v *Register) *wire.Register {
	if v == nil {
		return nil
	}
	out := &wire.Register{}
	out.Protocol = int(v.Protocol)
	out.WorkerID = v.WorkerId
	out.TenantID = v.TenantId
	for _, item := range v.Handlers {
		if item == nil {
			d.err = errors.New("nil worker entry")
			continue
		}
		out.Handlers = append(out.Handlers, value(d.decodeVersion(item)))
	}
	for _, item := range v.Drivers {
		if item == nil {
			d.err = errors.New("nil worker entry")
			continue
		}
		out.Drivers = append(out.Drivers, value(d.decodeVersion(item)))
	}
	out.DeploymentID = v.DeploymentId
	out.DeploymentGeneration = v.DeploymentGeneration
	return out
}

func encodeRegistered(v *wire.Registered) *Registered {
	if v == nil {
		return nil
	}
	out := &Registered{}
	out.Protocol = int32(v.Protocol)
	out.SessionId = v.SessionID
	out.TenantId = v.TenantID
	out.HeartbeatEveryNs = int64(v.HeartbeatEvery)
	out.DeploymentId = v.DeploymentID
	out.DeploymentGeneration = v.DeploymentGeneration
	return out
}

func (d *decoder) decodeRegistered(v *Registered) *wire.Registered {
	if v == nil {
		return nil
	}
	out := &wire.Registered{}
	out.Protocol = int(v.Protocol)
	out.SessionID = v.SessionId
	out.TenantID = v.TenantId
	out.HeartbeatEvery = time.Duration(v.HeartbeatEveryNs)
	out.DeploymentID = v.DeploymentId
	out.DeploymentGeneration = v.DeploymentGeneration
	return out
}

func encodeCommand(v *wire.Command) *Command {
	if v == nil {
		return nil
	}
	out := &Command{}
	out.Id = v.ID
	out.Sequence = v.Sequence
	out.Kind = string(v.Kind)
	out.BudgetNs = int64(v.Budget)
	out.TimeoutNs = int64(v.Timeout)
	out.View = encodeView(v.View)
	out.Run = encodeJobRun(v.Run)
	out.Action = encodeAction(v.Action)
	for i := range v.Gateways {
		out.Gateways = append(out.Gateways, encodeGateway(&v.Gateways[i]))
	}
	return out
}

func (d *decoder) decodeCommand(v *Command) *wire.Command {
	if v == nil {
		return nil
	}
	out := &wire.Command{}
	out.ID = v.Id
	out.Sequence = v.Sequence
	out.Kind = wire.Kind(v.Kind)
	out.Budget = time.Duration(v.BudgetNs)
	out.Timeout = time.Duration(v.TimeoutNs)
	out.View = d.decodeView(v.View)
	out.Run = d.decodeJobRun(v.Run)
	out.Action = d.decodeAction(v.Action)
	for _, item := range v.Gateways {
		if item == nil {
			d.err = errors.New("nil worker entry")
			continue
		}
		out.Gateways = append(out.Gateways, value(d.decodeGateway(item)))
	}
	return out
}

func encodeGateway(v *wire.Gateway) *Gateway {
	if v == nil {
		return nil
	}
	out := &Gateway{}
	out.ResourceId = v.ResourceID
	out.ResourceRevision = v.ResourceRevision
	out.ResourceEpoch = v.ResourceEpoch
	out.Duplex = v.Duplex
	out.Capacity = v.Capacity
	out.ChannelId = v.ChannelID
	out.GatewayId = v.GatewayID
	out.Protocol = v.Protocol
	out.Endpoint = v.Endpoint
	out.Revision = v.Revision
	out.Epoch = v.Epoch
	out.OwnerId = v.OwnerID
	out.LeaseUntil = timestamp(v.LeaseUntil)
	out.LastTransition = timestamp(v.LastTransition)
	return out
}

func (d *decoder) decodeGateway(v *Gateway) *wire.Gateway {
	if v == nil {
		return nil
	}
	out := &wire.Gateway{}
	out.ResourceID = v.ResourceId
	out.ResourceRevision = v.ResourceRevision
	out.ResourceEpoch = v.ResourceEpoch
	out.Duplex = v.Duplex
	out.Capacity = v.Capacity
	out.ChannelID = v.ChannelId
	out.GatewayID = v.GatewayId
	out.Protocol = v.Protocol
	out.Endpoint = v.Endpoint
	out.Revision = v.Revision
	out.Epoch = v.Epoch
	out.OwnerID = v.OwnerId
	out.LeaseUntil = d.timestamp(v.LeaseUntil)
	out.LastTransition = d.timestamp(v.LastTransition)
	return out
}

func encodeResult(v *wire.Result) *Result {
	if v == nil {
		return nil
	}
	out := &Result{}
	out.Id = v.ID
	out.Decision = encodeDecision(v.Decision)
	out.Effect = encodeEffect(v.Effect)
	out.Error = v.Error
	return out
}

func (d *decoder) decodeResult(v *Result) *wire.Result {
	if v == nil {
		return nil
	}
	out := &wire.Result{}
	out.ID = v.Id
	out.Decision = d.decodeDecision(v.Decision)
	out.Effect = d.decodeEffect(v.Effect)
	out.Error = v.Error
	return out
}

func encodeHeartbeat(v *wire.Heartbeat) *Heartbeat {
	if v == nil {
		return nil
	}
	out := &Heartbeat{}
	out.CommandId = v.CommandID
	return out
}

func (d *decoder) decodeHeartbeat(v *Heartbeat) *wire.Heartbeat {
	if v == nil {
		return nil
	}
	out := &wire.Heartbeat{}
	out.CommandID = v.CommandId
	return out
}

func encodeHeartbeatReply(v *wire.HeartbeatReply) *HeartbeatReply {
	if v == nil {
		return nil
	}
	out := &HeartbeatReply{}
	out.CommandId = v.CommandID
	out.Cancel = v.Cancel
	out.BudgetNs = int64(v.Budget)
	return out
}

func (d *decoder) decodeHeartbeatReply(v *HeartbeatReply) *wire.HeartbeatReply {
	if v == nil {
		return nil
	}
	out := &wire.HeartbeatReply{}
	out.CommandID = v.CommandId
	out.Cancel = v.Cancel
	out.Budget = time.Duration(v.BudgetNs)
	return out
}

func EncodeRegister(v *wire.Register) *Register { return encodeRegister(v) }
func DecodeRegister(v *Register) (*wire.Register, error) {
	d := new(decoder)
	out := d.decodeRegister(v)
	return out, d.err
}

func EncodeRegistered(v *wire.Registered) *Registered { return encodeRegistered(v) }
func DecodeRegistered(v *Registered) (*wire.Registered, error) {
	d := new(decoder)
	out := d.decodeRegistered(v)
	return out, d.err
}

func EncodeCommand(v *wire.Command) *Command { return encodeCommand(v) }
func DecodeCommand(v *Command) (*wire.Command, error) {
	d := new(decoder)
	out := d.decodeCommand(v)
	return out, d.err
}

func EncodeGateway(v *wire.Gateway) *Gateway { return encodeGateway(v) }
func DecodeGateway(v *Gateway) (*wire.Gateway, error) {
	d := new(decoder)
	out := d.decodeGateway(v)
	return out, d.err
}

func EncodeResult(v *wire.Result) *Result { return encodeResult(v) }
func DecodeResult(v *Result) (*wire.Result, error) {
	d := new(decoder)
	out := d.decodeResult(v)
	return out, d.err
}

func EncodeHeartbeat(v *wire.Heartbeat) *Heartbeat { return encodeHeartbeat(v) }
func DecodeHeartbeat(v *Heartbeat) (*wire.Heartbeat, error) {
	d := new(decoder)
	out := d.decodeHeartbeat(v)
	return out, d.err
}

func EncodeHeartbeatReply(v *wire.HeartbeatReply) *HeartbeatReply { return encodeHeartbeatReply(v) }
func DecodeHeartbeatReply(v *HeartbeatReply) (*wire.HeartbeatReply, error) {
	d := new(decoder)
	out := d.decodeHeartbeatReply(v)
	return out, d.err
}
