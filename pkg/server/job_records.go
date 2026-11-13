package server

import (
	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
)

func jobKind(kind jp.Kind) db.JobRecordKind {
	switch kind {
	case jp.Kind_GROUP_RETIREMENT:
		return db.JobGroupRetirementRecord
	case jp.Kind_DEFINITION_RETIREMENT:
		return db.JobDefinitionRetirementRecord
	case jp.Kind_BINDING_RETIREMENT:
		return db.JobBindingRetirementRecord
	case jp.Kind_TRIGGER_RETIREMENT:
		return db.JobTriggerRetirementRecord
	case jp.Kind_CONFIGURATION_RECEIPT_COMPACTION:
		return db.JobConfigurationReceiptCompactionRecord
	case jp.Kind_CONFIGURATION_LIMITS:
		return db.JobConfigurationLimitsRecord
	case jp.Kind_CONFIGURATION_USAGE:
		return db.JobConfigurationUsageRecord
	case jp.Kind_RECEIPT_COMPACTION:
		return db.JobReceiptCompactionRecord
	case jp.Kind_DELETION:
		return db.JobDeletionRecord
	case jp.Kind_RETENTION:
		return db.JobRetentionRecord
	case jp.Kind_LIMITS:
		return db.JobLimitsRecord
	case jp.Kind_USAGE:
		return db.JobUsageRecord
	case jp.Kind_ACTIVATION:
		return db.JobActivationRecord
	case jp.Kind_TRIGGER:
		return db.JobTriggerRecord
	case jp.Kind_TENANT:
		return db.JobTenantRecord
	case jp.Kind_GROUP:
		return db.JobGroupRecord
	case jp.Kind_DEFINITION:
		return db.JobDefinitionRecord
	case jp.Kind_BINDING:
		return db.JobBindingRecord
	case jp.Kind_RESOURCE:
		return db.JobResourceRecord
	case jp.Kind_GRANT:
		return db.JobGrantRecord
	case jp.Kind_RUN:
		return db.JobRunRecord
	case jp.Kind_ATTEMPT:
		return db.JobAttemptRecord
	case jp.Kind_ACTION:
		return db.JobActionRecord
	case jp.Kind_EVENT:
		return db.JobEventRecord
	case jp.Kind_RESOLUTION:
		return db.JobResolutionRecord
	}
	return ""
}

func jobConfigurationRetirementReference(kind db.JobRecordKind) jp.Kind {
	switch kind {
	case db.JobGroupRecord:
		return jp.Kind_GROUP_RETIREMENT
	case db.JobDefinitionRecord:
		return jp.Kind_DEFINITION_RETIREMENT
	case db.JobBindingRecord:
		return jp.Kind_BINDING_RETIREMENT
	}
	return jp.Kind_KIND_UNSPECIFIED
}

func genericDefinition(d *db.JobDefinition, summary bool) *jp.Definition {
	if d == nil {
		return nil
	}
	v := &jp.Definition{Id: d.ID, Revision: d.Revision, GroupId: d.GroupID, Runtime: d.Runtime, Handler: d.Handler, HandlerVersion: d.HandlerVersion, WorkerRelease: d.WorkerRelease, WorkerReleaseWeights: d.WorkerReleaseWeights, Input: d.Input, BindingId: d.BindingID, Resources: d.Resources}
	if summary {
		v.Input = nil
	}
	return v
}
func genericGroup(d *db.JobGroup) *jp.Group {
	if d == nil {
		return nil
	}
	return &jp.Group{Id: d.ID, Revision: d.Revision, Runtime: d.Runtime, Resources: d.Resources, MaxRunning: d.MaxRunning}
}
func genericBinding(d *db.JobBinding, summary bool) *jp.Binding {
	if d == nil {
		return nil
	}
	v := &jp.Binding{Id: d.ID, Revision: d.Revision, Driver: d.Driver, DriverVersion: d.DriverVersion, Config: d.Config, Resources: d.Resources}
	if summary {
		v.Config = nil
	}
	return v
}
func genericSpec(d db.ResourceSpec) *jp.ResourceSpec {
	return &jp.ResourceSpec{Id: d.ID, Duplex: d.Duplex, Capacity: d.Capacity, ChannelId: d.ChannelID}
}

func genericRecord(d db.JobRecord, summary bool) *jp.Record {
	r := &jp.Record{Reference: &jp.Reference{}}
	switch {
	case d.ConfigurationRetirement != nil:
		v := d.ConfigurationRetirement
		r.Reference.Kind, r.Reference.TenantId, r.Reference.Id = jobConfigurationRetirementReference(v.Kind), v.TenantID, v.ID
		kind := jp.Kind_KIND_UNSPECIFIED
		switch v.Kind {
		case db.JobGroupRecord:
			kind = jp.Kind_GROUP
		case db.JobDefinitionRecord:
			kind = jp.Kind_DEFINITION
		case db.JobBindingRecord:
			kind = jp.Kind_BINDING
		}
		r.Value = &jp.Record_ConfigurationRetirement{ConfigurationRetirement: &jp.ConfigurationRetirement{Kind: kind, Id: v.ID, Revision: v.Revision, RetiredAt: timestamp(v.RetiredAt), Actor: v.Actor, Reason: v.Reason, GroupMaxRunning: v.GroupMaxRunning}}
	case d.TriggerRetirement != nil:
		v := d.TriggerRetirement
		r.Reference.Kind, r.Reference.TenantId, r.Reference.Id = jp.Kind_TRIGGER_RETIREMENT, v.TenantID, v.ID
		r.Value = &jp.Record_TriggerRetirement{TriggerRetirement: &jp.TriggerRetirement{Id: v.ID, Revision: v.Revision, RetiredAt: timestamp(v.RetiredAt), Actor: v.Actor, Reason: v.Reason}}
	case d.ConfigurationReceiptCompaction != nil:
		v := d.ConfigurationReceiptCompaction
		r.Reference.Kind, r.Reference.TenantId, r.Reference.Id = jp.Kind_CONFIGURATION_RECEIPT_COMPACTION, v.TenantID, v.ID
		r.Value = &jp.Record_ConfigurationReceiptCompaction{ConfigurationReceiptCompaction: &jp.ConfigurationReceiptCompaction{Id: v.ID, Step: v.Step, Actor: v.Actor, Reason: v.Reason, StartedAt: timestamp(v.StartedAt), LastCleanupAt: timestamp(v.LastCleanupAt), ReceiptsCompacted: v.ReceiptsCompacted, PayloadBytesRemoved: v.PayloadBytesRemoved, Complete: v.Complete}}

	case d.ConfigurationLimits != nil:
		v := d.ConfigurationLimits
		r.Reference.Kind, r.Reference.TenantId = jp.Kind_CONFIGURATION_LIMITS, v.TenantID
		r.Value = &jp.Record_ConfigurationLimits{ConfigurationLimits: &jp.ConfigurationLimits{Revision: v.Revision, MaxGroups: v.MaxGroups, MaxDefinitions: v.MaxDefinitions, MaxBindings: v.MaxBindings, MaxTriggers: v.MaxTriggers, MaxPayloadBytes: v.MaxPayloadBytes}}
	case d.ConfigurationUsage != nil:
		v := d.ConfigurationUsage
		r.Reference.Kind, r.Reference.TenantId = jp.Kind_CONFIGURATION_USAGE, v.TenantID
		r.Value = &jp.Record_ConfigurationUsage{ConfigurationUsage: &jp.ConfigurationUsage{Groups: v.Groups, Definitions: v.Definitions, Bindings: v.Bindings, Triggers: v.Triggers, PayloadBytes: v.PayloadBytes}}
	case d.ReceiptCompaction != nil:
		v := d.ReceiptCompaction
		r.Reference.Kind, r.Reference.TenantId, r.Reference.Id = jp.Kind_RECEIPT_COMPACTION, v.TenantID, v.RunID
		r.Value = &jp.Record_ReceiptCompaction{ReceiptCompaction: &jp.ReceiptCompaction{RunId: v.RunID, Step: v.Step, Actor: v.Actor, Reason: v.Reason, StartedAt: timestamp(v.StartedAt), LastCleanupAt: timestamp(v.LastCleanupAt), ReceiptsCompacted: v.ReceiptsCompacted, PayloadBytesRemoved: v.PayloadBytesRemoved, Complete: v.Complete}}
	case d.Retention != nil:
		v := d.Retention
		r.Reference.Kind, r.Reference.TenantId = jp.Kind_RETENTION, v.TenantID
		r.Value = &jp.Record_Retention{Retention: &jp.Retention{Revision: v.Revision, Enabled: v.Enabled, KeepForNs: int64(v.KeepFor)}}
	case d.Deletion != nil:
		v := d.Deletion
		r.Reference.Kind, r.Reference.TenantId, r.Reference.Id = jp.Kind_DELETION, v.TenantID, v.RunID
		r.Value = &jp.Record_Deletion{Deletion: &jp.Deletion{RunId: v.RunID, Step: v.Step, Status: string(v.Status), DefinitionId: v.DefinitionID, DefinitionRevision: v.DefinitionRevision, TriggerId: v.TriggerID, TriggerRevision: v.TriggerRevision, Resources: v.Resources, CreatedAt: timestamp(v.CreatedAt), FinishedAt: timestamp(v.FinishedAt), DeletedAt: timestamp(v.DeletedAt), LastCleanupAt: timestamp(v.LastCleanupAt), Actor: v.Actor, Reason: v.Reason, PolicyRevision: v.PolicyRevision, RecordsDeleted: v.RecordsDeleted, CleanupComplete: v.CleanupComplete}}

	case d.Limits != nil:
		v := d.Limits
		r.Reference.Kind, r.Reference.TenantId = jp.Kind_LIMITS, v.TenantID
		r.Value = &jp.Record_Limits{Limits: &jp.Limits{Revision: v.Revision, MaxPendingRuns: v.MaxPendingRuns, MaxStoredRuns: v.MaxStoredRuns, MaxInboxEvents: v.MaxInboxEvents, MaxInboxBytes: v.MaxInboxBytes, MaxActivations: v.MaxActivations, MaxActivationBytes: v.MaxActivationBytes}}
	case d.Usage != nil:
		v := d.Usage
		r.Reference.Kind, r.Reference.TenantId = jp.Kind_USAGE, v.TenantID
		r.Value = &jp.Record_Usage{Usage: &jp.Usage{PendingRuns: v.PendingRuns, StoredRuns: v.StoredRuns, InboxEvents: v.InboxEvents, InboxBytes: v.InboxBytes, Activations: v.Activations, ActivationBytes: v.ActivationBytes}}
	case d.Activation != nil:
		v := d.Activation
		r.Reference.Kind, r.Reference.TenantId, r.Reference.Id, r.Reference.TriggerId, r.Reference.Source = jp.Kind_ACTIVATION, v.TenantID, v.EventID, v.TriggerID, v.Source
		r.Value = &jp.Record_Activation{Activation: genericActivation(v, summary)}
	case d.Trigger != nil:
		v := d.Trigger
		r.Reference.Kind, r.Reference.TenantId, r.Reference.Id = jp.Kind_TRIGGER, v.TenantID, v.ID
		r.Value = &jp.Record_Trigger{Trigger: genericTrigger(v, summary)}
	case d.Tenant != nil:
		v := d.Tenant
		r.Reference.Kind, r.Reference.TenantId = jp.Kind_TENANT, v.ID
		r.Value = &jp.Record_Tenant{Tenant: &jp.Tenant{Revision: v.Revision, MaxRunning: v.MaxRunning}}
	case d.Group != nil:
		v := d.Group
		r.Reference.Kind, r.Reference.TenantId, r.Reference.Id = jp.Kind_GROUP, v.TenantID, v.ID
		r.Value = &jp.Record_Group{Group: genericGroup(v)}
	case d.Definition != nil:
		v := d.Definition
		r.Reference.Kind, r.Reference.TenantId, r.Reference.Id = jp.Kind_DEFINITION, v.TenantID, v.ID
		r.Value = &jp.Record_Definition{Definition: genericDefinition(v, false)}
	case d.Binding != nil:
		v := d.Binding
		r.Reference.Kind, r.Reference.TenantId, r.Reference.Id = jp.Kind_BINDING, v.TenantID, v.ID
		r.Value = &jp.Record_Binding{Binding: genericBinding(v, false)}
	case d.Resource != nil:
		v := d.Resource
		r.Reference.Kind, r.Reference.Id = jp.Kind_RESOURCE, v.Spec.ID
		r.Value = &jp.Record_Resource{Resource: &jp.Resource{Spec: genericSpec(v.Spec), Revision: v.Revision, Epoch: v.Epoch, State: string(v.State), OwnerTenantId: v.TenantID, RunId: v.RunID, AttemptId: v.AttemptID, OwnerId: v.OwnerID, ChannelEpoch: v.ChannelEpoch, LeaseUntil: timestamp(v.LeaseUntil), LastTransition: timestamp(v.LastTransition)}}
	case d.Grant != nil:
		v := d.Grant
		r.Reference.Kind, r.Reference.TenantId, r.Reference.Id = jp.Kind_GRANT, v.TenantID, v.ResourceID
		r.Value = &jp.Record_Grant{Grant: &jp.Grant{ResourceId: v.ResourceID, Revision: v.Revision, Allowed: v.Allowed}}
	case d.Run != nil:
		v := d.Run
		r.Reference.Kind, r.Reference.TenantId, r.Reference.Id = jp.Kind_RUN, v.TenantID, v.ID
		p := &jp.Run{Id: v.ID, TriggerId: v.TriggerID, TriggerRevision: v.TriggerRevision, Definition: genericDefinition(&v.Definition, summary), Group: genericGroup(v.Group), Binding: genericBinding(v.Binding, summary), Runtime: v.Runtime, Resources: v.Resources, Status: string(v.Status), Step: v.Step, State: v.State, Result: v.Result, Error: v.Error, ActionCount: v.ActionCount, WaitKey: v.WaitKey, WakeAt: timestamp(v.WakeAt), OwnerId: v.OwnerID, AttemptId: v.AttemptID, LastAttemptId: v.LastAttemptID, AttemptCount: v.AttemptCount, LeaseUntil: timestamp(v.LeaseUntil), CancelRequested: v.CancelRequested, CancelRequestedAt: timestamp(v.CancelRequestedAt), CancelReason: v.CancelReason, CreatedAt: timestamp(v.CreatedAt), LastTransition: timestamp(v.LastTransition), Summary: summary}
		p.Activation = genericActivation(v.Activation, summary)
		if v.ScheduledAt != nil {
			p.ScheduledAt = timestamp(*v.ScheduledAt)
		}
		if v.PendingAction != nil {
			p.PendingActionSequence = v.PendingAction.Sequence
		}
		if summary {
			p.State, p.Result = nil, nil
		}
		r.Value = &jp.Record_Run{Run: p}
	case d.Attempt != nil:
		v := d.Attempt
		r.Reference.Kind, r.Reference.TenantId, r.Reference.RunId, r.Reference.Id = jp.Kind_ATTEMPT, v.TenantID, v.RunID, v.ID
		r.Value = &jp.Record_Attempt{Attempt: &jp.Attempt{Id: v.ID, RunId: v.RunID, Number: v.Number, OwnerId: v.OwnerID, Status: string(v.Status), StartedAt: timestamp(v.StartedAt), FinishedAt: timestamp(v.FinishedAt)}}
	case d.Action != nil:
		v := d.Action
		r.Reference.Kind, r.Reference.TenantId, r.Reference.RunId, r.Reference.Sequence = jp.Kind_ACTION, v.TenantID, v.RunID, v.Sequence
		r.Value = &jp.Record_Action{Action: &jp.Action{RunId: v.RunID, Sequence: v.Sequence, AttemptId: v.AttemptID, Operation: v.Operation, Input: v.Input, Status: string(v.Status), Result: v.Result, Error: v.Error, AuthorizedAt: timestamp(v.AuthorizedAt), FinishedAt: timestamp(v.FinishedAt)}}
	case d.Event != nil:
		v := d.Event
		r.Reference.Kind, r.Reference.TenantId, r.Reference.RunId, r.Reference.Id = jp.Kind_EVENT, v.TenantID, v.RunID, v.ID
		r.Value = &jp.Record_Event{Event: &jp.Event{RunId: v.RunID, Id: v.ID, Key: v.Key, Data: v.Data, At: timestamp(v.At), ConsumedAt: timestamp(v.ConsumedAt)}}
	case d.Resolution != nil:
		v := d.Resolution
		r.Reference.Kind, r.Reference.TenantId, r.Reference.Id = jp.Kind_RESOLUTION, v.TenantID, v.RequestID
		r.Value = &jp.Record_Resolution{Resolution: &jp.Resolution{RequestId: v.RequestID, TenantId: v.TenantID, RunId: v.RunID, ResourceId: v.ResourceID, Actor: v.Actor, Reason: v.Reason, Epoch: v.Epoch, Status: string(v.Status), Quiescent: v.Quiescent, At: timestamp(v.At)}}
	}
	return r
}

func genericTrigger(v *db.JobTrigger, summary bool) *jp.Trigger {
	d, t := v.JobTriggerDefinition, v.Timing
	timing := &jp.Timing{Kind: string(t.Spec.Kind), StartAt: timestamp(t.Spec.StartAt), IntervalNs: int64(t.Spec.Interval), Cron: t.Spec.Cron, TimeZone: t.Spec.TimeZone, MissedPolicy: string(t.MissedPolicy), MisfireGraceNs: int64(t.MisfireGrace), CatchUpLimit: uint32(t.CatchUpLimit)}
	definition := &jp.TriggerDefinition{Id: d.ID, DefinitionId: d.DefinitionID, DefinitionRevision: d.DefinitionRevision, Timing: timing, Paused: d.Paused, MaxPending: uint32(d.MaxPending)}
	if d.Event != nil {
		definition.Timing = nil
		definition.Event = &jp.EventTriggerSpec{Source: d.Event.Source}
	}
	return &jp.Trigger{Definition: definition, Revision: v.Revision, JobDefinition: genericDefinition(&v.Template.Definition, summary), Group: genericGroup(v.Template.Group), Binding: genericBinding(v.Template.Binding, summary), Runtime: v.Template.Runtime, Resources: v.Template.Resources, NextAt: timestamp(v.NextAt), Completed: v.Completed, LastTransition: timestamp(v.LastTransition), LastOccurrence: timestamp(v.LastOccurrence), SkippedThrough: timestamp(v.SkippedThrough), Summary: summary}
}

func genericActivation(v *db.JobActivation, summary bool) *jp.Activation {
	if v == nil {
		return nil
	}
	out := &jp.Activation{TriggerId: v.TriggerID, Source: v.Source, EventId: v.EventID, RunId: v.RunID, TriggerRevision: v.TriggerRevision, OccurredAt: timestamp(v.OccurredAt), AcceptedAt: timestamp(v.AcceptedAt), Data: v.Data, Summary: summary, DataDeleted: v.DataDeleted, DataSha256: v.DataSHA256}
	if summary {
		out.Data = nil
	}
	return out
}
