package server

import (
	"time"

	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func publicJobMutation(p *apiCredential, q *jp.MutationRequest) (*db.JobCommand, *jp.Reference, error) {
	role, platform, global, callback, producer := APIWriter, false, false, false, false
	switch v := q.Operation.(type) {
	case *jp.MutationRequest_RetireConfiguration:
		if v == nil || v.RetireConfiguration == nil {
			return nil, nil, invalidJobMutation()
		}
		switch v.RetireConfiguration.Kind {
		case jp.Kind_GROUP, jp.Kind_DEFINITION:
		case jp.Kind_BINDING:
			role, platform = APIOperator, true
		default:
			return nil, nil, invalidJobMutation()
		}
	case *jp.MutationRequest_PutTenant, *jp.MutationRequest_PutBinding, *jp.MutationRequest_PutGrant, *jp.MutationRequest_PutLimits, *jp.MutationRequest_PutRetention, *jp.MutationRequest_PutConfigurationLimits:
		role, platform = APIOperator, true
	case *jp.MutationRequest_PutResource, *jp.MutationRequest_ResolveResource:
		role, platform, global = APIOperator, true, true
	case *jp.MutationRequest_ResolveRun, *jp.MutationRequest_Purge, *jp.MutationRequest_CompactReceipts, *jp.MutationRequest_CompactConfigurationReceipts:
		role = APIOperator
	case *jp.MutationRequest_Signal:
		callback = true
	case *jp.MutationRequest_Activate:
		producer = true
	case *jp.MutationRequest_PutGroup, *jp.MutationRequest_PutDefinition, *jp.MutationRequest_Launch, *jp.MutationRequest_Cancel, *jp.MutationRequest_PutTrigger, *jp.MutationRequest_PauseTrigger, *jp.MutationRequest_RetireTrigger:
	default:
		return nil, nil, invalidJobMutation()
	}
	tenant, err := jobScope(p, q.TenantId, role, platform, global, callback, producer)
	if err != nil {
		return nil, nil, err
	}
	ref := &jp.Reference{TenantId: tenant}
	c := &db.JobCommand{}
	switch v := q.Operation.(type) {
	case *jp.MutationRequest_RetireConfiguration:
		x := v.RetireConfiguration // Validated before role and tenant authorization.
		if err := resolutionReason(x.Reason); err != nil {
			return nil, nil, err
		}
		kind := jobKind(x.Kind)
		ref.Kind, ref.Id = jobConfigurationRetirementReference(kind), x.Id
		c.RetireConfiguration = &db.RetireJobConfiguration{TenantID: tenant, Kind: kind, ID: x.Id, ExpectedRevision: x.ExpectedRevision, Actor: p.subject, Reason: x.Reason}
	case *jp.MutationRequest_RetireTrigger:
		if v == nil || v.RetireTrigger == nil {
			return nil, nil, invalidJobMutation()
		}
		x := v.RetireTrigger
		if err := resolutionReason(x.Reason); err != nil {
			return nil, nil, err
		}
		ref.Kind, ref.Id = jp.Kind_TRIGGER_RETIREMENT, x.Id
		c.RetireTrigger = &db.RetireJobTrigger{TenantID: tenant, ID: x.Id, ExpectedRevision: x.ExpectedRevision, Actor: p.subject, Reason: x.Reason}
	case *jp.MutationRequest_CompactConfigurationReceipts:
		if v == nil || v.CompactConfigurationReceipts == nil {
			return nil, nil, invalidJobMutation()
		}
		x := v.CompactConfigurationReceipts
		if err := resolutionReason(x.Reason); err != nil {
			return nil, nil, err
		}
		ref.Kind, ref.Id = jp.Kind_CONFIGURATION_RECEIPT_COMPACTION, x.CompactionId
		c.CompactConfigurationReceipts = &db.CompactJobConfigurationReceipts{TenantID: tenant, ID: x.CompactionId, ExpectedStep: x.ExpectedStep, BatchSize: int(x.BatchSize), Actor: p.subject, Reason: x.Reason}

	case *jp.MutationRequest_PutConfigurationLimits:
		if v == nil || v.PutConfigurationLimits == nil || v.PutConfigurationLimits.Limits == nil {
			return nil, nil, invalidJobMutation()
		}
		x := v.PutConfigurationLimits
		l := x.Limits
		c.PutConfigurationLimits = &db.PutJobConfigurationLimits{ExpectedRevision: x.ExpectedRevision, Limits: db.JobConfigurationLimits{TenantID: tenant, MaxGroups: l.MaxGroups, MaxDefinitions: l.MaxDefinitions, MaxBindings: l.MaxBindings, MaxTriggers: l.MaxTriggers, MaxPayloadBytes: l.MaxPayloadBytes}}
		ref.Kind = jp.Kind_CONFIGURATION_LIMITS
	case *jp.MutationRequest_CompactReceipts:
		if v == nil || v.CompactReceipts == nil {
			return nil, nil, invalidJobMutation()
		}
		x := v.CompactReceipts
		if err := resolutionReason(x.Reason); err != nil {
			return nil, nil, err
		}
		ref.Kind, ref.Id = jp.Kind_RECEIPT_COMPACTION, x.RunId
		c.CompactReceipts = &db.CompactJobReceipts{TenantID: tenant, RunID: x.RunId, ExpectedStep: x.ExpectedStep, BatchSize: int(x.BatchSize), Actor: p.subject, Reason: x.Reason}
	case *jp.MutationRequest_PutRetention:
		if v == nil || v.PutRetention == nil || v.PutRetention.Policy == nil {
			return nil, nil, invalidJobMutation()
		}
		x, d := v.PutRetention, v.PutRetention.Policy
		ref.Kind = jp.Kind_RETENTION
		c.PutRetention = &db.PutJobRetention{ExpectedRevision: x.ExpectedRevision, Policy: db.JobRetentionPolicy{TenantID: tenant, Enabled: d.Enabled, KeepFor: time.Duration(d.KeepForNs)}}
	case *jp.MutationRequest_Purge:
		if v == nil || v.Purge == nil || v.Purge.FinishedBefore == nil || v.Purge.FinishedBefore.CheckValid() != nil {
			return nil, nil, invalidJobMutation()
		}
		x := v.Purge
		if err := resolutionReason(x.Reason); err != nil {
			return nil, nil, err
		}
		ref.Kind, ref.Id = jp.Kind_DELETION, x.RunId
		c.Purge = &db.PurgeJob{TenantID: tenant, RunID: x.RunId, ExpectedStep: x.ExpectedStep, FinishedBefore: x.FinishedBefore.AsTime().UTC(), BatchSize: int(x.BatchSize), Actor: p.subject, Reason: x.Reason}
	case *jp.MutationRequest_PutLimits:
		if v == nil || v.PutLimits == nil || v.PutLimits.Limits == nil {
			return nil, nil, invalidJobMutation()
		}
		x, d := v.PutLimits, v.PutLimits.Limits
		ref.Kind = jp.Kind_LIMITS
		c.PutLimits = &db.PutJobLimits{ExpectedRevision: x.ExpectedRevision, Limits: db.JobLimits{TenantID: tenant, MaxPendingRuns: d.MaxPendingRuns, MaxStoredRuns: d.MaxStoredRuns, MaxInboxEvents: d.MaxInboxEvents, MaxInboxBytes: d.MaxInboxBytes, MaxActivations: d.MaxActivations, MaxActivationBytes: d.MaxActivationBytes}}
	case *jp.MutationRequest_Activate:
		if v == nil || v.Activate == nil {
			return nil, nil, invalidJobMutation()
		}
		x := v.Activate
		if p.role == APIProducer && p.sourceID != x.Source {
			return nil, nil, sourceDenied()
		}
		if x.OccurredAt == nil || x.OccurredAt.CheckValid() != nil || apiID(x.TriggerId) != nil || apiID(x.Source) != nil {
			return nil, nil, invalidJobMutation()
		}
		ref.Kind, ref.Id, ref.TriggerId, ref.Source = jp.Kind_ACTIVATION, x.EventId, x.TriggerId, x.Source
		c.Activate = &db.ActivateJob{TenantID: tenant, TriggerID: x.TriggerId, Source: x.Source, EventID: x.EventId, OccurredAt: x.OccurredAt.AsTime().UTC(), Data: x.Data}
	case *jp.MutationRequest_PutTrigger:
		if v == nil || v.PutTrigger == nil || v.PutTrigger.Definition == nil {
			return nil, nil, invalidJobMutation()
		}
		x, d := v.PutTrigger, v.PutTrigger.Definition
		if (d.Timing == nil) == (d.Event == nil) {
			return nil, nil, invalidJobMutation()
		}
		definition := db.JobTriggerDefinition{TenantID: tenant, ID: d.Id, DefinitionID: d.DefinitionId, DefinitionRevision: d.DefinitionRevision, Paused: d.Paused, MaxPending: int(d.MaxPending)}
		if d.Event != nil {
			definition.Event = &db.EventTriggerSpec{Source: d.Event.Source}
		} else {
			timing := d.Timing
			if timing.StartAt == nil || timing.StartAt.CheckValid() != nil {
				return nil, nil, invalidJobMutation()
			}
			definition.Timing = db.ScheduleTiming{Spec: db.ScheduleSpec{Kind: db.ScheduleKind(timing.Kind), StartAt: timing.StartAt.AsTime().UTC(), Interval: time.Duration(timing.IntervalNs), Cron: timing.Cron, TimeZone: timing.TimeZone}, MissedPolicy: db.MissedRunPolicy(timing.MissedPolicy), MisfireGrace: time.Duration(timing.MisfireGraceNs), CatchUpLimit: int(timing.CatchUpLimit)}
		}
		ref.Kind, ref.Id = jp.Kind_TRIGGER, d.Id
		c.PutTrigger = &db.PutJobTrigger{Definition: definition, ExpectedRevision: x.ExpectedRevision}
	case *jp.MutationRequest_PauseTrigger:
		if v == nil || v.PauseTrigger == nil {
			return nil, nil, invalidJobMutation()
		}
		x := v.PauseTrigger
		ref.Kind, ref.Id = jp.Kind_TRIGGER, x.Id
		c.PauseTrigger = &db.PauseJobTrigger{TenantID: tenant, ID: x.Id, ExpectedRevision: x.ExpectedRevision, Paused: x.Paused}
	case *jp.MutationRequest_PutTenant:
		if v == nil || v.PutTenant == nil {
			return nil, nil, invalidJobMutation()
		}
		x := v.PutTenant
		ref.Kind = jp.Kind_TENANT
		c.PutTenant = &db.PutJobTenant{ExpectedRevision: x.ExpectedRevision, Tenant: db.JobTenant{ID: tenant, MaxRunning: x.MaxRunning}}
	case *jp.MutationRequest_PutGroup:
		if v == nil || v.PutGroup == nil || v.PutGroup.Group == nil {
			return nil, nil, invalidJobMutation()
		}
		x, d := v.PutGroup, v.PutGroup.Group
		ref.Kind, ref.Id = jp.Kind_GROUP, d.Id
		c.PutGroup = &db.PutJobGroup{ExpectedRevision: x.ExpectedRevision, Group: db.JobGroup{TenantID: tenant, ID: d.Id, Runtime: d.Runtime, Resources: d.Resources, MaxRunning: d.MaxRunning}}
	case *jp.MutationRequest_PutDefinition:
		if v == nil || v.PutDefinition == nil || v.PutDefinition.Definition == nil {
			return nil, nil, invalidJobMutation()
		}
		x, d := v.PutDefinition, v.PutDefinition.Definition
		ref.Kind, ref.Id = jp.Kind_DEFINITION, d.Id
		c.PutDefinition = &db.PutJobDefinition{ExpectedRevision: x.ExpectedRevision, Definition: db.JobDefinition{TenantID: tenant, ID: d.Id, GroupID: d.GroupId, Runtime: d.Runtime, Handler: d.Handler, HandlerVersion: d.HandlerVersion, WorkerRelease: d.WorkerRelease, WorkerReleaseWeights: d.WorkerReleaseWeights, Input: d.Input, BindingID: d.BindingId, Resources: d.Resources}}
	case *jp.MutationRequest_PutBinding:
		if v == nil || v.PutBinding == nil || v.PutBinding.Binding == nil {
			return nil, nil, invalidJobMutation()
		}
		x, d := v.PutBinding, v.PutBinding.Binding
		ref.Kind, ref.Id = jp.Kind_BINDING, d.Id
		c.PutBinding = &db.PutJobBinding{ExpectedRevision: x.ExpectedRevision, Binding: db.JobBinding{TenantID: tenant, ID: d.Id, Driver: d.Driver, DriverVersion: d.DriverVersion, Config: d.Config, Resources: d.Resources}}
	case *jp.MutationRequest_PutResource:
		if v == nil || v.PutResource == nil || v.PutResource.Spec == nil {
			return nil, nil, invalidJobMutation()
		}
		x, d := v.PutResource, v.PutResource.Spec
		ref.Kind, ref.Id = jp.Kind_RESOURCE, d.Id
		c.PutResource = &db.PutJobResource{ExpectedRevision: x.ExpectedRevision, Spec: db.ResourceSpec{ID: d.Id, Duplex: d.Duplex, Capacity: d.Capacity, ChannelID: d.ChannelId}}
	case *jp.MutationRequest_PutGrant:
		if v == nil || v.PutGrant == nil {
			return nil, nil, invalidJobMutation()
		}
		x := v.PutGrant
		ref.Kind, ref.Id = jp.Kind_GRANT, x.ResourceId
		c.Grant = &db.PutResourceGrant{ExpectedRevision: x.ExpectedRevision, Grant: db.ResourceGrant{TenantID: tenant, ResourceID: x.ResourceId, Allowed: x.Allowed}}
	case *jp.MutationRequest_Launch:
		if v == nil || v.Launch == nil {
			return nil, nil, invalidJobMutation()
		}
		x := v.Launch
		ref.Kind, ref.Id = jp.Kind_RUN, x.RunId
		c.Launch = &db.LaunchJob{TenantID: tenant, DefinitionID: x.DefinitionId, DefinitionRevision: x.DefinitionRevision, RunID: x.RunId}
	case *jp.MutationRequest_Cancel:
		if v == nil || v.Cancel == nil {
			return nil, nil, invalidJobMutation()
		}
		x := v.Cancel
		ref.Kind, ref.Id = jp.Kind_RUN, x.RunId
		c.Cancel = &db.CancelJob{TenantID: tenant, RunID: x.RunId, Reason: x.Reason}
	case *jp.MutationRequest_Signal:
		if v == nil || v.Signal == nil {
			return nil, nil, invalidJobMutation()
		}
		x := v.Signal
		ref.Kind, ref.RunId, ref.Id = jp.Kind_EVENT, x.RunId, x.EventId
		c.Signal = &db.SignalJob{TenantID: tenant, RunID: x.RunId, EventID: x.EventId, Key: x.Key, Data: x.Data}
	case *jp.MutationRequest_ResolveResource:
		if v == nil || v.ResolveResource == nil {
			return nil, nil, invalidJobMutation()
		}
		x := v.ResolveResource
		ref.Kind, ref.Id = jp.Kind_RESOURCE, x.ResourceId
		if err := resolutionReason(x.Reason); err != nil {
			return nil, nil, err
		}
		c.ResolveResource = &db.ResolveJobResource{ResourceID: x.ResourceId, Epoch: x.ExpectedEpoch, Actor: p.subject, Reason: x.Reason, Quiescent: x.Quiescent}
	case *jp.MutationRequest_ResolveRun:
		if v == nil || v.ResolveRun == nil {
			return nil, nil, invalidJobMutation()
		}
		x := v.ResolveRun
		ref.Kind, ref.Id = jp.Kind_RUN, x.RunId
		if err := resolutionReason(x.Reason); err != nil {
			return nil, nil, err
		}
		c.Resolve = &db.ResolveJob{TenantID: tenant, RunID: x.RunId, ExpectedStep: x.ExpectedStep, Status: db.JobStatus(x.Status), Actor: p.subject, Reason: x.Reason, Result: x.Result}
	}
	if !tenantJobKind(ref.Kind) {
		if err := apiID(ref.Id); err != nil {
			return nil, nil, err
		}
	}
	return c, ref, nil
}

func sourceDenied() error {
	return status.Error(codes.PermissionDenied, "producer credential does not permit this event source")
}
