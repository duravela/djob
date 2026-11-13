package db

import (
	"bytes"
	"fmt"
	"maps"
	"reflect"
	"time"
	"unicode/utf8"

	djob "github.com/duravela/djob/sdk/go"
)

// Public job values belong to the SDK. Aliases preserve existing engine APIs
// and serialized field shapes; catalog records and durable commands stay here.
type (
	JobGroup      = djob.JobGroup
	JobDefinition = djob.JobDefinition
	JobBinding    = djob.JobBinding
	JobStatus     = djob.JobStatus
	EffectStatus  = djob.EffectStatus
	JobAction     = djob.JobAction
	JobRun        = djob.JobRun
	JobEvent      = djob.JobEvent
)

const (
	StepRuntime      = djob.StepRuntime
	JobReady         = djob.JobReady
	JobRunning       = djob.JobRunning
	JobWaiting       = djob.JobWaiting
	JobSucceeded     = djob.JobSucceeded
	JobFailed        = djob.JobFailed
	JobCancelled     = djob.JobCancelled
	JobUnknown       = djob.JobUnknown
	EffectAuthorized = djob.EffectAuthorized
	EffectSucceeded  = djob.EffectSucceeded
	EffectFailed     = djob.EffectFailed
	EffectUnknown    = djob.EffectUnknown
)

type JobTenant struct {
	ID         string
	Revision   uint64
	MaxRunning uint64 // Zero means no configured limit.
}

type ResourceState string

const (
	ResourceAvailable ResourceState = "available"
	ResourceOwned     ResourceState = "owned"
	ResourceBlocked   ResourceState = "blocked"
)

// ResourceSpec describes a canonical, cluster-wide contention boundary.
// The initial profile is exclusive, including full duplex links.
type ResourceSpec struct {
	ID        string
	Duplex    string // "none", "half", or "full"; a catalog fact, not group policy.
	Capacity  uint32 // Currently exactly one. Other admission profiles can follow.
	ChannelID string // Optional bridge to the existing gateway authority; equals ID.
}

type JobResource struct {
	Spec           ResourceSpec
	Revision       uint64
	Epoch          uint64
	State          ResourceState
	TenantID       string
	RunID          string
	AttemptID      string
	OwnerID        string
	ChannelEpoch   uint64
	LeaseUntil     time.Time
	LastTransition time.Time
}

type ResourceGrant struct {
	TenantID   string
	ResourceID string
	Revision   uint64
	Allowed    bool
}

type JobAttempt struct {
	TenantID   string
	RunID      string
	ID         string
	Number     uint64
	OwnerID    string
	Status     JobStatus
	StartedAt  time.Time
	FinishedAt time.Time
}

type PutJobTenant struct {
	Tenant           JobTenant
	ExpectedRevision uint64
}
type PutJobGroup struct {
	Group            JobGroup
	ExpectedRevision uint64
}
type PutJobDefinition struct {
	Definition       JobDefinition
	ExpectedRevision uint64
}
type PutJobBinding struct {
	Binding          JobBinding
	ExpectedRevision uint64
}
type PutJobResource struct {
	Spec             ResourceSpec
	ExpectedRevision uint64
}
type PutResourceGrant struct {
	Grant            ResourceGrant
	ExpectedRevision uint64
}
type LaunchJob struct {
	TenantID, DefinitionID, RunID string
	DefinitionRevision            uint64
}

type JobOwner struct{ TenantID, RunID, AttemptID, OwnerID string }
type ClaimJob struct {
	JobOwner
	LeaseUntil time.Time
}
type RenewJob = ClaimJob
type YieldJob struct{ JobOwner }

type JobDecisionKind = djob.JobDecisionKind

const (
	JobDoAction   = djob.JobDoAction
	JobCheckpoint = djob.JobCheckpoint
	JobWait       = djob.JobWait
	JobComplete   = djob.JobComplete
)

// AdvanceJob atomically consumes an event/previous result, checkpoints logical
// state, and authorizes the next action, wait, or completion. Actions cannot be
// authorized while a previous action's result is uncertain.
type AdvanceJob struct {
	JobOwner
	ExpectedStep uint64
	Kind         JobDecisionKind
	State        []byte `json:",omitempty"`
	EventID      string
	Operation    string
	Input        []byte `json:",omitempty"`
	WaitKey      string
	WakeAt       time.Time
	Status       JobStatus
	Result       []byte `json:",omitempty"`
	Error        string
	Quiescent    bool // Required to release reservations on completion.
}

type CompleteJobAction struct {
	JobOwner
	Sequence uint64
	Status   EffectStatus
	Result   []byte `json:",omitempty"`
	Error    string
}

type SignalJob struct {
	TenantID, RunID, EventID, Key string
	Data                          []byte `json:",omitempty"`
}
type CancelJob struct{ TenantID, RunID, Reason string }
type ExpireJob struct {
	JobOwner
	LeaseUntil time.Time
}
type WakeJob struct {
	TenantID, RunID string
	WakeAt          time.Time
}
type ResolveJobResource struct {
	ResourceID    string
	Epoch         uint64
	Actor, Reason string
	Quiescent     bool
}
type ResolveJob struct {
	TenantID, RunID string
	ExpectedStep    uint64
	Status          JobStatus
	Actor, Reason   string
	Result          []byte `json:",omitempty"`
}

type JobResolution struct {
	RequestID, TenantID, RunID, ResourceID, Actor, Reason string
	Epoch                                                 uint64
	Status                                                JobStatus
	Quiescent                                             bool
	At                                                    time.Time
}

// JobCommand contains generic execution, activation, admission and retention
// operations. Exactly one member is set. All identities and timestamps are
// chosen before proposing to Raft.
type JobCommand struct {
	RetireConfiguration          *RetireJobConfiguration          `json:",omitempty"`
	RetireTrigger                *RetireJobTrigger                `json:",omitempty"`
	CompactConfigurationReceipts *CompactJobConfigurationReceipts `json:",omitempty"`
	PutConfigurationLimits       *PutJobConfigurationLimits       `json:",omitempty"`
	CompactReceipts              *CompactJobReceipts              `json:",omitempty"`
	PutRetention                 *PutJobRetention                 `json:",omitempty"`
	Purge                        *PurgeJob                        `json:",omitempty"`
	PutLimits                    *PutJobLimits                    `json:",omitempty"`
	Activate                     *ActivateJob                     `json:",omitempty"`
	PutTrigger                   *PutJobTrigger                   `json:",omitempty"`
	PauseTrigger                 *PauseJobTrigger                 `json:",omitempty"`
	Materialize                  *MaterializeJobs                 `json:",omitempty"`
	PutTenant                    *PutJobTenant                    `json:",omitempty"`
	PutGroup                     *PutJobGroup                     `json:",omitempty"`
	PutDefinition                *PutJobDefinition                `json:",omitempty"`
	PutBinding                   *PutJobBinding                   `json:",omitempty"`
	PutResource                  *PutJobResource                  `json:",omitempty"`
	Grant                        *PutResourceGrant                `json:",omitempty"`
	Launch                       *LaunchJob                       `json:",omitempty"`
	Claim                        *ClaimJob                        `json:",omitempty"`
	Renew                        *RenewJob                        `json:",omitempty"`
	Yield                        *YieldJob                        `json:",omitempty"`
	Advance                      *AdvanceJob                      `json:",omitempty"`
	CompleteAction               *CompleteJobAction               `json:",omitempty"`
	Signal                       *SignalJob                       `json:",omitempty"`
	Cancel                       *CancelJob                       `json:",omitempty"`
	Expire                       *ExpireJob                       `json:",omitempty"`
	Wake                         *WakeJob                         `json:",omitempty"`
	ResolveResource              *ResolveJobResource              `json:",omitempty"`
	Resolve                      *ResolveJob                      `json:",omitempty"`
}

type JobResult struct {
	ConfigurationRetirement        *JobConfigurationRetirement        `json:",omitempty"`
	TriggerRetirement              *JobTriggerRetirement              `json:",omitempty"`
	ConfigurationReceiptCompaction *JobConfigurationReceiptCompaction `json:",omitempty"`
	ConfigurationLimits            *JobConfigurationLimits            `json:",omitempty"`
	ReceiptCompaction              *JobReceiptCompaction              `json:",omitempty"`
	Retention                      *JobRetentionPolicy                `json:",omitempty"`
	Deletion                       *JobDeletion                       `json:",omitempty"`
	Limits                         *JobLimits                         `json:",omitempty"`
	Activation                     *JobActivation                     `json:",omitempty"`
	Trigger                        *JobTrigger                        `json:",omitempty"`
	Created                        int                                `json:",omitempty"`
	Tenant                         *JobTenant
	Group                          *JobGroup
	Definition                     *JobDefinition
	Binding                        *JobBinding
	Resource                       *JobResource
	Grant                          *ResourceGrant
	Run                            *JobRun
	Action                         *JobAction
	Event                          *JobEvent
}

// All command text must round-trip through JSON fingerprints losslessly; raw
// bytes remain arbitrary data. No interfaces or maps are part of this schema.
func (c JobCommand) validateEnvelope() error {
	v := reflect.ValueOf(c)
	count := 0
	for i := 0; i < v.NumField(); i++ {
		if !v.Field(i).IsNil() {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("job command requires exactly one operation")
	}
	var validText func(reflect.Value) bool
	validText = func(v reflect.Value) bool {
		switch v.Kind() {
		case reflect.Pointer:
			return v.IsNil() || validText(v.Elem())
		case reflect.Struct:
			if v.Type() == reflect.TypeOf(time.Time{}) {
				return true
			}
			for i := 0; i < v.NumField(); i++ {
				if !validText(v.Field(i)) {
					return false
				}
			}
		case reflect.Slice:
			if v.Type().Elem().Kind() == reflect.Uint8 {
				return true
			}
			for i := 0; i < v.Len(); i++ {
				if !validText(v.Index(i)) {
					return false
				}
			}
		case reflect.String:
			return utf8.ValidString(v.String())
		}
		return true
	}
	if !validText(v) {
		return fmt.Errorf("command text must be valid UTF-8")
	}
	return nil
}

func (c JobCommand) normalized() JobCommand {
	if c.Purge != nil {
		v := *c.Purge
		v.FinishedBefore = v.FinishedBefore.UTC()
		c.Purge = &v
	}
	if c.Activate != nil {
		v := *c.Activate
		v.OccurredAt = v.OccurredAt.UTC()
		c.Activate = &v
	}
	if c.PutTrigger != nil {
		v := *c.PutTrigger
		v.Definition.Timing.Spec.StartAt = v.Definition.Timing.Spec.StartAt.UTC()
		v.NextAt = v.NextAt.UTC()
		c.PutTrigger = &v
	}
	if c.Materialize != nil {
		v := *c.Materialize
		v.Plan.ExpectedNext = v.Plan.ExpectedNext.UTC()
		v.Plan.NextAt = v.Plan.NextAt.UTC()
		v.Plan.SkippedThrough = v.Plan.SkippedThrough.UTC()
		v.Plan.Occurrences = append([]time.Time(nil), v.Plan.Occurrences...)
		for i := range v.Plan.Occurrences {
			v.Plan.Occurrences[i] = v.Plan.Occurrences[i].UTC()
		}
		c.Materialize = &v
	}

	if c.Claim != nil {
		v := *c.Claim
		v.LeaseUntil = v.LeaseUntil.UTC()
		c.Claim = &v
	}
	if c.Renew != nil {
		v := *c.Renew
		v.LeaseUntil = v.LeaseUntil.UTC()
		c.Renew = &v
	}
	if c.Expire != nil {
		v := *c.Expire
		v.LeaseUntil = v.LeaseUntil.UTC()
		c.Expire = &v
	}
	if c.Wake != nil {
		v := *c.Wake
		v.WakeAt = v.WakeAt.UTC()
		c.Wake = &v
	}
	if c.Advance != nil {
		v := *c.Advance
		v.WakeAt = v.WakeAt.UTC()
		c.Advance = &v
	}
	return c
}

func (r *JobResult) clone() *JobResult {
	out := *r
	out.ConfigurationRetirement = jobCopy(r.ConfigurationRetirement)
	out.TriggerRetirement = jobCopy(r.TriggerRetirement)
	out.ConfigurationReceiptCompaction = jobCopy(r.ConfigurationReceiptCompaction)
	out.ConfigurationLimits = jobCopy(r.ConfigurationLimits)
	out.ReceiptCompaction = jobCopy(r.ReceiptCompaction)
	out.Trigger = cloneJobTrigger(r.Trigger)
	out.Limits = jobCopy(r.Limits)
	out.Retention = jobCopy(r.Retention)
	out.Deletion = cloneJobDeletion(r.Deletion)
	out.Activation = cloneJobActivation(r.Activation)
	out.Tenant = jobCopy(r.Tenant)
	out.Group = cloneJobGroup(r.Group)
	out.Definition = cloneJobDefinition(r.Definition)
	out.Binding = cloneJobBinding(r.Binding)
	out.Resource = jobCopy(r.Resource)
	out.Grant = jobCopy(r.Grant)
	out.Action = cloneJobAction(r.Action)
	out.Event = jobCopy(r.Event)
	if out.Event != nil {
		out.Event.Data = bytes.Clone(r.Event.Data)
	}
	out.Run = jobCopy(r.Run)
	if out.Run != nil {
		out.Run.Definition = *cloneJobDefinition(&r.Run.Definition)
		out.Run.Group = cloneJobGroup(r.Run.Group)
		out.Run.Binding = cloneJobBinding(r.Run.Binding)
		out.Run.Resources = append([]string(nil), r.Run.Resources...)
		out.Run.State = bytes.Clone(r.Run.State)
		out.Run.Result = bytes.Clone(r.Run.Result)
		out.Run.PendingAction = cloneJobAction(r.Run.PendingAction)
		out.Run.ScheduledAt = jobCopy(r.Run.ScheduledAt)
		out.Run.Activation = cloneJobActivation(r.Run.Activation)
	}
	return &out
}

func jobCopy[T any](v *T) *T {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}
func cloneJobDefinition(v *JobDefinition) *JobDefinition {
	out := jobCopy(v)
	if out != nil {
		out.Input = bytes.Clone(v.Input)
		out.WorkerReleaseWeights = maps.Clone(v.WorkerReleaseWeights)
		out.Resources = append([]string(nil), v.Resources...)
	}
	return out
}
func cloneJobGroup(v *JobGroup) *JobGroup {
	out := jobCopy(v)
	if out != nil {
		out.Resources = append([]string(nil), v.Resources...)
	}
	return out
}
func cloneJobBinding(v *JobBinding) *JobBinding {
	out := jobCopy(v)
	if out != nil {
		out.Config = bytes.Clone(v.Config)
		out.Resources = append([]string(nil), v.Resources...)
	}
	return out
}
func cloneJobAction(v *JobAction) *JobAction {
	out := jobCopy(v)
	if out != nil {
		out.Input = bytes.Clone(v.Input)
		out.Result = bytes.Clone(v.Result)
	}
	return out
}
