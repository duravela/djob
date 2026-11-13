package db

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

type ChannelState string

const (
	ChannelAvailable ChannelState = "available"
	ChannelOwned     ChannelState = "owned"
	ChannelDraining  ChannelState = "draining"
	ChannelBlocked   ChannelState = "blocked"
)

// CommunicationChannel currently covers an entire gateway. Distinct slave
// addresses and connections must use this same ID. Splitting verified independent
// buses requires a future topology configuration change.
type CommunicationChannel struct {
	ID              string
	GatewayID       string
	Protocol        string
	Endpoint        string
	Revision        uint64
	Epoch           uint64
	State           ChannelState
	OwnerID         string // Must identify a process incarnation, not just a host.
	LeaseUntil      time.Time
	ActiveAttemptID string
	LastTransition  time.Time
}

type RunState string

const (
	RunReady     RunState = "ready"
	RunRunning   RunState = "running"
	RunSucceeded RunState = "succeeded"
	RunFailed    RunState = "failed"
	RunUnknown   RunState = "unknown"
	RunCancelled RunState = "cancelled"
	RunExpired   RunState = "expired"
	RunRetryWait RunState = "retry_wait"
)

type Run struct {
	ID                string // Stable operation identity across authorized retries.
	ChannelID         string
	Executor          string
	Payload           []byte
	State             RunState
	AttemptCount      uint64
	ActiveAttemptID   string
	LastAttemptID     string
	CreatedAt         time.Time
	ReadySince        time.Time `json:",omitzero"` // Diagnostic only; zero means unknown.
	LastTransition    time.Time
	ScheduleID        string
	ScheduleRevision  uint64
	ScheduledAt       time.Time
	CancelRequestedAt time.Time
	CancelRequestID   string
	CancelReason      string
	StartDeadline     time.Time // Optional latest start time; does not interrupt an exchange.
	RetryPolicy       *RetryPolicy
	RetryAt           time.Time
}

func (r *Run) CancellationRequested() bool { return r != nil && !r.CancelRequestedAt.IsZero() }
func (r *Run) StartDeadlinePassed(at time.Time) bool {
	return r != nil && !r.StartDeadline.IsZero() && !at.Before(r.StartDeadline)
}

type AttemptOutcome string

const (
	AttemptRunning   AttemptOutcome = "running"
	AttemptSucceeded AttemptOutcome = "succeeded"
	AttemptFailed    AttemptOutcome = "failed"
	AttemptUnknown   AttemptOutcome = "unknown"
	AttemptCancelled AttemptOutcome = "cancelled" // Only when no gateway exchange began.
	AttemptExpired   AttemptOutcome = "expired"   // Only when no gateway exchange began.
)

type Attempt struct {
	ID           string
	RunID        string
	Number       uint64
	OwnerID      string
	Epoch        uint64
	ClaimToken   string
	StartedAt    time.Time
	FinishedAt   time.Time
	Outcome      AttemptOutcome
	Result       []byte
	Error        string
	Quiescent    bool
	AuthorizedAt time.Time  // Committed permission to begin; does not prove delivery.
	NoDelivery   bool       // Owner attests that Exchange was never called.
	RetryClass   RetryClass // Adapter's transient failure and no-request-bytes attestation.
	RetryAt      time.Time  // Planned retry time, retained after promotion for audit.
}

type ConfigureChannel struct {
	ID               string
	GatewayID        string
	Protocol         string
	Endpoint         string
	ExpectedRevision uint64
}

type CreateRun struct {
	ID            string
	ChannelID     string
	Executor      string
	Payload       []byte
	StartDeadline *time.Time   `json:",omitempty"` // Pointer preserves pre-v6 request fingerprints.
	RetryPolicy   *RetryPolicy `json:",omitempty"`
}

type AcquireChannel struct {
	ChannelID     string
	ExpectedEpoch uint64
	OwnerID       string
	LeaseUntil    time.Time
}

type RenewChannel = AcquireChannel

type ChannelOwner struct {
	ChannelID string
	Epoch     uint64
	OwnerID   string
}

type ReleaseChannel struct {
	ChannelOwner
	Quiescent bool
}

type ExpireChannel struct {
	ChannelID string
	Epoch     uint64
}

type ClaimRun struct {
	ChannelOwner
	RunID      string
	AttemptID  string
	ClaimToken string
}

type StartAttempt struct {
	ChannelOwner
	RunID         string
	AttemptID     string
	ClaimToken    string
	DeadlineAware bool `json:",omitempty"`
	RetryAware    bool `json:",omitempty"`
}

type ExpireRun struct {
	RunID         string
	StartDeadline time.Time // Immutable deadline observed by the sweep; compared before expiry.
	RetryAware    bool      `json:",omitempty"`
}

type ReleaseRetry struct {
	RunID     string
	AttemptID string
	RetryAt   time.Time
}

type CancelRun struct {
	RunID  string
	Reason string
}

type FinishAttempt struct {
	ChannelOwner
	RunID             string
	AttemptID         string
	ClaimToken        string
	Outcome           AttemptOutcome
	Result            []byte
	Error             string `json:",omitempty"` // Keep fingerprints of earlier empty-error commands stable.
	Quiescent         bool
	CancellationAware bool       `json:",omitempty"`
	NoDelivery        bool       `json:",omitempty"`
	DeadlineAware     bool       `json:",omitempty"`
	RetryAware        bool       `json:",omitempty"`
	RetryClass        RetryClass `json:",omitempty"`
}

type ResolveChannel struct {
	ChannelID string
	Epoch     uint64
	Actor     string
	Reason    string
	Quiescent bool
}

type ResolveRun struct {
	RunID     string
	AttemptID string   // Resolution is tied to one uncertain attempt.
	State     RunState // Ready explicitly authorizes another attempt of an unknown run.
	Actor     string
	Reason    string
}

// StateCommand contains exactly one operation. All time and identity values are
// chosen before proposal and retained unchanged for retries. The state machine
// never reads clocks, generates tokens, or performs I/O with a gateway.
type StateCommand struct {
	RequestID        string
	At               time.Time
	Configure        *ConfigureChannel
	CreateRun        *CreateRun
	Acquire          *AcquireChannel
	Renew            *RenewChannel
	Drain            *ChannelOwner
	Release          *ReleaseChannel
	Expire           *ExpireChannel
	Claim            *ClaimRun
	Finish           *FinishAttempt
	ResolveChannel   *ResolveChannel
	ResolveRun       *ResolveRun
	PutSchedule      *PutSchedule             `json:",omitempty"`
	PauseSchedule    *PauseSchedule           `json:",omitempty"`
	Materialize      *MaterializeRuns         `json:",omitempty"`
	EnqueueRun       *CreateRun               `json:",omitempty"` // Independent queue insertion; preserves v2 CreateRun replay semantics.
	CancelRun        *CancelRun               `json:",omitempty"`
	StartAttempt     *StartAttempt            `json:",omitempty"`
	ExpireRun        *ExpireRun               `json:",omitempty"`
	ReleaseRetry     *ReleaseRetry            `json:",omitempty"`
	Job              *JobCommand              `json:",omitempty"`
	WorkerEnrollment *WorkerEnrollmentCommand `json:",omitempty"`
	WorkerAdmission  *WorkerAdmission         `json:",omitempty"`
	WorkerDeployment *WorkerDeploymentCommand `json:",omitempty"`
}

func (c StateCommand) Validate() error {
	if !validIdentity(c.RequestID) || c.At.IsZero() {
		return fmt.Errorf("request ID and explicit command time are required")
	}
	count := 0
	for _, present := range []bool{c.Configure != nil, c.CreateRun != nil, c.Acquire != nil,
		c.Renew != nil, c.Drain != nil, c.Release != nil, c.Expire != nil, c.Claim != nil,
		c.Finish != nil, c.ResolveChannel != nil, c.ResolveRun != nil} {
		if present {
			count++
		}
	}
	for _, present := range []bool{c.PutSchedule != nil, c.PauseSchedule != nil, c.Materialize != nil, c.EnqueueRun != nil, c.CancelRun != nil, c.StartAttempt != nil, c.ExpireRun != nil, c.ReleaseRetry != nil, c.Job != nil, c.WorkerEnrollment != nil, c.WorkerDeployment != nil} {
		if present {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("state command requires exactly one operation")
	}
	if c.WorkerAdmission != nil && (c.Job == nil || c.Job.Claim == nil || !validWorkerID(c.WorkerAdmission.WorkerID) || c.WorkerAdmission.CredentialHash == [32]byte{}) {
		return fmt.Errorf("worker admission is valid only on an identified worker claim")
	}
	if c.WorkerAdmission != nil && ((c.WorkerAdmission.DeploymentID == "") != (c.WorkerAdmission.DeploymentGeneration == 0) || c.WorkerAdmission.DeploymentID != "" && (!validWorkerID(c.WorkerAdmission.DeploymentID) || !c.WorkerAdmission.Enrolled)) {
		return fmt.Errorf("invalid managed worker claim precondition")
	}
	if c.Job != nil {
		return c.Job.validateEnvelope()
	}
	if c.WorkerEnrollment != nil {
		return c.WorkerEnrollment.validate()
	}
	if c.WorkerDeployment != nil {
		return c.WorkerDeployment.validate()
	}
	// JSON fingerprints must preserve text exactly; invalid UTF-8 would otherwise
	// be replaced during encoding. Gateway payloads and results remain raw bytes.
	text := []string{c.RequestID}
	ownerText := func(owner ChannelOwner) { text = append(text, owner.ChannelID, owner.OwnerID) }
	switch {
	case c.Configure != nil:
		v := c.Configure
		text = append(text, v.ID, v.GatewayID, v.Protocol, v.Endpoint)
	case c.CreateRun != nil || c.EnqueueRun != nil:
		v := c.CreateRun
		if v == nil {
			v = c.EnqueueRun
		}
		text = append(text, v.ID, v.ChannelID, v.Executor)
	case c.Acquire != nil:
		v := c.Acquire
		text = append(text, v.ChannelID, v.OwnerID)
	case c.Renew != nil:
		v := c.Renew
		text = append(text, v.ChannelID, v.OwnerID)
	case c.Drain != nil:
		ownerText(*c.Drain)
	case c.Release != nil:
		ownerText(c.Release.ChannelOwner)
	case c.Expire != nil:
		text = append(text, c.Expire.ChannelID)
	case c.Claim != nil:
		v := c.Claim
		ownerText(v.ChannelOwner)
		text = append(text, v.RunID, v.AttemptID, v.ClaimToken)
	case c.Finish != nil:
		v := c.Finish
		ownerText(v.ChannelOwner)
		text = append(text, v.RunID, v.AttemptID, v.ClaimToken, string(v.Outcome), v.Error)
		text = append(text, string(v.RetryClass))
	case c.ResolveChannel != nil:
		v := c.ResolveChannel
		text = append(text, v.ChannelID, v.Actor, v.Reason)
	case c.ResolveRun != nil:
		v := c.ResolveRun
		text = append(text, v.RunID, v.AttemptID, string(v.State), v.Actor, v.Reason)
	case c.PutSchedule != nil:
		v := c.PutSchedule.Definition
		text = append(text, v.ID, v.ChannelID, v.Executor, string(v.Spec.Kind), v.Spec.Cron, v.Spec.TimeZone, string(v.MissedPolicy))
	case c.PauseSchedule != nil:
		text = append(text, c.PauseSchedule.ID)
	case c.Materialize != nil:
		text = append(text, c.Materialize.ScheduleID)
	case c.CancelRun != nil:
		text = append(text, c.CancelRun.RunID, c.CancelRun.Reason)
	case c.StartAttempt != nil:
		v := c.StartAttempt
		ownerText(v.ChannelOwner)
		text = append(text, v.RunID, v.AttemptID, v.ClaimToken)
	case c.ExpireRun != nil:
		text = append(text, c.ExpireRun.RunID)
	case c.ReleaseRetry != nil:
		text = append(text, c.ReleaseRetry.RunID, c.ReleaseRetry.AttemptID)
	}
	for _, value := range text {
		if !utf8.ValidString(value) {
			return fmt.Errorf("command text must be valid UTF-8")
		}
	}
	return nil
}

func (c StateCommand) Fingerprint() ([32]byte, error) {
	if err := c.Validate(); err != nil {
		return [32]byte{}, err
	}
	// UTC also removes process-local monotonic clock data.
	c.At = c.At.UTC()
	if c.Job != nil {
		v := c.Job.normalized()
		c.Job = &v
	}
	if c.Acquire != nil {
		v := *c.Acquire
		v.LeaseUntil = v.LeaseUntil.UTC()
		c.Acquire = &v
	}
	if c.Renew != nil {
		v := *c.Renew
		v.LeaseUntil = v.LeaseUntil.UTC()
		c.Renew = &v
	}
	// Gob does not preserve nil versus empty byte slices. Both represent an
	// empty payload, so fingerprint them identically before and after transport.
	if c.CreateRun != nil {
		v := *c.CreateRun
		if len(v.Payload) == 0 {
			v.Payload = nil
		}
		if v.StartDeadline != nil {
			at := v.StartDeadline.UTC()
			v.StartDeadline = &at
		}
		c.CreateRun = &v
	}
	if c.EnqueueRun != nil {
		v := *c.EnqueueRun
		if len(v.Payload) == 0 {
			v.Payload = nil
		}
		if v.StartDeadline != nil {
			at := v.StartDeadline.UTC()
			v.StartDeadline = &at
		}
		c.EnqueueRun = &v
	}
	if c.ExpireRun != nil {
		v := *c.ExpireRun
		v.StartDeadline = v.StartDeadline.UTC()
		c.ExpireRun = &v
	}
	if c.ReleaseRetry != nil {
		v := *c.ReleaseRetry
		v.RetryAt = v.RetryAt.UTC()
		c.ReleaseRetry = &v
	}
	if c.Finish != nil && len(c.Finish.Result) == 0 {
		v := *c.Finish
		v.Result = nil
		c.Finish = &v
	}
	if c.PutSchedule != nil {
		v := *c.PutSchedule
		v.NextAt = v.NextAt.UTC()
		v.Definition.Spec.StartAt = v.Definition.Spec.StartAt.UTC()
		if len(v.Definition.Payload) == 0 {
			v.Definition.Payload = nil
		}
		c.PutSchedule = &v
	}
	if c.Materialize != nil {
		v := *c.Materialize
		v.ExpectedNext = v.ExpectedNext.UTC()
		v.NextAt = v.NextAt.UTC()
		v.SkippedThrough = v.SkippedThrough.UTC()
		v.Occurrences = append([]time.Time(nil), v.Occurrences...)
		for i := range v.Occurrences {
			v.Occurrences[i] = v.Occurrences[i].UTC()
		}
		c.Materialize = &v
	}
	data, err := json.Marshal(c)
	return sha256.Sum256(data), err
}

type CommandRejection struct {
	Code    string
	Message string
}

func (r *CommandRejection) Error() string { return r.Code + ": " + r.Message }

type StateResult struct {
	RequestID        string
	Fingerprint      [32]byte
	Compacted        bool
	Rejection        *CommandRejection
	Channel          *CommunicationChannel
	Run              *Run
	Attempt          *Attempt
	Schedule         *Schedule
	Runs             []Run
	Job              *JobResult
	WorkerEnrollment *WorkerEnrollment
	WorkerDeployment *WorkerDeployment
}

func (r StateResult) Err() error {
	if r.Rejection != nil {
		return r.Rejection
	}
	if r.Compacted {
		return &CommandRejection{Code: ReceiptCompactedCode, Message: "historical job receipt was compacted; acknowledgement only, no execution authority"}
	}
	return nil
}

func (r StateResult) Clone() StateResult {
	if r.WorkerEnrollment != nil {
		v := *r.WorkerEnrollment
		r.WorkerEnrollment = &v
	}
	r.WorkerDeployment = cloneWorkerDeployment(r.WorkerDeployment)
	if r.Job != nil {
		r.Job = r.Job.clone()
	}
	if r.Rejection != nil {
		v := *r.Rejection
		r.Rejection = &v
	}
	if r.Channel != nil {
		v := *r.Channel
		r.Channel = &v
	}
	if r.Run != nil {
		v := *r.Run
		v.Payload = bytes.Clone(v.Payload)
		v.RetryPolicy = v.RetryPolicy.Clone()
		r.Run = &v
	}
	if r.Attempt != nil {
		v := *r.Attempt
		v.Result = bytes.Clone(v.Result)
		r.Attempt = &v
	}
	if r.Schedule != nil {
		v := *r.Schedule
		v.Payload = bytes.Clone(v.Payload)
		v.RetryPolicy = v.RetryPolicy.Clone()
		r.Schedule = &v
	}
	if r.Runs != nil {
		r.Runs = append([]Run(nil), r.Runs...)
		for i := range r.Runs {
			r.Runs[i].Payload = bytes.Clone(r.Runs[i].Payload)
			r.Runs[i].RetryPolicy = r.Runs[i].RetryPolicy.Clone()
		}
	}
	return r
}

func (c StateCommand) CommandVersion() uint32 {
	if c.Job != nil && c.Job.PutDefinition != nil && len(c.Job.PutDefinition.Definition.WorkerReleaseWeights) > 0 {
		return WorkerRoutingCommandVersion
	}
	if c.WorkerDeployment != nil && c.WorkerDeployment.ProtectOutstanding {
		return WorkerLifecycleCommandVersion
	}
	if c.WorkerDeployment != nil && (c.WorkerDeployment.Action == "publish" || c.WorkerDeployment.Action == "retiring" || c.WorkerDeployment.Action == "resume") ||
		c.Job != nil && c.Job.PutDefinition != nil && c.Job.PutDefinition.Definition.WorkerRelease != "" {
		return WorkerReleaseCommandVersion
	}
	if c.WorkerDeployment != nil || c.WorkerAdmission != nil && c.WorkerAdmission.DeploymentID != "" {
		return WorkerDeploymentCommandVersion
	}
	if c.WorkerEnrollment != nil || c.WorkerAdmission != nil {
		return WorkerEnrollmentCommandVersion
	}
	if c.Job != nil {
		if c.Job.RetireConfiguration != nil {
			return JobConfigurationRetirementCommandVersion
		}
		if c.Job.RetireTrigger != nil {
			return JobTriggerRetirementCommandVersion
		}
		if c.Job.CompactConfigurationReceipts != nil {
			return JobConfigurationReceiptCommandVersion
		}
		if c.Job.PutConfigurationLimits != nil {
			return JobConfigurationCommandVersion
		}
		if c.Job.CompactReceipts != nil {
			return JobReceiptCommandVersion
		}
		if c.Job.PutRetention != nil || c.Job.Purge != nil {
			return JobRetentionCommandVersion
		}
		if c.Job.PutLimits != nil {
			return JobQuotaCommandVersion
		}
		if c.Job.Activate != nil || (c.Job.PutTrigger != nil && c.Job.PutTrigger.Definition.Event != nil) {
			return JobActivationCommandVersion
		}
		if c.Job.PutTrigger != nil || c.Job.PauseTrigger != nil || c.Job.Materialize != nil {
			return JobTriggerCommandVersion
		}
		return JobCommandVersion
	}
	if c.ReleaseRetry != nil || (c.CreateRun != nil && c.CreateRun.RetryPolicy != nil) || (c.EnqueueRun != nil && c.EnqueueRun.RetryPolicy != nil) ||
		(c.PutSchedule != nil && c.PutSchedule.Definition.RetryPolicy != nil) || (c.Materialize != nil && c.Materialize.RetryAware) ||
		(c.StartAttempt != nil && c.StartAttempt.RetryAware) || (c.Finish != nil && (c.Finish.RetryAware || c.Finish.RetryClass != "")) || (c.ExpireRun != nil && c.ExpireRun.RetryAware) {
		return RetryCommandVersion
	}
	if c.ExpireRun != nil || (c.CreateRun != nil && c.CreateRun.StartDeadline != nil) || (c.EnqueueRun != nil && c.EnqueueRun.StartDeadline != nil) ||
		(c.PutSchedule != nil && c.PutSchedule.Definition.StartWindow != 0) || (c.Materialize != nil && c.Materialize.DeadlineAware) ||
		(c.StartAttempt != nil && c.StartAttempt.DeadlineAware) || (c.Finish != nil && (c.Finish.DeadlineAware || c.Finish.Outcome == AttemptExpired)) {
		return DeadlineCommandVersion
	}
	if c.CancelRun != nil || c.StartAttempt != nil || (c.Finish != nil && (c.Finish.CancellationAware || c.Finish.NoDelivery || c.Finish.Outcome == AttemptCancelled)) {
		return CancellationCommandVersion
	}
	if c.EnqueueRun != nil {
		return EnqueueCommandVersion
	}
	if c.PutSchedule != nil || c.PauseSchedule != nil || c.Materialize != nil {
		return SchedulingCommandVersion
	}
	return ChannelCommandVersion
}

type Resolution struct {
	RequestID string
	ChannelID string
	RunID     string
	AttemptID string
	Epoch     uint64
	RunState  RunState
	At        time.Time
	Actor     string
	Reason    string
	Quiescent bool
}

func validIdentity(id string) bool {
	return len(id) <= 256 && strings.TrimSpace(id) != "" && utf8.ValidString(id)
}
