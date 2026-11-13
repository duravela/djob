// Package executor serializes gateway transactions through committed channel
// ownership. It provides no physical fencing for legacy gateways.
package executor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/duravela/djob/pkg/db"
)

var (
	ErrClosed                = errors.New("channel executor closed")
	ErrQueueFull             = errors.New("channel executor queue full")
	ErrOwnershipLost         = errors.New("channel ownership is uncertain")
	ErrLeaseExpiring         = errors.New("channel lease safety deadline reached")
	ErrRecovery              = errors.New("channel recovery did not confirm quiescence")
	ErrResultUncertain       = errors.New("attempt result was not durably acknowledged")
	ErrUnknownOutcome        = errors.New("gateway operation outcome is unknown")
	ErrExecutionFailed       = errors.New("gateway operation failed")
	ErrCancellationRequested = errors.New("durable run cancellation requested")
	ErrRunCancelled          = errors.New("run cancelled before gateway exchange")
	ErrRunExpired            = errors.New("run start deadline passed before gateway exchange")
	ErrRetryScheduled        = errors.New("failed attempt has a durable retry scheduled")
)

type Backend interface {
	ProposeStateCommand(context.Context, db.StateCommand) (db.StateResult, error)
	GetChannel(string) (*db.CommunicationChannel, error)
	GetRun(string) (*db.Run, error)
}

type Request struct {
	Channel   db.CommunicationChannel
	RunID     string
	AttemptID string
	Executor  string
	Payload   []byte
}

type Outcome struct {
	Status     db.AttemptOutcome
	Data       []byte
	RetryClass db.RetryClass // Only RetryNotSent is supported; default means no retry.
}

// Transport performs an entire run while its channel is occupied. Exchange must
// return only after all exchanges have stopped. Recover includes protocol quiet
// periods and must succeed before the channel can be reused. Cancellation asks
// the adapter to stop; the executor never detaches a still-running adapter.
type Transport interface {
	Exchange(context.Context, Request) (Outcome, error)
	Recover(context.Context) error
	Close(context.Context) error
}

// Factory allocates a transport without performing I/O. Connection setup belongs
// inside Exchange so every access remains covered by the committed claim.
type Factory func(db.CommunicationChannel) (Transport, error)

type Config struct {
	ChannelID                string
	QueueSize                int
	LeaseDuration            time.Duration
	LeaseSafetyMargin        time.Duration
	RenewInterval            time.Duration
	CommitTimeout            time.Duration
	OperationTimeout         time.Duration
	RecoveryTimeout          time.Duration
	CloseTimeout             time.Duration
	CancellationPollInterval time.Duration
	// OnExecution runs synchronously at the start and return of actual worker
	// execution, including cleanup and result acknowledgement. It must be quick
	// and concurrency-safe across executors. Queued work never started is excluded.
	OnExecution func(ExecutionObservation)
	// OnAdmission observes only a run's first verified claim, including the
	// acknowledgement delay. It must be quick and concurrency-safe.
	OnAdmission func(AdmissionObservation)
	// OnReadyAdmission includes later retry claims. It reports a duration from
	// the latest ready transition and whether that timestamp is known.
	OnReadyAdmission func(time.Duration, bool)
}

func (c *Config) defaults() error {
	if c.ChannelID == "" {
		return fmt.Errorf("channel ID is required")
	}
	if c.QueueSize == 0 {
		c.QueueSize = 16
	}
	if c.LeaseDuration == 0 {
		c.LeaseDuration = 30 * time.Second
	}
	if c.LeaseSafetyMargin == 0 {
		c.LeaseSafetyMargin = c.LeaseDuration / 5
	}
	if c.RenewInterval == 0 {
		c.RenewInterval = c.LeaseDuration / 3
	}
	if c.CommitTimeout == 0 {
		c.CommitTimeout = 2 * time.Second
	}
	if c.OperationTimeout == 0 {
		c.OperationTimeout = 10 * time.Second
	}
	if c.RecoveryTimeout == 0 {
		c.RecoveryTimeout = 3 * time.Second
	}
	if c.CloseTimeout == 0 {
		c.CloseTimeout = 2 * time.Second
	}
	if c.CancellationPollInterval == 0 {
		c.CancellationPollInterval = 50 * time.Millisecond
	}
	if c.QueueSize < 1 || c.LeaseDuration <= 0 || c.LeaseSafetyMargin <= 0 || c.LeaseSafetyMargin >= c.LeaseDuration ||
		c.RenewInterval <= 0 || c.CommitTimeout <= 0 || c.OperationTimeout <= 0 || c.RecoveryTimeout <= 0 || c.CloseTimeout <= 0 || c.CancellationPollInterval <= 0 {
		return fmt.Errorf("executor capacities and timeouts must be positive; safety margin must be shorter than the lease")
	}
	if c.RenewInterval+2*c.CommitTimeout >= c.LeaseDuration-c.LeaseSafetyMargin {
		return fmt.Errorf("renewal interval and commit timeout must fit before the lease safety deadline")
	}
	return nil
}

type Result struct {
	RunID     string
	AttemptID string
	Outcome   db.AttemptOutcome
	Data      []byte
	Quiescent bool
	Committed bool
	RetryAt   time.Time // Nonzero when this failed attempt scheduled another attempt.
}
