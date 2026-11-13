package jobs

import (
	"time"

	"github.com/duravela/djob/pkg/db"
)

// EventConsumptionObservation measures wall time from an inbox event's persisted
// At timestamp through local acknowledgement of the decision that consumes it.
// It includes any time queued before a wait, admission/resume, handler execution,
// cleanup before completion and identical proposal retries. It is not time since
// the latest ready transition or evidence that subsequent driver I/O started.
// Missing timestamps and negative delays are preserved as diagnostics. No event,
// tenant, job, protocol or application data is included.
type EventConsumptionObservation struct {
	Delay     time.Duration
	TimeKnown bool
}

func (e *execution) observeEventConsumed(before *db.JobRun, event *db.JobEvent, result db.StateResult, at time.Time) {
	observe := e.w.cfg.OnEventConsumed
	if observe == nil || event == nil || before == nil || result.Compacted || result.Job == nil || result.Job.Run == nil {
		return
	}
	after := result.Job.Run
	if event.TenantID != e.owner.TenantID || event.RunID != e.owner.RunID || event.ID == "" ||
		event.Key == "" || event.Key != before.WaitKey || !event.ConsumedAt.IsZero() ||
		after.TenantID != before.TenantID || after.ID != before.ID ||
		after.Step <= before.Step || after.Step-before.Step != 1 {
		return
	}
	observe(EventConsumptionObservation{Delay: at.Sub(event.At), TimeKnown: !event.At.IsZero()})
}
