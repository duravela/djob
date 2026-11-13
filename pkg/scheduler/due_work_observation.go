package scheduler

import (
	"time"

	"github.com/duravela/djob/pkg/db"
)

// DueWorkObservation measures wall time from a persisted due instant to local
// acknowledgement of ready work. It includes polling, proposal retries and
// acknowledgement delay, but excludes subsequent admission and execution.
// TimeKnown distinguishes a missing origin from a genuine zero duration.
// Negative lateness can indicate clock skew. No job or protocol data is exposed.
type DueWorkObservation struct {
	Lateness  time.Duration
	TimeKnown bool
}

func (p *RetryPromoter) observeReleased(plan db.ReleaseRetry, result db.StateResult) {
	if p.cfg.OnReleased == nil || result.Compacted || result.Run == nil || result.Attempt == nil {
		return
	}
	run, attempt := result.Run, result.Attempt
	if run.ID != plan.RunID || run.State != db.RunReady || run.LastAttemptID != plan.AttemptID ||
		!run.RetryAt.IsZero() || run.ActiveAttemptID != "" || attempt.ID != plan.AttemptID ||
		attempt.RunID != plan.RunID || !attempt.RetryAt.Equal(plan.RetryAt) {
		return
	}
	p.cfg.OnReleased(DueWorkObservation{Lateness: time.Now().Sub(plan.RetryAt), TimeKnown: !plan.RetryAt.IsZero()})
}

func (s *JobMaintenance) observeWoken(plan db.WakeJob, result db.StateResult) {
	if s.cfg.OnWoken == nil || result.Compacted || result.Job == nil || result.Job.Run == nil {
		return
	}
	run := result.Job.Run
	if run.TenantID != plan.TenantID || run.ID != plan.RunID || run.Status != db.JobReady ||
		run.OwnerID != "" || len(run.Resources) != 0 || !run.WakeAt.Equal(plan.WakeAt) {
		return
	}
	s.cfg.OnWoken(DueWorkObservation{Lateness: time.Now().Sub(plan.WakeAt), TimeKnown: !plan.WakeAt.IsZero()})
}
