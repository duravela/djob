package scheduler

import (
	"time"

	"github.com/duravela/djob/pkg/db"
)

// MaterializationObservation measures the wall time from a calendar occurrence
// to the local acknowledgement of its materialization. It includes proposal
// retries and acknowledgement delay; it does not measure execution. Negative
// ages can indicate clock skew. Known distinguishes missing time from zero age.
// The observation contains no tenant, schedule, job or protocol identifiers.
type MaterializationObservation struct {
	Lateness  time.Duration
	TimeKnown bool
}

func (m *Materializer) observeMaterialized(plan db.MaterializeRuns, result db.StateResult) {
	if m.cfg.OnMaterialized == nil || result.Schedule == nil || result.Schedule.ID != plan.ScheduleID ||
		result.Schedule.Revision != plan.Revision || len(result.Runs) != len(plan.Occurrences) {
		return
	}
	// Validate the whole batch before observing any occurrence. A mismatched
	// response must not produce a partially reported success batch.
	for i, run := range result.Runs {
		if run.ScheduleID != plan.ScheduleID || run.ScheduleRevision != plan.Revision || !run.ScheduledAt.Equal(plan.Occurrences[i]) {
			return
		}
	}
	at := time.Now()
	for _, run := range result.Runs {
		m.cfg.OnMaterialized(MaterializationObservation{Lateness: at.Sub(run.ScheduledAt), TimeKnown: !run.ScheduledAt.IsZero()})
	}
}

func (m *JobTriggerMaterializer) observeMaterialized(plan db.MaterializeJobs, result db.StateResult) {
	if m.cfg.OnMaterialized == nil || result.Job == nil || result.Job.Trigger == nil ||
		result.Job.Trigger.TenantID != plan.TenantID || result.Job.Trigger.ID != plan.Plan.ScheduleID ||
		result.Job.Trigger.Revision != plan.Plan.Revision || result.Job.Created != len(plan.Plan.Occurrences) {
		return
	}
	at := time.Now()
	for _, occurrence := range plan.Plan.Occurrences {
		m.cfg.OnMaterialized(MaterializationObservation{Lateness: at.Sub(occurrence), TimeKnown: !occurrence.IsZero()})
	}
}
