package jobs

import (
	"time"

	"github.com/duravela/djob/pkg/db"
)

// AdmissionObservation measures launch-to-first-claim age and, for calendar
// jobs, occurrence-to-first-claim lateness at local acknowledgement. Ages use
// wall timestamps, can be negative on clock skew, and do not prove I/O started.
// Known flags distinguish missing timestamps from a genuine zero duration.
// Resumes and rejected claims emit no observation. No identities are included.
type AdmissionObservation struct {
	QueueAge          time.Duration
	QueueTimeKnown    bool
	Scheduled         bool
	ScheduleLateness  time.Duration
	ScheduleTimeKnown bool
}

func (w *Worker) observeAdmission(run *db.JobRun) {
	if w.cfg.OnReadyAdmission != nil {
		w.cfg.OnReadyAdmission(time.Since(run.ReadySince), !run.ReadySince.IsZero())
	}
	if w.cfg.OnAdmission == nil || run.AttemptCount != 1 {
		return
	}
	at := time.Now()
	o := AdmissionObservation{QueueAge: at.Sub(run.CreatedAt), QueueTimeKnown: !run.CreatedAt.IsZero(), Scheduled: run.ScheduledAt != nil}
	if run.ScheduledAt != nil {
		o.ScheduleLateness, o.ScheduleTimeKnown = at.Sub(*run.ScheduledAt), !run.ScheduledAt.IsZero()
	}
	w.cfg.OnAdmission(o)
}
