package executor

import (
	"time"

	"github.com/duravela/djob/pkg/db"
)

// AdmissionObservation measures launch-to-first-claim age and, for calendar
// runs, occurrence-to-first-claim lateness at local acknowledgement. Ages use
// wall timestamps, can be negative on clock skew, and do not prove I/O started.
// Known flags distinguish missing timestamps from a genuine zero duration.
// Retries and rejected claims emit no observation. No identities are included.
type AdmissionObservation struct {
	QueueAge          time.Duration
	QueueTimeKnown    bool
	Scheduled         bool
	ScheduleLateness  time.Duration
	ScheduleTimeKnown bool
}

func (e *Executor) observeAdmission(run *db.Run) {
	if e.cfg.OnReadyAdmission != nil {
		e.cfg.OnReadyAdmission(time.Since(run.ReadySince), !run.ReadySince.IsZero())
	}
	if e.cfg.OnAdmission == nil || run.AttemptCount != 1 {
		return
	}
	at := time.Now()
	e.cfg.OnAdmission(AdmissionObservation{
		QueueAge: at.Sub(run.CreatedAt), QueueTimeKnown: !run.CreatedAt.IsZero(),
		Scheduled: run.ScheduleID != "", ScheduleLateness: at.Sub(run.ScheduledAt), ScheduleTimeKnown: !run.ScheduledAt.IsZero(),
	})
}
