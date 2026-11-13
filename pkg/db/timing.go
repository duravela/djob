package db

import "time"

// ScheduleTiming is shared calendar policy, independent of a job or transport.
// Calendar expressions are parsed before proposal; applying replicas use only
// explicit instants and deterministic structural checks.
type ScheduleTiming struct {
	Spec         ScheduleSpec
	MissedPolicy MissedRunPolicy
	MisfireGrace time.Duration
	CatchUpLimit int
}

type TimingCursor struct {
	ScheduleTiming
	ID                string
	Revision          uint64
	NextAt            time.Time
	Paused, Completed bool
	LastTransition    time.Time
}

func (s ScheduleDefinition) Timing() ScheduleTiming {
	return ScheduleTiming{Spec: s.Spec, MissedPolicy: s.MissedPolicy, MisfireGrace: s.MisfireGrace, CatchUpLimit: s.CatchUpLimit}
}

func (s Schedule) TimingCursor() TimingCursor {
	return TimingCursor{ScheduleTiming: s.Timing(), ID: s.ID, Revision: s.Revision, NextAt: s.NextAt, Paused: s.Paused, Completed: s.Completed, LastTransition: s.LastTransition}
}
