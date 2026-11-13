package pool

import "time"

type JobResult struct {
	JobID      string
	Group      string
	NodeID     string
	Status     string
	StartedAt  time.Time
	FinishedAt time.Time
	Output     []byte
	Error      string
}
