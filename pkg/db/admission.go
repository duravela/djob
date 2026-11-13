package db

import "errors"

// ErrAdmissionHeld is a transient local refusal before proposal. It has no
// durable receipt and never changes the replicated state machine's decisions.
var ErrAdmissionHeld = errors.New("new work admission is held")

// NeedsAdmission distinguishes new work and execution authorizations from
// cancellation, lease maintenance, result recording and recovery/cleanup.
func (c StateCommand) NeedsAdmission() bool {
	if c.WorkerDeployment != nil {
		return c.WorkerDeployment.Action == "publish" || c.WorkerDeployment.Action == "resume" || c.WorkerDeployment.Action == "put" || c.WorkerDeployment.Action == "restart" || c.WorkerDeployment.Action == "scale" && c.WorkerDeployment.Replicas > 0
	}
	if c.WorkerEnrollment != nil {
		return c.WorkerEnrollment.Action == "enroll" || c.WorkerEnrollment.Action == "rotate" || c.WorkerEnrollment.Action == "resume"
	}
	if j := c.Job; j != nil {
		return j.PutTenant != nil || j.PutGroup != nil || j.PutDefinition != nil || j.PutBinding != nil || j.PutResource != nil || j.Grant != nil || j.PutTrigger != nil || j.Materialize != nil || j.Launch != nil || j.Activate != nil || j.Claim != nil || (j.Advance != nil && j.Advance.Kind == JobDoAction)
	}
	return c.Configure != nil || c.CreateRun != nil || c.EnqueueRun != nil || c.PutSchedule != nil || c.Materialize != nil || c.Acquire != nil || c.Claim != nil || c.StartAttempt != nil || c.ReleaseRetry != nil
}
