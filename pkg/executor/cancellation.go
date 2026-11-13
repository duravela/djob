package executor

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Local reads can revoke work, never authorize I/O. StartAttempt separately
// checks cancellation and ownership in a fresh committed transition. One watcher
// belongs to the active run and is joined before the next run can start.
func (e *Executor) watchCancellation(caller context.Context, id string) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(e.life)
	stopCaller := context.AfterFunc(caller, func() { cancel(context.Cause(caller)) })
	if err := caller.Err(); err != nil {
		cancel(context.Cause(caller))
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(e.cfg.CancellationPollInterval)
		defer ticker.Stop()
		for {
			if ctx.Err() != nil {
				return
			}
			run, err := e.backend.GetRun(id)
			if ctx.Err() != nil {
				return
			}
			if err == nil && (run == nil || run.ID != id) {
				err = fmt.Errorf("run inspection does not match active run")
			}
			if err != nil {
				e.fail(errors.Join(ErrOwnershipLost, fmt.Errorf("inspect cancellation: %w", err)))
				return
			}
			if run.CancellationRequested() {
				cancel(ErrCancellationRequested)
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return ctx, func() { stopCaller(); cancel(context.Canceled); <-done }
}
