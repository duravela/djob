package executor

import (
	"errors"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func TestExecutionClassificationDoesNotInventCompletion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		result  Result
		err     error
		claimed bool
		want    string
	}{
		{"bad retry receipt", Result{Committed: true, Outcome: db.AttemptFailed, RetryAt: time.Now()}, ErrResultUncertain, true, "result_uncertain"},
		{"confirmed despite later authority loss", Result{Committed: true, Outcome: db.AttemptSucceeded}, ErrOwnershipLost, true, "succeeded"},
		{"unconfirmed effect", Result{Outcome: db.AttemptSucceeded}, ErrOwnershipLost, true, "ownership_lost"},
		{"already cancelled", Result{Outcome: db.AttemptCancelled}, ErrRunCancelled, false, "not_admitted"},
		{"already expired", Result{Outcome: db.AttemptExpired}, ErrRunExpired, false, "not_admitted"},
		{"expired acknowledgement", Result{Committed: true, Outcome: db.AttemptExpired}, ErrRunExpired, false, "expired"},
		{"cancel acknowledgement", Result{Committed: true, Outcome: db.AttemptCancelled}, ErrRunCancelled, true, "cancelled"},
		{"preclaim contention", Result{}, &db.CommandRejection{Code: "run_unavailable"}, false, "not_admitted"},
		{"postclaim rejection", Result{}, &db.CommandRejection{Code: "not_found"}, true, "error"},
		{"arbitrary error", Result{}, errors.New("secret tenant device error"), false, "error"},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, executionResult(tc.result, tc.err, tc.claimed)) })
	}
}
