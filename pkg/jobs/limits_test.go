package jobs

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func TestCallbackInboxQuotaIsRetryableAfterPolicyIncrease(t *testing.T) {
	b := newBackend(t)
	prepare(t, b, "tenant", "", "work", "", nil)
	launch(t, b, "tenant", "work", "run")
	state(t, b, db.JobCommand{PutLimits: &db.PutJobLimits{Limits: db.JobLimits{TenantID: "tenant", MaxInboxBytes: 1}}})
	callback, err := CallbackHandler(b, func(*http.Request) (string, error) { return "tenant", nil })
	require.NoError(t, err)
	data, err := json.Marshal(Callback{RunID: "run", EventID: "reply", Key: "ready", Data: []byte("ab")})
	require.NoError(t, err)
	send := func() int {
		r := httptest.NewRecorder()
		callback.ServeHTTP(r, httptest.NewRequest(http.MethodPost, "/callback", bytes.NewReader(data)))
		return r.Code
	}
	require.Equal(t, http.StatusTooManyRequests, send())
	usage, err := b.GetJobUsage("tenant")
	require.NoError(t, err)
	require.Zero(t, usage.InboxEvents)
	state(t, b, db.JobCommand{PutLimits: &db.PutJobLimits{ExpectedRevision: 1, Limits: db.JobLimits{TenantID: "tenant", MaxInboxBytes: 2}}})
	require.Equal(t, http.StatusAccepted, send())
	require.Equal(t, http.StatusAccepted, send())
	usage, err = b.GetJobUsage("tenant")
	require.NoError(t, err)
	require.Equal(t, uint64(1), usage.InboxEvents)
	require.Equal(t, uint64(2), usage.InboxBytes)
}
