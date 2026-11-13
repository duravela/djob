package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJobRecordQueriesKeepTenantAndHistoryScopes(t *testing.T) {
	h := newChannelHarness(t)
	for _, tenant := range []string{"alpha", "beta"} {
		h.job(tenant, "group", "job", "driver")
		h.launch(tenant, "job", "r")
	}
	owner := h.claimJob("alpha", "r", "attempt")
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobDoAction, Operation: "write", Input: []byte("private")}}))
	query := JobRecordQuery{Kind: JobActionRecord, TenantID: "alpha", RunID: "r", Sequence: 1}
	record, err := h.db.GetJobRecord(query)
	require.NoError(t, err)
	require.Equal(t, []byte("private"), record.Action.Input)
	query.TenantID = "beta"
	_, err = h.db.GetJobRecord(query)
	require.Error(t, err)
	page, err := h.db.ListJobRecords(JobRecordQuery{Kind: JobActionRecord, TenantID: "beta", RunID: "r"}, nil, 2)
	require.NoError(t, err)
	require.Empty(t, page.Entries)
	_, err = h.db.ListJobRecords(JobRecordQuery{Kind: JobActionRecord, TenantID: "alpha", RunID: "r"}, []byte(jobKey("beta", "r", "x")), 2)
	require.Error(t, err)
	for _, kind := range []JobRecordKind{JobGroupRecord, JobDefinitionRecord, JobBindingRecord, JobRunRecord} {
		page, err := h.db.ListJobRecords(JobRecordQuery{Kind: kind, TenantID: "alpha"}, nil, 1)
		require.NoError(t, err)
		require.Len(t, page.Entries, 1)
		require.NotEmpty(t, page.Entries[0].Cursor)
	}
	_, err = h.db.ListJobRecords(JobRecordQuery{Kind: JobResolutionRecord, TenantID: "alpha"}, nil, 1)
	require.Error(t, err)
}

func TestDispatchDiscoverySeeksPastFullTenantBacklogs(t *testing.T) {
	h := newChannelHarness(t)
	h.job("a", "", "job", "")
	h.job("b", "", "job", "")
	for i := range 50 {
		h.launch("a", "job", fmt.Sprintf("r-%03d", i))
	}
	h.launch("b", "job", "r")
	first, err := h.db.ListDispatchJobs(nil, 1, nil)
	require.NoError(t, err)
	require.Equal(t, "a", first.Entries[0].Record.Run.TenantID)
	other, err := h.db.ListDispatchJobs(first.NextCursor, 1, []string{"a"})
	require.NoError(t, err)
	require.Len(t, other.Entries, 1)
	require.Equal(t, "b", other.Entries[0].Record.Run.TenantID)
	page, err := h.db.ListDispatchJobs(nil, 2, []string{"a", "b"})
	require.NoError(t, err)
	require.Empty(t, page.Entries)
	require.Empty(t, page.NextCursor)
}
