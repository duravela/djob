package server

import (
	"net/http"
	"strings"
	"testing"

	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestWorkerReleaseRunInspectionScopesPagesAndDoesNotWrite(t *testing.T) {
	s, httpClient := workerAPI(t)
	remotePrepare(t, s)
	token := "djobw1." + strings.Repeat("d", 64)
	enrollmentAPI(t, s, httpClient, "company-bus", "enroll", 0, token)
	managedDeploymentAPI(t, s, httpClient, "put", 0, 1)
	managedDeploymentAPI(t, s, httpClient, "publish", 1, 0)
	remoteSeed(t, s, db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "alpha", ID: "pinned", Runtime: db.StepRuntime, Handler: "test", HandlerVersion: "1", WorkerRelease: "line-one"}}})
	for _, id := range []string{"run-a", "run-b", "run-c"} {
		remoteLaunch(t, s, "alpha", "pinned", id)
	}
	remoteLaunch(t, s, "alpha", "compute", "unpinned")
	conn, err := grpc.NewClient(s.RPCAddr(), grpc.WithTransportCredentials(credentials.NewTLS(httpClient.Transport.(*http.Transport).TLSClientConfig.Clone())))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := jp.NewJobsClient(conn)
	before := s.application.Store.LastAppliedIndex()
	query := &jp.ListRequest{Kind: jp.Kind_RUN, TenantId: "alpha", WorkerRelease: "line-one", PageSize: 1}
	page, err := client.List(genericContext("alpha/reader"), query)
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	require.NotEmpty(t, page.NextPageToken)
	require.True(t, page.Records[0].GetRun().Summary)
	require.Equal(t, "line-one", page.Records[0].GetRun().Definition.WorkerRelease)
	firstToken := page.NextPageToken
	var ids []string
	for {
		for _, record := range page.Records {
			ids = append(ids, record.Reference.Id)
			require.Equal(t, "alpha", record.Reference.TenantId)
			require.Equal(t, "line-one", record.GetRun().Definition.WorkerRelease)
		}
		if page.NextPageToken == "" {
			break
		}
		query.PageToken = page.NextPageToken
		page, err = client.List(genericContext("alpha/reader"), query)
		require.NoError(t, err)
	}
	require.ElementsMatch(t, []string{"run-a", "run-b", "run-c"}, ids)
	require.Equal(t, before, s.application.Store.LastAppliedIndex())
	_, err = client.List(genericContext("beta/reader"), query)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	for _, q := range []*jp.ListRequest{
		{Kind: jp.Kind_RUN, TenantId: "alpha", WorkerRelease: "different", PageToken: firstToken},
		{Kind: jp.Kind_RUN, TenantId: "alpha", PageToken: firstToken},
		{Kind: jp.Kind_RUN, TenantId: "alpha", WorkerRelease: "line-one", ReadyOnly: true},
		{Kind: jp.Kind_DEFINITION, TenantId: "alpha", WorkerRelease: "line-one"},
	} {
		_, err = client.List(genericContext("alpha/reader"), q)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	query.PageToken = ""
	managedDeploymentAPI(t, s, httpClient, "retiring", 2, 0)
	page, err = client.List(genericContext("alpha/reader"), query)
	require.NoError(t, err)
	require.NotEmpty(t, page.Records, "retiring releases remain inspectable")
	remoteSeed(t, s, db.JobCommand{Cancel: &db.CancelJob{TenantID: "alpha", RunID: "run-a", Reason: "withdraw queued run"}})
	page, err = client.List(genericContext("alpha/reader"), query)
	require.NoError(t, err)
	require.NotEqual(t, "run-a", page.Records[0].Reference.Id)
}
