package server

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/duravela/djob/pkg/base"
	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/duravela/djob/pkg/testutils/serverutils"
	"github.com/duravela/djob/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestListJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	client, err := s.ApplicationLayer().GetSchedulerHTTPClient()
	require.NoError(t, err)
	response, err := client.Get(s.ApplicationLayer().SchedulerURL().String() + "/api/v1/jobs/")
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	data, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	var jobs pb.ListJobsResponse
	require.NoError(t, protojson.Unmarshal(data, &jobs))
	require.Empty(t, jobs.Jobs)
}
