package systemtest

import (
	"context"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func TestMultiNodeWaitForLeader(t *testing.T) {
	clus := newCluster(t, 4)
	defer clus.closeNoErrors(t)

	ctx, cancelFn := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFn()
	jobResults, err := clus.jobQueues[clus.leaderID-1].ProposeCommand(ctx, db.Command{
		Type: db.CmdAddJobs,
		Jobs: []db.Job{{
			ID:   "job-1",
			Data: "Hello, World!",
			TS:   time.Now(),
		}}})
	require.NoError(t, err)
	require.Nil(t, jobResults["job-1"])

	assertCluster(t, clus, jobQueues, hasPendingJobs(1), 5*time.Second)
}
