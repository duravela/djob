package store

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/duravela/djob/pkg/commit"
	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func TestStoreMetricsArePrivateAndIncludeOnlySuccessfulApplication(t *testing.T) {
	a, proposals, commits := newTestJobQueue(t)
	b, _, _ := newTestJobQueue(t)
	ra, rb := prometheus.NewPedanticRegistry(), prometheus.NewPedanticRegistry()
	require.NoError(t, a.RegisterMetrics(ra))
	require.NoError(t, b.RegisterMetrics(rb))
	value := func(registry *prometheus.Registry, name string) float64 {
		families, err := registry.Gather()
		require.NoError(t, err)
		for _, family := range families {
			if family.GetName() == name {
				return family.Metric[0].GetCounter().GetValue()
			}
		}
		t.Fatalf("metric %s missing", name)
		return 0
	}
	command := db.StateCommand{RequestID: "metrics-config", At: time.Now().UTC(), Configure: &db.ConfigureChannel{
		ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "private-endpoint:502",
	}}
	reply := proposeStateAsync(a, context.Background(), command)
	data := receiveProposal(t, proposals)
	applyCommit(t, commits, &commit.Commit{Entries: []commit.Entry{{Index: 1, Data: data}}})
	require.NoError(t, receiveStateReply(t, reply).err)
	require.Equal(t, float64(1), value(ra, "jobqueue_raft_proposals_total"))
	require.Equal(t, float64(1), value(ra, "jobqueue_raft_commits_total"))
	require.Zero(t, value(rb, "jobqueue_raft_proposals_total"))
	require.Zero(t, value(rb, "jobqueue_raft_commits_total"))
	applicationError := make(chan error, 1)
	failed := &commit.Commit{Entries: []commit.Entry{{Index: 2, Data: []byte("malformed command")}}, ApplyErrorC: applicationError}
	commits <- failed
	select {
	case <-applicationError:
	case <-time.After(time.Second):
		t.Fatal("malformed entry did not fail application")
	}
	require.Equal(t, float64(1), value(ra, "jobqueue_raft_commits_total"), "failed batches must not count as applied")
	require.NoError(t, a.RegisterMetrics(prometheus.NewRegistry()), "a new registry can collect the same member")
	require.Error(t, a.RegisterMetrics(ra), "duplicate registration is explicit, never silently attaches another store")
}
