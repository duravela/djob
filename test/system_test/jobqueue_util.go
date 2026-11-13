package systemtest

import (
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"

	"github.com/duravela/djob/pkg/store"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func newJobQueue(t *testing.T, proposec chan<- []byte, commitc <-chan *commit.Commit, snapshotter *snap.Snapshotter) *store.Store {
	jq, err := store.NewStore(
		store.StoreConfig{
			DataDir:         t.TempDir(),
			ProposalTimeout: 30 * time.Second,
			Logger:          zaptest.NewLogger(t),
		},
		proposec,
		commitc,
		snapshotter,
	)
	if err != nil {
		t.Fatal("Could not initialize jobQueue!")
	}
	require.NoError(t, jq.Start())

	return jq
}
