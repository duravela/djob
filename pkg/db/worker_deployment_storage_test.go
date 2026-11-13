//go:build darwin || linux

package db

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestWorkerDeploymentSurvivesSnapshotRecoveryAndPhysicalCompaction(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "", "work", "")
	h.ok(enrollmentCommand("worker-one", "enroll", 0, "private-secret"))
	h.ok(deploymentCommand("line-one", "put", 0))
	expected := h.ok(deploymentCommand("line-one", "restart", 1)).WorkerDeployment
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	path := filepath.Join(t.TempDir(), "restored.db")
	restored, err := NewDB(path)
	require.NoError(t, err)
	require.NoError(t, restored.RestoreAt(bytes.NewReader(snapshot.Bytes()), h.index))
	require.NoError(t, restored.ActivateRecoveryFormat())
	actual, err := restored.GetWorkerDeployment("alpha", "line-one")
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	require.NoError(t, restored.Close())
	compacted, err := CompactFile(context.Background(), path, FileCompactionOptions{})
	require.NoError(t, err)
	require.Equal(t, byte(14), compacted.Format)
	restored, err = NewDB(path)
	require.NoError(t, err)
	actual, err = restored.GetWorkerDeployment("alpha", "line-one")
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	foreign, err := restored.GetWorkerDeployment("beta", "line-one")
	require.ErrorContains(t, err, "not_found")
	require.Nil(t, foreign)
	require.NoError(t, restored.db.Update(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{14}, tx.Bucket(metadataBucket).Get(formatKey))
		return tx.Bucket(metadataBucket).Delete(workerDeploymentVersionKey)
	}))
	require.NoError(t, restored.Close())
	_, err = NewDB(path)
	require.ErrorContains(t, err, "worker deployment marker")
}
