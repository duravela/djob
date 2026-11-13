package server

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	djob "github.com/duravela/djob/sdk/go"
	"github.com/duravela/djob/sdk/go/worker"
	"github.com/stretchr/testify/require"
)

func TestWorkerReleasesRouteSameCapabilitiesAndResumeWhileRetiring(t *testing.T) {
	s, client := workerAPI(t)
	remotePrepare(t, s)
	mutate := func(id, action string, revision uint64, want int) jp.WorkerDeployment {
		t.Helper()
		nonce, err := workerNonce()
		require.NoError(t, err)
		q := jp.WorkerDeploymentMutation{Command: &jp.WorkerManagementCommand{RequestID: nonce, At: time.Now().UTC()}, TenantID: "alpha", ID: id, WorkerID: id, Action: action, ExpectedRevision: revision, Reason: "release lifecycle"}
		if action == "put" {
			q.Spec = &jp.WorkerDeploymentSpec{Image: "registry.example/acme/worker@sha256:" + strings.Repeat(map[string]string{"v1": "a", "v2": "b"}[id], 64), ConfigSecret: "config-" + id, Replicas: 1}
		}
		var ack jp.WorkerDeploymentAcknowledgement
		workerPost(t, client, s, genericToken("ops/alice"), "/api/jobs/v1/worker-deployments/mutate", q, &ack, want)
		return ack.Deployment
	}
	for i, id := range []string{"v1", "v2"} {
		token := "djobw1." + strings.Repeat(fmt.Sprint(i+1), 64)
		enrollmentAPI(t, s, client, id, "enroll", 0, token)
		mutate(id, "put", 0, 200)
		mutate(id, "publish", 1, 200)
		remoteSeed(t, s, db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "alpha", ID: id, Runtime: db.StepRuntime, Handler: "same-handler", HandlerVersion: "1", WorkerRelease: id, Input: []byte(id)}}})
		release := id
		registry := djob.Registry{Handlers: map[djob.Version]djob.Handler{{Name: "same-handler", Version: "1"}: func(view djob.View) (djob.Decision, error) {
			if view.Run.Definition.WorkerRelease != release {
				return djob.Decision{}, fmt.Errorf("wrong release %s executed %s", release, view.Run.Definition.WorkerRelease)
			}
			if release == "v1" && len(view.Run.State) == 0 {
				return djob.Decision{Kind: djob.JobWait, WaitKey: "continue", State: []byte("waiting")}, nil
			}
			return djob.Decision{Kind: djob.JobComplete, Status: djob.JobSucceeded, Result: []byte(release)}, nil
		}}}
		runRemoteWorker(t, s, client, "alpha", registry, func(cfg *worker.Config) {
			cfg.WorkerID, cfg.Token, cfg.DeploymentID, cfg.DeploymentGeneration = release, token, release, 1
		})
	}
	remoteLaunch(t, s, "alpha", "v1", "old-run")
	remoteState(t, s, "alpha", "old-run", db.JobWaiting)
	// Definitions can move independently; the old run keeps its immutable release.
	remoteSeed(t, s, db.JobCommand{PutDefinition: &db.PutJobDefinition{ExpectedRevision: 1, Definition: db.JobDefinition{TenantID: "alpha", ID: "v1", Runtime: db.StepRuntime, Handler: "same-handler", HandlerVersion: "1", WorkerRelease: "v2"}}})
	retiring := mutate("v1", "retiring", 2, 200)
	require.EqualValues(t, 1, retiring.OutstandingRuns)
	require.EqualValues(t, 1, retiring.Generation)
	mutate("v1", "scale", 3, 400) // No live worker may be removed while this run waits.
	remoteLaunch(t, s, "alpha", "v2", "new-run")
	require.Equal(t, []byte("v2"), remoteState(t, s, "alpha", "new-run", db.JobSucceeded).Result)
	remoteSeed(t, s, db.JobCommand{Launch: &db.LaunchJob{TenantID: "alpha", DefinitionID: "v1", DefinitionRevision: 2, RunID: "migrated-run"}})
	require.Equal(t, []byte("v2"), remoteState(t, s, "alpha", "migrated-run", db.JobSucceeded).Result)
	remoteSeed(t, s, db.JobCommand{Signal: &db.SignalJob{TenantID: "alpha", RunID: "old-run", EventID: "continue", Key: "continue"}})
	finished := remoteState(t, s, "alpha", "old-run", db.JobSucceeded)
	require.Equal(t, []byte("v1"), finished.Result)
	require.Equal(t, "v1", finished.Definition.WorkerRelease)
	require.Equal(t, "retired", mutate("v1", "retire", 3, 200).State)
}
