package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

type mockSubmitter struct {
	js *Store
}

func (m *mockSubmitter) Submit(jobId string) error {
	return nil
}

func (m *mockSubmitter) Close() {}

func newTestJobQueue(t *testing.T) (*Store, chan []byte, chan *commit.Commit) {
	proposeC := make(chan []byte, 1)
	commitC := make(chan *commit.Commit, 1)
	cfg := StoreConfig{
		DataDir:         t.TempDir(),
		ProposalTimeout: 2 * time.Second,
		Logger:          zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel)),
	}

	jq, err := NewStore(cfg, proposeC, commitC, nil)
	require.NoError(t, err)
	jq.jobSubmitter = &mockSubmitter{js: jq}
	jq.OnJobIDReceived = func(jobID string) {
		jq.jobReportC <- &pool.JobResult{JobID: jobID}
	}
	require.NoError(t, jq.Start())
	t.Cleanup(jq.Stop)

	select {
	case <-jq.SnapshotterReady:
	case <-time.After(5 * time.Second):
		panic("snapshotter not ready")
	}

	return jq, proposeC, commitC
}

// not working due to removal of notifyProposal
func TestProposeCommand_Success(t *testing.T) {
	jq, proposeC, commitC := newTestJobQueue(t)
	defer jq.Stop()

	job := db.Job{ID: "job1"}
	cmd := db.Command{Type: db.CmdAddJobs, Jobs: []db.Job{job}}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan struct{})

	go func() {
		// Simulate commit processing after proposal
		data := <-proposeC

		var decoded db.Command
		require.NoError(t, gob.NewDecoder(bytes.NewReader(data)).Decode(&decoded))

		commitC <- &commit.Commit{
			Entries:    []commit.Entry{{Index: 7, Data: data}},
			ApplyDoneC: make(chan struct{}),
		}

		close(done)
	}()

	_, err := jq.ProposeCommand(ctx, cmd)
	assert.NoError(t, err)

	<-done
}

func TestProposeCommand_EmptyCommand(t *testing.T) {
	jq, _, _ := newTestJobQueue(t)
	defer jq.Stop()

	ctx := context.Background()
	_, err := jq.ProposeCommand(ctx, db.Command{})
	assert.ErrorContains(t, err, "empty command")
}

func TestProposeCommand_ContextTimeout(t *testing.T) {
	jq, _, _ := newTestJobQueue(t)
	defer jq.Stop()

	job := db.Job{ID: "job1"}
	cmd := db.Command{Jobs: []db.Job{job}}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	time.Sleep(20 * time.Millisecond) // ensure timeout
	_, err := jq.ProposeCommand(ctx, cmd)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestProposeCommand_StoppedQueue(t *testing.T) {
	jq, _, _ := newTestJobQueue(t)
	jq.Stop() // manually stop first

	job := db.Job{ID: "job1"}
	cmd := db.Command{Jobs: []db.Job{job}}

	ctx := context.Background()
	_, err := jq.ProposeCommand(ctx, cmd)
	assert.ErrorContains(t, err, "job queue stopped")
}

func TestProcessCommits_LoadsSnapshotOnNilCommit(t *testing.T) {
	jq, _, commitC := newTestJobQueue(t)
	defer jq.Stop()

	commitC <- nil // triggers snapshot load

	time.Sleep(50 * time.Millisecond) // wait for processing
	// Expect no panic, logs snapshot loading
}

func TestNotifyProposals_SendsErrors(t *testing.T) {
	jq, _, _ := newTestJobQueue(t)

	jobResults := make(map[string]error)
	jobResults["abc"] = errors.New("apply error")
	reqc := make(chan error, 1)

	jq.mu.Lock()
	jq.pendingReqs["abc"] = &pendingRequest{reqc: reqc}
	jq.mu.Unlock()

	jq.notifyProposals(jobResults)
	select {
	case err := <-reqc:
		assert.Equal(t, jobResults["abc"], err)
	case <-time.After(time.Second):
		t.Fatal("did not receive error on reqc")
	}
}

// Test that GetJobReport and DeleteJobReport work as expected.
// Covers: retrieval, deletion, and non-existent report edge case.
func TestJobStore_GetAndDeleteJobReport(t *testing.T) {
	jq, _, _ := newTestJobQueue(t)
	report := &pool.JobResult{JobID: "job-report-1", FinishedAt: time.Now()}
	jq.reportStore.Add(report)

	// Should retrieve the report
	got, ok := jq.GetJobReport("job-report-1")
	assert.True(t, ok)
	assert.Equal(t, "job-report-1", got.JobID)

	// Should delete the report
	jq.DeleteJobReport("job-report-1")
	_, ok = jq.GetJobReport("job-report-1")
	assert.False(t, ok)

	// Should gracefully handle deleting a non-existent report
	jq.DeleteJobReport("does-not-exist")
}

// Test that AssignJob sends the job ID to the worker pool's channel.
// Covers: correct delivery and channel closed edge case.
func TestJobStore_AssignJob_SendsToChannel(t *testing.T) {
	reportC := make(chan *pool.JobResult)
	jq, _, _ := newTestJobQueue(t)
	jq.OnJobReportReceived = func(report *pool.JobResult) {
		reportC <- report
	}
	jobID := "job-assign-1"

	_ = jq.db.AddJob(db.Job{
		ID:   jobID,
		Data: "test data",
		TS:   time.Now(),
	})
	_ = jq.db.AddJob(db.Job{
		ID:   "job-after-close",
		Data: "test data",
		TS:   time.Now(),
	})
	// AssignJob should send jobID to the channel
	go func() {
		_ = jq.AssignJob(jobID)
	}()

	select {
	case got := <-reportC:
		assert.Equal(t, jobID, got.JobID)
	case <-time.After(time.Second):
		t.Fatal("AssignJob did not send jobID to channel")
	}

	close(jq.jobC)
	// Edge case: AssignJob on closed channel should panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when sending to closed channel")
		}
	}()
	_ = jq.AssignJob("job-after-close")
}

// Test that BucketStats returns the DBMetrics struct.
// Covers: non-nil return and correct type.
func TestJobStore_BucketStats(t *testing.T) {
	jq, _, _ := newTestJobQueue(t)
	stats := jq.BucketStats()
	assert.NotNil(t, stats)
}

// Test edge case: StatusJob panics as not implemented.
func TestJobStore_StatusJob_Panics(t *testing.T) {
	jq, _, _ := newTestJobQueue(t)
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic from StatusJob")
		}
	}()
	_, _ = jq.StatusJob("any-job")
}
