package db

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

type channelHarness struct {
	t     *testing.T
	db    *DB
	at    time.Time
	index uint64
}

func newChannelHarness(t *testing.T) *channelHarness {
	t.Helper()
	q, cleanup := NewTestDB(t)
	t.Cleanup(cleanup)
	return &channelHarness{t: t, db: q, at: time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)}
}

func (h *channelHarness) apply(command StateCommand) StateResult {
	h.t.Helper()
	h.index++
	if command.RequestID == "" {
		command.RequestID = fmt.Sprintf("request-%d", h.index)
	}
	if command.At.IsZero() {
		command.At = h.at
	}
	result := h.db.ApplyEntries([]commit.Entry{logEntry(h.t, h.index, Command{Version: command.CommandVersion(), Type: CmdState, State: &command})})
	require.NoError(h.t, result.Error)
	require.Len(h.t, result.StateResults, 1)
	require.Equal(h.t, h.index, h.db.LastAppliedIndex(), "rejections must also advance durable application")
	return result.StateResults[0]
}

func (h *channelHarness) ok(command StateCommand) StateResult {
	h.t.Helper()
	result := h.apply(command)
	require.NoError(h.t, result.Err())
	return result
}

func (h *channelHarness) reject(command StateCommand, code string) StateResult {
	h.t.Helper()
	result := h.apply(command)
	require.NotNil(h.t, result.Rejection)
	require.Equal(h.t, code, result.Rejection.Code)
	return result
}

func (h *channelHarness) setup(channel string, runs ...string) ChannelOwner {
	h.t.Helper()
	h.ok(StateCommand{Configure: &ConfigureChannel{ID: channel, GatewayID: channel, Protocol: "fake", Endpoint: "gateway:502"}})
	h.ok(StateCommand{Acquire: &AcquireChannel{ChannelID: channel, OwnerID: "worker/incarnation-1", LeaseUntil: h.at.Add(time.Hour)}})
	for _, run := range runs {
		h.ok(StateCommand{CreateRun: &CreateRun{ID: run, ChannelID: channel, Executor: "fake", Payload: []byte(run)}})
	}
	return ChannelOwner{ChannelID: channel, Epoch: 1, OwnerID: "worker/incarnation-1"}
}

func claim(owner ChannelOwner, run, attempt string) StateCommand {
	return StateCommand{Claim: &ClaimRun{ChannelOwner: owner, RunID: run, AttemptID: attempt, ClaimToken: attempt + "-token"}}
}

func TestClaimsSerializeWholeGatewayAndPermitIndependentGateways(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway-1", "slave-3", "slave-7")
	other := h.setup("gateway-2", "independent")
	first := h.ok(claim(owner, "slave-3", "attempt-1"))
	require.Equal(t, "attempt-1", first.Channel.ActiveAttemptID)
	h.reject(claim(owner, "slave-7", "attempt-2"), "channel_busy")
	h.ok(claim(other, "independent", "attempt-independent"))
	second, err := h.db.GetRun("slave-7")
	require.NoError(t, err)
	require.Equal(t, RunReady, second.State)
	require.Zero(t, second.AttemptCount)
	h.reject(StateCommand{Finish: &FinishAttempt{ChannelOwner: owner, RunID: "slave-3", AttemptID: "attempt-1", ClaimToken: "stale", Outcome: AttemptSucceeded, Quiescent: true}}, "stale_attempt")
	h.ok(StateCommand{Finish: &FinishAttempt{ChannelOwner: owner, RunID: "slave-3", AttemptID: "attempt-1", ClaimToken: "attempt-1-token", Outcome: AttemptSucceeded, Quiescent: true}})
	h.ok(claim(owner, "slave-7", "attempt-2"))
	firstRun, err := h.db.GetRun("slave-3")
	require.NoError(t, err)
	require.Equal(t, RunSucceeded, firstRun.State)
}

func TestDrainHandoverRetainsEpochAndRejectsStaleOwner(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway", "first", "next")
	h.ok(claim(owner, "first", "attempt-1"))
	h.ok(StateCommand{Drain: &owner})
	h.reject(claim(owner, "next", "attempt-2"), "channel_busy")
	h.reject(StateCommand{Release: &ReleaseChannel{ChannelOwner: owner, Quiescent: true}}, "channel_busy")
	h.ok(StateCommand{Finish: &FinishAttempt{ChannelOwner: owner, RunID: "first", AttemptID: "attempt-1", ClaimToken: "attempt-1-token", Outcome: AttemptFailed, Quiescent: true}})
	h.reject(StateCommand{Release: &ReleaseChannel{ChannelOwner: owner}}, "channel_busy")
	h.ok(StateCommand{Release: &ReleaseChannel{ChannelOwner: owner, Quiescent: true}})
	h.reject(StateCommand{Configure: &ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "new:502"}}, "revision_conflict")
	reconfigured := h.ok(StateCommand{Configure: &ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "new:502", ExpectedRevision: 1}})
	require.Equal(t, uint64(1), reconfigured.Channel.Epoch)
	require.Equal(t, uint64(2), reconfigured.Channel.Revision)
	acquired := h.ok(StateCommand{Acquire: &AcquireChannel{ChannelID: "gateway", ExpectedEpoch: 1, OwnerID: "worker/incarnation-2", LeaseUntil: h.at.Add(time.Hour)}})
	require.Equal(t, uint64(2), acquired.Channel.Epoch)
	h.reject(StateCommand{Renew: &RenewChannel{ChannelID: "gateway", ExpectedEpoch: 1, OwnerID: owner.OwnerID, LeaseUntil: h.at.Add(2 * time.Hour)}}, "stale_owner")
	h.reject(claim(owner, "next", "stale-attempt"), "stale_owner")
	owner.Epoch, owner.OwnerID = 2, "worker/incarnation-2"
	h.ok(claim(owner, "next", "attempt-2"))
}

func TestExpiredOwnershipRequiresSeparateChannelAndRunResolution(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway", "uncertain")
	h.ok(claim(owner, "uncertain", "attempt-1"))
	h.reject(StateCommand{Expire: &ExpireChannel{ChannelID: "gateway", Epoch: 1}}, "not_expired")
	h.at = h.at.Add(time.Hour)
	h.reject(StateCommand{Acquire: &AcquireChannel{ChannelID: "gateway", ExpectedEpoch: 1, OwnerID: "new-process", LeaseUntil: h.at.Add(time.Hour)}}, "channel_busy")
	h.reject(StateCommand{Renew: &RenewChannel{ChannelID: "gateway", ExpectedEpoch: 1, OwnerID: owner.OwnerID, LeaseUntil: h.at.Add(time.Hour)}}, "lease_expired")
	expired := h.ok(StateCommand{Expire: &ExpireChannel{ChannelID: "gateway", Epoch: 1}})
	require.Equal(t, ChannelBlocked, expired.Channel.State)
	require.Equal(t, RunUnknown, expired.Run.State)
	require.Equal(t, AttemptUnknown, expired.Attempt.Outcome)
	require.Equal(t, "attempt-1", expired.Channel.ActiveAttemptID)
	h.reject(StateCommand{Finish: &FinishAttempt{ChannelOwner: owner, RunID: "uncertain", AttemptID: "attempt-1", ClaimToken: "attempt-1-token", Outcome: AttemptSucceeded, Quiescent: true}}, "not_owned")
	h.reject(StateCommand{ResolveRun: &ResolveRun{RunID: "uncertain", AttemptID: "attempt-1", State: RunReady, Actor: "operator", Reason: "retry requested"}}, "channel_busy")
	h.reject(StateCommand{ResolveChannel: &ResolveChannel{ChannelID: "gateway", Epoch: 1, Actor: "operator", Reason: "waited a minute"}}, "not_quiescent")
	resolved := h.ok(StateCommand{RequestID: "access-resolved", ResolveChannel: &ResolveChannel{ChannelID: "gateway", Epoch: 1, Actor: "operator", Reason: "previous access revoked and bus confirmed idle", Quiescent: true}})
	require.Equal(t, ChannelAvailable, resolved.Channel.State)
	run, err := h.db.GetRun("uncertain")
	require.NoError(t, err)
	require.Equal(t, RunUnknown, run.State, "physical resolution must not silently authorize replay")
	h.ok(StateCommand{ResolveRun: &ResolveRun{RunID: "uncertain", AttemptID: "attempt-1", State: RunReady, Actor: "operator", Reason: "read operation may safely repeat"}})
	h.ok(StateCommand{Acquire: &AcquireChannel{ChannelID: "gateway", ExpectedEpoch: 1, OwnerID: "new-process", LeaseUntil: h.at.Add(time.Hour)}})
	owner.OwnerID, owner.Epoch = "new-process", 2
	claimed := h.ok(claim(owner, "uncertain", "attempt-2"))
	require.Equal(t, uint64(2), claimed.Attempt.Number)
	h.at = h.at.Add(time.Hour)
	h.ok(StateCommand{Expire: &ExpireChannel{ChannelID: "gateway", Epoch: 2}})
	h.reject(StateCommand{ResolveRun: &ResolveRun{RunID: "uncertain", AttemptID: "attempt-1", State: RunSucceeded, Actor: "operator", Reason: "late resolution of old attempt"}}, "stale_attempt")
	audit, err := h.db.GetResolution("access-resolved")
	require.NoError(t, err)
	require.Equal(t, "operator", audit.Actor)
	require.Equal(t, uint64(1), audit.Epoch)
}

func TestOutcomeAndPhysicalRecoveryAreIndependent(t *testing.T) {
	for _, quiescent := range []bool{false, true} {
		t.Run(fmt.Sprint(quiescent), func(t *testing.T) {
			h := newChannelHarness(t)
			owner := h.setup("gateway", "first", "next")
			h.ok(claim(owner, "first", "attempt"))
			outcome := AttemptSucceeded
			if quiescent {
				outcome = AttemptUnknown
			}
			finished := h.ok(StateCommand{Finish: &FinishAttempt{ChannelOwner: owner, RunID: "first", AttemptID: "attempt", ClaimToken: "attempt-token", Outcome: outcome, Quiescent: quiescent}})
			if quiescent {
				require.Equal(t, RunUnknown, finished.Run.State)
				require.Equal(t, ChannelOwned, finished.Channel.State)
				h.ok(claim(owner, "next", "next-attempt"))
			} else {
				require.Equal(t, RunSucceeded, finished.Run.State)
				require.Equal(t, ChannelBlocked, finished.Channel.State)
				h.reject(claim(owner, "next", "next-attempt"), "not_owned")
			}
		})
	}
}

func TestRequestDeduplicationPersistsSuccessRejectionAndConflict(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway", "run")
	request := claim(owner, "run", "attempt")
	request.RequestID = "claim-request"
	first := h.ok(request)
	require.Equal(t, first, h.ok(request))
	conflict := request
	changed := *request.Claim
	changed.ClaimToken = "different-token"
	conflict.Claim = &changed
	h.reject(conflict, "request_conflict")
	h.ok(StateCommand{Finish: &FinishAttempt{ChannelOwner: owner, RunID: "run", AttemptID: "attempt", ClaimToken: "attempt-token", Outcome: AttemptUnknown, Error: "write response timed out", Quiescent: false}})
	require.Equal(t, first, h.ok(request), "retries return the original receipt without repeating its transition")
	failed := StateCommand{RequestID: "rejected-request", Acquire: &AcquireChannel{ChannelID: "gateway", ExpectedEpoch: 1, OwnerID: "new-owner", LeaseUntil: h.at.Add(time.Hour)}}
	rejection := h.reject(failed, "channel_busy")
	h.ok(StateCommand{RequestID: "resolved-access", ResolveChannel: &ResolveChannel{ChannelID: "gateway", Epoch: 1, Actor: "operator", Reason: "previous session terminated", Quiescent: true}})
	require.Equal(t, rejection, h.reject(failed, "channel_busy"), "a cached rejection must not turn into a successful acquire")

	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	path := filepath.Join(t.TempDir(), "restored.db")
	restored, err := NewDB(path)
	require.NoError(t, err)
	require.NoError(t, restored.Restore(bytes.NewReader(snapshot.Bytes())))
	require.NoError(t, restored.Close())
	restored, err = NewDB(path)
	require.NoError(t, err)
	t.Cleanup(func() { restored.Close() })
	h.db = restored
	require.Equal(t, first, h.ok(request))
	require.Equal(t, rejection, h.reject(failed, "channel_busy"))
	channel, err := restored.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, ChannelAvailable, channel.State)
	attempt, err := restored.GetAttempt("attempt")
	require.NoError(t, err)
	require.Equal(t, AttemptUnknown, attempt.Outcome)
	require.Equal(t, "write response timed out", attempt.Error)
	audit, err := restored.GetResolution("resolved-access")
	require.NoError(t, err)
	require.Equal(t, "operator", audit.Actor)
}

func TestStateBatchRollbackIncludesRequestReceipts(t *testing.T) {
	h := newChannelHarness(t)
	command := StateCommand{RequestID: "configure", At: h.at, Configure: &ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: "gateway:502"}}
	entry := logEntry(t, 1, Command{Version: ChannelCommandVersion, Type: CmdState, State: &command})
	result := h.db.ApplyEntries([]commit.Entry{entry, {Index: 2, Data: []byte("bad gob")}})
	require.Error(t, result.Error)
	require.Empty(t, result.StateResults)
	require.Zero(t, h.db.LastAppliedIndex())
	_, err := h.db.GetChannel("gateway")
	require.Error(t, err)
	require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
		require.Nil(t, tx.Bucket(requestsBucket).Get([]byte("configure")))
		return nil
	}))
	h.ok(command)
}

func TestChannelSnapshotMigrationAndMissingBucketDetection(t *testing.T) {
	h := newChannelHarness(t)
	var original bytes.Buffer
	require.NoError(t, h.db.Snapshot(&original))
	var snapshot Snapshot
	require.NoError(t, gob.NewDecoder(bytes.NewReader(original.Bytes())).Decode(&snapshot))
	var legacyBuckets []SnapshotBucket
	for _, bucket := range snapshot.Buckets {
		isChannel := false
		for _, name := range channelBuckets {
			if bytes.Equal(name, bucket.Name) {
				isChannel = true
			}
		}
		if isChannel {
			continue
		}
		if bytes.Equal(bucket.Name, metadataBucket) {
			var records []SnapshotRecord
			for _, record := range bucket.Records {
				if !bytes.Equal(record.Key, channelStateVersionKey) {
					records = append(records, record)
				}
			}
			bucket.Records = records
		}
		legacyBuckets = append(legacyBuckets, bucket)
	}
	snapshot.Buckets = legacyBuckets
	data, err := encode(snapshot)
	require.NoError(t, err)
	require.NoError(t, h.db.Restore(bytes.NewReader(data)), "previous complete indexed snapshots gain empty channel buckets")
	h.setup("gateway", "run")
	var current bytes.Buffer
	require.NoError(t, h.db.Snapshot(&current))
	require.NoError(t, gob.NewDecoder(bytes.NewReader(current.Bytes())).Decode(&snapshot))
	for i, bucket := range snapshot.Buckets {
		if bytes.Equal(bucket.Name, channelsBucket) {
			snapshot.Buckets = append(snapshot.Buckets[:i], snapshot.Buckets[i+1:]...)
			break
		}
	}
	data, err = encode(snapshot)
	require.NoError(t, err)
	require.ErrorContains(t, h.db.Restore(bytes.NewReader(data)), "missing bucket")
	channel, err := h.db.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, ChannelOwned, channel.State)
}

func TestChannelClockAndConfigurationChecks(t *testing.T) {
	h := newChannelHarness(t)
	h.reject(StateCommand{Configure: &ConfigureChannel{ID: "slave-7", GatewayID: "gateway", Protocol: "fake", Endpoint: "gateway:502"}}, "invalid")
	owner := h.setup("gateway", "run")
	h.reject(StateCommand{At: h.at.Add(-time.Second), Renew: &RenewChannel{ChannelID: "gateway", ExpectedEpoch: 1, OwnerID: owner.OwnerID, LeaseUntil: h.at.Add(time.Hour)}}, "stale_time")
	h.reject(StateCommand{Renew: &RenewChannel{ChannelID: "gateway", ExpectedEpoch: 1, OwnerID: owner.OwnerID, LeaseUntil: h.at.Add(time.Minute)}}, "invalid")
	h.ok(StateCommand{Renew: &RenewChannel{ChannelID: "gateway", ExpectedEpoch: 1, OwnerID: owner.OwnerID, LeaseUntil: h.at.Add(2 * time.Hour)}})
	h.reject(StateCommand{Configure: &ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "other", Endpoint: "other:502", ExpectedRevision: 1}}, "channel_busy")
}

func TestResolvedRunCannotBeClaimedWithAnOlderCommandTime(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway", "run")
	h.ok(claim(owner, "run", "first"))
	h.ok(StateCommand{Finish: &FinishAttempt{ChannelOwner: owner, RunID: "run", AttemptID: "first", ClaimToken: "first-token", Outcome: AttemptUnknown, Quiescent: true}})
	h.ok(StateCommand{RequestID: "resolved-run", At: h.at.Add(10 * time.Second), ResolveRun: &ResolveRun{RunID: "run", AttemptID: "first", State: RunReady, Actor: "operator", Reason: "safe read may repeat"}})
	stale := claim(owner, "run", "second")
	stale.At = h.at.Add(5 * time.Second)
	h.reject(stale, "stale_time")
	audit, err := h.db.GetResolution("resolved-run")
	require.NoError(t, err)
	require.Equal(t, "first", audit.AttemptID)
}

func TestStateFingerprintSurvivesGobAndTimeZoneConversion(t *testing.T) {
	at := time.Now()
	for _, operation := range []StateCommand{
		{CreateRun: &CreateRun{ID: "run", ChannelID: "gateway", Executor: "fake", Payload: []byte{}}},
		{CreateRun: &CreateRun{ID: "run", ChannelID: "gateway", Executor: "fake", Payload: []byte{0, 0xff, 0x10}}},
		{Acquire: &AcquireChannel{ChannelID: "gateway", OwnerID: "process", LeaseUntil: at.Add(time.Hour)}},
		{Finish: &FinishAttempt{ChannelOwner: ChannelOwner{ChannelID: "gateway", OwnerID: "process", Epoch: 1}, RunID: "run", AttemptID: "attempt", ClaimToken: "token", Result: []byte{}, Outcome: AttemptSucceeded, Quiescent: true}},
	} {
		operation.RequestID, operation.At = "request", at
		fingerprint, err := operation.Fingerprint()
		require.NoError(t, err)
		data, err := encode(operation)
		require.NoError(t, err)
		var decoded StateCommand
		require.NoError(t, gob.NewDecoder(bytes.NewReader(data)).Decode(&decoded))
		decoded.At = decoded.At.In(time.FixedZone("different zone", 2*60*60))
		if decoded.Acquire != nil {
			decoded.Acquire.LeaseUntil = decoded.Acquire.LeaseUntil.In(time.FixedZone("different zone", 2*60*60))
		}
		roundTrip, err := decoded.Fingerprint()
		require.NoError(t, err)
		require.Equal(t, fingerprint, roundTrip)
	}
	invalid := StateCommand{RequestID: "invalid-text", At: at, Configure: &ConfigureChannel{ID: "gateway", GatewayID: "gateway", Protocol: "fake", Endpoint: string([]byte{0xff})}}
	_, err := invalid.Fingerprint()
	require.ErrorContains(t, err, "UTF-8")
}

func TestFinishFingerprintRetainsPreviousFormatWhenErrorIsEmpty(t *testing.T) {
	// Produced by the preceding channel implementation, before Error was added.
	command := StateCommand{RequestID: "legacy-finish", At: time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC),
		Finish: &FinishAttempt{ChannelOwner: ChannelOwner{ChannelID: "gateway", Epoch: 1, OwnerID: "worker/process-1"},
			RunID: "run", AttemptID: "attempt", ClaimToken: "token", Outcome: AttemptSucceeded, Result: []byte{0, 255}, Quiescent: true}}
	fingerprint, err := command.Fingerprint()
	require.NoError(t, err)
	require.Equal(t, "ec9c95e4f680ab12daf94297f24ff2e027199feca0cd33791c6e1cd50192647b", hex.EncodeToString(fingerprint[:]))
}
