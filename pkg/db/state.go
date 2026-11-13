package db

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"

	"github.com/duravela/djob/pkg/commit"
	"go.etcd.io/bbolt"
)

const (
	LegacyCommandVersion                     uint32 = 1
	ChannelCommandVersion                    uint32 = 2
	SchedulingCommandVersion                 uint32 = 3
	EnqueueCommandVersion                    uint32 = 4
	CancellationCommandVersion               uint32 = 5
	DeadlineCommandVersion                   uint32 = 6
	RetryCommandVersion                      uint32 = 7
	JobCommandVersion                        uint32 = 8
	JobTriggerCommandVersion                 uint32 = 9
	JobActivationCommandVersion              uint32 = 10
	JobQuotaCommandVersion                   uint32 = 11
	JobRetentionCommandVersion               uint32 = 12
	JobReceiptCommandVersion                 uint32 = 13
	JobConfigurationCommandVersion           uint32 = 14
	JobConfigurationReceiptCommandVersion    uint32 = 15
	JobTriggerRetirementCommandVersion       uint32 = 16
	JobConfigurationRetirementCommandVersion uint32 = 17
	WorkerEnrollmentCommandVersion           uint32 = 18
	WorkerDeploymentCommandVersion           uint32 = 19
	ConsoleSessionCommandVersion             uint32 = 20
	ConsoleSessionPolicyCommandVersion       uint32 = 21
	WorkerReleaseCommandVersion              uint32 = 22
	WorkerLifecycleCommandVersion            uint32 = 23
	WorkerRoutingCommandVersion              uint32 = 24
	OIDCSessionCommandVersion                uint32 = 25
	OIDCContinuityCommandVersion             uint32 = 26
	CurrentCommandVersion                           = OIDCContinuityCommandVersion
	CurrentDatabaseFormat                    byte   = 19 // OIDC renewal and durable logout cutoffs.
)

var (
	metadataBucket = []byte("raft_metadata")
	formatKey      = []byte("format_version")
	appliedKey     = []byte("applied_index")
)

func initializeMetadata(tx *bbolt.Tx) error {
	if tx.Bucket(metadataBucket) != nil {
		_, err := readAppliedIndex(tx)
		return err
	}
	for _, name := range jobBuckets {
		if b := tx.Bucket(name); b != nil && b.Stats().KeyN != 0 {
			return fmt.Errorf("legacy database has jobs without a durable Raft index; preserve it and explicitly rebuild from its WAL and a complete snapshot")
		}
	}
	b, err := tx.CreateBucket(metadataBucket)
	if err != nil {
		return err
	}
	if err := b.Put(formatKey, []byte{1}); err != nil {
		return err
	}
	return writeAppliedIndex(tx, 0)
}

func readAppliedIndex(tx *bbolt.Tx) (uint64, error) {
	b := tx.Bucket(metadataBucket)
	if b == nil {
		return 0, fmt.Errorf("missing or unsupported database format")
	}
	format := b.Get(formatKey)
	if len(format) != 1 || format[0] < 1 || format[0] > CurrentDatabaseFormat {
		return 0, fmt.Errorf("missing or unsupported database format")
	}
	data := b.Get(appliedKey)
	if len(data) != 8 {
		return 0, fmt.Errorf("invalid persisted Raft index")
	}
	return binary.BigEndian.Uint64(data), nil
}

func writeAppliedIndex(tx *bbolt.Tx, index uint64) error {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], index)
	if tx.Bucket(workerReleaseRunsBucket) != nil {
		if err := tx.Bucket(metadataBucket).Put(workerReleaseRunsAppliedKey, data[:]); err != nil {
			return err
		}
	}
	return tx.Bucket(metadataBucket).Put(appliedKey, data[:])
}

func (q *DB) refreshMetrics(tx *bbolt.Tx) {
	q.Metrics.Pending = tx.Bucket(jobBuckets[PENDING_PRIO_NORMAL]).Stats().KeyN + tx.Bucket(jobBuckets[PENDING_PRIO_HIGH]).Stats().KeyN
	q.Metrics.Progress = tx.Bucket(jobBuckets[IN_PROGRESS]).Stats().KeyN
}

type commandBatch struct {
	index uint64
	data  [][]byte
}

// ApplyCommand applies a logical batch at one index. Raft callers must use
// ApplyEntries so replay can skip individual entries across different batches.
func (q *DB) ApplyCommand(data [][]byte, index uint64) *ApplyCommandResult {
	return q.applyBatches([]commandBatch{{index: index, data: data}})
}

// ApplyEntries persists all new entries and their highest index atomically.
func (q *DB) ApplyEntries(entries []commit.Entry) *ApplyCommandResult {
	batches := make([]commandBatch, len(entries))
	for i, entry := range entries {
		batches[i].index = entry.Index
		if len(entry.Data) != 0 {
			batches[i].data = [][]byte{entry.Data}
		}
	}
	return q.applyBatches(batches)
}

func (q *DB) applyBatches(batches []commandBatch) *ApplyCommandResult {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := &ApplyCommandResult{JobErrors: make(map[string]error)}
	var commands []Command
	index := q.lastAppliedIndex
	for i, batch := range batches {
		if batch.index == 0 || (i > 0 && batch.index <= batches[i-1].index) {
			result.Error = fmt.Errorf("Raft entry indexes must be positive and ordered")
			return result
		}
		if batch.index <= q.lastAppliedIndex {
			continue
		}
		for _, data := range batch.data {
			var cmd Command
			if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&cmd); err != nil {
				result.Error = fmt.Errorf("decode command at index %d: %w", batch.index, err)
				return result
			}
			if cmd.Version > CurrentCommandVersion {
				result.Error = fmt.Errorf("unsupported command version %d at index %d", cmd.Version, batch.index)
				return result
			}
			commands = append(commands, cmd)
		}
		index = batch.index
	}
	if index == q.lastAppliedIndex {
		return result
	}
	before := *q.Metrics
	err := q.db.Update(func(tx *bbolt.Tx) error {
		for _, cmd := range commands {
			switch cmd.Type {
			case CmdAddJobs, CmdAddBatchJobs:
				for _, job := range cmd.Jobs {
					if err := q.addJob(tx, job); err != nil {
						return err
					}
					result.JobErrors[job.ID] = nil
				}
			case CmdDeleteJob:
				if len(cmd.Jobs) == 0 {
					return fmt.Errorf("delete command requires at least one job")
				}
				for _, job := range cmd.Jobs {
					if err := q.deleteJob(tx, job.ID); err != nil {
						return err
					}
					result.JobErrors[job.ID] = nil
				}
			case CmdConsoleSession:
				if cmd.Version < ConsoleSessionCommandVersion || cmd.ConsoleSession == nil || cmd.State != nil || len(cmd.Jobs) != 0 {
					return fmt.Errorf("console sessions require version %d and an exclusive payload", ConsoleSessionCommandVersion)
				}
				if cmd.ConsoleSession.Action == "configure" && cmd.Version < ConsoleSessionPolicyCommandVersion {
					return fmt.Errorf("session policy updates require version %d", ConsoleSessionPolicyCommandVersion)
				}
				continuity := cmd.ConsoleSession.Action == "renew" || cmd.ConsoleSession.Action == "oidc_logout" || cmd.ConsoleSession.Renewal != nil || cmd.ConsoleSession.Logout != nil || cmd.ConsoleSession.Creation != nil && (!cmd.ConsoleSession.Creation.Binding.OIDCIssuedAt.IsZero() || cmd.ConsoleSession.Creation.Binding.OIDCSID != "")
				if continuity {
					if cmd.Version < OIDCContinuityCommandVersion {
						return fmt.Errorf("OIDC continuity requires version 26")
					}
					if _, err := tx.CreateBucketIfNotExists(oidcLogoutBucket); err != nil {
						return err
					}
					if tx.Bucket(metadataBucket).Get(formatKey)[0] < 19 {
						if err := tx.Bucket(metadataBucket).Put(formatKey, []byte{19}); err != nil {
							return err
						}
					}
				}
				if cmd.ConsoleSession.Creation != nil && cmd.ConsoleSession.Creation.Binding.OIDCSubject != "" {
					if cmd.Version < OIDCSessionCommandVersion {
						return fmt.Errorf("OIDC sessions require version %d", OIDCSessionCommandVersion)
					}
					if tx.Bucket(metadataBucket).Get(formatKey)[0] < 18 {
						if err := tx.Bucket(metadataBucket).Put(formatKey, []byte{18}); err != nil {
							return err
						}
					}
				}
				sessionResult, err := applyConsoleSession(tx, *cmd.ConsoleSession)
				if err != nil {
					return err
				}
				result.ConsoleSessionResults = append(result.ConsoleSessionResults, sessionResult)
			case CmdState:
				if cmd.Version < ChannelCommandVersion || cmd.State == nil {
					return fmt.Errorf("channel commands require version %d and a payload", ChannelCommandVersion)
				}
				if cmd.Version < cmd.State.CommandVersion() {
					return fmt.Errorf("state operation requires version %d", cmd.State.CommandVersion())
				}
				stateResult, err := applyStateCommand(tx, *cmd.State)
				if err != nil {
					return err
				}
				result.StateResults = append(result.StateResults, stateResult)
			default:
				return fmt.Errorf("unknown command type: %d", cmd.Type)
			}
		}
		return writeAppliedIndex(tx, index)
	})
	if err != nil {
		*q.Metrics = before
		return &ApplyCommandResult{Error: fmt.Errorf("apply through index %d: %w", index, err)}
	}
	q.lastAppliedIndex = index
	_ = q.db.View(func(tx *bbolt.Tx) error { q.refreshMetrics(tx); return nil })
	return result
}
