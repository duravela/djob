package db

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

const (
	MaxStateSampleRows  = 65536
	MaxStateSampleBytes = 8 << 20 // Per primary bucket, checked before decoding.
)

var (
	ErrStateInventoryBusy        = errors.New("database state inventory busy")
	ErrStateInventoryUnavailable = errors.New("database state inventory unavailable")
)

type StateInventoryKind int

const (
	InventoryGatewayRuns StateInventoryKind = iota
	InventoryGenericRuns
	InventoryChannels
	InventoryResources
	StateInventoryKindCount
)

type StateInventoryState int

const (
	InventoryReady StateInventoryState = iota
	InventoryRunning
	InventoryWaiting
	InventoryRetryWait
	InventorySucceeded
	InventoryFailed
	InventoryCancelled
	InventoryExpired
	InventoryUnknown
	InventoryAvailable
	InventoryOwned
	InventoryDraining
	InventoryBlocked
	StateInventoryStateCount
)

func (k StateInventoryKind) String() string {
	if k < 0 || k >= StateInventoryKindCount {
		return ""
	}
	return [...]string{"gateway_runs", "generic_runs", "channels", "resources"}[k]
}

func (s StateInventoryState) String() string {
	if s < 0 || s >= StateInventoryStateCount {
		return ""
	}
	return [...]string{"ready", "running", "waiting", "retry_wait", "succeeded", "failed", "cancelled", "expired", "unknown", "available", "owned", "draining", "blocked"}[s]
}

// States returns only the fixed states valid for this kind, never labels taken
// from application data. The returned slice belongs to the caller.
func (k StateInventoryKind) States() []StateInventoryState {
	switch k {
	case InventoryGatewayRuns:
		return []StateInventoryState{InventoryReady, InventoryRunning, InventoryRetryWait, InventorySucceeded, InventoryFailed, InventoryCancelled, InventoryExpired, InventoryUnknown}
	case InventoryGenericRuns:
		return []StateInventoryState{InventoryReady, InventoryRunning, InventoryWaiting, InventorySucceeded, InventoryFailed, InventoryCancelled, InventoryUnknown}
	case InventoryChannels:
		return []StateInventoryState{InventoryAvailable, InventoryOwned, InventoryDraining, InventoryBlocked}
	case InventoryResources:
		return []StateInventoryState{InventoryAvailable, InventoryOwned, InventoryBlocked}
	}
	return nil
}

type StateInventorySample struct {
	Observed, DecodedBytes uint64
	Complete               bool
	Counts                 [StateInventoryStateCount]uint64
}

// Counts describe retained primary records at one local application index.
// Incomplete samples expose lower bounds, including for zero observed counts.
// No record identities, payloads, ownership tokens or endpoints leave the query.
type ReleaseInventory struct {
	Active, Retiring, Retired, Outstanding, Unknown uint64
	DeploymentsComplete                             bool
	OldestRetiring                                  time.Time
}

type StateInventory struct {
	Releases     ReleaseInventory
	AppliedIndex uint64
	Kinds        [StateInventoryKindCount]StateInventorySample
}

func stateInventoryHeader(kind StateInventoryKind, key, raw []byte) (StateInventoryState, error) {
	var state string
	var identity bool
	switch kind {
	case InventoryGatewayRuns:
		var h struct {
			ID    string
			State RunState
		}
		if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&h); err != nil {
			return 0, err
		}
		state, identity = string(h.State), validIdentity(h.ID) && string(key) == h.ID
	case InventoryGenericRuns:
		var h struct {
			TenantID, ID string
			Status       JobStatus
		}
		if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&h); err != nil {
			return 0, err
		}
		state, identity = string(h.Status), validIdentity(h.TenantID) && validIdentity(h.ID) && string(key) == jobKey(h.TenantID, h.ID)
	case InventoryChannels:
		var h struct {
			ID    string
			State ChannelState
		}
		if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&h); err != nil {
			return 0, err
		}
		state, identity = string(h.State), validIdentity(h.ID) && string(key) == h.ID
	case InventoryResources:
		var h struct {
			Spec  struct{ ID string }
			State ResourceState
		}
		if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&h); err != nil {
			return 0, err
		}
		state, identity = string(h.State), validIdentity(h.Spec.ID) && string(key) == h.Spec.ID
	}
	if identity {
		for _, known := range kind.States() {
			if known.String() == state {
				return known, nil
			}
		}
	}
	return 0, fmt.Errorf("inconsistent state inventory metadata")
}

// StateInventory is a bounded background diagnostic query. It skips busy
// application/restore/close locks and reads four state kinds plus the release
// catalog in one transaction.
// Caps apply independently to each kind; a failed/cancelled probe returns none.
func (q *DB) StateInventory(ctx context.Context, limit int) (StateInventory, error) {
	if limit < 1 || limit > MaxStateSampleRows {
		return StateInventory{}, fmt.Errorf("state sample limit must be in 1..%d", MaxStateSampleRows)
	}
	if err := ctx.Err(); err != nil {
		return StateInventory{}, err
	}
	if !q.mu.TryLock() {
		return StateInventory{}, ErrStateInventoryBusy
	}
	defer q.mu.Unlock()
	if q.closed || q.db == nil {
		return StateInventory{}, ErrStateInventoryUnavailable
	}
	stats := StateInventory{AppliedIndex: q.lastAppliedIndex}
	err := q.db.View(func(tx *bbolt.Tx) error {
		for kind, name := range [...][]byte{runsBucket, jobRunsBucket, channelsBucket, jobResourcesBucket} {
			b := tx.Bucket(name)
			if b == nil {
				return ErrStateInventoryUnavailable
			}
			sample := &stats.Kinds[kind]
			c := b.Cursor()
			key, raw := c.First()
			for key != nil {
				if err := ctx.Err(); err != nil {
					return err
				}
				if sample.Observed >= uint64(limit) || uint64(len(raw)) > MaxStateSampleBytes-sample.DecodedBytes {
					break
				}
				if raw == nil {
					return fmt.Errorf("nested bucket in state inventory")
				}
				state, err := stateInventoryHeader(StateInventoryKind(kind), key, raw)
				if err != nil {
					return fmt.Errorf("decode %s state inventory: %w", StateInventoryKind(kind), err)
				}
				sample.Observed++
				sample.DecodedBytes += uint64(len(raw))
				sample.Counts[state]++
				if StateInventoryKind(kind) == InventoryGenericRuns {
					var h struct {
						Definition struct{ WorkerRelease string }
					}
					if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&h); err != nil {
						return err
					}
					if h.Definition.WorkerRelease != "" {
						switch state {
						case InventoryReady, InventoryRunning, InventoryWaiting, InventoryUnknown:
							stats.Releases.Outstanding++
						}
						if state == InventoryUnknown {
							stats.Releases.Unknown++
						}
					}
				}
				key, raw = c.Next()
			}
			sample.Complete = key == nil
		}
		b := tx.Bucket(workerDeploymentsBucket)
		if b == nil {
			return ErrStateInventoryUnavailable
		}
		c := b.Cursor()
		key, raw := c.First()
		rows, decoded := 0, 0
		for key != nil && rows < limit && decoded+len(raw) <= MaxStateSampleBytes {
			if err := ctx.Err(); err != nil {
				return err
			}
			var h struct {
				TenantID, ID, State string
				Published           bool
				ChangedAt           time.Time
			}
			if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&h); err != nil {
				return err
			}
			if !validIdentity(h.TenantID) || !validWorkerID(h.ID) || string(key) != jobKey(h.TenantID, h.ID) {
				return fmt.Errorf("inconsistent release inventory metadata")
			}
			if h.Published {
				switch h.State {
				case "active":
					stats.Releases.Active++
				case "retiring":
					stats.Releases.Retiring++
					if stats.Releases.OldestRetiring.IsZero() || h.ChangedAt.Before(stats.Releases.OldestRetiring) {
						stats.Releases.OldestRetiring = h.ChangedAt
					}
				case "retired":
					stats.Releases.Retired++
				default:
					return fmt.Errorf("unknown release inventory state")
				}
			}
			rows++
			decoded += len(raw)
			key, raw = c.Next()
		}
		stats.Releases.DeploymentsComplete = key == nil
		return ctx.Err()
	})
	if err != nil {
		return StateInventory{}, err
	}
	return stats, nil
}
