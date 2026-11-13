package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/duravela/djob/pkg/db"
)

type OwnershipBackend interface {
	ProposeStateCommand(context.Context, db.StateCommand) (db.StateResult, error)
	ListChannels([]byte, int) (db.ChannelPage, error)
}

type OwnershipSweeperConfig struct {
	PollInterval  time.Duration
	BatchSize     int
	CommitTimeout time.Duration
	OnError       func(error)
}

// OwnershipSweeper discovers abandoned leases even without queued work or a
// local transport. Expiry only blocks access; it never grants physical takeover.
// Its cursor is local discovery progress, rebuilt after restart or leader loss.
type OwnershipSweeper struct {
	cfg      OwnershipSweeperConfig
	backend  OwnershipBackend
	isLeader func() bool
	mu       sync.Mutex
	cursor   []byte
	prefix   string
	sequence uint64
}

func NewOwnershipSweeper(cfg OwnershipSweeperConfig, backend OwnershipBackend, isLeader func() bool) (*OwnershipSweeper, error) {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 64
	}
	if cfg.CommitTimeout == 0 {
		cfg.CommitTimeout = 2 * time.Second
	}
	if backend == nil || isLeader == nil || cfg.PollInterval <= 0 || cfg.BatchSize < 1 || cfg.BatchSize > db.MaxQueryPage || cfg.CommitTimeout <= 0 {
		return nil, fmt.Errorf("invalid ownership sweeper configuration")
	}
	prefix, err := newPrefix()
	if err != nil {
		return nil, err
	}
	return &OwnershipSweeper{cfg: cfg, backend: backend, isLeader: isLeader, prefix: "ownership/" + prefix}, nil
}

// Tick examines at most one page of channels. The existing primary records
// avoid a new lease index that older writers could leave stale during upgrades.
// The state machine rechecks epoch, ownership, lease, and transition time.
func (s *OwnershipSweeper) Tick(ctx context.Context, at time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if at.IsZero() {
		return 0, fmt.Errorf("ownership sweep requires an explicit timestamp")
	}
	if !s.isLeader() {
		s.cursor = nil
		return 0, nil
	}
	page, err := s.backend.ListChannels(s.cursor, s.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	s.cursor = page.NextCursor
	blocked := 0
	var failures []error
	for _, channel := range page.Channels {
		if err := ctx.Err(); err != nil {
			return blocked, err
		}
		if !s.isLeader() {
			s.cursor = nil
			break
		}
		if channel.State != db.ChannelOwned && channel.State != db.ChannelDraining || at.Before(channel.LeaseUntil) {
			continue
		}
		if s.sequence == ^uint64(0) {
			return blocked, fmt.Errorf("ownership request sequence exhausted")
		}
		s.sequence++
		command := db.StateCommand{RequestID: fmt.Sprintf("%s/%d", s.prefix, s.sequence), At: at.UTC(),
			Expire: &db.ExpireChannel{ChannelID: channel.ID, Epoch: channel.Epoch}}
		var result db.StateResult
		for range 2 {
			commit, cancel := context.WithTimeout(ctx, s.cfg.CommitTimeout)
			result, err = s.backend.ProposeStateCommand(commit, command)
			cancel()
			var rejection *db.CommandRejection
			if err == nil || errors.As(err, &rejection) || ctx.Err() != nil {
				break
			}
		}
		if err == nil {
			err = result.Err()
		}
		if err != nil {
			var rejection *db.CommandRejection
			if errors.As(err, &rejection) {
				switch rejection.Code {
				case "stale_owner", "not_owned", "not_expired", "stale_time", "not_found":
					continue // A concurrent transition invalidated this observation.
				}
			}
			failures = append(failures, err)
			continue
		}
		fingerprint, err := command.Fingerprint()
		if err != nil || result.RequestID != command.RequestID || result.Fingerprint != fingerprint || result.Channel == nil ||
			result.Channel.ID != channel.ID || result.Channel.Epoch != channel.Epoch || result.Channel.State != db.ChannelBlocked {
			failures = append(failures, fmt.Errorf("ownership expiry receipt does not match request"))
			continue
		}
		blocked++
	}
	return blocked, errors.Join(failures...)
}

func (s *OwnershipSweeper) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		_, err := s.Tick(ctx, time.Now().UTC())
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && s.cfg.OnError != nil {
			s.cfg.OnError(err)
		}
	}
}
