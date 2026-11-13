package db

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

type JobRunPage struct {
	Runs       []JobRun
	NextCursor []byte
}
type JobView struct {
	Run   *JobRun
	Event *JobEvent
}

// Queries inspect one local replica; only a newly committed claim/authorization
// can grant I/O permission. Tenant scope is mandatory, including pagination.
func (q *DB) GetJobTenant(id string) (*JobTenant, error) {
	return getStateRecord[JobTenant](q, jobTenantsBucket, id)
}
func (q *DB) GetJobGroup(tenant, id string) (*JobGroup, error) {
	return getStateRecord[JobGroup](q, jobGroupsBucket, jobKey(tenant, id))
}
func (q *DB) GetJobDefinition(tenant, id string) (*JobDefinition, error) {
	return getStateRecord[JobDefinition](q, jobDefinitionsBucket, jobKey(tenant, id))
}
func (q *DB) GetJobBinding(tenant, id string) (*JobBinding, error) {
	return getStateRecord[JobBinding](q, jobBindingsBucket, jobKey(tenant, id))
}

// Resource inspection is a control-plane query; resources have global identity.
func (q *DB) GetJobResource(id string) (*JobResource, error) {
	return getStateRecord[JobResource](q, jobResourcesBucket, id)
}
func (q *DB) GetJobRun(tenant, id string) (*JobRun, error) {
	return getStateRecord[JobRun](q, jobRunsBucket, jobKey(tenant, id))
}
func (q *DB) GetJobAttempt(tenant, run, id string) (*JobAttempt, error) {
	return getStateRecord[JobAttempt](q, jobAttemptsBucket, jobKey(tenant, run, id))
}
func (q *DB) GetJobAction(tenant, run string, sequence uint64) (*JobAction, error) {
	return getStateRecord[JobAction](q, jobActionsBucket, jobActionKey(tenant, run, sequence))
}
func (q *DB) GetJobEvent(tenant, run, id string) (*JobEvent, error) {
	return getStateRecord[JobEvent](q, jobEventsBucket, jobKey(tenant, run, id))
}
func (q *DB) GetJobResolution(requestID string) (*JobResolution, error) {
	return getStateRecord[JobResolution](q, jobResolutionsBucket, requestID)
}

func (q *DB) GetJobView(tenant, id string) (JobView, error) {
	var view JobView
	err := q.db.View(func(tx *bbolt.Tx) error {
		var err error
		view.Run, err = readRecord[JobRun](tx, jobRunsBucket, jobKey(tenant, id))
		if err != nil {
			return err
		}
		view.Event, err = nextJobEvent(tx, view.Run)
		return err
	})
	return view, err
}

func (q *DB) listTenantJobRuns(tenant string, after []byte, limit int, ready bool) (JobRunPage, error) {
	if !validIdentity(tenant) {
		return JobRunPage{}, fmt.Errorf("tenant ID is required")
	}
	if err := queryLimit(limit); err != nil {
		return JobRunPage{}, err
	}
	prefix := []byte(jobKey(tenant))
	if len(after) != 0 && !bytes.HasPrefix(after, prefix) {
		return JobRunPage{}, fmt.Errorf("cursor belongs to another tenant")
	}
	page := JobRunPage{}
	err := q.db.View(func(tx *bbolt.Tx) error {
		bucket := jobRunsBucket
		if ready {
			bucket = jobReadyBucket
		}
		cursor := tx.Bucket(bucket).Cursor()
		key, value := cursor.Seek(prefix)
		if len(after) != 0 {
			key, value = pageStart(cursor, after)
		}
		for key != nil && bytes.HasPrefix(key, prefix) && len(page.Runs) < limit {
			var run *JobRun
			if ready {
				var err error
				run, err = readRecord[JobRun](tx, jobRunsBucket, string(value))
				if err != nil {
					return err
				}
			} else {
				run = &JobRun{}
				if err := gob.NewDecoder(bytes.NewReader(value)).Decode(run); err != nil {
					return err
				}
			}
			page.Runs = append(page.Runs, *run)
			page.NextCursor = bytes.Clone(key)
			key, value = cursor.Next()
		}
		if key == nil || !bytes.HasPrefix(key, prefix) {
			page.NextCursor = nil
		}
		return nil
	})
	return page, err
}

func (q *DB) ListJobRuns(tenant string, after []byte, limit int) (JobRunPage, error) {
	return q.listTenantJobRuns(tenant, after, limit, false)
}
func (q *DB) ListReadyJobs(tenant string, after []byte, limit int) (JobRunPage, error) {
	return q.listTenantJobRuns(tenant, after, limit, true)
}

// The following bounded scans are control-plane queries for leader sweepers.
// Their command proposals recheck the exact observed claim/timer before acting.
func (q *DB) listTimedJobs(bucket []byte, at time.Time, after []byte, limit int) (JobRunPage, error) {
	if err := queryLimit(limit); err != nil {
		return JobRunPage{}, err
	}
	page := JobRunPage{}
	err := q.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(bucket).Cursor()
		key, value := pageStart(cursor, after)
		end := orderedTime(at)
		for key != nil && len(page.Runs) < limit {
			if len(key) < len(end) {
				return fmt.Errorf("invalid generic job timer index")
			}
			if bytes.Compare(key[:len(end)], end) > 0 {
				break
			}
			run, err := readRecord[JobRun](tx, jobRunsBucket, string(value))
			if err != nil {
				return err
			}
			page.Runs = append(page.Runs, *run)
			page.NextCursor = bytes.Clone(key)
			key, value = cursor.Next()
		}
		if key == nil || (len(key) >= len(end) && bytes.Compare(key[:len(end)], end) > 0) {
			page.NextCursor = nil
		}
		return nil
	})
	return page, err
}
func (q *DB) ListExpiredJobClaims(at time.Time, after []byte, limit int) (JobRunPage, error) {
	return q.listTimedJobs(jobLeasesBucket, at, after, limit)
}
func (q *DB) ListDueJobTimers(at time.Time, after []byte, limit int) (JobRunPage, error) {
	return q.listTimedJobs(jobWaitsBucket, at, after, limit)
}
