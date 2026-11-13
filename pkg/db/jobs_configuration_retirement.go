package db

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

// RetireJobConfiguration removes current configuration, preserving independent
// templates already pinned by runs and triggers. A retired identity is permanent.
type RetireJobConfiguration struct {
	TenantID, ID     string
	Kind             JobRecordKind // Group, definition or binding.
	ExpectedRevision uint64
	Actor, Reason    string
}

type JobConfigurationRetirement struct {
	TenantID, ID    string
	Kind            JobRecordKind
	Revision        uint64
	RetiredAt       time.Time
	Actor, Reason   string
	GroupMaxRunning uint64 // Frozen admission policy for existing pinned templates.
}

var (
	jobConfigurationRetirementVersionKey = []byte("job_configuration_retirement_state_version")
	jobConfigurationRetirementsBucket    = []byte("job_configuration_retirements")
	jobConfigurationReferencesBucket     = []byte("job_configuration_references")
	jobConfigurationRetirementBuckets    = [][]byte{jobConfigurationRetirementsBucket, jobConfigurationReferencesBucket}
)

func configurationPrimaryBucket(kind JobRecordKind) []byte {
	switch kind {
	case JobGroupRecord:
		return jobGroupsBucket
	case JobDefinitionRecord:
		return jobDefinitionsBucket
	case JobBindingRecord:
		return jobBindingsBucket
	}
	return nil
}

func configurationRetirementKind(kind JobRecordKind) JobRecordKind {
	switch kind {
	case JobGroupRetirementRecord:
		return JobGroupRecord
	case JobDefinitionRetirementRecord:
		return JobDefinitionRecord
	case JobBindingRetirementRecord:
		return JobBindingRecord
	}
	return ""
}

func configurationIdentity(tenant string, kind JobRecordKind, id string) string {
	return jobKey(tenant, string(kind), id)
}

func initializeJobConfigurationRetirementState(tx *bbolt.Tx) error {
	meta := tx.Bucket(metadataBucket)
	version := meta.Get(jobConfigurationRetirementVersionKey)
	activated := meta.Get(formatKey)[0] >= 11
	if version != nil && !bytes.Equal(version, []byte{1}) {
		return fmt.Errorf("unsupported configuration retirement state version")
	}
	if version == nil && activated {
		return fmt.Errorf("configuration retirement state is missing its format marker")
	}
	for _, name := range jobConfigurationRetirementBuckets {
		if version != nil && tx.Bucket(name) == nil {
			return fmt.Errorf("configuration retirement state is missing bucket %q", name)
		}
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}
	if !activated {
		if tx.Bucket(jobConfigurationRetirementsBucket).Stats().KeyN != 0 {
			return fmt.Errorf("configuration retirements without an activated database format")
		}
		// v8-v16 writers do not maintain this derived index. Rebuild on every
		// open/restore until format 11 excludes those writers.
		if err := tx.DeleteBucket(jobConfigurationReferencesBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucket(jobConfigurationReferencesBucket); err != nil {
			return err
		}
		if err := tx.Bucket(jobDefinitionsBucket).ForEach(func(k, v []byte) error {
			var d JobDefinition
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&d); err != nil {
				return err
			}
			if string(k) != jobKey(d.TenantID, d.ID) {
				return fmt.Errorf("inconsistent definition identity in configuration reference index")
			}
			var s jobChanges
			stageDefinitionReferences(&s, &d, false)
			return s.apply(tx)
		}); err != nil {
			return err
		}
	}
	return meta.Put(jobConfigurationRetirementVersionKey, []byte{1})
}

func stageDefinitionReferences(s *jobChanges, d *JobDefinition, remove bool) {
	for _, ref := range []struct {
		kind JobRecordKind
		id   string
	}{{JobGroupRecord, d.GroupID}, {JobBindingRecord, d.BindingID}} {
		if ref.id == "" {
			continue
		}
		key := jobKey(d.TenantID, string(ref.kind), ref.id, d.ID)
		if remove {
			s.del(jobConfigurationReferencesBucket, key)
		} else {
			s.index(jobConfigurationReferencesBucket, key, []byte(jobKey(d.TenantID, d.ID)))
		}
	}
}

func indexJobConfigurationReferences(tx *bbolt.Tx, s *jobChanges) error {
	for _, w := range *s {
		if !bytes.Equal(w.bucket, jobDefinitionsBucket) {
			continue
		}
		old, err := quotaValue[JobDefinition](tx.Bucket(jobDefinitionsBucket).Get([]byte(w.key)), nil)
		if err != nil {
			return err
		}
		if old != nil {
			stageDefinitionReferences(s, old, true)
		}
		if !w.remove {
			next, err := quotaValue[JobDefinition](w.raw, w.value)
			if err != nil {
				return err
			}
			stageDefinitionReferences(s, next, false)
		}
	}
	return nil
}

func checkJobConfigurationIdentity(tx *bbolt.Tx, kind JobRecordKind, tenant, id string) error {
	if tx.Bucket(jobConfigurationRetirementsBucket).Get([]byte(configurationIdentity(tenant, kind, id))) != nil {
		return reject("gone", "configuration identity has been permanently retired")
	}
	return nil
}

func retireJobConfiguration(tx *bbolt.Tx, s *jobChanges, at time.Time, c RetireJobConfiguration, result *JobResult) error {
	bucket := configurationPrimaryBucket(c.Kind)
	if bucket == nil || !validIdentity(c.TenantID) || !validIdentity(c.ID) || c.ExpectedRevision == 0 {
		return reject("invalid", "configuration retirement requires a group, definition or binding, tenant, ID and positive expected revision")
	}
	if err := validResolution(c.Actor, c.Reason); err != nil {
		return err
	}
	if err := checkJobConfigurationIdentity(tx, c.Kind, c.TenantID, c.ID); err != nil {
		return err
	}
	r := &JobConfigurationRetirement{TenantID: c.TenantID, Kind: c.Kind, ID: c.ID, RetiredAt: at, Actor: c.Actor, Reason: c.Reason}
	key := jobKey(c.TenantID, c.ID)
	switch c.Kind {
	case JobGroupRecord:
		v, err := readRecord[JobGroup](tx, bucket, key)
		if err != nil {
			return err
		}
		r.Revision, r.GroupMaxRunning = v.Revision, v.MaxRunning
	case JobDefinitionRecord:
		v, err := readRecord[JobDefinition](tx, bucket, key)
		if err != nil {
			return err
		}
		r.Revision = v.Revision
	case JobBindingRecord:
		v, err := readRecord[JobBinding](tx, bucket, key)
		if err != nil {
			return err
		}
		r.Revision = v.Revision
	}
	if r.Revision != c.ExpectedRevision {
		return reject("revision_conflict", "configuration revision changed")
	}
	if c.Kind != JobDefinitionRecord {
		prefix := []byte(configurationIdentity(c.TenantID, c.Kind, c.ID))
		k, _ := tx.Bucket(jobConfigurationReferencesBucket).Cursor().Seek(prefix)
		if bytes.HasPrefix(k, prefix) {
			return reject("configuration_in_use", "current definitions still reference this configuration; replace or retire them first")
		}
	}
	s.put(jobConfigurationRetirementsBucket, configurationIdentity(c.TenantID, c.Kind, c.ID), r)
	s.del(bucket, key)
	result.ConfigurationRetirement = r
	return nil
}

func jobGroupQuotaMaximum(tx *bbolt.Tx, tenant, id string) (uint64, error) {
	key := jobKey(tenant, id)
	if tx.Bucket(jobGroupsBucket).Get([]byte(key)) != nil {
		group, err := readRecord[JobGroup](tx, jobGroupsBucket, key)
		if err != nil {
			return 0, err
		}
		return group.MaxRunning, nil
	}
	r, err := readRecord[JobConfigurationRetirement](tx, jobConfigurationRetirementsBucket, configurationIdentity(tenant, JobGroupRecord, id))
	if err != nil {
		return 0, err
	}
	if r.TenantID != tenant || r.ID != id || r.Kind != JobGroupRecord || r.Revision == 0 {
		return 0, fmt.Errorf("inconsistent retired group admission policy")
	}
	return r.GroupMaxRunning, nil
}

func (q *DB) GetJobConfigurationRetirement(tenant string, kind JobRecordKind, id string) (*JobConfigurationRetirement, error) {
	if configurationPrimaryBucket(kind) == nil || !validIdentity(tenant) || !validIdentity(id) {
		return nil, reject("invalid", "configuration retirement requires a tenant, kind and ID")
	}
	return getStateRecord[JobConfigurationRetirement](q, jobConfigurationRetirementsBucket, configurationIdentity(tenant, kind, id))
}
