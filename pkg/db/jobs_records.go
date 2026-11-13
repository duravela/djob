package db

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"go.etcd.io/bbolt"
)

// JobRecordKind identifies inspection records, never executable commands.
type JobRecordKind string

const (
	JobGroupRetirementRecord                JobRecordKind = "group_retirement"
	JobDefinitionRetirementRecord           JobRecordKind = "definition_retirement"
	JobBindingRetirementRecord              JobRecordKind = "binding_retirement"
	JobTriggerRetirementRecord              JobRecordKind = "trigger_retirement"
	JobConfigurationReceiptCompactionRecord JobRecordKind = "configuration_receipt_compaction"
	JobConfigurationLimitsRecord            JobRecordKind = "configuration_limits"
	JobConfigurationUsageRecord             JobRecordKind = "configuration_usage"
	JobReceiptCompactionRecord              JobRecordKind = "receipt_compaction"
	JobDeletionRecord                       JobRecordKind = "deletion"
	JobRetentionRecord                      JobRecordKind = "retention"
	JobLimitsRecord                         JobRecordKind = "limits"
	JobUsageRecord                          JobRecordKind = "usage"
	JobActivationRecord                     JobRecordKind = "activation"
	JobTriggerRecord                        JobRecordKind = "trigger"
	JobTenantRecord                         JobRecordKind = "tenant"
	JobGroupRecord                          JobRecordKind = "group"
	JobDefinitionRecord                     JobRecordKind = "definition"
	JobBindingRecord                        JobRecordKind = "binding"
	JobResourceRecord                       JobRecordKind = "resource"
	JobGrantRecord                          JobRecordKind = "grant"
	JobRunRecord                            JobRecordKind = "run"
	JobAttemptRecord                        JobRecordKind = "attempt"
	JobActionRecord                         JobRecordKind = "action"
	JobEventRecord                          JobRecordKind = "event"
	JobResolutionRecord                     JobRecordKind = "resolution"
)

type JobRecordQuery struct {
	TriggerID, Source   string
	WorkerRelease       string
	Kind                JobRecordKind
	TenantID, RunID, ID string
	Sequence            uint64
	ReadyOnly           bool
}
type JobRecord struct {
	ConfigurationRetirement        *JobConfigurationRetirement
	TriggerRetirement              *JobTriggerRetirement
	ConfigurationReceiptCompaction *JobConfigurationReceiptCompaction
	ConfigurationLimits            *JobConfigurationLimits
	ConfigurationUsage             *JobConfigurationUsage
	ReceiptCompaction              *JobReceiptCompaction
	Deletion                       *JobDeletion
	Retention                      *JobRetentionPolicy
	Limits                         *JobLimits
	Usage                          *JobUsage
	Activation                     *JobActivation
	Trigger                        *JobTrigger
	Tenant                         *JobTenant
	Group                          *JobGroup
	Definition                     *JobDefinition
	Binding                        *JobBinding
	Resource                       *JobResource
	Grant                          *ResourceGrant
	Run                            *JobRun
	Attempt                        *JobAttempt
	Action                         *JobAction
	Event                          *JobEvent
	Resolution                     *JobResolution
}
type JobRecordEntry struct {
	Record JobRecord
	Cursor []byte
}
type JobRecordPage struct {
	Entries    []JobRecordEntry
	NextCursor []byte
}
type JobTenantPage struct {
	Tenants    []JobTenant
	NextCursor []byte
}

func jobRecordLocation(q JobRecordQuery, list bool) (bucket []byte, prefix, key string, target func(*JobRecord) any, err error) {
	tenant := q.TenantID
	if q.Kind != JobResourceRecord && q.Kind != JobResolutionRecord && !(list && q.Kind == JobTenantRecord) && !validIdentity(tenant) {
		err = reject("invalid", "tenant ID is required")
		return
	}
	if q.ReadyOnly && (q.Kind != JobRunRecord || !list) {
		err = reject("invalid", "ready_only is supported for run lists")
		return
	}
	if q.WorkerRelease != "" && (!list || q.Kind != JobRunRecord || q.ReadyOnly || !validWorkerID(q.WorkerRelease)) {
		err = reject("invalid", "worker_release selects outstanding run lists only")
		return
	}
	prefix, key = jobKey(tenant), jobKey(tenant, q.ID)
	if q.Kind != JobActivationRecord && (q.TriggerID != "" || q.Source != "") {
		err = reject("invalid", "activation scope is supported only for activation records")
		return
	}
	switch q.Kind {
	case JobGroupRetirementRecord, JobDefinitionRetirementRecord, JobBindingRetirementRecord:
		kind := configurationRetirementKind(q.Kind)
		prefix, key = jobKey(tenant, string(kind)), configurationIdentity(tenant, kind, q.ID)
		bucket, target = jobConfigurationRetirementsBucket, func(r *JobRecord) any {
			r.ConfigurationRetirement = &JobConfigurationRetirement{}
			return r.ConfigurationRetirement
		}
	case JobTriggerRetirementRecord:
		bucket, target = jobTriggerRetirementsBucket, func(r *JobRecord) any { r.TriggerRetirement = &JobTriggerRetirement{}; return r.TriggerRetirement }
	case JobConfigurationReceiptCompactionRecord:
		bucket, target = jobConfigurationReceiptCompactionsBucket, func(r *JobRecord) any {
			r.ConfigurationReceiptCompaction = &JobConfigurationReceiptCompaction{}
			return r.ConfigurationReceiptCompaction
		}
	case JobConfigurationLimitsRecord, JobConfigurationUsageRecord:
		if list {
			err = reject("invalid", "configuration limits and usage are looked up by tenant")
			return
		}
		prefix, key = "", tenant
		if q.Kind == JobConfigurationLimitsRecord {
			bucket, target = jobConfigurationLimitsBucket, func(r *JobRecord) any {
				r.ConfigurationLimits = &JobConfigurationLimits{}
				return r.ConfigurationLimits
			}
		} else {
			bucket, target = jobConfigurationUsageBucket, func(r *JobRecord) any { r.ConfigurationUsage = &JobConfigurationUsage{}; return r.ConfigurationUsage }
		}
	case JobReceiptCompactionRecord:
		bucket, target = jobReceiptCompactionBucket, func(r *JobRecord) any { r.ReceiptCompaction = &JobReceiptCompaction{}; return r.ReceiptCompaction }
	case JobDeletionRecord:
		bucket, target = jobDeletionBucket, func(r *JobRecord) any { r.Deletion = &JobDeletion{}; return r.Deletion }
	case JobRetentionRecord:
		if list {
			err = reject("invalid", "retention policy is looked up by tenant")
			return
		}
		prefix, key = "", tenant
		bucket, target = jobRetentionBucket, func(r *JobRecord) any { r.Retention = &JobRetentionPolicy{}; return r.Retention }
	case JobLimitsRecord, JobUsageRecord:
		if list {
			err = reject("invalid", "job limits and usage are looked up by tenant")
			return
		}
		prefix, key = "", tenant
		if q.Kind == JobLimitsRecord {
			bucket, target = jobLimitsBucket, func(r *JobRecord) any { r.Limits = &JobLimits{}; return r.Limits }
		} else {
			bucket, target = jobUsageBucket, func(r *JobRecord) any { r.Usage = &JobUsage{}; return r.Usage }
		}
	case JobActivationRecord:
		if q.RunID != "" || q.Sequence != 0 || !validIdentity(q.TriggerID) || (q.Source != "" && !validIdentity(q.Source)) || (!list && !validIdentity(q.Source)) {
			err = reject("invalid", "activation history requires a trigger and lookup requires its source")
			return
		}
		bucket, prefix, key, target = jobActivationsBucket, jobKey(tenant, q.TriggerID), activationKey(tenant, q.TriggerID, q.Source, q.ID), func(r *JobRecord) any { r.Activation = &JobActivation{}; return r.Activation }
		if q.Source != "" {
			prefix = jobKey(tenant, q.TriggerID, q.Source)
		}

	case JobTriggerRecord:
		bucket, target = jobTriggersBucket, func(r *JobRecord) any { r.Trigger = &JobTrigger{}; return r.Trigger }
	case JobTenantRecord:
		bucket, prefix, key, target = jobTenantsBucket, "", tenant, func(r *JobRecord) any { r.Tenant = &JobTenant{}; return r.Tenant }
	case JobGroupRecord:
		bucket, target = jobGroupsBucket, func(r *JobRecord) any { r.Group = &JobGroup{}; return r.Group }
	case JobDefinitionRecord:
		bucket, target = jobDefinitionsBucket, func(r *JobRecord) any { r.Definition = &JobDefinition{}; return r.Definition }
	case JobBindingRecord:
		bucket, target = jobBindingsBucket, func(r *JobRecord) any { r.Binding = &JobBinding{}; return r.Binding }
	case JobResourceRecord:
		bucket, prefix, key, target = jobResourcesBucket, "", q.ID, func(r *JobRecord) any { r.Resource = &JobResource{}; return r.Resource }
	case JobGrantRecord:
		bucket, target = jobGrantsBucket, func(r *JobRecord) any { r.Grant = &ResourceGrant{}; return r.Grant }
	case JobRunRecord:
		bucket, target = jobRunsBucket, func(r *JobRecord) any { r.Run = &JobRun{}; return r.Run }
		if q.WorkerRelease != "" {
			prefix = jobKey(tenant, q.WorkerRelease)
		}
	case JobAttemptRecord:
		bucket, target = jobAttemptsBucket, func(r *JobRecord) any { r.Attempt = &JobAttempt{}; return r.Attempt }
	case JobActionRecord:
		bucket, target = jobActionsBucket, func(r *JobRecord) any { r.Action = &JobAction{}; return r.Action }
	case JobEventRecord:
		bucket, target = jobEventsBucket, func(r *JobRecord) any { r.Event = &JobEvent{}; return r.Event }
	case JobResolutionRecord:
		if list {
			err = reject("invalid", "resolutions are looked up by request ID")
			return
		}
		bucket, prefix, key, target = jobResolutionsBucket, "", q.ID, func(r *JobRecord) any { r.Resolution = &JobResolution{}; return r.Resolution }
	default:
		err = reject("invalid", "unknown generic record kind")
		return
	}
	if q.Kind == JobAttemptRecord || q.Kind == JobActionRecord || q.Kind == JobEventRecord {
		if !validIdentity(q.RunID) {
			err = reject("invalid", "run ID is required for history")
			return
		}
		prefix, key = jobKey(tenant, q.RunID), jobKey(tenant, q.RunID, q.ID)
		if q.Kind == JobActionRecord {
			key = jobActionKey(tenant, q.RunID, q.Sequence)
		}
	}
	if !list && q.Kind != JobTenantRecord && q.Kind != JobLimitsRecord && q.Kind != JobUsageRecord && q.Kind != JobRetentionRecord && q.Kind != JobConfigurationLimitsRecord && q.Kind != JobConfigurationUsageRecord && q.Kind != JobActionRecord && !validIdentity(q.ID) {
		err = reject("invalid", "record ID is required")
	}
	return
}

func (q *DB) GetJobRecord(query JobRecordQuery) (JobRecord, error) {
	bucket, _, key, target, err := jobRecordLocation(query, false)
	if err != nil {
		return JobRecord{}, err
	}
	var r JobRecord
	err = q.db.View(func(tx *bbolt.Tx) error {
		if query.Kind == JobConfigurationLimitsRecord || query.Kind == JobConfigurationUsageRecord {
			if _, err := requireJobTenant(tx, query.TenantID); err != nil {
				return err
			}
			var err error
			if query.Kind == JobConfigurationLimitsRecord {
				r.ConfigurationLimits, err = configurationLimitsInTx(tx, query.TenantID)
			} else {
				r.ConfigurationUsage, err = configurationUsageInTx(tx, query.TenantID)
			}
			return err
		}
		if query.Kind == JobLimitsRecord || query.Kind == JobUsageRecord || query.Kind == JobRetentionRecord {
			if _, err := requireJobTenant(tx, query.TenantID); err != nil {
				return err
			}
			var err error
			if query.Kind == JobRetentionRecord {
				r.Retention, err = retentionPolicyInTx(tx, query.TenantID)
			} else if query.Kind == JobLimitsRecord {
				r.Limits, err = jobLimitsInTx(tx, query.TenantID)
			} else {
				r.Usage, err = jobUsageInTx(tx, query.TenantID)
			}
			return err
		}
		data := tx.Bucket(bucket).Get([]byte(key))
		if data == nil && query.Kind == JobActivationRecord {
			data = tx.Bucket(jobActivationReceiptsBucket).Get([]byte(key))
		}
		if data == nil && query.Kind == JobRunRecord && tx.Bucket(jobDeletionBucket).Get([]byte(key)) != nil {
			return reject("gone", "run was purged; inspect its deletion record")
		}
		if data == nil {
			return reject("not_found", "generic record does not exist")
		}
		return gob.NewDecoder(bytes.NewReader(data)).Decode(target(&r))
	})
	return r, err
}

// Each returned cursor identifies its row, allowing APIs to stop at a response
// byte budget without skipping the first row that did not fit. Tenant-prefix
// validation is repeated here even if the API validated its opaque page token.
func (q *DB) ListJobRecords(query JobRecordQuery, after []byte, limit int) (JobRecordPage, error) {
	if err := queryLimit(limit); err != nil {
		return JobRecordPage{}, err
	}
	bucket, prefix, _, target, err := jobRecordLocation(query, true)
	if err != nil {
		return JobRecordPage{}, err
	}
	if len(after) != 0 && !bytes.HasPrefix(after, []byte(prefix)) {
		return JobRecordPage{}, reject("invalid", "cursor belongs to another namespace")
	}
	page := JobRecordPage{}
	err = q.db.View(func(tx *bbolt.Tx) error {
		if query.WorkerRelease != "" {
			v, err := readRecord[WorkerDeployment](tx, workerDeploymentsBucket, jobKey(query.TenantID, query.WorkerRelease))
			if err != nil {
				return err
			}
			if !v.Published {
				return reject("invalid", "select a published worker release")
			}
		}
		if query.Kind == JobActivationRecord {
			key := jobKey(query.TenantID, query.TriggerID)
			if tx.Bucket(jobTriggersBucket).Get([]byte(key)) != nil {
				if _, err := readRecord[JobTrigger](tx, jobTriggersBucket, key); err != nil {
					return err
				}
			} else if _, err := readRecord[JobTriggerRetirement](tx, jobTriggerRetirementsBucket, key); err != nil {
				return err
			}
		}
		if query.RunID != "" {
			if _, err := readRecord[JobRun](tx, jobRunsBucket, jobKey(query.TenantID, query.RunID)); err != nil {
				return err
			}
		}
		index := bucket
		if query.ReadyOnly {
			index = jobReadyBucket
		}
		if query.WorkerRelease != "" {
			index = workerReleaseRunsBucket
		}
		cursor := tx.Bucket(index).Cursor()
		key, value := cursor.Seek([]byte(prefix))
		if len(after) != 0 {
			key, value = pageStart(cursor, after)
		}
		for key != nil && bytes.HasPrefix(key, []byte(prefix)) && len(page.Entries) < limit {
			var record JobRecord
			if query.ReadyOnly || query.WorkerRelease != "" {
				value = tx.Bucket(jobRunsBucket).Get(value)
				if value == nil {
					return fmt.Errorf("ready job index references a missing run")
				}
			}
			if err := gob.NewDecoder(bytes.NewReader(value)).Decode(target(&record)); err != nil {
				return err
			}
			if query.WorkerRelease != "" && (record.Run == nil || outstandingWorkerRelease(record.Run) != query.WorkerRelease || record.Run.TenantID != query.TenantID || !bytes.Equal(key, []byte(jobKey(record.Run.TenantID, query.WorkerRelease, record.Run.ID)))) {
				return fmt.Errorf("worker release index does not match its run")
			}
			page.Entries = append(page.Entries, JobRecordEntry{Record: record, Cursor: bytes.Clone(key)})
			page.NextCursor = bytes.Clone(key)
			key, value = cursor.Next()
		}
		if key == nil || !bytes.HasPrefix(key, []byte(prefix)) {
			page.NextCursor = nil
		}
		return nil
	})
	return page, err
}

func (q *DB) ListJobTenants(after []byte, limit int) (JobTenantPage, error) {
	rows, next, err := listRecords[JobTenant](q, jobTenantsBucket, after, limit)
	return JobTenantPage{Tenants: rows, NextCursor: next}, err
}
