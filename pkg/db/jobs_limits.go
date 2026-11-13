package db

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math"
	"sort"

	"go.etcd.io/bbolt"
)

// Limits apply to logical retained records and unique event payload bytes, not
// filesystem usage, WAL, receipts, or execution history. Zero is unlimited.
type JobLimits struct {
	TenantID                           string
	Revision                           uint64
	MaxPendingRuns, MaxStoredRuns      uint64
	MaxInboxEvents, MaxInboxBytes      uint64
	MaxActivations, MaxActivationBytes uint64
}

type PutJobLimits struct {
	Limits           JobLimits
	ExpectedRevision uint64
}

type JobUsage struct {
	TenantID                     string
	PendingRuns, StoredRuns      uint64
	InboxEvents, InboxBytes      uint64
	Activations, ActivationBytes uint64
}

var (
	jobQuotaVersionKey = []byte("job_quota_state_version")
	jobLimitsBucket    = []byte("job_limits")
	jobUsageBucket     = []byte("job_usage")
)

func initializeJobQuotaState(tx *bbolt.Tx) error {
	meta := tx.Bucket(metadataBucket)
	version := meta.Get(jobQuotaVersionKey)
	if version != nil && !bytes.Equal(version, []byte{1}) {
		return fmt.Errorf("unsupported generic job quota state version")
	}
	if version == nil && meta.Get(formatKey)[0] >= 5 {
		return fmt.Errorf("generic job quota state is missing its format marker")
	}
	for _, name := range [][]byte{jobLimitsBucket, jobUsageBucket} {
		if version != nil && tx.Bucket(name) == nil {
			return fmt.Errorf("generic job quota state is missing bucket %q", name)
		}
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}
	if meta.Get(formatKey)[0] < 5 {
		if tx.Bucket(jobLimitsBucket).Stats().KeyN != 0 {
			return fmt.Errorf("job limits without an activated database format")
		}
		// v8-v10 writers do not maintain usage. Rebuild on every open/restore until
		// policy activation raises format 5 and excludes those writers.
		if err := rebuildJobUsage(tx); err != nil {
			return err
		}
	}
	return meta.Put(jobQuotaVersionKey, []byte{1})
}

func jobLimitsInTx(tx *bbolt.Tx, tenant string) (*JobLimits, error) {
	if tx.Bucket(jobLimitsBucket).Get([]byte(tenant)) == nil {
		return &JobLimits{TenantID: tenant}, nil
	}
	limits, err := readRecord[JobLimits](tx, jobLimitsBucket, tenant)
	if err == nil && limits.TenantID != tenant {
		return nil, fmt.Errorf("inconsistent job limits tenant")
	}
	return limits, err
}
func jobUsageInTx(tx *bbolt.Tx, tenant string) (*JobUsage, error) {
	if tx.Bucket(jobUsageBucket).Get([]byte(tenant)) == nil {
		return nil, fmt.Errorf("missing job usage for tenant")
	}
	usage, err := readRecord[JobUsage](tx, jobUsageBucket, tenant)
	if err == nil && usage.TenantID != tenant {
		return nil, fmt.Errorf("inconsistent job usage tenant")
	}
	return usage, err
}
func putJobLimits(tx *bbolt.Tx, s *jobChanges, c PutJobLimits, r *JobResult) error {
	v := c.Limits
	if _, err := requireJobTenant(tx, v.TenantID); err != nil {
		return err
	}
	old, err := jobLimitsInTx(tx, v.TenantID)
	if err != nil {
		return err
	}
	if old.Revision != c.ExpectedRevision || old.Revision == math.MaxUint64 {
		return reject("revision_conflict", "job limits revision changed")
	}
	v.Revision = old.Revision + 1
	s.put(jobLimitsBucket, v.TenantID, &v)
	r.Limits = &v
	return nil
}

func quotaValue[T any](data []byte, value any) (*T, error) {
	if value != nil {
		v, ok := value.(*T)
		if !ok {
			return nil, fmt.Errorf("invalid staged job quota value")
		}
		return v, nil
	}
	if data == nil {
		return nil, nil
	}
	var v T
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&v); err != nil {
		return nil, err
	}
	return &v, nil
}

func quotaRecord(bucket []byte, data []byte, value any) (JobUsage, error) {
	var counts JobUsage
	switch {
	case bytes.Equal(bucket, jobRunsBucket):
		v, err := quotaValue[JobRun](data, value)
		if err != nil || v == nil {
			return counts, err
		}
		counts.TenantID, counts.StoredRuns = v.TenantID, 1
		if scheduledJobPending(v) {
			counts.PendingRuns = 1
		}
	case bytes.Equal(bucket, jobEventsBucket):
		v, err := quotaValue[JobEvent](data, value)
		if err != nil || v == nil {
			return counts, err
		}
		counts.TenantID, counts.InboxEvents, counts.InboxBytes = v.TenantID, 1, uint64(len(v.Data))
	case bytes.Equal(bucket, jobActivationsBucket):
		v, err := quotaValue[JobActivation](data, value)
		if err != nil || v == nil {
			return counts, err
		}
		counts.TenantID, counts.Activations, counts.ActivationBytes = v.TenantID, 1, uint64(len(v.Data))
	case bytes.Equal(bucket, jobTenantsBucket):
		v, err := quotaValue[JobTenant](data, value)
		if err != nil || v == nil {
			return counts, err
		}
		counts.TenantID = v.ID
	}
	return counts, nil
}

func quotaMetrics(v *JobUsage) []*uint64 {
	return []*uint64{&v.PendingRuns, &v.StoredRuns, &v.InboxEvents, &v.InboxBytes, &v.Activations, &v.ActivationBytes}
}
func replaceJobUsage(usage *JobUsage, before, after JobUsage) error {
	current, old, next := quotaMetrics(usage), quotaMetrics(&before), quotaMetrics(&after)
	for i, n := range current {
		if *n < *old[i] {
			return fmt.Errorf("job usage counter underflow")
		}
		value := *n - *old[i]
		if math.MaxUint64-value < *next[i] {
			return fmt.Errorf("job usage counter overflow")
		}
		*n = value + *next[i]
	}
	return nil
}
func validateJobUsage(limits JobLimits, before, after JobUsage) error {
	caps := []uint64{limits.MaxPendingRuns, limits.MaxStoredRuns, limits.MaxInboxEvents, limits.MaxInboxBytes, limits.MaxActivations, limits.MaxActivationBytes}
	names := []string{"pending runs", "stored runs", "retained inbox events", "retained inbox payload bytes", "accepted activations", "retained activation payload bytes"}
	old, next := quotaMetrics(&before), quotaMetrics(&after)
	for i, maximum := range caps {
		// A lowered limit never blocks completion, cleanup, consumption, or a
		// duplicate acknowledgement. Only growth in the corresponding metric stops.
		if maximum != 0 && *next[i] > *old[i] && *next[i] > maximum {
			return reject("quota_exceeded", "tenant limit reached for "+names[i])
		}
	}
	return nil
}

func quotaBucket(name []byte) bool {
	return bytes.Equal(name, jobRunsBucket) || bytes.Equal(name, jobEventsBucket) || bytes.Equal(name, jobActivationsBucket) || bytes.Equal(name, jobTenantsBucket)
}

// Check the final staged value for each primary key before applying any writes.
// This covers old command versions as well as launches, materialization, event
// acceptance, signal delivery, and all later run transitions.
func accountJobChanges(tx *bbolt.Tx, s *jobChanges) error {
	last := make(map[string]int)
	for i, w := range *s {
		if quotaBucket(w.bucket) {
			last[jobKey(string(w.bucket), w.key)] = i
		}
	}
	before := make(map[string]JobUsage)
	after := make(map[string]*JobUsage)
	for i, w := range *s {
		if !quotaBucket(w.bucket) || last[jobKey(string(w.bucket), w.key)] != i {
			continue
		}
		raw := tx.Bucket(w.bucket).Get([]byte(w.key))
		old, err := quotaRecord(w.bucket, raw, nil)
		if err != nil {
			return err
		}
		var next JobUsage
		if !w.remove {
			next, err = quotaRecord(w.bucket, nil, w.value)
			if err != nil {
				return err
			}
		}
		tenant := next.TenantID
		if tenant == "" {
			tenant = old.TenantID
		}
		if tenant == "" {
			continue
		}
		if old.TenantID != "" && next.TenantID != "" && old.TenantID != next.TenantID {
			return fmt.Errorf("job quota record changed tenant")
		}
		usage := after[tenant]
		if usage == nil {
			if bytes.Equal(w.bucket, jobTenantsBucket) && raw == nil {
				usage = &JobUsage{TenantID: tenant}
			} else {
				usage, err = jobUsageInTx(tx, tenant)
				if err != nil {
					return err
				}
			}
			before[tenant] = *usage
			after[tenant] = usage
		}
		if err := replaceJobUsage(usage, old, next); err != nil {
			return err
		}
	}
	tenants := make([]string, 0, len(after))
	for id := range after {
		tenants = append(tenants, id)
	}
	sort.Strings(tenants)
	for _, tenant := range tenants {
		limits, err := jobLimitsInTx(tx, tenant)
		if err != nil {
			return err
		}
		if err := validateJobUsage(*limits, before[tenant], *after[tenant]); err != nil {
			return err
		}
		if before[tenant] != *after[tenant] || tx.Bucket(jobUsageBucket).Get([]byte(tenant)) == nil {
			s.put(jobUsageBucket, tenant, after[tenant])
		}
	}
	return nil
}

func rebuildJobUsage(tx *bbolt.Tx) error {
	if err := tx.DeleteBucket(jobUsageBucket); err != nil {
		return err
	}
	if _, err := tx.CreateBucket(jobUsageBucket); err != nil {
		return err
	}
	if err := tx.Bucket(jobTenantsBucket).ForEach(func(k, v []byte) error {
		return writeRecord(tx, jobUsageBucket, string(k), &JobUsage{TenantID: string(k)})
	}); err != nil {
		return err
	}
	for _, bucket := range [][]byte{jobRunsBucket, jobEventsBucket, jobActivationsBucket} {
		if err := tx.Bucket(bucket).ForEach(func(k, data []byte) error {
			counts, err := quotaRecord(bucket, data, nil)
			if err != nil {
				return err
			}
			usage, err := jobUsageInTx(tx, counts.TenantID)
			if err != nil {
				return err
			}
			if err := replaceJobUsage(usage, JobUsage{}, counts); err != nil {
				return err
			}
			return writeRecord(tx, jobUsageBucket, usage.TenantID, usage)
		}); err != nil {
			return err
		}
	}
	return nil
}

func limitJobCapacity(capacity int, used, maximum uint64) int {
	if maximum == 0 {
		return capacity
	}
	if used >= maximum {
		return 0
	}
	return int(min(uint64(capacity), maximum-used))
}
func tenantJobCapacity(tx *bbolt.Tx, tenant string, capacity int) (int, error) {
	limits, err := jobLimitsInTx(tx, tenant)
	if err != nil {
		return 0, err
	}
	usage, err := jobUsageInTx(tx, tenant)
	if err != nil {
		return 0, err
	}
	capacity = limitJobCapacity(capacity, usage.PendingRuns, limits.MaxPendingRuns)
	return limitJobCapacity(capacity, usage.StoredRuns, limits.MaxStoredRuns), nil
}

func (q *DB) GetJobLimits(tenant string) (*JobLimits, error) {
	var result *JobLimits
	err := q.db.View(func(tx *bbolt.Tx) error {
		if _, err := requireJobTenant(tx, tenant); err != nil {
			return err
		}
		var err error
		result, err = jobLimitsInTx(tx, tenant)
		return err
	})
	return result, err
}
func (q *DB) GetJobUsage(tenant string) (*JobUsage, error) {
	var result *JobUsage
	err := q.db.View(func(tx *bbolt.Tx) error {
		if _, err := requireJobTenant(tx, tenant); err != nil {
			return err
		}
		var err error
		result, err = jobUsageInTx(tx, tenant)
		return err
	})
	return result, err
}
