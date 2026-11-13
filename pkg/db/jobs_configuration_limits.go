package db

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	"go.etcd.io/bbolt"
)

// Configuration limits cover current primary configuration and the execution
// templates pinned by triggers. They do not limit execution or recovery writes.
type JobConfigurationLimits struct {
	TenantID                                            string
	Revision                                            uint64
	MaxGroups, MaxDefinitions, MaxBindings, MaxTriggers uint64
	MaxPayloadBytes                                     uint64
}

type PutJobConfigurationLimits struct {
	Limits           JobConfigurationLimits
	ExpectedRevision uint64
}

type JobConfigurationUsage struct {
	TenantID                                string
	Groups, Definitions, Bindings, Triggers uint64
	PayloadBytes                            uint64
}

var (
	jobConfigurationVersionKey   = []byte("job_configuration_state_version")
	jobConfigurationLimitsBucket = []byte("job_configuration_limits")
	jobConfigurationUsageBucket  = []byte("job_configuration_usage")
	jobConfigurationBuckets      = [][]byte{jobConfigurationLimitsBucket, jobConfigurationUsageBucket}
)

func initializeJobConfigurationState(tx *bbolt.Tx) error {
	meta := tx.Bucket(metadataBucket)
	version := meta.Get(jobConfigurationVersionKey)
	if version != nil && !bytes.Equal(version, []byte{1}) {
		return fmt.Errorf("unsupported job configuration accounting version")
	}
	activated := meta.Get(formatKey)[0] >= 8
	if activated && version == nil {
		return fmt.Errorf("job configuration accounting is missing its format marker")
	}
	for _, name := range jobConfigurationBuckets {
		if version != nil && tx.Bucket(name) == nil {
			return fmt.Errorf("job configuration accounting is missing bucket %q", name)
		}
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}
	if !activated {
		if tx.Bucket(jobConfigurationLimitsBucket).Stats().KeyN != 0 {
			return fmt.Errorf("configuration limits without an activated database format")
		}
		// Older writers do not maintain these counters. Rebuild on every open
		// and restore until format 8 excludes them.
		if err := rebuildJobConfigurationUsage(tx); err != nil {
			return err
		}
	}
	return meta.Put(jobConfigurationVersionKey, []byte{1})
}

func configurationLimitsInTx(tx *bbolt.Tx, tenant string) (*JobConfigurationLimits, error) {
	if tx.Bucket(jobConfigurationLimitsBucket).Get([]byte(tenant)) == nil {
		return &JobConfigurationLimits{TenantID: tenant}, nil
	}
	v, err := readRecord[JobConfigurationLimits](tx, jobConfigurationLimitsBucket, tenant)
	if err == nil && v.TenantID != tenant {
		return nil, fmt.Errorf("inconsistent configuration limits tenant")
	}
	return v, err
}

func configurationUsageInTx(tx *bbolt.Tx, tenant string) (*JobConfigurationUsage, error) {
	v, err := readRecord[JobConfigurationUsage](tx, jobConfigurationUsageBucket, tenant)
	if err != nil {
		return nil, fmt.Errorf("read configuration usage: %w", err)
	}
	if v.TenantID != tenant {
		return nil, fmt.Errorf("inconsistent configuration usage tenant")
	}
	return v, nil
}

func putJobConfigurationLimits(tx *bbolt.Tx, s *jobChanges, c PutJobConfigurationLimits, r *JobResult) error {
	v := c.Limits
	if _, err := requireJobTenant(tx, v.TenantID); err != nil {
		return err
	}
	old, err := configurationLimitsInTx(tx, v.TenantID)
	if err != nil {
		return err
	}
	if old.Revision != c.ExpectedRevision || old.Revision == math.MaxUint64 {
		return reject("revision_conflict", "configuration limits revision changed")
	}
	v.Revision = old.Revision + 1
	s.put(jobConfigurationLimitsBucket, v.TenantID, &v)
	r.ConfigurationLimits = &v
	return nil
}

func configurationBucket(bucket []byte) bool {
	return bytes.Equal(bucket, jobGroupsBucket) || bytes.Equal(bucket, jobDefinitionsBucket) || bytes.Equal(bucket, jobBindingsBucket) || bytes.Equal(bucket, jobTriggersBucket) || bytes.Equal(bucket, jobTenantsBucket)
}

// Byte accounting uses raw application payloads, never local serialization.
// Trigger templates count their own copies independently of current definitions
// and bindings. Keep this field set stable for v14 replay.
func configurationRecord(bucket []byte, data []byte, value any) (JobConfigurationUsage, error) {
	var u JobConfigurationUsage
	switch {
	case bytes.Equal(bucket, jobGroupsBucket):
		v, err := quotaValue[JobGroup](data, value)
		if err != nil || v == nil {
			return u, err
		}
		u.TenantID, u.Groups = v.TenantID, 1
	case bytes.Equal(bucket, jobDefinitionsBucket):
		v, err := quotaValue[JobDefinition](data, value)
		if err != nil || v == nil {
			return u, err
		}
		u.TenantID, u.Definitions, u.PayloadBytes = v.TenantID, 1, uint64(len(v.Input))
	case bytes.Equal(bucket, jobBindingsBucket):
		v, err := quotaValue[JobBinding](data, value)
		if err != nil || v == nil {
			return u, err
		}
		u.TenantID, u.Bindings, u.PayloadBytes = v.TenantID, 1, uint64(len(v.Config))
	case bytes.Equal(bucket, jobTriggersBucket):
		v, err := quotaValue[JobTrigger](data, value)
		if err != nil || v == nil {
			return u, err
		}
		u.TenantID, u.Triggers, u.PayloadBytes = v.TenantID, 1, uint64(len(v.Template.Definition.Input))
		if v.Template.Binding != nil {
			u.PayloadBytes += uint64(len(v.Template.Binding.Config))
		}
	case bytes.Equal(bucket, jobTenantsBucket):
		v, err := quotaValue[JobTenant](data, value)
		if err != nil || v == nil {
			return u, err
		}
		u.TenantID = v.ID
	}
	return u, nil
}

func configurationMetrics(v *JobConfigurationUsage) []*uint64 {
	return []*uint64{&v.Groups, &v.Definitions, &v.Bindings, &v.Triggers, &v.PayloadBytes}
}

func replaceConfigurationUsage(usage *JobConfigurationUsage, old, next JobConfigurationUsage) error {
	current, previous, incoming := configurationMetrics(usage), configurationMetrics(&old), configurationMetrics(&next)
	for i, value := range current {
		if *value < *previous[i] {
			return fmt.Errorf("configuration usage counter underflow")
		}
		remainder := *value - *previous[i]
		if math.MaxUint64-remainder < *incoming[i] {
			return fmt.Errorf("configuration usage counter overflow")
		}
		*value = remainder + *incoming[i]
	}
	return nil
}

func validateConfigurationUsage(limits JobConfigurationLimits, old, next JobConfigurationUsage) error {
	maximum := []uint64{limits.MaxGroups, limits.MaxDefinitions, limits.MaxBindings, limits.MaxTriggers, limits.MaxPayloadBytes}
	names := []string{"groups", "definitions", "bindings", "triggers", "payload bytes"}
	before, after := configurationMetrics(&old), configurationMetrics(&next)
	for i, max := range maximum {
		if max != 0 && *after[i] > max && *after[i] > *before[i] {
			return reject("quota_exceeded", "tenant configuration "+names[i]+" limit exceeded")
		}
	}
	return nil
}

func accountJobConfigurationChanges(tx *bbolt.Tx, s *jobChanges) error {
	last := make(map[string]int)
	for i, w := range *s {
		if configurationBucket(w.bucket) {
			last[jobKey(string(w.bucket), w.key)] = i
		}
	}
	before := make(map[string]JobConfigurationUsage)
	after := make(map[string]*JobConfigurationUsage)
	for i, w := range *s {
		if !configurationBucket(w.bucket) || last[jobKey(string(w.bucket), w.key)] != i {
			continue
		}
		raw := tx.Bucket(w.bucket).Get([]byte(w.key))
		old, err := configurationRecord(w.bucket, raw, nil)
		if err != nil {
			return err
		}
		var next JobConfigurationUsage
		if !w.remove {
			next, err = configurationRecord(w.bucket, w.raw, w.value)
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
			return fmt.Errorf("configuration record changed tenant")
		}
		usage := after[tenant]
		if usage == nil {
			if bytes.Equal(w.bucket, jobTenantsBucket) && raw == nil {
				usage = &JobConfigurationUsage{TenantID: tenant}
			} else {
				usage, err = configurationUsageInTx(tx, tenant)
				if err != nil {
					return err
				}
			}
			before[tenant], after[tenant] = *usage, usage
		}
		if err := replaceConfigurationUsage(usage, old, next); err != nil {
			return err
		}
	}
	tenants := make([]string, 0, len(after))
	for tenant := range after {
		tenants = append(tenants, tenant)
	}
	sort.Strings(tenants)
	for _, tenant := range tenants {
		limits, err := configurationLimitsInTx(tx, tenant)
		if err != nil {
			return err
		}
		if err := validateConfigurationUsage(*limits, before[tenant], *after[tenant]); err != nil {
			return err
		}
		if before[tenant] != *after[tenant] || tx.Bucket(jobConfigurationUsageBucket).Get([]byte(tenant)) == nil {
			s.put(jobConfigurationUsageBucket, tenant, after[tenant])
		}
	}
	return nil
}

func rebuildJobConfigurationUsage(tx *bbolt.Tx) error {
	if err := tx.DeleteBucket(jobConfigurationUsageBucket); err != nil {
		return err
	}
	if _, err := tx.CreateBucket(jobConfigurationUsageBucket); err != nil {
		return err
	}
	if err := tx.Bucket(jobTenantsBucket).ForEach(func(k, _ []byte) error {
		return writeRecord(tx, jobConfigurationUsageBucket, string(k), &JobConfigurationUsage{TenantID: string(k)})
	}); err != nil {
		return err
	}
	for _, bucket := range [][]byte{jobGroupsBucket, jobDefinitionsBucket, jobBindingsBucket, jobTriggersBucket} {
		if err := tx.Bucket(bucket).ForEach(func(_, data []byte) error {
			counts, err := configurationRecord(bucket, data, nil)
			if err != nil {
				return err
			}
			usage, err := configurationUsageInTx(tx, counts.TenantID)
			if err != nil {
				return err
			}
			if err := replaceConfigurationUsage(usage, JobConfigurationUsage{}, counts); err != nil {
				return err
			}
			return writeRecord(tx, jobConfigurationUsageBucket, usage.TenantID, usage)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (q *DB) GetJobConfigurationLimits(tenant string) (*JobConfigurationLimits, error) {
	var result *JobConfigurationLimits
	err := q.db.View(func(tx *bbolt.Tx) error {
		if _, err := requireJobTenant(tx, tenant); err != nil {
			return err
		}
		var err error
		result, err = configurationLimitsInTx(tx, tenant)
		return err
	})
	return result, err
}

func (q *DB) GetJobConfigurationUsage(tenant string) (*JobConfigurationUsage, error) {
	var result *JobConfigurationUsage
	err := q.db.View(func(tx *bbolt.Tx) error {
		if _, err := requireJobTenant(tx, tenant); err != nil {
			return err
		}
		var err error
		result, err = configurationUsageInTx(tx, tenant)
		return err
	})
	return result, err
}
