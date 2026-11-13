package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"time"

	"go.etcd.io/bbolt"
)

var (
	jobStateVersionKey   = []byte("job_state_version")
	jobTenantsBucket     = []byte("job_tenants")
	jobGroupsBucket      = []byte("job_groups")
	jobDefinitionsBucket = []byte("job_definitions")
	jobBindingsBucket    = []byte("job_bindings")
	jobResourcesBucket   = []byte("job_resources")
	jobGrantsBucket      = []byte("job_resource_grants")
	jobRunsBucket        = []byte("job_runs")
	jobAttemptsBucket    = []byte("job_attempts")
	jobActionsBucket     = []byte("job_actions")
	jobEventsBucket      = []byte("job_events")
	jobEventKeysBucket   = []byte("job_event_keys")
	jobReadyBucket       = []byte("job_ready")
	jobLeasesBucket      = []byte("job_leases")
	jobWaitsBucket       = []byte("job_waits")
	jobCountersBucket    = []byte("job_counters")
	jobResolutionsBucket = []byte("job_resolutions")
	jobStateBuckets      = [][]byte{jobTenantsBucket, jobGroupsBucket, jobDefinitionsBucket, jobBindingsBucket,
		jobResourcesBucket, jobGrantsBucket, jobRunsBucket, jobAttemptsBucket, jobActionsBucket,
		jobEventsBucket, jobEventKeysBucket, jobReadyBucket, jobLeasesBucket, jobWaitsBucket, jobCountersBucket, jobResolutionsBucket}
)

func initializeJobState(tx *bbolt.Tx) error {
	meta := tx.Bucket(metadataBucket)
	version := meta.Get(jobStateVersionKey)
	if version != nil {
		if !bytes.Equal(version, []byte{1}) {
			return fmt.Errorf("unsupported generic job state version")
		}
		for _, name := range jobStateBuckets {
			if tx.Bucket(name) == nil {
				return fmt.Errorf("generic job state is missing bucket %q", name)
			}
		}
		return nil
	}
	if meta.Get(formatKey)[0] >= 2 {
		return fmt.Errorf("generic job state is missing its format marker")
	}
	for _, name := range jobStateBuckets {
		if b := tx.Bucket(name); b != nil && b.Stats().KeyN != 0 {
			return fmt.Errorf("generic job bucket %q has data without a format marker", name)
		}
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}
	return meta.Put(jobStateVersionKey, []byte{1})
}

// Length prefixes keep tenant/run namespaces unambiguous, including identities
// containing slashes or NULs. Physical resource IDs are intentionally global.
func jobKey(parts ...string) string {
	var key []byte
	for _, part := range parts {
		key = binary.BigEndian.AppendUint32(key, uint32(len(part)))
		key = append(key, part...)
	}
	return string(key)
}

func jobActionKey(tenant, run string, sequence uint64) string {
	return jobKey(tenant, run, fmt.Sprintf("%020d", sequence))
}

// Expected conflicts are recorded as receipts in the same Bolt transaction.
// Stage every mutation until all checks pass, so a rejection never commits a
// partially acquired resource set, consumed event, or changed quota counter.
type jobWrite struct {
	bucket []byte
	key    string
	value  any
	raw    []byte
	remove bool
}
type jobChanges []jobWrite

func (s *jobChanges) put(bucket []byte, key string, value any) {
	*s = append(*s, jobWrite{bucket: bucket, key: key, value: value})
}
func (s *jobChanges) index(bucket []byte, key string, value []byte) {
	*s = append(*s, jobWrite{bucket: bucket, key: key, raw: value})
}
func (s *jobChanges) del(bucket []byte, key string) {
	*s = append(*s, jobWrite{bucket: bucket, key: key, remove: true})
}
func (s jobChanges) apply(tx *bbolt.Tx) error {
	if err := updateWorkerReleaseUsage(tx, s); err != nil {
		return err
	}
	for _, w := range s {
		var err error
		switch {
		case w.remove:
			err = tx.Bucket(w.bucket).Delete([]byte(w.key))
		case w.value != nil:
			err = writeRecord(tx, w.bucket, w.key, w.value)
		default:
			err = tx.Bucket(w.bucket).Put([]byte(w.key), w.raw)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func saveJobRun(s *jobChanges, before, after *JobRun) {
	key := jobKey(after.TenantID, after.ID)
	if before != nil && knownTerminalJob(before) {
		s.del(jobRetentionIndexBucket, retentionIndexKey(before))
	}
	if knownTerminalJob(after) {
		s.index(jobRetentionIndexBucket, retentionIndexKey(after), []byte(key))
	}
	if before != nil && before.TriggerID != "" && scheduledJobPending(before) {
		s.del(jobTriggerPendingBucket, jobKey(before.TenantID, before.TriggerID, before.ID))
	}
	if after.TriggerID != "" && scheduledJobPending(after) {
		s.index(jobTriggerPendingBucket, jobKey(after.TenantID, after.TriggerID, after.ID), []byte(key))
	}
	if before != nil {
		if before.Status == JobReady {
			s.del(jobReadyBucket, key)
		}
		if before.OwnerID != "" {
			s.del(jobLeasesBucket, string(timedKey(before.LeaseUntil, key)))
		}
		if before.Status == JobWaiting && before.OwnerID == "" && !before.WakeAt.IsZero() {
			s.del(jobWaitsBucket, string(timedKey(before.WakeAt, key)))
		}
	}
	s.put(jobRunsBucket, key, after)
	if after.Status == JobReady {
		s.index(jobReadyBucket, key, []byte(key))
	}
	if after.OwnerID != "" {
		s.index(jobLeasesBucket, string(timedKey(after.LeaseUntil, key)), []byte(key))
	}
	if after.Status == JobWaiting && after.OwnerID == "" && !after.WakeAt.IsZero() {
		s.index(jobWaitsBucket, string(timedKey(after.WakeAt, key)), []byte(key))
	}
}

func jobRevision(tx *bbolt.Tx, bucket []byte, key string, expected uint64, revision func() (uint64, error)) error {
	current := uint64(0)
	if tx.Bucket(bucket).Get([]byte(key)) != nil {
		var err error
		current, err = revision()
		if err != nil {
			return err
		}
	}
	if current != expected || current == math.MaxUint64 {
		return reject("revision_conflict", "configuration revision changed")
	}
	return nil
}

func requireJobTenant(tx *bbolt.Tx, id string) (*JobTenant, error) {
	if !validIdentity(id) {
		return nil, reject("invalid", "tenant ID is required")
	}
	return readRecord[JobTenant](tx, jobTenantsBucket, id)
}

func validateResourceIDs(ids []string) error {
	if len(ids) > 32 {
		return reject("invalid", "at most 32 resource requirements are supported")
	}
	seen := make(map[string]bool)
	for _, id := range ids {
		if !validIdentity(id) || seen[id] {
			return reject("invalid", "resource IDs must be valid and distinct")
		}
		seen[id] = true
	}
	return nil
}

func checkJobGrants(tx *bbolt.Tx, tenant string, ids []string) error {
	for _, id := range ids {
		if _, err := readRecord[JobResource](tx, jobResourcesBucket, id); err != nil {
			return err
		}
		key := jobKey(tenant, id)
		if tx.Bucket(jobGrantsBucket).Get([]byte(key)) == nil {
			return reject("resource_denied", "tenant has no grant for a required resource")
		}
		g, err := readRecord[ResourceGrant](tx, jobGrantsBucket, key)
		if err != nil {
			return err
		}
		if !g.Allowed {
			return reject("resource_denied", "tenant's resource grant was revoked")
		}
	}
	return nil
}

// A gateway's legacy owner record remains the physical authority. While a
// generic conversation holds it, its complete reservation must also exclude
// legacy claims/releases using the same owner identity (including between
// actions). This adds no second acquisition path or synthetic legacy attempt.
func noJobChannelReservation(tx *bbolt.Tx, channel *CommunicationChannel) error {
	b := tx.Bucket(jobResourcesBucket)
	if b == nil || b.Get([]byte(channel.ID)) == nil {
		return nil
	}
	resource, err := readRecord[JobResource](tx, jobResourcesBucket, channel.ID)
	if err != nil {
		return err
	}
	if resource.Spec.ChannelID == channel.ID && resource.ChannelEpoch == channel.Epoch && resource.OwnerID == channel.OwnerID && (resource.State == ResourceOwned || resource.State == ResourceBlocked) {
		return reject("channel_busy", "gateway is reserved by a generic job conversation")
	}
	return nil
}

func transitionJob(tx *bbolt.Tx, command StateCommand, result *StateResult) error {
	c := command.Job.normalized()
	s := jobChanges{}
	r := &JobResult{}
	var err error
	switch {
	case c.RetireConfiguration != nil:
		err = retireJobConfiguration(tx, &s, command.At, *c.RetireConfiguration, r)
	case c.RetireTrigger != nil:
		err = retireJobTrigger(tx, &s, command.At, *c.RetireTrigger, r)
	case c.CompactConfigurationReceipts != nil:
		err = compactJobConfigurationReceipts(tx, &s, command, *c.CompactConfigurationReceipts, r)
	case c.PutConfigurationLimits != nil:
		err = putJobConfigurationLimits(tx, &s, *c.PutConfigurationLimits, r)
	case c.CompactReceipts != nil:
		err = compactJobReceipts(tx, &s, command, *c.CompactReceipts, r)
	case c.PutRetention != nil:
		err = putJobRetention(tx, &s, *c.PutRetention, r)
	case c.Purge != nil:
		err = purgeJob(tx, &s, command, *c.Purge, r)
	case c.PutLimits != nil:
		err = putJobLimits(tx, &s, *c.PutLimits, r)
	case c.Activate != nil:
		err = activateJob(tx, &s, command.At, *c.Activate, r)
	case c.PutTrigger != nil:
		err = putJobTrigger(tx, &s, command.At, *c.PutTrigger, r)
	case c.PauseTrigger != nil:
		err = pauseJobTrigger(tx, &s, command.At, *c.PauseTrigger, r)
	case c.Materialize != nil:
		err = materializeJobs(tx, &s, command.At, *c.Materialize, r)

	case c.PutTenant != nil:
		v := c.PutTenant.Tenant
		if !validIdentity(v.ID) {
			return reject("invalid", "tenant ID is required")
		}
		err = jobRevision(tx, jobTenantsBucket, v.ID, c.PutTenant.ExpectedRevision, func() (uint64, error) {
			old, e := readRecord[JobTenant](tx, jobTenantsBucket, v.ID)
			if e != nil {
				return 0, e
			}
			return old.Revision, nil
		})
		if err != nil {
			return err
		}
		v.Revision = c.PutTenant.ExpectedRevision + 1
		s.put(jobTenantsBucket, v.ID, &v)
		r.Tenant = &v
	case c.PutGroup != nil:
		v := c.PutGroup.Group
		if err := checkJobConfigurationIdentity(tx, JobGroupRecord, v.TenantID, v.ID); err != nil {
			return err
		}
		if _, err := requireJobTenant(tx, v.TenantID); err != nil {
			return err
		}
		if !validIdentity(v.ID) || !validIdentity(v.Runtime) {
			return reject("invalid", "group ID and default runtime are required")
		}
		if err := validateResourceIDs(v.Resources); err != nil {
			return err
		}
		if err := checkJobGrants(tx, v.TenantID, v.Resources); err != nil {
			return err
		}
		key := jobKey(v.TenantID, v.ID)
		err = jobRevision(tx, jobGroupsBucket, key, c.PutGroup.ExpectedRevision, func() (uint64, error) {
			old, e := readRecord[JobGroup](tx, jobGroupsBucket, key)
			if e != nil {
				return 0, e
			}
			return old.Revision, nil
		})
		if err != nil {
			return err
		}
		v.Revision = c.PutGroup.ExpectedRevision + 1
		s.put(jobGroupsBucket, key, &v)
		r.Group = &v
	case c.PutBinding != nil:
		v := c.PutBinding.Binding
		if err := checkJobConfigurationIdentity(tx, JobBindingRecord, v.TenantID, v.ID); err != nil {
			return err
		}
		if _, err := requireJobTenant(tx, v.TenantID); err != nil {
			return err
		}
		if !validIdentity(v.ID) || !validIdentity(v.Driver) || !validIdentity(v.DriverVersion) || len(v.Config) > 1<<20 {
			return reject("invalid", "binding ID, versioned driver, and config of at most 1 MiB are required")
		}
		if err := validateResourceIDs(v.Resources); err != nil {
			return err
		}
		if err := checkJobGrants(tx, v.TenantID, v.Resources); err != nil {
			return err
		}
		key := jobKey(v.TenantID, v.ID)
		err = jobRevision(tx, jobBindingsBucket, key, c.PutBinding.ExpectedRevision, func() (uint64, error) {
			old, e := readRecord[JobBinding](tx, jobBindingsBucket, key)
			if e != nil {
				return 0, e
			}
			return old.Revision, nil
		})
		if err != nil {
			return err
		}
		v.Revision = c.PutBinding.ExpectedRevision + 1
		s.put(jobBindingsBucket, key, &v)
		r.Binding = &v
	case c.PutDefinition != nil:
		v := c.PutDefinition.Definition
		if err := checkWorkerRouting(tx, v.TenantID, v); err != nil {
			return err
		}
		if err := checkJobConfigurationIdentity(tx, JobDefinitionRecord, v.TenantID, v.ID); err != nil {
			return err
		}
		if _, err := requireJobTenant(tx, v.TenantID); err != nil {
			return err
		}
		if !validIdentity(v.ID) || !validIdentity(v.Handler) || !validIdentity(v.HandlerVersion) || len(v.Input) > 1<<20 {
			return reject("invalid", "definition ID, versioned handler, and input of at most 1 MiB are required")
		}
		if v.GroupID == "" && !validIdentity(v.Runtime) {
			return reject("invalid", "definition needs a runtime or group default")
		}
		if v.Runtime != "" && !validIdentity(v.Runtime) {
			return reject("invalid", "runtime must be a valid identity")
		}
		if err := validateResourceIDs(v.Resources); err != nil {
			return err
		}
		if err := checkJobGrants(tx, v.TenantID, v.Resources); err != nil {
			return err
		}
		if v.GroupID != "" {
			if _, err := readRecord[JobGroup](tx, jobGroupsBucket, jobKey(v.TenantID, v.GroupID)); err != nil {
				return err
			}
		}
		if v.BindingID != "" {
			if _, err := readRecord[JobBinding](tx, jobBindingsBucket, jobKey(v.TenantID, v.BindingID)); err != nil {
				return err
			}
		}
		key := jobKey(v.TenantID, v.ID)
		err = jobRevision(tx, jobDefinitionsBucket, key, c.PutDefinition.ExpectedRevision, func() (uint64, error) {
			old, e := readRecord[JobDefinition](tx, jobDefinitionsBucket, key)
			if e != nil {
				return 0, e
			}
			return old.Revision, nil
		})
		if err != nil {
			return err
		}
		v.Revision = c.PutDefinition.ExpectedRevision + 1
		s.put(jobDefinitionsBucket, key, &v)
		r.Definition = &v
	case c.PutResource != nil:
		v := c.PutResource.Spec
		if !validIdentity(v.ID) || v.Capacity != 1 || (v.Duplex != "none" && v.Duplex != "half" && v.Duplex != "full") || (v.ChannelID != "" && v.ChannelID != v.ID) {
			return reject("invalid", "resource requires a canonical ID, duplex mode, and exclusive capacity 1; gateway links must use the channel ID")
		}
		resource := &JobResource{Spec: v, State: ResourceAvailable}
		if tx.Bucket(jobResourcesBucket).Get([]byte(v.ID)) != nil {
			resource, err = readRecord[JobResource](tx, jobResourcesBucket, v.ID)
			if err != nil {
				return err
			}
			if resource.State != ResourceAvailable || resource.Spec.ChannelID != v.ChannelID {
				return reject("resource_busy", "resource must be available and its gateway authority cannot be changed")
			}
		}
		if resource.Revision != c.PutResource.ExpectedRevision || resource.Revision == math.MaxUint64 {
			return reject("revision_conflict", "resource revision changed")
		}
		if command.At.Before(resource.LastTransition) {
			return reject("stale_time", "resource configuration predates its last transition")
		}
		if v.ChannelID != "" {
			if _, err := readRecord[CommunicationChannel](tx, channelsBucket, v.ChannelID); err != nil {
				return err
			}
		}
		resource.Spec, resource.Revision, resource.LastTransition = v, resource.Revision+1, command.At
		s.put(jobResourcesBucket, v.ID, resource)
		r.Resource = resource
	case c.Grant != nil:
		v := c.Grant.Grant
		if _, err := requireJobTenant(tx, v.TenantID); err != nil {
			return err
		}
		if _, err := readRecord[JobResource](tx, jobResourcesBucket, v.ResourceID); err != nil {
			return err
		}
		key := jobKey(v.TenantID, v.ResourceID)
		err = jobRevision(tx, jobGrantsBucket, key, c.Grant.ExpectedRevision, func() (uint64, error) {
			old, e := readRecord[ResourceGrant](tx, jobGrantsBucket, key)
			if e != nil {
				return 0, e
			}
			return old.Revision, nil
		})
		if err != nil {
			return err
		}
		v.Revision = c.Grant.ExpectedRevision + 1
		s.put(jobGrantsBucket, key, &v)
		r.Grant = &v
	case c.Launch != nil:
		err = launchJob(tx, &s, command.At, *c.Launch, r)
	case c.Claim != nil:
		err = claimJob(tx, &s, command.At, *c.Claim, r)
	case c.Renew != nil:
		err = renewJob(tx, &s, command.At, *c.Renew, r)
	case c.Yield != nil:
		err = yieldJob(tx, &s, command.At, *c.Yield, r)
	case c.Advance != nil:
		err = advanceJob(tx, &s, command.At, *c.Advance, r)
	case c.CompleteAction != nil:
		err = completeJobAction(tx, &s, command.At, *c.CompleteAction, r)
	case c.Signal != nil:
		err = signalJob(tx, &s, command.At, *c.Signal, r)
	case c.Cancel != nil:
		err = cancelJob(tx, &s, command.At, *c.Cancel, r)
	case c.Expire != nil:
		err = expireJob(tx, &s, command.At, *c.Expire, r)
	case c.Wake != nil:
		err = wakeJob(tx, &s, command.At, *c.Wake, r)
	case c.ResolveResource != nil:
		err = resolveJobResource(tx, &s, command, *c.ResolveResource, r)
	case c.Resolve != nil:
		err = resolveJob(tx, &s, command, *c.Resolve, r)
	}
	if err != nil {
		return err
	}
	if err := accountJobChanges(tx, &s); err != nil {
		return err
	}
	if err := accountJobConfigurationChanges(tx, &s); err != nil {
		return err
	}
	if err := indexJobConfigurationReferences(tx, &s); err != nil {
		return err
	}
	if err := s.apply(tx); err != nil {
		return err
	}
	result.Job = r
	return nil
}

func launchJob(tx *bbolt.Tx, s *jobChanges, at time.Time, c LaunchJob, r *JobResult) error {
	if _, err := requireJobTenant(tx, c.TenantID); err != nil {
		return err
	}
	if !validIdentity(c.RunID) {
		return reject("invalid", "run ID is required")
	}
	key := jobKey(c.TenantID, c.RunID)
	if tx.Bucket(jobDeletionBucket).Get([]byte(key)) != nil {
		return reject("gone", "run ID was purged and cannot be reused")
	}
	if tx.Bucket(jobRunsBucket).Get([]byte(key)) != nil {
		return reject("already_exists", "run already exists in this tenant")
	}
	template, err := resolveJobTemplate(tx, c.TenantID, c.DefinitionID, c.DefinitionRevision)
	if err != nil {
		return err
	}
	run := template.newRun(c.TenantID, c.RunID, at)
	saveJobRun(s, nil, run)
	r.Run = run
	return nil
}

func resolveJobTemplate(tx *bbolt.Tx, tenant, definitionID string, revision uint64) (*JobTemplate, error) {
	d, err := readRecord[JobDefinition](tx, jobDefinitionsBucket, jobKey(tenant, definitionID))
	if err != nil {
		return nil, err
	}
	if d.Revision != revision {
		return nil, reject("revision_conflict", "definition revision changed")
	}
	if err := checkWorkerRouting(tx, tenant, *d); err != nil {
		return nil, err
	}
	run := &JobTemplate{Definition: *d, Runtime: d.Runtime}
	ids := append([]string(nil), d.Resources...)
	if d.GroupID != "" {
		run.Group, err = readRecord[JobGroup](tx, jobGroupsBucket, jobKey(tenant, d.GroupID))
		if err != nil {
			return nil, err
		}
		if run.Runtime == "" {
			run.Runtime = run.Group.Runtime
		}
		ids = append(ids, run.Group.Resources...)
	}
	if d.BindingID != "" {
		run.Binding, err = readRecord[JobBinding](tx, jobBindingsBucket, jobKey(tenant, d.BindingID))
		if err != nil {
			return nil, err
		}
		ids = append(ids, run.Binding.Resources...)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if len(run.Resources) == 0 || run.Resources[len(run.Resources)-1] != id {
			run.Resources = append(run.Resources, id)
		}
	}
	if err := validateResourceIDs(run.Resources); err != nil {
		return nil, err
	}
	if err := checkJobGrants(tx, tenant, run.Resources); err != nil {
		return nil, err
	}
	return run, nil
}

func (t JobTemplate) newRun(tenant, id string, at time.Time) *JobRun {
	t = cloneJobTemplate(t)
	t.Definition.WorkerRelease = routedRelease(tenant, id, t.Definition)
	return &JobRun{TenantID: tenant, ID: id, Definition: t.Definition, Runtime: t.Runtime, Group: t.Group, Binding: t.Binding, Resources: t.Resources, Status: JobReady, CreatedAt: at, ReadySince: at, LastTransition: at}
}
