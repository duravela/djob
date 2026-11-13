package db

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/distribution/reference"
	"go.etcd.io/bbolt"
)

// MaxWorkerDeployments bounds both replicated records and reconciliation memory.
const MaxWorkerDeployments = 2048

// WorkerDeployment is desired process state. Worker credentials, Kubernetes
// credentials, observed Pods and company code are never replicated here.
type WorkerDeployment struct {
	TenantID, ID, WorkerID string
	Revision, Generation   uint64
	State                  string // active, retiring or retired; replicas zero means stopped.
	OutstandingRuns        uint64 `json:"-"` // Read-only usage sample, not part of desired state.
	Published              bool   // Publishing freezes the execution template and makes ID a worker release.
	Spec                   WorkerDeploymentSpec
	ChangedAt              time.Time
	Actor, Reason          string
}

type WorkerDeploymentSpec struct {
	Pool, Image, ConfigSecret, PullSecret        string
	Args                                         []string
	Replicas, CPUMillis, MemoryMiB, DrainSeconds uint32
}

func (s WorkerDeploymentSpec) WithDefaults() WorkerDeploymentSpec {
	s.Args = append([]string(nil), s.Args...)
	if s.Pool == "" {
		s.Pool = "default"
	}
	if s.CPUMillis == 0 {
		s.CPUMillis = 100
	}
	if s.MemoryMiB == 0 {
		s.MemoryMiB = 128
	}
	if s.DrainSeconds == 0 {
		s.DrainSeconds = 45
	}
	return s
}

type WorkerDeploymentCommand struct {
	TenantID, ID, WorkerID, Action, Actor, Reason string
	ExpectedRevision                              uint64
	Spec                                          *WorkerDeploymentSpec
	Replicas                                      uint32 // scale only
	ProtectOutstanding                            bool   `json:",omitempty"` // Versioned lifecycle safety; absent on historical commands.
}

var workerDeploymentVersionKey = []byte("worker_deployment_version")
var workerDeploymentsBucket = []byte("worker_deployments")
var workerDeploymentIdentitiesBucket = []byte("worker_deployment_identities")
var workerDeploymentBuckets = [][]byte{workerDeploymentsBucket, workerDeploymentIdentitiesBucket}

func initializeWorkerDeployments(tx *bbolt.Tx) error {
	meta := tx.Bucket(metadataBucket)
	version := meta.Get(workerDeploymentVersionKey)
	format := meta.Get(formatKey)[0]
	if version != nil && !bytes.Equal(version, []byte{1}) || version == nil && format >= 14 {
		return fmt.Errorf("missing or unsupported worker deployment marker")
	}
	for _, name := range workerDeploymentBuckets {
		b := tx.Bucket(name)
		if format < 14 && b != nil && b.Stats().KeyN != 0 {
			return fmt.Errorf("worker deployments without an activated format")
		}
		if version != nil && b == nil || version == nil && b != nil && b.Stats().KeyN != 0 {
			return fmt.Errorf("inconsistent worker deployment bucket")
		}
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}
	return meta.Put(workerDeploymentVersionKey, []byte{1})
}

// Kubernetes names are validated here rather than passed through to the API.
// Pool names use the same portable subset. Deployment IDs stay independent of
// Kubernetes object names, which the runtime derives from a cluster identity.
func validWorkerSecretName(s string) bool {
	if len(s) == 0 || len(s) > 253 {
		return false
	}
	for _, label := range strings.Split(s, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
	}
	return true
}

func validWorkerImage(s string) bool {
	if len(s) > 512 {
		return false
	}
	named, err := reference.ParseNormalizedNamed(s)
	if err != nil || named.String() != s {
		return false
	}
	canonical, ok := named.(reference.Canonical)
	return ok && canonical.Digest().Algorithm().String() == "sha256" && canonical.Digest().Encoded() == strings.ToLower(canonical.Digest().Encoded())
}

func (s WorkerDeploymentSpec) Validate() error {
	s = s.WithDefaults()
	if !validWorkerSecretName(s.Pool) || strings.Contains(s.Pool, ".") || len(s.Pool) > 63 || !validWorkerImage(s.Image) || !validWorkerSecretName(s.ConfigSecret) || s.PullSecret != "" && !validWorkerSecretName(s.PullSecret) {
		return fmt.Errorf("provide a pool, immutable registry/repository@sha256 image and Kubernetes secret names")
	}
	if s.Replicas > 32 || s.CPUMillis < 10 || s.CPUMillis > 64000 || s.MemoryMiB < 32 || s.MemoryMiB > 65536 || s.DrainSeconds < 10 || s.DrainSeconds > 600 || len(s.Args) > 32 {
		return fmt.Errorf("worker deployment resources, replicas or drain period exceed their bounds")
	}
	bytes := 0
	for _, arg := range s.Args {
		bytes += len(arg)
		if len(arg) > 1024 || strings.ContainsRune(arg, 0) || !utf8.ValidString(arg) {
			return fmt.Errorf("invalid worker argument")
		}
	}
	if bytes > 4096 {
		return fmt.Errorf("worker arguments exceed 4096 bytes")
	}
	return nil
}

func (c WorkerDeploymentCommand) validate() error {
	if !validIdentity(c.TenantID) || !utf8.ValidString(c.TenantID) || !validWorkerID(c.ID) || !validWorkerID(c.WorkerID) {
		return fmt.Errorf("provide tenant, deployment and enrolled worker IDs")
	}
	if err := validResolution(c.Actor, c.Reason); err != nil {
		return err
	}
	if len(c.Reason) > 1024 || !utf8.ValidString(c.Reason) {
		return fmt.Errorf("deployment reason exceeds 1024 UTF-8 bytes")
	}
	switch c.Action {
	case "put":
		if c.Spec == nil || c.Replicas != 0 {
			return fmt.Errorf("put requires a complete deployment spec")
		}
		return c.Spec.Validate()
	case "scale", "restart", "retire", "publish", "retiring", "resume":
		if c.Spec != nil || c.ExpectedRevision == 0 || c.Replicas > 32 || c.Action != "scale" && c.Replicas != 0 {
			return fmt.Errorf("lifecycle changes require an existing revision and no replacement spec")
		}
	default:
		return fmt.Errorf("unknown worker deployment action")
	}
	return nil
}

func cloneWorkerDeployment(v *WorkerDeployment) *WorkerDeployment {
	if v == nil {
		return nil
	}
	copy := *v
	copy.Spec.Args = append([]string(nil), v.Spec.Args...)
	return &copy
}

func transitionWorkerDeployment(tx *bbolt.Tx, command StateCommand, result *StateResult) error {
	c := command.WorkerDeployment
	if _, err := requireJobTenant(tx, c.TenantID); err != nil {
		return err
	}
	key := jobKey(c.TenantID, c.ID)
	v := WorkerDeployment{TenantID: c.TenantID, ID: c.ID, WorkerID: c.WorkerID, State: "active"}
	if tx.Bucket(workerDeploymentsBucket).Get([]byte(key)) != nil {
		old, err := readRecord[WorkerDeployment](tx, workerDeploymentsBucket, key)
		if err != nil {
			return err
		}
		v = *old
	}
	if v.Revision != c.ExpectedRevision || v.Revision == math.MaxUint64 || v.Generation == math.MaxUint64 {
		return reject("revision_conflict", "worker deployment revision changed")
	}
	if v.State == "retired" {
		return reject("invalid", "retired worker deployments cannot be reused")
	}
	if v.WorkerID != c.WorkerID {
		return reject("invalid", "deployment worker identity is immutable")
	}
	if c.Action != "put" && v.Revision == 0 {
		return reject("not_found", "worker deployment does not exist")
	}
	if v.Revision != 0 && command.At.Before(v.ChangedAt) {
		return reject("stale_time", "deployment command predates its last change")
	}
	if v.Published {
		if c.Action == "put" {
			oldTemplate, nextTemplate := v.Spec, c.Spec.WithDefaults()
			oldTemplate.Replicas, nextTemplate.Replicas = 0, 0
			if !reflect.DeepEqual(oldTemplate, nextTemplate) {
				return reject("release_immutable", "published release template is immutable; create another deployment")
			}
		}
		stopping := c.Action == "retire" || c.Action == "scale" && c.Replicas == 0 || c.Action == "put" && c.Spec.Replicas == 0
		disruptive := stopping || c.ProtectOutstanding && (c.Action == "restart" || c.Action == "scale" && c.Replicas < v.Spec.Replicas || c.Action == "put" && c.Spec.Replicas < v.Spec.Replicas)
		if disruptive {
			outstanding, err := workerReleaseOutstanding(tx, c.TenantID, c.ID)
			if err != nil {
				return err
			}
			if outstanding != 0 {
				return reject("release_busy", "release still has outstanding runs; restart and replica reductions require a completed drain")
			}
		}
	}
	if c.Action == "publish" {
		if v.Published || v.State != "active" {
			return reject("invalid", "publish an active, unpublished deployment")
		}
		v.Published = true
	} else if c.Action == "retiring" || c.Action == "resume" {
		if !v.Published {
			return reject("invalid", "publish the deployment before changing release admission")
		}
		if c.Action == "retiring" {
			v.State = "retiring"
		} else {
			v.State = "active"
		}
	} else if c.Action == "put" {
		spec := c.Spec.WithDefaults()
		if v.Revision == 0 {
			enrolled, err := readRecord[WorkerEnrollment](tx, workerEnrollmentsBucket, jobKey(c.TenantID, c.WorkerID))
			if err != nil {
				return err
			}
			if enrolled.State == "revoked" {
				return reject("worker_denied", "deployment worker is revoked")
			}
			if tx.Bucket(workerDeploymentIdentitiesBucket).Get([]byte(jobKey(c.TenantID, c.WorkerID))) != nil {
				return reject("invalid", "worker identity already belongs to a deployment")
			}
			prefix, count := []byte(jobKey(c.TenantID)), 0
			cursor := tx.Bucket(workerDeploymentsBucket).Cursor()
			for k, _ := cursor.Seek(prefix); bytes.HasPrefix(k, prefix); k, _ = cursor.Next() {
				count++
			}
			if count >= 64 || tx.Bucket(workerDeploymentsBucket).Stats().KeyN >= MaxWorkerDeployments {
				return reject("quota_exceeded", "worker deployment capacity reached")
			}
			v.Generation = 1
			if err := tx.Bucket(workerDeploymentIdentitiesBucket).Put([]byte(jobKey(c.TenantID, c.WorkerID)), []byte(key)); err != nil {
				return err
			}
		} else {
			if spec.Pool != v.Spec.Pool {
				return reject("invalid", "deployment pool is immutable")
			}
			oldTemplate, newTemplate := v.Spec, spec
			oldTemplate.Replicas, newTemplate.Replicas = 0, 0
			if !reflect.DeepEqual(oldTemplate, newTemplate) || v.Spec.Replicas > 0 && spec.Replicas == 0 {
				v.Generation++
			}
		}
		v.Spec = spec
	} else if c.Action == "scale" {
		if v.Spec.Replicas > 0 && c.Replicas == 0 {
			v.Generation++
		}
		v.Spec.Replicas = c.Replicas
	} else {
		v.Generation++
		if c.Action == "retire" {
			v.State, v.Spec.Replicas = "retired", 0
		}
	}
	v.Revision++
	v.ChangedAt, v.Actor, v.Reason = command.At.UTC(), c.Actor, c.Reason
	if err := writeRecord(tx, workerDeploymentsBucket, key, &v); err != nil {
		return err
	}
	outstanding, err := workerReleaseOutstanding(tx, v.TenantID, v.ID)
	if err != nil {
		return err
	}
	v.OutstandingRuns = outstanding
	result.WorkerDeployment = cloneWorkerDeployment(&v)
	return nil
}

func checkWorkerDeploymentAdmission(tx *bbolt.Tx, c *WorkerAdmission, tenant string) error {
	key := tx.Bucket(workerDeploymentIdentitiesBucket).Get([]byte(jobKey(tenant, c.WorkerID)))
	if len(key) == 0 {
		if c.DeploymentID != "" || c.DeploymentGeneration != 0 {
			return reject("worker_denied", "worker does not belong to a managed deployment")
		}
		return nil
	}
	v, err := readRecord[WorkerDeployment](tx, workerDeploymentsBucket, string(key))
	if err != nil {
		return err
	}
	if v.TenantID != tenant || v.WorkerID != c.WorkerID || v.ID != c.DeploymentID || v.Generation != c.DeploymentGeneration || (v.State != "active" && v.State != "retiring") || v.Spec.Replicas == 0 {
		return reject("worker_denied", "worker deployment does not permit new claims")
	}
	return nil
}

func (q *DB) GetWorkerDeployment(tenant, id string) (*WorkerDeployment, error) {
	if !validIdentity(tenant) || !validWorkerID(id) {
		return nil, fmt.Errorf("invalid worker deployment identity")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	var v *WorkerDeployment
	err := q.db.View(func(tx *bbolt.Tx) error {
		var err error
		v, err = readRecord[WorkerDeployment](tx, workerDeploymentsBucket, jobKey(tenant, id))
		if err == nil && v != nil && (v.TenantID != tenant || v.ID != id) {
			return fmt.Errorf("worker deployment identity mismatch")
		}
		if err == nil && v != nil {
			v.OutstandingRuns, err = workerReleaseOutstanding(tx, tenant, id)
		}
		return err
	})
	return v, err
}

func (q *DB) CheckWorkerDeploymentAdmission(tenant string, guard WorkerAdmission) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.db.View(func(tx *bbolt.Tx) error { return checkWorkerDeploymentAdmission(tx, &guard, tenant) })
}

func (q *DB) ListWorkerDeployments(tenant, after string, limit int) ([]WorkerDeployment, string, error) {
	if !validIdentity(tenant) || after != "" && !validWorkerID(after) {
		return nil, "", fmt.Errorf("invalid worker deployment page")
	}
	var start []byte
	if after != "" {
		start = []byte(jobKey(tenant, after))
	}
	values, next, err := q.workerDeploymentPage([]byte(jobKey(tenant)), start, limit)
	if len(next) != 0 {
		return values, values[len(values)-1].ID, err
	}
	return values, "", err
}

// ListWorkerDeploymentRecords is an internal, bounded cluster-wide scan for
// the deployment reconciler. HTTP callers always use tenant-scoped pages.
func (q *DB) ListWorkerDeploymentRecords(after []byte, limit int) ([]WorkerDeployment, []byte, error) {
	return q.workerDeploymentPage(nil, after, limit)
}

func (q *DB) workerDeploymentPage(prefix, after []byte, limit int) ([]WorkerDeployment, []byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if limit < 1 || limit > 32 || len(after) > 1024 {
		return nil, nil, fmt.Errorf("invalid worker deployment page")
	}
	values := []WorkerDeployment{}
	var next []byte
	err := q.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(workerDeploymentsBucket).Cursor()
		start := prefix
		if len(after) != 0 {
			start = after
		}
		for k, _ := cursor.Seek(start); k != nil && bytes.HasPrefix(k, prefix); k, _ = cursor.Next() {
			if len(after) != 0 && bytes.Equal(k, after) {
				continue
			}
			if len(values) == limit {
				next = append([]byte(nil), []byte(jobKey(values[len(values)-1].TenantID, values[len(values)-1].ID))...)
				break
			}
			v, err := readRecord[WorkerDeployment](tx, workerDeploymentsBucket, string(k))
			if err != nil {
				return err
			}
			if !bytes.Equal(k, []byte(jobKey(v.TenantID, v.ID))) {
				return fmt.Errorf("inconsistent worker deployment identity")
			}
			v.OutstandingRuns, err = workerReleaseOutstanding(tx, v.TenantID, v.ID)
			if err != nil {
				return err
			}
			values = append(values, *v)
		}
		return nil
	})
	return values, next, err
}
