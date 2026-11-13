package db

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"go.etcd.io/bbolt"
)

// Enrollment is management state. Live capabilities, sessions and heartbeats
// remain local to controllers; company code and credential secrets never enter Raft.
type WorkerEnrollment struct {
	TenantID, WorkerID, Subject string
	Revision                    uint64
	State                       string
	CredentialHash              [32]byte
	ChangedAt                   time.Time
	Actor, Reason               string
}

type WorkerEnrollmentCommand struct {
	TenantID, WorkerID, Action, Subject, Actor, Reason string
	ExpectedRevision                                   uint64
	CredentialHash                                     [32]byte
}

// WorkerAdmission is a claim precondition, independent of the job's handler or
// protocol. Raft orders it against enrollment changes in the same state machine.
type WorkerAdmission struct {
	WorkerID             string
	CredentialHash       [32]byte
	Enrolled             bool
	DeploymentID         string `json:",omitempty"`
	DeploymentGeneration uint64 `json:",omitempty"`
}

func checkWorkerAdmission(tx *bbolt.Tx, c *WorkerAdmission, tenant string) error {
	key := jobKey(tenant, c.WorkerID)
	if tx.Bucket(workerEnrollmentsBucket).Get([]byte(key)) == nil {
		if !c.Enrolled {
			return nil
		}
		return reject("worker_denied", "worker enrollment is unavailable")
	}
	v, err := readRecord[WorkerEnrollment](tx, workerEnrollmentsBucket, key)
	if err != nil {
		return err
	}
	if v.State != "enabled" || v.CredentialHash != c.CredentialHash {
		return reject("worker_denied", "worker enrollment does not permit new claims")
	}
	return checkWorkerDeploymentAdmission(tx, c, tenant)
}

func checkWorkerClaim(tx *bbolt.Tx, command StateCommand) error {
	claim := command.Job.Claim
	if err := checkWorkerReleaseClaim(tx, command); err != nil {
		return err
	}
	if command.WorkerAdmission != nil {
		return checkWorkerAdmission(tx, command.WorkerAdmission, claim.TenantID)
	}
	// This prefix is reserved by remote controllers. An older/static session
	// racing enrollment cannot bypass its policy by omitting the new guard.
	parts := strings.Split(claim.OwnerID, "/")
	if len(parts) >= 3 && parts[0] == "remote-worker" && validWorkerID(parts[2]) && tx.Bucket(workerEnrollmentsBucket).Get([]byte(jobKey(claim.TenantID, parts[2]))) != nil {
		return reject("worker_denied", "enrolled worker claims require a credential precondition")
	}
	return nil
}

var workerEnrollmentVersionKey = []byte("worker_enrollment_version")
var workerEnrollmentsBucket = []byte("worker_enrollments")
var workerCredentialsBucket = []byte("worker_credentials")
var workerEnrollmentBuckets = [][]byte{workerEnrollmentsBucket, workerCredentialsBucket}

func initializeWorkerEnrollments(tx *bbolt.Tx) error {
	meta := tx.Bucket(metadataBucket)
	version := meta.Get(workerEnrollmentVersionKey)
	if version != nil && !bytes.Equal(version, []byte{1}) || version == nil && meta.Get(formatKey)[0] >= 13 {
		return fmt.Errorf("missing or unsupported worker enrollment marker")
	}
	for _, name := range workerEnrollmentBuckets {
		b := tx.Bucket(name)
		if meta.Get(formatKey)[0] < 13 && b != nil && b.Stats().KeyN != 0 {
			return fmt.Errorf("worker enrollment data without an activated database format")
		}
		if version != nil && b == nil || version == nil && b != nil && b.Stats().KeyN != 0 {
			return fmt.Errorf("inconsistent worker enrollment bucket")
		}
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}
	return meta.Put(workerEnrollmentVersionKey, []byte{1})
}

func validWorkerID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-_.", c)) {
			return false
		}
	}
	return true
}

func (c WorkerEnrollmentCommand) validate() error {
	if !validIdentity(c.TenantID) || !utf8.ValidString(c.TenantID) || !validWorkerID(c.WorkerID) || len(c.Actor) == 0 || len(c.Actor) > 128 || strings.TrimSpace(c.Reason) == "" || len(c.Reason) > 1024 || !utf8.ValidString(c.Reason) {
		return fmt.Errorf("worker enrollment requires tenant, worker ID, actor and a reason of at most 1024 UTF-8 bytes")
	}
	for _, s := range []string{c.Actor, c.Subject} {
		for _, r := range s {
			if r < '!' || r > '~' {
				return fmt.Errorf("worker subjects must use visible ASCII")
			}
		}
	}
	credential := c.CredentialHash != [32]byte{}
	switch c.Action {
	case "enroll":
		if c.ExpectedRevision != 0 || len(c.Subject) == 0 || len(c.Subject) > 128 || !credential {
			return fmt.Errorf("enrollment requires a subject, new credential and revision zero")
		}
	case "rotate":
		if c.ExpectedRevision == 0 || c.Subject != "" || !credential {
			return fmt.Errorf("rotation requires an existing revision and new credential")
		}
	case "drain", "resume", "revoke":
		if c.ExpectedRevision == 0 || c.Subject != "" || credential {
			return fmt.Errorf("lifecycle changes require an existing revision without a credential or subject")
		}
	default:
		return fmt.Errorf("unknown worker enrollment action")
	}
	return nil
}

func transitionWorkerEnrollment(tx *bbolt.Tx, command StateCommand, result *StateResult) error {
	c := command.WorkerEnrollment
	if _, err := requireJobTenant(tx, c.TenantID); err != nil {
		return err
	}
	key := jobKey(c.TenantID, c.WorkerID)
	v := WorkerEnrollment{TenantID: c.TenantID, WorkerID: c.WorkerID}
	if tx.Bucket(workerEnrollmentsBucket).Get([]byte(key)) != nil {
		old, err := readRecord[WorkerEnrollment](tx, workerEnrollmentsBucket, key)
		if err != nil {
			return err
		}
		v = *old
	}
	if v.Revision != c.ExpectedRevision || v.Revision == math.MaxUint64 {
		return reject("revision_conflict", "worker enrollment revision changed")
	}
	if v.State == "revoked" {
		return reject("invalid", "revoked worker identities cannot be reused")
	}
	if c.Action != "enroll" && v.Revision == 0 {
		return reject("not_found", "worker enrollment does not exist")
	}
	if c.Action == "enroll" {
		// Tombstones remain charged so a caller cannot grow permanent metadata
		// without bound by repeatedly enrolling and revoking new identities.
		prefix := []byte(jobKey(c.TenantID))
		count := 0
		cursor := tx.Bucket(workerEnrollmentsBucket).Cursor()
		for k, _ := cursor.Seek(prefix); bytes.HasPrefix(k, prefix); k, _ = cursor.Next() {
			count++
		}
		if count >= 128 || tx.Bucket(workerEnrollmentsBucket).Stats().KeyN >= 4096 {
			return reject("quota_exceeded", "worker enrollment capacity reached")
		}
		v.Subject, v.State = c.Subject, "enabled"
	}
	if c.Action == "enroll" || c.Action == "rotate" {
		if tx.Bucket(workerCredentialsBucket).Get(c.CredentialHash[:]) != nil || c.CredentialHash == v.CredentialHash {
			return reject("invalid", "worker credential is already in use")
		}
	}
	if c.Action == "rotate" || c.Action == "revoke" {
		// Retain a credential tombstone: a rotated or revoked secret must never
		// acquire authority again under a later enrollment.
		if err := tx.Bucket(workerCredentialsBucket).Put(v.CredentialHash[:], []byte{}); err != nil {
			return err
		}
	}
	switch c.Action {
	case "enroll", "rotate":
		v.CredentialHash = c.CredentialHash
	case "drain":
		v.State = "draining"
	case "resume":
		v.State = "enabled"
	case "revoke":
		v.State, v.CredentialHash = "revoked", [32]byte{}
	}
	v.Revision++
	v.ChangedAt, v.Actor, v.Reason = command.At.UTC(), c.Actor, c.Reason
	if err := writeRecord(tx, workerEnrollmentsBucket, key, &v); err != nil {
		return err
	}
	if v.CredentialHash != [32]byte{} {
		if err := tx.Bucket(workerCredentialsBucket).Put(v.CredentialHash[:], []byte(key)); err != nil {
			return err
		}
	}
	result.WorkerEnrollment = &v
	return nil
}

func (q *DB) GetWorkerEnrollment(tenant, id string) (*WorkerEnrollment, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var v *WorkerEnrollment
	err := q.db.View(func(tx *bbolt.Tx) error {
		var err error
		v, err = readRecord[WorkerEnrollment](tx, workerEnrollmentsBucket, jobKey(tenant, id))
		if err == nil && (v.TenantID != tenant || v.WorkerID != id) {
			return fmt.Errorf("inconsistent worker enrollment identity")
		}
		return err
	})
	return v, err
}

func (q *DB) LookupWorkerCredential(hash [32]byte) (*WorkerEnrollment, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var v *WorkerEnrollment
	err := q.db.View(func(tx *bbolt.Tx) error {
		key := tx.Bucket(workerCredentialsBucket).Get(hash[:])
		if len(key) == 0 {
			return reject("not_found", "worker credential does not exist")
		}
		var err error
		v, err = readRecord[WorkerEnrollment](tx, workerEnrollmentsBucket, string(key))
		if err == nil && (v.CredentialHash != hash || !bytes.Equal(key, []byte(jobKey(v.TenantID, v.WorkerID)))) {
			return fmt.Errorf("inconsistent worker credential index")
		}
		return err
	})
	return v, err
}

func (q *DB) ListWorkerEnrollments(tenant, after string, limit int) ([]WorkerEnrollment, string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !validIdentity(tenant) || !utf8.ValidString(tenant) || limit < 1 || limit > 32 || after != "" && !validWorkerID(after) {
		return nil, "", fmt.Errorf("invalid worker enrollment page")
	}
	values := []WorkerEnrollment{}
	next := ""
	err := q.db.View(func(tx *bbolt.Tx) error {
		prefix := []byte(jobKey(tenant))
		cursor := tx.Bucket(workerEnrollmentsBucket).Cursor()
		start := prefix
		if after != "" {
			start = []byte(jobKey(tenant, after))
		}
		for k, _ := cursor.Seek(start); bytes.HasPrefix(k, prefix); k, _ = cursor.Next() {
			if after != "" && bytes.Equal(k, start) {
				continue
			}
			if len(values) == limit {
				next = values[len(values)-1].WorkerID
				break
			}
			v, err := readRecord[WorkerEnrollment](tx, workerEnrollmentsBucket, string(k))
			if err != nil {
				return err
			}
			if v.TenantID != tenant || !bytes.Equal(k, []byte(jobKey(v.TenantID, v.WorkerID))) {
				return fmt.Errorf("inconsistent worker enrollment tenant")
			}
			values = append(values, *v)
		}
		return nil
	})
	return values, next, err
}
