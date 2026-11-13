package jobspb

import "time"

// HTTP management records intentionally exclude credential hashes and secrets.
type WorkerEnrollment struct {
	TenantID  string    `json:"tenant_id"`
	WorkerID  string    `json:"worker_id"`
	Subject   string    `json:"subject"`
	Revision  uint64    `json:"revision,string"`
	State     string    `json:"state"`
	ChangedAt time.Time `json:"changed_at"`
	Actor     string    `json:"actor"`
	Reason    string    `json:"reason"`
}

type WorkerEnrollmentQuery struct {
	TenantID  string `json:"tenant_id"`
	WorkerID  string `json:"worker_id,omitempty"`
	PageToken string `json:"page_token,omitempty"`
}

type WorkerEnrollmentPage struct {
	TenantID      string             `json:"tenant_id"`
	Enrollments   []WorkerEnrollment `json:"enrollments"`
	NextPageToken string             `json:"next_page_token,omitempty"`
}

type WorkerManagementCommand struct {
	RequestID string    `json:"request_id"`
	At        time.Time `json:"at"`
}

type WorkerEnrollmentMutation struct {
	Command          *WorkerManagementCommand `json:"command,omitempty"`
	TenantID         string                   `json:"tenant_id"`
	WorkerID         string                   `json:"worker_id"`
	Action           string                   `json:"action"`
	ExpectedRevision uint64                   `json:"expected_revision,string"`
	Subject          string                   `json:"subject,omitempty"`
	CredentialHash   string                   `json:"credential_hash,omitempty"`
	Reason           string                   `json:"reason"`
}

type WorkerEnrollmentAcknowledgement struct {
	RequestID  string           `json:"request_id"`
	Enrollment WorkerEnrollment `json:"enrollment"`
}
