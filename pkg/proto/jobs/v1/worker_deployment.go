package jobspb

import "time"

type WorkerDeploymentSpec struct {
	Pool         string   `json:"pool"`
	Image        string   `json:"image"`
	ConfigSecret string   `json:"config_secret"`
	PullSecret   string   `json:"pull_secret,omitempty"`
	Args         []string `json:"args,omitempty"`
	Replicas     uint32   `json:"replicas"`
	CPUMillis    uint32   `json:"cpu_millis"`
	MemoryMiB    uint32   `json:"memory_mib"`
	DrainSeconds uint32   `json:"drain_seconds"`
}

type WorkerDeployment struct {
	Published       bool                 `json:"published"`
	OutstandingRuns uint64               `json:"outstanding_runs,string"`
	TenantID        string               `json:"tenant_id"`
	ID              string               `json:"id"`
	WorkerID        string               `json:"worker_id"`
	Revision        uint64               `json:"revision,string"`
	Generation      uint64               `json:"generation,string"`
	State           string               `json:"state"`
	Spec            WorkerDeploymentSpec `json:"spec"`
	ChangedAt       time.Time            `json:"changed_at"`
	Actor           string               `json:"actor"`
	Reason          string               `json:"reason"`
}

type WorkerDeploymentQuery struct {
	TenantID  string `json:"tenant_id"`
	ID        string `json:"id,omitempty"`
	PageToken string `json:"page_token,omitempty"`
}

// WorkerDeploymentSessions is a process-local observation, not execution
// permission or a count of distinct Pods. Counts other than OtherGenerations
// describe the desired record's current generation only.
type WorkerDeploymentSessions struct {
	ControllerID     string    `json:"controller_id"`
	InstanceID       string    `json:"instance_id"`
	Generation       uint64    `json:"generation,string"`
	ObservedAt       time.Time `json:"observed_at"`
	Enabled          bool      `json:"enabled"`
	Connected        uint32    `json:"connected"`
	Registered       uint32    `json:"registered"`
	Draining         uint32    `json:"draining"`
	Relocating       uint32    `json:"relocating"`
	Stale            uint32    `json:"stale"`
	Unavailable      uint32    `json:"unavailable"`
	OtherGenerations uint32    `json:"other_generations"`
}

// WorkerDeploymentAccess observes the enrolled identity's policy on this
// controller. It does not verify the Pod's credential or authorize execution.
// Unknown observations omit the enrollment revision and state.
type WorkerDeploymentAccess struct {
	WorkerID   string    `json:"worker_id"`
	ObservedAt time.Time `json:"observed_at"`
	Observed   bool      `json:"observed"`
	Revision   uint64    `json:"revision,string,omitempty"`
	State      string    `json:"state,omitempty"`
}

type WorkerDeploymentPage struct {
	TenantID      string             `json:"tenant_id"`
	Deployments   []WorkerDeployment `json:"deployments"`
	NextPageToken string             `json:"next_page_token,omitempty"`
}

type WorkerDeploymentMutation struct {
	Command          *WorkerManagementCommand `json:"command,omitempty"`
	TenantID         string                   `json:"tenant_id"`
	ID               string                   `json:"id"`
	WorkerID         string                   `json:"worker_id"`
	Action           string                   `json:"action"`
	ExpectedRevision uint64                   `json:"expected_revision,string"`
	Spec             *WorkerDeploymentSpec    `json:"spec,omitempty"`
	Replicas         uint32                   `json:"replicas,omitempty"`
	Reason           string                   `json:"reason"`
}

type WorkerDeploymentAcknowledgement struct {
	RequestID  string           `json:"request_id"`
	Deployment WorkerDeployment `json:"deployment"`
}
