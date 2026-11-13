package jobspb

import "time"

// Inventory records are HTTP-only, process-local observations. They are not
// worker protocol messages or replicated commands.
type WorkerVersion struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type WorkerInfo struct {
	ID                    string          `json:"id"` // Display-only identity, never a worker session credential.
	WorkerID              string          `json:"worker_id"`
	DeploymentID          string          `json:"deployment_id,omitempty"`
	DeploymentGeneration  uint64          `json:"deployment_generation,string,omitempty"`
	TenantID              string          `json:"tenant_id"`
	Subject               string          `json:"subject,omitempty"`
	Mode                  string          `json:"mode"`
	State                 string          `json:"state"`
	Runtimes              []string        `json:"runtimes"`
	Handlers              []WorkerVersion `json:"handlers"`
	Drivers               []WorkerVersion `json:"drivers"`
	CapabilitiesTruncated bool            `json:"capabilities_truncated"`
	LocalTasks            int             `json:"local_tasks"` // Queried tenant only; may include a pending claim.
	LastSeen              *time.Time      `json:"last_seen,omitempty"`
}

type WorkerInventory struct {
	ControllerID  string       `json:"controller_id"` // Decimal uint64, without JSON number rounding.
	InstanceID    string       `json:"instance_id"`   // Changes on restart; no execution authority.
	TenantID      string       `json:"tenant_id"`
	ObservedAt    time.Time    `json:"observed_at"`
	Workers       []WorkerInfo `json:"workers"`
	NextPageToken string       `json:"next_page_token"`
}
