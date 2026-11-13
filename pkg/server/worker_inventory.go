package server

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/jobs"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const workerInventoryPageLimit = 16
const workerInventoryReplyLimit = 512 << 10

func workerDisplayID(session string) string {
	hash := sha256.Sum256([]byte(session))
	return "remote/" + hex.EncodeToString(hash[:])
}

// Cache only bounded names/versions. An omitted capability is explicit, so
// clients cannot infer incompatibility from a partial registration listing.
func workerCapabilities(registry jobs.Registry) jp.WorkerInfo {
	info := jp.WorkerInfo{Runtimes: []string{db.StepRuntime}, Handlers: []jp.WorkerVersion{}, Drivers: []jp.WorkerVersion{}}
	add := func(dst *[]jp.WorkerVersion, version jobs.Version) {
		if len(*dst) >= 64 || len(version.Name) > 256 || len(version.Version) > 256 || !utf8.ValidString(version.Name) || !utf8.ValidString(version.Version) {
			info.CapabilitiesTruncated = true
			return
		}
		*dst = append(*dst, jp.WorkerVersion{Name: version.Name, Version: version.Version})
	}
	for version := range registry.Handlers {
		add(&info.Handlers, version)
	}
	for version := range registry.Drivers {
		add(&info.Drivers, version)
	}
	for _, versions := range [][]jp.WorkerVersion{info.Handlers, info.Drivers} {
		sort.Slice(versions, func(i, j int) bool {
			return versions[i].Name < versions[j].Name || versions[i].Name == versions[j].Name && versions[i].Version < versions[j].Version
		})
	}
	return info
}

func (s *topLevelServer) workerInventory(tenant string, at time.Time) jp.WorkerInventory {
	result := jp.WorkerInventory{ControllerID: strconv.FormatUint(s.nodeID, 10), InstanceID: s.workerInstance, TenantID: tenant, ObservedAt: at.UTC(), Workers: []jp.WorkerInfo{}}
	if s.embeddedInfo != nil && s.jobDispatcher != nil {
		info := *s.embeddedInfo
		info.TenantID, info.State = tenant, "connected"
		active, paused, closed := s.jobDispatcher.InspectTenant(tenant)
		info.LocalTasks = active
		if closed {
			info.State = "stopped"
		} else if paused {
			info.State = "draining"
		}
		result.Workers = append(result.Workers, info)
	}
	if s.remoteWorkers != nil {
		accepting := s.remoteWorkers.acceptingWorkers()
		for _, session := range s.remoteWorkers.snapshot() {
			if session.tenant != tenant {
				continue
			}
			info, visible := session.observation(at, accepting)
			if !visible {
				continue
			}
			info.LocalTasks, _, _ = session.dispatcher.InspectTenant(tenant)
			result.Workers = append(result.Workers, info)
		}
	}
	sort.Slice(result.Workers, func(i, j int) bool { return result.Workers[i].ID < result.Workers[j].ID })
	return result
}

func (s *workerSession) observation(at time.Time, accepting bool) (jp.WorkerInfo, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	info := s.info
	seen := s.lastSeen.UTC()
	info.LastSeen, info.State = &seen, "connected"
	switch {
	case s.relocating:
		info.State = "relocating"
	case s.draining:
		info.State = "draining"
	case at.Sub(s.lastSeen) >= s.hub.cfg.IdleTimeout:
		info.State = "stale"
	case !accepting:
		info.State = "unavailable"
	case !s.started:
		info.State = "registered"
	}
	return info, !s.closed
}

type workerInventoryCursor struct {
	Controller string `json:"controller"`
	Instance   string `json:"instance"`
	Tenant     string `json:"tenant"`
	After      string `json:"after"`
}

func (s *jobsServer) workersHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Vary", "Authorization")
	p, err := s.authenticate(r.Context())
	if err != nil {
		writeAPIError(w, err)
		return
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeAPIError(w, invalidHTTP("invalid worker query parameters"))
		return
	}
	for key, values := range q {
		if (key != "tenant_id" && key != "page_size" && key != "page_token") || len(values) != 1 {
			writeAPIError(w, invalidHTTP("use one tenant_id, page_size and page_token parameter"))
			return
		}
	}
	tenant, err := jobScope(p, q.Get("tenant_id"), APIReader, false, false, false, false)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	limit := 8
	if raw := q.Get("page_size"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > workerInventoryPageLimit {
			writeAPIError(w, invalidHTTP("worker page_size must be in 1..16"))
			return
		}
	}
	var cursor workerInventoryCursor
	if raw := q.Get("page_token"); raw != "" {
		if len(raw) > 4096 {
			writeAPIError(w, invalidHTTP("invalid worker page token"))
			return
		}
		data, decodeErr := base64.RawURLEncoding.DecodeString(raw)
		if decodeErr != nil || json.Unmarshal(data, &cursor) != nil || len(cursor.After) > 128 || cursor.After == "" {
			writeAPIError(w, invalidHTTP("invalid worker page token"))
			return
		}
	}
	if s.inventory == nil {
		writeAPIError(w, status.Error(codes.Unavailable, "worker inventory is unavailable on this service"))
		return
	}
	observed := s.inventory(tenant, time.Now())
	if cursor.After != "" && (cursor.Tenant != tenant || cursor.Controller != observed.ControllerID || cursor.Instance != observed.InstanceID) {
		writeAPIError(w, invalidHTTP("worker page token belongs to another tenant, controller or process; refresh from the first page"))
		return
	}
	all := observed.Workers
	observed.Workers = []jp.WorkerInfo{}
	observed.NextPageToken = ""
	base, err := json.Marshal(observed)
	if err != nil {
		writeAPIError(w, status.Error(codes.Internal, "cannot encode worker inventory"))
		return
	}
	budget, continuation := len(base)+1, "" // Encoder's trailing newline.
	for _, info := range all {
		if info.ID <= cursor.After {
			continue
		}
		data, marshalErr := json.Marshal(info)
		if marshalErr != nil {
			writeAPIError(w, status.Error(codes.Internal, "cannot encode worker inventory"))
			return
		}
		cursorJSON, _ := json.Marshal(workerInventoryCursor{observed.ControllerID, observed.InstanceID, tenant, info.ID})
		token := base64.RawURLEncoding.EncodeToString(cursorJSON)
		size := len(data)
		if len(observed.Workers) > 0 {
			size++ // JSON array separator.
		}
		// Include the exact escaped envelope and continuation token, even on a
		// page that may turn out to be the last one. No metadata-size estimate.
		if len(observed.Workers) == limit || budget+size+len(token) > workerInventoryReplyLimit {
			if continuation == "" {
				writeAPIError(w, status.Error(codes.ResourceExhausted, "worker metadata exceeds the response limit"))
				return
			}
			observed.NextPageToken = continuation
			break
		}
		observed.Workers = append(observed.Workers, info)
		budget += size
		continuation = token
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(observed)
}
