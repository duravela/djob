package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/duravela/djob/pkg/scheduler"
	"github.com/duravela/djob/pkg/workercontrol"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const maxJobReplyBytes = 3 << 20

type JobsBackend interface {
	ProposeStateCommand(context.Context, db.StateCommand) (db.StateResult, error)
	GetJobRecord(db.JobRecordQuery) (db.JobRecord, error)
	ListJobRecords(db.JobRecordQuery, []byte, int) (db.JobRecordPage, error)
}

type jobsServer struct {
	oidcLogoutGuard func(context.Context) error
	sessions        *consoleSessions
	deployments     *workercontrol.Reconciler
	jp.UnimplementedJobsServer
	backend            JobsBackend
	auth               *apiAuthorizer
	validateTime       func(time.Time) error
	inventory          func(string, time.Time) jp.WorkerInventory
	deploymentSessions func(db.WorkerDeployment, time.Time) jp.WorkerDeploymentSessions
	membership         *node.RaftNode
	membershipGuard    func() error
}

// NewJobsService requires explicit credentials even when legacy APIs are open.
// Embedded HTTP gateways must use JobsHTTPHandler to preserve the actual header.
func NewJobsService(backend JobsBackend, cfg APIAuthConfig) (jp.JobsServer, error) {
	auth, err := newAPIAuthorizer(&cfg)
	if err != nil {
		return nil, err
	}
	return &jobsServer{backend: backend, auth: auth}, nil
}

func JobsHTTPHandler(next http.Handler) http.Handler { return SchedulerHTTPHandler(next) }

func (s *jobsServer) authenticate(ctx context.Context) (*apiCredential, error) {
	p, err := s.auth.authenticate(ctx)
	if err != nil && s.sessions != nil {
		p, _, err = s.sessions.authenticate(ctx, false)
	}
	if err != nil {
		return nil, err
	}
	if p.role == APIWorker {
		return nil, status.Error(codes.PermissionDenied, "worker credentials are restricted to the worker protocol")
	}
	if p.role == APIMonitor {
		return nil, status.Error(codes.PermissionDenied, "monitor credentials are restricted to metrics")
	}
	return p, nil
}

func jobScope(p *apiCredential, tenant string, role APIRole, platform, global, callback, producer bool) (string, error) {
	denied := func() (string, error) {
		return "", status.Error(codes.PermissionDenied, "credential does not permit this tenant operation")
	}
	if p.role == APIWorker || p.role == APIMonitor {
		return denied()
	}
	if p.role == APICallback {
		if !callback {
			return denied()
		}
	} else if p.role == APIProducer {
		if !producer {
			return denied()
		}
	} else if roleLevel(p.role) < roleLevel(role) {
		return denied()
	}
	if platform && (p.role != APIOperator || p.tenantID != "") {
		return denied()
	}
	if global {
		if tenant != "" {
			return "", status.Error(codes.InvalidArgument, "global resource operations do not accept tenant_id")
		}
		return "", nil
	}
	if p.tenantID != "" {
		if tenant == "" {
			tenant = p.tenantID
		}
		if tenant != p.tenantID {
			return denied()
		}
	} else if p.role != APIOperator {
		return denied()
	}
	if err := apiID(tenant); err != nil {
		return "", err
	}
	return tenant, nil
}

func jobRequestID(tenant, id string) string {
	data, _ := json.Marshal([]string{"jobs/v1", tenant, id})
	hash := sha256.Sum256(data)
	return "jobs-api/" + hex.EncodeToString(hash[:])
}

func jobCommand(meta *jp.Command, tenant string, operation *db.JobCommand) (db.StateCommand, error) {
	if meta == nil || apiID(meta.RequestId) != nil || len(meta.RequestId) > 240 || meta.At == nil || meta.At.CheckValid() != nil || meta.At.AsTime().IsZero() {
		return db.StateCommand{}, status.Error(codes.InvalidArgument, "command requires a request_id of at most 240 UTF-8 bytes and an explicit timestamp")
	}
	c := db.StateCommand{RequestID: jobRequestID(tenant, meta.RequestId), At: meta.At.AsTime().UTC(), Job: operation}
	if err := c.Validate(); err != nil {
		return db.StateCommand{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return c, nil
}

func (s *jobsServer) Mutate(ctx context.Context, q *jp.MutationRequest) (*jp.MutationResponse, error) {
	p, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, status.Error(codes.InvalidArgument, "mutation is required")
	}
	if proto.Size(q) > maxAPIRequestBytes {
		return nil, status.Error(codes.ResourceExhausted, "request exceeds 128 KiB")
	}
	job, ref, err := publicJobMutation(p, q)
	if err != nil {
		return nil, err
	}
	command, err := jobCommand(q.Command, ref.TenantId, job)
	if err != nil {
		return nil, err
	}
	if s.validateTime != nil {
		err = s.validateTime(command.At)
	} else {
		err = validatePublicCommandTime(command.At, 5*time.Second)
	}
	if err != nil {
		return nil, err
	}
	if job.PutTrigger != nil {
		prepared, err := scheduler.PrepareJobTrigger(job.PutTrigger.Definition, job.PutTrigger.ExpectedRevision, command.At)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		command.Job.PutTrigger = &prepared
	}
	if s.backend == nil {
		return nil, status.Error(codes.Unavailable, "job storage is not ready")
	}
	commit, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	r, err := s.backend.ProposeStateCommand(commit, command)
	if err == nil {
		err = r.Err()
	}
	if err != nil {
		var rejection *db.CommandRejection
		if !r.Compacted || r.Rejection != nil || !errors.As(err, &rejection) || rejection.Code != db.ReceiptCompactedCode {
			return nil, apiError(err)
		}
	}
	hash, hashErr := command.Fingerprint()
	if hashErr != nil || r.RequestID != command.RequestID || r.Fingerprint != hash || r.Job == nil {
		return nil, status.Error(codes.Internal, "job receipt does not match request")
	}
	ack := &jp.MutationResponse{RequestId: q.Command.RequestId, Reference: ref}
	if !jobAcknowledgement(ack, r.Job) {
		return nil, status.Error(codes.Internal, "job receipt has an unexpected record")
	}
	return ack, nil
}

func (s *jobsServer) Get(ctx context.Context, q *jp.GetRequest) (*jp.Record, error) {
	p, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if q == nil || q.Reference == nil {
		return nil, status.Error(codes.InvalidArgument, "record reference is required")
	}
	r := proto.Clone(q.Reference).(*jp.Reference)
	global := r.Kind == jp.Kind_RESOURCE || (r.Kind == jp.Kind_RESOLUTION && r.TenantId == "" && p.tenantID == "")
	role := APIReader
	if r.Kind == jp.Kind_RESOLUTION || global {
		role = APIOperator
	}
	r.TenantId, err = jobScope(p, r.TenantId, role, global, global, false, false)
	if err != nil {
		return nil, err
	}
	query, err := jobReferenceQuery(r)
	if err != nil {
		return nil, err
	}
	if q.Summary && r.Kind != jp.Kind_RUN && r.Kind != jp.Kind_TRIGGER && r.Kind != jp.Kind_ACTIVATION {
		return nil, status.Error(codes.InvalidArgument, "summary is supported for runs, triggers and activations only")
	}
	if s.backend == nil {
		return nil, status.Error(codes.Unavailable, "job storage is not ready")
	}
	record, err := s.backend.GetJobRecord(query)
	if err != nil {
		return nil, apiError(err)
	}
	out := genericRecord(record, q.Summary)
	if r.Kind != jp.Kind_RESOLUTION && !jobRecordScope(out, r.Kind, r.TenantId, r.RunId) {
		return nil, status.Error(codes.Internal, "record does not match its authorized scope")
	}
	if !tenantJobKind(r.Kind) && r.Kind != jp.Kind_RESOLUTION && (out.Reference.Id != r.Id || out.Reference.Sequence != r.Sequence) {
		return nil, status.Error(codes.Internal, "record identity does not match request")
	}
	if r.Kind == jp.Kind_ACTIVATION && (out.Reference.TriggerId != r.TriggerId || out.Reference.Source != r.Source) {
		return nil, status.Error(codes.Internal, "activation does not match requested scope")
	}
	if r.Kind == jp.Kind_RESOLUTION {
		if out.GetResolution() == nil || (r.TenantId != "" && out.GetResolution().TenantId != r.TenantId) {
			return nil, status.Error(codes.NotFound, "resolution does not exist")
		}
		out.Reference, out.GetResolution().RequestId = r, r.Id
	}
	if proto.Size(out) > maxJobReplyBytes {
		return nil, status.Error(codes.ResourceExhausted, "record exceeds response budget; request a summary and inspect actions separately")
	}
	return out, nil
}

func jobReferenceQuery(r *jp.Reference) (db.JobRecordQuery, error) {
	q := db.JobRecordQuery{Kind: jobKind(r.Kind), TenantID: r.TenantId, ID: r.Id, RunID: r.RunId, Sequence: r.Sequence, TriggerID: r.TriggerId, Source: r.Source}
	bad := func() (db.JobRecordQuery, error) {
		return q, status.Error(codes.InvalidArgument, "invalid record reference")
	}
	if q.Kind == "" {
		return bad()
	}
	if r.Kind == jp.Kind_ACTIVATION {
		if apiID(r.TriggerId) != nil || apiID(r.Source) != nil {
			return bad()
		}
	} else if r.TriggerId != "" || r.Source != "" {
		return bad()
	}
	history := r.Kind == jp.Kind_ACTION || r.Kind == jp.Kind_ATTEMPT || r.Kind == jp.Kind_EVENT
	if history {
		if apiID(r.RunId) != nil {
			return bad()
		}
	} else if r.RunId != "" {
		return bad()
	}
	if r.Kind == jp.Kind_ACTION {
		if r.Sequence == 0 || r.Id != "" {
			return bad()
		}
	} else if r.Sequence != 0 {
		return bad()
	}
	if tenantJobKind(r.Kind) {
		if r.Id != "" && r.Id != r.TenantId {
			return bad()
		}
		q.ID = r.TenantId
	} else if r.Kind != jp.Kind_ACTION && apiID(r.Id) != nil {
		return bad()
	}
	if r.Kind == jp.Kind_RESOLUTION {
		if len(r.Id) > 240 {
			return bad()
		}
		q.ID = jobRequestID(r.TenantId, r.Id)
	}
	return q, nil
}

func jobPageScope(kind jp.Kind, tenant, run string, ready bool) []byte {
	data, _ := json.Marshal([]any{"jobs/v1", int32(kind), tenant, run, ready})
	hash := sha256.Sum256(data)
	return hash[:]
}
func jobPageToken(scope, cursor []byte) string {
	if len(cursor) == 0 {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(append(append([]byte(nil), scope...), cursor...))
}

func (s *jobsServer) List(ctx context.Context, q *jp.ListRequest) (*jp.ListResponse, error) {
	p, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, status.Error(codes.InvalidArgument, "list request is required")
	}
	global := q.Kind == jp.Kind_RESOURCE || q.Kind == jp.Kind_TENANT
	tenant, err := jobScope(p, q.TenantId, APIReader, global, global, false, false)
	if err != nil {
		return nil, err
	}
	kind := jobKind(q.Kind)
	if kind == "" || q.Kind == jp.Kind_RESOLUTION || q.Kind == jp.Kind_LIMITS || q.Kind == jp.Kind_USAGE || q.Kind == jp.Kind_RETENTION || q.Kind == jp.Kind_CONFIGURATION_LIMITS || q.Kind == jp.Kind_CONFIGURATION_USAGE {
		return nil, status.Error(codes.InvalidArgument, "unsupported list kind")
	}
	history := q.Kind == jp.Kind_ACTION || q.Kind == jp.Kind_ATTEMPT || q.Kind == jp.Kind_EVENT
	if (history && apiID(q.RunId) != nil) || (!history && q.RunId != "") || (q.ReadyOnly && q.Kind != jp.Kind_RUN) {
		return nil, status.Error(codes.InvalidArgument, "invalid list scope")
	}
	if q.Kind == jp.Kind_ACTIVATION {
		if apiID(q.TriggerId) != nil || (q.Source != "" && apiID(q.Source) != nil) {
			return nil, status.Error(codes.InvalidArgument, "activation history requires a trigger and optional source")
		}
	} else if q.TriggerId != "" || q.Source != "" {
		return nil, status.Error(codes.InvalidArgument, "activation scope requires activation kind")
	}
	if q.WorkerRelease != "" && (q.Kind != jp.Kind_RUN || q.ReadyOnly || !wire.ValidWorkerID(q.WorkerRelease)) {
		return nil, status.Error(codes.InvalidArgument, "worker_release selects outstanding run lists only")
	}
	limit := q.PageSize
	if limit == 0 {
		limit = maxAPIPageSize
	}
	if limit > maxAPIPageSize || len(q.PageToken) > 4096 {
		return nil, status.Error(codes.InvalidArgument, "invalid page size or token")
	}
	scope := jobPageScope(q.Kind, tenant, q.RunId, q.ReadyOnly)
	if q.Kind == jp.Kind_ACTIVATION {
		data, _ := json.Marshal([]string{"jobs/activations/v1", tenant, q.TriggerId, q.Source})
		hash := sha256.Sum256(data)
		scope = hash[:]
	}
	if q.WorkerRelease != "" {
		data, _ := json.Marshal([]string{"jobs/release-runs/v1", tenant, q.WorkerRelease})
		hash := sha256.Sum256(data)
		scope = hash[:]
	}
	var after []byte
	if q.PageToken != "" {
		data, err := base64.RawURLEncoding.DecodeString(q.PageToken)
		if err != nil || len(data) <= len(scope) || !bytes.Equal(data[:len(scope)], scope) {
			return nil, status.Error(codes.InvalidArgument, "page token belongs to another query scope")
		}
		after = data[len(scope):]
	}
	if s.backend == nil {
		return nil, status.Error(codes.Unavailable, "job storage is not ready")
	}
	page, err := s.backend.ListJobRecords(db.JobRecordQuery{Kind: kind, TenantID: tenant, RunID: q.RunId, ReadyOnly: q.ReadyOnly, WorkerRelease: q.WorkerRelease, TriggerID: q.TriggerId, Source: q.Source}, after, int(limit))
	if err != nil {
		return nil, apiError(err)
	}
	out := &jp.ListResponse{}
	budget := 1024
	for _, entry := range page.Entries {
		record := genericRecord(entry.Record, true)
		if !jobRecordScope(record, q.Kind, tenant, q.RunId) {
			return nil, status.Error(codes.Internal, "record does not match its authorized scope")
		}
		if q.Kind == jp.Kind_ACTIVATION && (record.Reference.TriggerId != q.TriggerId || (q.Source != "" && record.Reference.Source != q.Source)) {
			return nil, status.Error(codes.Internal, "activation does not match authorized list scope")
		}
		if q.WorkerRelease != "" && (record.GetRun() == nil || record.GetRun().GetDefinition().GetWorkerRelease() != q.WorkerRelease) {
			return nil, status.Error(codes.Internal, "run does not match authorized release scope")
		}
		budget += proto.Size(record) + 8
		if budget > maxJobReplyBytes {
			if len(out.Records) == 0 {
				return nil, status.Error(codes.ResourceExhausted, "record exceeds list response budget")
			}
			return out, nil // NextPageToken still identifies the last included row.
		}
		out.Records = append(out.Records, record)
		out.NextPageToken = jobPageToken(scope, entry.Cursor)
	}
	out.NextPageToken = jobPageToken(scope, page.NextCursor)
	return out, nil
}

func jobRecordScope(record *jp.Record, kind jp.Kind, tenant, run string) bool {
	if record == nil || record.Reference == nil || record.Reference.Kind != kind {
		return false
	}
	ref := record.Reference
	if kind == jp.Kind_TENANT && tenant == "" {
		return apiID(ref.TenantId) == nil
	}
	return ref.TenantId == tenant && ref.RunId == run
}

func (s *jobsServer) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	if s.sessions != nil {
		mux.HandleFunc("POST /api/jobs/v1/console-sessions/{action}", s.sessions.http)
	}
	mux.HandleFunc("GET /api/jobs/v1/identity", s.identityHTTP)
	mux.HandleFunc("GET /api/jobs/v1/cluster/members", s.membershipHTTP)
	mux.HandleFunc("POST /api/jobs/v1/cluster/members", s.membershipHTTP)
	mux.HandleFunc("GET /api/jobs/v1/workers", s.workersHTTP)
	mux.HandleFunc("POST /api/jobs/v1/worker-enrollments/{method}", s.workerEnrollmentsHTTP)
	mux.HandleFunc("POST /api/jobs/v1/oidc/logout", s.oidcLogoutHTTP)
	mux.HandleFunc("POST /api/jobs/v1/worker-deployments/{method}", s.workerDeploymentsHTTP)
	bind := func(r *http.Request) error {
		if len(r.URL.Query()) != 0 {
			return status.Error(codes.InvalidArgument, "query parameters are unsupported")
		}
		return nil
	}
	mux.HandleFunc("POST /api/jobs/v1/mutate", jsonRPC(func() *jp.MutationRequest { return new(jp.MutationRequest) }, s.Mutate, func(r *http.Request, _ *jp.MutationRequest) error { return bind(r) }))
	mux.HandleFunc("POST /api/jobs/v1/get", jsonRPC(func() *jp.GetRequest { return new(jp.GetRequest) }, s.Get, func(r *http.Request, _ *jp.GetRequest) error { return bind(r) }))
	mux.HandleFunc("POST /api/jobs/v1/list", jsonRPC(func() *jp.ListRequest { return new(jp.ListRequest) }, s.List, func(r *http.Request, _ *jp.ListRequest) error { return bind(r) }))
	return JobsHTTPHandler(mux)
}

func invalidJobMutation() error {
	return status.Error(codes.InvalidArgument, "one complete job mutation operation is required")
}

func jobAcknowledgement(ack *jp.MutationResponse, r *db.JobResult) bool {
	ref := ack.Reference
	switch ref.Kind {
	case jp.Kind_GROUP_RETIREMENT, jp.Kind_DEFINITION_RETIREMENT, jp.Kind_BINDING_RETIREMENT:
		v := r.ConfigurationRetirement
		if v == nil || v.TenantID != ref.TenantId || v.ID != ref.Id || jobConfigurationRetirementReference(v.Kind) != ref.Kind {
			return false
		}
		ack.Revision, ack.Status = v.Revision, "retired"
	case jp.Kind_TRIGGER_RETIREMENT:
		v := r.TriggerRetirement
		if v == nil || v.TenantID != ref.TenantId || v.ID != ref.Id {
			return false
		}
		ack.Revision, ack.Status = v.Revision, "retired"
	case jp.Kind_CONFIGURATION_RECEIPT_COMPACTION:
		v := r.ConfigurationReceiptCompaction
		if v == nil || v.TenantID != ref.TenantId || v.ID != ref.Id {
			return false
		}
		ack.Step, ack.Status = v.Step, "compacting"
		if v.Complete {
			ack.Status = "complete"
		}

	case jp.Kind_CONFIGURATION_LIMITS:
		if r.ConfigurationLimits == nil || r.ConfigurationLimits.TenantID != ref.TenantId {
			return false
		}
		ack.Revision = r.ConfigurationLimits.Revision
	case jp.Kind_RECEIPT_COMPACTION:
		if r.ReceiptCompaction == nil || r.ReceiptCompaction.TenantID != ref.TenantId || r.ReceiptCompaction.RunID != ref.Id {
			return false
		}
		ack.Step, ack.Status = r.ReceiptCompaction.Step, "compacting"
		if r.ReceiptCompaction.Complete {
			ack.Status = "complete"
		}
	case jp.Kind_RETENTION:
		if r.Retention == nil || r.Retention.TenantID != ref.TenantId {
			return false
		}
		ack.Revision = r.Retention.Revision
	case jp.Kind_DELETION:
		if r.Deletion == nil || r.Deletion.TenantID != ref.TenantId || r.Deletion.RunID != ref.Id {
			return false
		}
		ack.Step, ack.Status = r.Deletion.Step, string(r.Deletion.Status)
	case jp.Kind_LIMITS:
		if r.Limits == nil || r.Limits.TenantID != ref.TenantId {
			return false
		}
		ack.Revision = r.Limits.Revision
	case jp.Kind_ACTIVATION:
		if r.Activation == nil || r.Activation.TenantID != ref.TenantId || r.Activation.TriggerID != ref.TriggerId || r.Activation.Source != ref.Source || r.Activation.EventID != ref.Id {
			return false
		}
		ack.Revision, ack.RunId, ack.Status = r.Activation.TriggerRevision, r.Activation.RunID, "accepted"
	case jp.Kind_TRIGGER:
		if r.Trigger == nil || r.Trigger.TenantID != ref.TenantId || r.Trigger.ID != ref.Id {
			return false
		}
		ack.Revision = r.Trigger.Revision
	case jp.Kind_TENANT:
		if r.Tenant == nil || r.Tenant.ID != ref.TenantId {
			return false
		}
		ack.Revision = r.Tenant.Revision
	case jp.Kind_GROUP:
		if r.Group == nil || r.Group.TenantID != ref.TenantId || r.Group.ID != ref.Id {
			return false
		}
		ack.Revision = r.Group.Revision
	case jp.Kind_DEFINITION:
		if r.Definition == nil || r.Definition.TenantID != ref.TenantId || r.Definition.ID != ref.Id {
			return false
		}
		ack.Revision = r.Definition.Revision
	case jp.Kind_BINDING:
		if r.Binding == nil || r.Binding.TenantID != ref.TenantId || r.Binding.ID != ref.Id {
			return false
		}
		ack.Revision = r.Binding.Revision
	case jp.Kind_RESOURCE:
		if r.Resource == nil || r.Resource.Spec.ID != ref.Id {
			return false
		}
		ack.Revision, ack.Epoch, ack.Status = r.Resource.Revision, r.Resource.Epoch, string(r.Resource.State)
	case jp.Kind_GRANT:
		if r.Grant == nil || r.Grant.TenantID != ref.TenantId || r.Grant.ResourceID != ref.Id {
			return false
		}
		ack.Revision = r.Grant.Revision
	case jp.Kind_RUN:
		if r.Run == nil || r.Run.TenantID != ref.TenantId || r.Run.ID != ref.Id {
			return false
		}
		ack.Step, ack.Status = r.Run.Step, string(r.Run.Status)
	case jp.Kind_EVENT:
		if r.Event == nil || r.Event.TenantID != ref.TenantId || r.Event.RunID != ref.RunId || r.Event.ID != ref.Id {
			return false
		}
	default:
		return false
	}
	return true
}

func tenantJobKind(kind jp.Kind) bool {
	return kind == jp.Kind_TENANT || kind == jp.Kind_LIMITS || kind == jp.Kind_USAGE || kind == jp.Kind_RETENTION || kind == jp.Kind_CONFIGURATION_LIMITS || kind == jp.Kind_CONFIGURATION_USAGE
}
