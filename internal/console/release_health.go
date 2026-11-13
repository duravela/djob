package console

import (
	"context"
	"encoding/json"
	"fmt"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/duravela/djob/pkg/workercontrol"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"net/http"
	"net/url"
	"time"
)

func freshReleaseObservation(at time.Time) bool {
	age := time.Since(at)
	return !at.IsZero() && age >= -5*time.Second && age <= 30*time.Second
}
func (s *Server) releasePreflight(ctx context.Context, sess *session, p *releasePlan) error {
	current, err := s.releaseDeployment(ctx, sess, p.Tenant, p.Target)
	if err != nil {
		return err
	}
	if current.State != "active" || current.Spec.Replicas == 0 || current.Spec.Image != p.TargetSnapshot.Spec.Image || current.Generation != p.TargetSnapshot.Generation {
		return fmt.Errorf("target image, generation or admission changed; rebuild the plan")
	}
	if !p.AllowUnqualified {
		proof := *p
		proof.TargetSnapshot = current
		if err = s.verifyReleaseCanary(ctx, sess, &proof); err != nil {
			return err
		}
	}
	if p.AllowUnobserved {
		return nil
	}
	available := false
	var workers []jp.WorkerInfo
	for index := range s.controllers {
		raw, _ := json.Marshal(jp.WorkerDeploymentQuery{TenantID: p.Tenant, ID: p.Target})
		response, _, err := s.request(ctx, sess.credential(), index, "worker-deployments/status", http.MethodPost, raw, false)
		var sample struct {
			Tenant   string                    `json:"tenant_id"`
			ID       string                    `json:"id"`
			Observed bool                      `json:"observed"`
			Runtime  workercontrol.Observation `json:"runtime"`
		}
		if err == nil && response.status == 200 && json.Unmarshal(response.body, &sample) == nil && sample.Tenant == p.Tenant && sample.ID == p.Target && sample.Observed && freshReleaseObservation(sample.Runtime.ObservedAt) && sample.Runtime.Revision == current.Revision && sample.Runtime.Generation == current.Generation && sample.Runtime.State == "available" && sample.Runtime.Available > 0 {
			available = true
		}
		token := ""
		seen := map[string]bool{}
		for page := 0; page < 512; page++ {
			params := url.Values{"tenant_id": {p.Tenant}, "page_size": {"16"}}
			if token != "" {
				params.Set("page_token", token)
			}
			response, _, err := s.request(ctx, sess.credential(), index, "workers", http.MethodGet, nil, false, params)
			var inventory jp.WorkerInventory
			if err != nil || response.status != 200 || json.Unmarshal(response.body, &inventory) != nil || inventory.TenantID != p.Tenant || !freshReleaseObservation(inventory.ObservedAt) {
				break
			}
			for _, w := range inventory.Workers {
				if w.TenantID == p.Tenant && w.DeploymentID == p.Target && w.DeploymentGeneration == current.Generation && w.State == "connected" {
					workers = append(workers, w)
				}
			}
			token = inventory.NextPageToken
			if token == "" || seen[token] {
				break
			}
			seen[token] = true
		}
	}
	if !available || len(workers) == 0 {
		return fmt.Errorf("target availability and connected workers could not be established")
	}
	for _, raw := range p.Required {
		var definition jp.Definition
		if protojson.Unmarshal(raw, &definition) != nil {
			return fmt.Errorf("invalid required definition")
		}
		d := &definition
		runtime := d.Runtime
		if runtime == "" {
			r, err := s.releaseRecord(ctx, sess, p.Tenant, jp.Kind_GROUP, d.GroupId)
			if err != nil {
				return err
			}
			runtime = r.GetGroup().Runtime
		}
		var binding *jp.Binding
		if d.BindingId != "" {
			r, err := s.releaseRecord(ctx, sess, p.Tenant, jp.Kind_BINDING, d.BindingId)
			if err != nil {
				return err
			}
			binding = r.GetBinding()
		}
		capable := false
		for _, w := range workers {
			hasRuntime := false
			for _, v := range w.Runtimes {
				hasRuntime = hasRuntime || v == runtime
			}
			hasHandler := false
			for _, v := range w.Handlers {
				hasHandler = hasHandler || v.Name == d.Handler && v.Version == d.HandlerVersion
			}
			hasDriver := binding == nil
			for _, v := range w.Drivers {
				hasDriver = hasDriver || binding != nil && v.Name == binding.Driver && v.Version == binding.DriverVersion
			}
			capable = capable || hasRuntime && hasHandler && hasDriver
		}
		if !capable {
			return fmt.Errorf("target does not advertise complete capabilities for %s", d.Id)
		}
	}
	return nil
}
func (s *Server) rollbackReleasePlan(ctx context.Context, sess *session, old, p *releasePlan) error {
	if old == nil {
		return fmt.Errorf("no retained promotion to roll back")
	}
	p.Tenant, p.Source, p.Target = old.Tenant, old.Target, old.Source
	target, err := s.releaseDeployment(ctx, sess, p.Tenant, p.Target)
	if err != nil {
		return err
	}
	if target.State == "retired" {
		return fmt.Errorf("permanently retired releases cannot be resumed")
	}
	p.TargetSnapshot = target
	restored := map[string]*jp.Definition{}
	versions := map[string]uint64{}
	restorable := false
	for _, step := range old.Steps {
		restorable = restorable || step.Confirmed && len(step.Before) > 0
	}
	addDeployment := func(action string, revision uint64) {
		q := jp.WorkerDeploymentMutation{TenantID: p.Tenant, ID: p.Target, WorkerID: target.WorkerID, Action: action, ExpectedRevision: revision, Replicas: 1, Reason: "Resume reviewed release rollback"}
		raw, _ := json.Marshal(q)
		p.Steps = append(p.Steps, releaseStep{Kind: "WORKER_DEPLOYMENT", ID: p.Target, Expected: revision, Mutation: raw})
	}
	revision := target.Revision
	if restorable && target.Spec.Replicas == 0 {
		addDeployment("scale", revision)
		revision++
	}
	if restorable && target.State == "retiring" {
		addDeployment("resume", revision)
	}
	for _, step := range old.Steps {
		if !step.Confirmed || step.Kind != "DEFINITION" || len(step.Before) == 0 {
			continue
		}
		current, err := s.releaseRecord(ctx, sess, p.Tenant, jp.Kind_DEFINITION, step.ID)
		if err != nil {
			return err
		}
		if current.GetDefinition().Revision != step.Expected+1 {
			return fmt.Errorf("job changed after promotion")
		}
		var before jp.Record
		if protojson.Unmarshal(step.Before, &before) != nil {
			return fmt.Errorf("invalid original job snapshot")
		}
		d := proto.Clone(before.GetDefinition()).(*jp.Definition)
		d.Revision = 0
		versions[d.Id] = current.GetDefinition().Revision + 1
		restored[d.Id] = d
		if err = p.add("DEFINITION", d.Id, current.GetDefinition().Revision, current, &jp.MutationRequest{Operation: &jp.MutationRequest_PutDefinition{PutDefinition: &jp.PutDefinition{ExpectedRevision: current.GetDefinition().Revision, Definition: d}}}); err != nil {
			return err
		}
	}
	for _, step := range old.Steps {
		if !step.Confirmed || step.Kind != "TRIGGER" {
			continue
		}
		current, err := s.releaseRecord(ctx, sess, p.Tenant, jp.Kind_TRIGGER, step.ID)
		if err != nil {
			return err
		}
		if current.GetTrigger().Revision != step.Expected+1 {
			return fmt.Errorf("schedule changed after promotion")
		}
		var before jp.Record
		if protojson.Unmarshal(step.Before, &before) != nil {
			return fmt.Errorf("invalid original schedule snapshot")
		}
		t := before.GetTrigger()
		d := proto.Clone(t.Definition).(*jp.TriggerDefinition)
		definition := proto.Clone(t.JobDefinition).(*jp.Definition)
		definition.Revision = 0
		if original := restored[definition.Id]; original != nil && proto.Equal(original, definition) {
			d.DefinitionRevision = versions[definition.Id]
		} else {
			definition.Id = "rollback-" + p.ID[:12] + "-" + fmt.Sprint(len(p.Steps))
			d.DefinitionId = definition.Id
			d.DefinitionRevision = 1
			if err = p.add("DEFINITION", definition.Id, 0, nil, &jp.MutationRequest{Operation: &jp.MutationRequest_PutDefinition{PutDefinition: &jp.PutDefinition{Definition: definition}}}); err != nil {
				return err
			}
		}
		if err = p.add("TRIGGER", d.Id, current.GetTrigger().Revision, current, &jp.MutationRequest{Operation: &jp.MutationRequest_PutTrigger{PutTrigger: &jp.PutTrigger{ExpectedRevision: current.GetTrigger().Revision, Definition: d}}}); err != nil {
			return err
		}
	}
	return nil
}
