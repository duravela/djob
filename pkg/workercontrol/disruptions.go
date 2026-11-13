package workercontrol

import (
	"context"
	"github.com/duravela/djob/pkg/db"
	"net/http"
)

func (k *Kubernetes) reconcileBudget(ctx context.Context, v db.WorkerDeployment, deployment map[string]any) error {
	name := deploymentName(k.cfg.ClusterID, v)
	annotations := map[string]any{"djob.io/cluster": k.cfg.ClusterID, "djob.io/tenant": v.TenantID, "djob.io/deployment": v.ID}
	desired := map[string]any{"apiVersion": "policy/v1", "kind": "PodDisruptionBudget", "metadata": map[string]any{"name": name, "namespace": k.cfg.Namespace, "annotations": annotations}, "spec": map[string]any{"maxUnavailable": 0, "unhealthyPodEvictionPolicy": "IfHealthyBudget", "selector": map[string]any{"matchLabels": map[string]any{"djob.io/instance": name}}}}
	if uid := text(object(deployment["metadata"])["uid"]); uid != "" {
		object(desired["metadata"])["ownerReferences"] = []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": name, "uid": uid, "controller": true, "blockOwnerDeletion": false}}
	}
	current, code, err := k.request(ctx, http.MethodGet, "budget:"+name, nil)
	if err != nil {
		return err
	}
	if code == 404 {
		_, code, err = k.request(ctx, http.MethodPost, "budget:", desired)
	} else {
		if code != 200 {
			return runtimeFailure("disruption_budget_unavailable")
		}
		if text(object(current["metadata"])["deletionTimestamp"]) != "" {
			return runtimeFailure("disruption_budget_unavailable")
		}
		if !contains(object(object(current["metadata"])["annotations"]), annotations) {
			return runtimeFailure("disruption_budget_conflict")
		}
		if !contains(current["spec"], desired["spec"]) || !contains(object(current["metadata"])["ownerReferences"], object(desired["metadata"])["ownerReferences"]) {
			rv := text(object(current["metadata"])["resourceVersion"])
			if rv == "" {
				return runtimeFailure("invalid_resource_version")
			}
			desired["metadata"] = mergeWorkloadField(current["metadata"], desired["metadata"], "")
			object(desired["metadata"])["resourceVersion"] = rv
			_, code, err = k.request(ctx, http.MethodPut, "budget:"+name, desired)
		}
	}
	if err != nil {
		return err
	}
	if code != 200 && code != 201 {
		return runtimeFailure("disruption_budget_unavailable")
	}
	return nil
}
