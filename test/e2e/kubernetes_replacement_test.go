package e2e_test

import (
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Qualify Kubernetes resource creation, UID preconditions and resume against a
// real API. Physical fencing is synthetic; the Raft replacement protocol has
// separate real-cluster tests. This fixture never reads the host kubeconfig.
func TestKubernetesReplacementInfrastructure(t *testing.T) {
	var kube *kubernetesFixture
	h := newHarnessWithTimeout(t, false, 8*time.Minute, func(h *harness) { h.remote = true; kube = startKubernetesFixture(h) })
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	script := `
import copy, importlib.util, json, sys, tempfile
from pathlib import Path
from unittest.mock import patch
root, container = Path(sys.argv[1]), sys.argv[2]
spec=importlib.util.spec_from_file_location('replacement_fixture', root/'scripts/replace_kubernetes_member_test.py')
f=importlib.util.module_from_spec(spec);spec.loader.exec_module(f)
k=f.k
kube=k.Kubernetes('/etc/rancher/k3s/k3s.yaml','default','controllers')
kube.prefix=['docker','exec','-i',container,'/bin/kubectl','--kubeconfig','/etc/rancher/k3s/k3s.yaml','--context','default','--namespace','controllers','--request-timeout=15s']
kube.run(['create','-f','-','-o','json'],{'apiVersion':'v1','kind':'Namespace','metadata':{'name':'controllers'}})
objects=f.bundle()
old_pvc=copy.deepcopy(objects['items'][0]);old_pvc['metadata']['name']='lost'
kube.run(['create','-f','-','-o','json'],old_pvc)
old=copy.deepcopy(objects['items'][1]);old['metadata']['name']='old';old['spec']['replicas']=1
pod=old['spec']['template']['spec'];pod['volumes'][0]['persistentVolumeClaim']['claimName']='lost';pod['containers'][0]['args'][1]='2'
kube.run(['create','-f','-','-o','json'],old)
proof=f.evidence();proof['deployment_uid']=kube.get('deployment','old')['metadata']['uid'];proof['pvc_uid']=kube.get('pvc','lost')['metadata']['uid']
api=f.fixture.Cluster()
value=k.build(api,kube,2,4,'synthetic isolated volume-loss rehearsal','old','lost',proof,objects)
def raft_apply(*args):
    assert kube.get('pvc','fresh')['metadata']['uid']
    assert kube.get('deployment','new')['spec']['replicas']==1
    assert kube.get('deployment','old')['spec']['replicas']==0
    return {'confirmed':True}
with tempfile.TemporaryDirectory() as directory, patch.object(k.r,'apply',side_effect=raft_apply):
    path=Path(directory)/'plan'
    assert k.apply(api,kube,value,path,k.m.digest(value),30)['confirmed']
    assert k.apply(api,kube,value,path,k.m.digest(value),30)['confirmed']
    assert kube.get('pvc','lost')['metadata']['uid']==proof['pvc_uid']
    # A recreated PVC under the same name cannot be adopted as the original.
    kube.run(['delete','--raw','/apis/apps/v1/namespaces/controllers/deployments/new'])
    try:k.apply(api,kube,value,path,k.m.digest(value),30)
    except ValueError as error:assert 'identity changed' in str(error)
    else:raise AssertionError('recreated Deployment must be rejected')
print('Real Kubernetes provisioning, retained PVC, UID pinning and resume passed; physical isolation and Raft acceptance are synthetic in this provider test')
`
	cmd := exec.CommandContext(h.ctx, "python3", "-c", script, root, kube.node.GetContainerID())
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Kubernetes replacement provider: %v\n%s", err, output)
	}
	t.Log(string(output))
}
