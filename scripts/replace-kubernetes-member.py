#!/usr/bin/env python3
"""Provision a reviewed Kubernetes controller replacement after confirmed volume loss."""
import argparse
import datetime
import fcntl
import importlib.util
import json
import os
import re
from pathlib import Path
import subprocess
import sys

spec = importlib.util.spec_from_file_location('member_replacement', Path(__file__).with_name('replace-member.py'))
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
m = r.m
FORMAT = 'djob-kubernetes-replacement/v1'


class KubernetesOperationError(ValueError):
    """A failed operation; raw kubectl output may contain private material."""


class Kubernetes:
    def __init__(self, kubeconfig, context, namespace):
        if not kubeconfig or not context or not namespace:
            raise ValueError('Explicit kubeconfig, context and namespace are required')
        self.prefix = ['kubectl', '--kubeconfig', kubeconfig, '--context', context,
                       '--namespace', namespace, '--request-timeout=15s']
        self.namespace = namespace

    def run(self, args, value=None):
        result = subprocess.run(self.prefix+args, input=None if value is None else m.canonical(value),
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        if result.returncode:
            raise KubernetesOperationError('Kubernetes operation failed; inspect workload events and resume the retained plan')
        return json.loads(result.stdout or b'{}')

    def get(self, kind, name):
        return self.run(['get', kind, name, '--ignore-not-found', '-o', 'json']) or None

    def cluster(self):
        return self.get('namespace', 'kube-system')['metadata']['uid']


def evidence_check(evidence, deployment, pvc):
    required = {'volume_loss_confirmed', 'isolation_confirmed', 'deployment_uid', 'pvc_uid', 'incident', 'confirmed_at'}
    if set(evidence) != required or evidence['volume_loss_confirmed'] is not True or evidence['isolation_confirmed'] is not True:
        raise ValueError('Independent confirmed volume-loss AND persistent physical-isolation evidence is required')
    if evidence['deployment_uid'] != deployment['metadata']['uid'] or evidence['pvc_uid'] != pvc['metadata']['uid']:
        raise ValueError('Evidence does not identify the reviewed workload and lost volume')
    if not isinstance(evidence['incident'], str) or not evidence['incident'].strip() or len(evidence['incident']) > 1024:
        raise ValueError('Provide a bounded incident/fencing evidence reference')
    try:
        confirmed = datetime.datetime.fromisoformat(evidence['confirmed_at'].replace('Z', '+00:00'))
        age = (datetime.datetime.now(datetime.timezone.utc)-confirmed).total_seconds()
    except (ValueError, TypeError, AttributeError):
        raise ValueError('Evidence requires a UTC confirmation timestamp') from None
    if not 0 <= age <= 86400:
        raise ValueError('Evidence must have been independently confirmed within the last day')


def replacement_objects(bundle, namespace, new_id, old_pvc):
    if bundle.get('apiVersion') != 'v1' or bundle.get('kind') != 'List' or len(bundle.get('items', [])) != 2:
        raise ValueError('Provide exactly one fresh PVC and one controller Deployment in a JSON List')
    kinds = {item.get('kind'): item for item in bundle['items']}
    if set(kinds) != {'PersistentVolumeClaim', 'Deployment'}:
        raise ValueError('Only a PVC and Deployment may be provisioned')
    pvc, deployment = kinds['PersistentVolumeClaim'], kinds['Deployment']
    for item in (pvc, deployment):
        metadata = item.get('metadata', {})
        if metadata.get('namespace') != namespace or not metadata.get('name') or any(key in metadata for key in ['uid', 'resourceVersion', 'ownerReferences', 'generateName']):
            raise ValueError('Fresh objects need exact names and the explicit namespace, without existing identities or owners')
    if pvc['metadata']['name'] == old_pvc or pvc.get('spec', {}).get('volumeName') or pvc.get('spec', {}).get('dataSource') or pvc.get('spec', {}).get('dataSourceRef'):
        raise ValueError('Replacement storage must be a new empty dynamically provisioned PVC')
    pod = deployment.get('spec', {}).get('template', {}).get('spec', {})
    containers = pod.get('containers', [])
    if deployment.get('apiVersion') != 'apps/v1' or deployment.get('spec', {}).get('replicas') != 1 or len(containers) != 1:
        raise ValueError('Use a single-replica apps/v1 Deployment with exactly one controller container')
    container = containers[0]
    args = container.get('args', [])
    if not isinstance(args, list) or not all(isinstance(arg, str) for arg in args):
        raise ValueError('Controller arguments must be strings')
    if container.get('name') != 'djob' or container.get('command') or not re.fullmatch(r'[^\s@]+@sha256:[a-f0-9]{64}', container.get('image', '')):
        raise ValueError('The djob container must use its image entrypoint and an immutable digest')
    # Require unambiguous identity flags; reject duplicate/equal-sign variants.
    for flag in ['--id', '--join', '--dynamic-membership']:
        if args.count(flag) != 1 or any(arg.startswith(flag+'=') for arg in args):
            raise ValueError('Explicit unique --id, --join and --dynamic-membership flags are required')
    if args[args.index('--id')+1] != str(new_id) or any(arg.split('=')[0] == '--bootstrap' for arg in args):
        raise ValueError('Replacement must join under its reviewed fresh member ID, never bootstrap')
    claims = [v['persistentVolumeClaim']['claimName'] for v in pod.get('volumes', []) if 'persistentVolumeClaim' in v]
    if claims != [pvc['metadata']['name']]:
        raise ValueError('The controller must reference only its fresh PVC')
    if any(v.get('hostPath') or v.get('emptyDir') for v in pod.get('volumes', [])):
        raise ValueError('Use the fresh PVC for controller data, never hostPath or emptyDir')
    volumes = [v for v in pod.get('volumes', []) if 'persistentVolumeClaim' in v]
    if args.count('--data-dir') != 1 or any(arg.startswith('--data-dir=') for arg in args):
        raise ValueError('An explicit data directory on the fresh PVC is required')
    directory = args[args.index('--data-dir')+1]
    mounts = [mount for mount in container.get('volumeMounts', []) if mount.get('name') == volumes[0]['name']]
    if len(mounts) != 1 or mounts[0].get('mountPath') != directory or mounts[0].get('subPath') or mounts[0].get('readOnly'):
        raise ValueError('Mount the fresh PVC directly at the controller data directory')
    return pvc, deployment


def build(api, kube, old, new, reason, old_deployment, old_pvc, evidence, bundle):
    raft = r.plan(api, old, new, reason)
    deployment = kube.get('deployment', old_deployment)
    pvc = kube.get('pvc', old_pvc)
    if not deployment or not pvc:
        raise ValueError('The old Deployment and PVC identities must still be present for review')
    evidence_check(evidence, deployment, pvc)
    claims = [v.get('persistentVolumeClaim', {}).get('claimName') for v in deployment['spec']['template']['spec'].get('volumes', [])]
    if old_pvc not in claims or len(deployment['spec']['template']['spec'].get('containers', [])) != 1:
        raise ValueError('Old workload does not identify one controller and its lost PVC')
    old_args = deployment['spec']['template']['spec']['containers'][0].get('args', [])
    if old_args.count('--id') != 1 or old_args[old_args.index('--id')+1] != str(old):
        raise ValueError('Old Deployment identity differs from the reviewed Raft member')
    fresh_pvc, fresh_deployment = replacement_objects(bundle, kube.namespace, new, old_pvc)
    if fresh_deployment['metadata']['name'] == old_deployment:
        raise ValueError('Use a fresh controller Deployment name')
    for item in [fresh_pvc, fresh_deployment]:
        if kube.get(item['kind'], item['metadata']['name']):
            raise ValueError('Replacement names must be unused at plan time')
    return {'format': FORMAT, 'raft': raft, 'kubernetes_cluster': kube.cluster(), 'namespace': kube.namespace,
            'old_deployment': old_deployment, 'old_deployment_uid': deployment['metadata']['uid'],
            'old_pvc': old_pvc, 'old_pvc_uid': pvc['metadata']['uid'], 'evidence': evidence, 'objects': bundle}


def subset(expected, actual):
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(key in actual and subset(value, actual[key]) for key, value in expected.items())
    if isinstance(expected, list):
        return isinstance(actual, list) and len(expected) == len(actual) and all(subset(a, b) for a, b in zip(expected, actual))
    return expected == actual


def scale_down(kube, name, original):
    """Retry status-only resourceVersion races without relaxing UID/CAS checks."""
    uid, spec = original['metadata']['uid'], original['spec']
    stopped = dict(spec, replicas=0)
    for attempt in range(3):
        patch = [{'op': 'test', 'path': '/metadata/uid', 'value': uid},
                 {'op': 'test', 'path': '/metadata/resourceVersion', 'value': original['metadata']['resourceVersion']},
                 {'op': 'replace', 'path': '/spec/replicas', 'value': 0}]
        try:
            kube.run(['patch', 'deployment', name, '--type=json', '-p', json.dumps(patch), '-o', 'json'])
            return
        except KubernetesOperationError:
            current = kube.get('deployment', name)
            if not current or current['metadata']['uid'] != uid:
                raise ValueError('Old workload identity changed during scale-down') from None
            if current['spec'] == stopped:
                return  # A committed scale-down whose response was lost.
            if current['spec'] != spec:
                raise ValueError('Old workload specification changed during scale-down') from None
            if attempt == 2 or current['metadata']['resourceVersion'] == original['metadata']['resourceVersion']:
                raise
            original = current


def apply(api, kube, value, path, approval, timeout):
    checksum = m.digest(value)
    if value.get('format') != FORMAT or checksum != approval:
        raise ValueError('Approve the exact reviewed Kubernetes replacement SHA256')
    fd = os.open(str(path)+'.infra-lock', os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if kube.cluster() != value['kubernetes_cluster'] or kube.namespace != value['namespace']:
            raise ValueError('Kubernetes cluster or namespace differs from the reviewed plan')
        _, membership = r.leader(api)
        reviewed = value['raft']['snapshot']
        voters, new = sorted(reviewed['voters']), value['raft']['new']
        shapes = [(voters, []), (voters, [new]), (sorted(voters+[new]), []),
                  (sorted([v for v in voters if v != value['raft']['old']]+[new]), [])]
        if any(membership.get(key) != reviewed.get(key) for key in ['cluster_id', 'catalog', 'scope']) or (sorted(membership['voters']), sorted(membership['learners'] or [])) not in shapes:
            raise ValueError('Raft scope or topology changed outside the reviewed replacement')
        original = kube.get('deployment', value['old_deployment'])
        pvc = kube.get('pvc', value['old_pvc'])
        if not original or original['metadata']['uid'] != value['old_deployment_uid'] or not pvc or pvc['metadata']['uid'] != value['old_pvc_uid']:
            raise ValueError('Old workload or PVC identity changed; do not act on replacement names')
        journal_path = str(path)+'.infrastructure'
        journal = json.loads(m.private_read(journal_path)) if Path(journal_path).exists() else {'digest': checksum}
        if journal.get('digest') != checksum:
            raise ValueError('Infrastructure journal belongs to a different plan')
        if not journal.get('isolated'):
            evidence_check(value['evidence'], original, pvc)
        # External persistent physical isolation is the prerequisite; scaling
        # down alone never proves that a partitioned kubelet stopped the process.
        if original['spec'].get('replicas') != 0:
            scale_down(kube, value['old_deployment'], original)
        journal['isolated'] = True
        m.save(journal_path, journal)
        replacement_objects(value['objects'], kube.namespace, value['raft']['new'], value['old_pvc'])
        for obj in sorted(value['objects']['items'], key=lambda item: item['kind'] != 'PersistentVolumeClaim'):
            obj = json.loads(json.dumps(obj))
            obj['metadata'].setdefault('annotations', {})['djob.dev/replacement-plan'] = checksum
            kind, name = obj['kind'], obj['metadata']['name']
            current = kube.get(kind, name)
            if current is None:
                if journal.get(kind):
                    raise ValueError("Provisioned resource identity changed; refusing to recreate storage or workload")
                # create, never apply: a lost response is reconciled using the
                # operation annotation and exact reviewed desired fields.
                kube.run(['create', '-f', '-', '-o', 'json'], obj)
                current = kube.get(kind, name)
            if not current or current['metadata'].get('annotations', {}).get('djob.dev/replacement-plan') != checksum or not subset(obj['spec'], current['spec']):
                raise ValueError('Provisioned object differs from the reviewed plan; no membership write was made')
            uid = current['metadata']['uid']
            if journal.get(kind) not in (None, uid):
                raise ValueError('Provisioned resource identity changed; replacement storage cannot be reused')
            journal[kind] = uid
            m.save(journal_path, journal)
        # The new Deployment retries --join until add_learner commits. Promotion
        # waits for actual Raft catch-up, not Kubernetes Pod readiness.
        result = r.apply(api, value['raft'], str(path)+'.raft', m.digest(value['raft']), timeout)
        journal['confirmed'] = result['confirmed']
        m.save(journal_path, journal)
        return journal


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['plan', 'review', 'apply'])
    parser.add_argument('--plan-file', required=True)
    parser.add_argument('--kubeconfig'); parser.add_argument('--context'); parser.add_argument('--namespace')
    parser.add_argument('--controllers', default='http://127.0.0.1:8080')
    parser.add_argument('--credential-file'); parser.add_argument('--ca-file')
    parser.add_argument('--old-member', type=int); parser.add_argument('--new-member', type=int)
    parser.add_argument('--old-deployment'); parser.add_argument('--old-pvc')
    parser.add_argument('--evidence-file'); parser.add_argument('--objects-file'); parser.add_argument('--reason')
    parser.add_argument('--approve'); parser.add_argument('--timeout', type=int, default=900)
    args = parser.parse_args()
    if args.action == 'review':
        value = json.loads(m.private_read(args.plan_file)); print(json.dumps(value, indent=2)); print('SHA256:', m.digest(value)); return
    if not args.credential_file or not 5 <= args.timeout <= 3600:
        parser.error('provide --credential-file and --timeout in 5..3600 seconds')
    kube = Kubernetes(args.kubeconfig, args.context, args.namespace)
    api = m.API(args.controllers, args.credential_file, args.ca_file)
    if args.action == 'plan':
        if not all([args.old_member, args.new_member, args.old_deployment, args.old_pvc, args.evidence_file, args.objects_file, args.reason]):
            parser.error('plan requires member IDs, old workload/PVC, evidence, objects and reason')
        value = build(api, kube, args.old_member, args.new_member, args.reason, args.old_deployment, args.old_pvc,
                      json.loads(m.private_read(args.evidence_file)), json.loads(m.private_read(args.objects_file)))
        m.save(args.plan_file, value, exclusive=True)
        print('Review', args.plan_file, 'SHA256', m.digest(value))
    else:
        value = json.loads(m.private_read(args.plan_file))
        apply(api, kube, value, args.plan_file, args.approve, args.timeout)
        print('Kubernetes member replacement confirmed')


if __name__ == '__main__':
    try: main()
    except (ValueError, OSError, KeyError, IndexError) as error:
        print(error, file=sys.stderr); sys.exit(1)
