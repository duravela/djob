import copy
import datetime
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('kubernetes_replacement', Path(__file__).with_name('replace-kubernetes-member.py'))
k = importlib.util.module_from_spec(spec)
spec.loader.exec_module(k)
spec2 = importlib.util.spec_from_file_location('raft_fixture', Path(__file__).with_name('replace_member_test.py'))
fixture = importlib.util.module_from_spec(spec2)
spec2.loader.exec_module(fixture)


class Kubernetes:
    namespace = 'controllers'

    def __init__(self):
        self.objects = {
            ('Deployment', 'old'): {'metadata': {'uid': 'old-uid', 'resourceVersion': '1'}, 'spec': {'replicas': 1, 'template': {'spec': {'containers': [{'args': ['--id', '2']}], 'volumes': [{'persistentVolumeClaim': {'claimName': 'lost'}}]}}}},
            ('PersistentVolumeClaim', 'lost'): {'metadata': {'uid': 'lost-uid'}}}
        self.writes = []
        self.drop = True

    def cluster(self): return 'cluster-uid'

    def get(self, kind, name):
        kind = {'deployment': 'Deployment', 'pvc': 'PersistentVolumeClaim'}.get(kind, kind)
        return copy.deepcopy(self.objects.get((kind, name)))

    def run(self, args, value=None):
        self.writes.append(args[0])
        if args[0] == 'patch':
            obj = self.objects['Deployment', 'old']
            checks = json.loads(args[args.index('-p')+1])
            assert checks[0]['value'] == obj['metadata']['uid']
            assert checks[1]['value'] == obj['metadata']['resourceVersion']
            obj['spec']['replicas'] = 0
        elif args[0] == 'create':
            obj = copy.deepcopy(value)
            obj['metadata']['uid'] = 'new-'+obj['kind']
            self.objects[obj['kind'], obj['metadata']['name']] = obj
            if self.drop:
                self.drop = False
                raise ValueError('response lost after create')
        else: raise AssertionError(args)
        return {}


def bundle():
    return {'apiVersion': 'v1', 'kind': 'List', 'items': [
        {'apiVersion': 'v1', 'kind': 'PersistentVolumeClaim', 'metadata': {'name': 'fresh', 'namespace': 'controllers'}, 'spec': {'accessModes': ['ReadWriteOnce'], 'resources': {'requests': {'storage': '1Gi'}}}},
        {'apiVersion': 'apps/v1', 'kind': 'Deployment', 'metadata': {'name': 'new', 'namespace': 'controllers'}, 'spec': {'replicas': 1, 'selector': {'matchLabels': {'member': '4'}}, 'template': {'metadata': {'labels': {'member': '4'}}, 'spec': {
            'containers': [{'name': 'djob', 'image': 'example/controller@sha256:'+'a'*64, 'args': ['--id', '4', '--join', '--dynamic-membership', '--data-dir', '/data'], 'volumeMounts': [{'name': 'data', 'mountPath': '/data'}]}],
            'volumes': [{'name': 'data', 'persistentVolumeClaim': {'claimName': 'fresh'}}]}}}}]}


def evidence():
    return {'volume_loss_confirmed': True, 'isolation_confirmed': True, 'deployment_uid': 'old-uid', 'pvc_uid': 'lost-uid',
            'incident': 'incident/123, hypervisor power fence verified', 'confirmed_at': datetime.datetime.now(datetime.timezone.utc).isoformat()}


class KubernetesReplacementTests(unittest.TestCase):
    def test_resume_after_lost_create_response_never_reuses_or_deletes_storage(self):
        api, kube = fixture.Cluster(), Kubernetes()
        value = k.build(api, kube, 2, 4, 'lost volume', 'old', 'lost', evidence(), bundle())
        with tempfile.TemporaryDirectory() as directory, patch.object(k.r.time, 'sleep'):
            path = Path(directory)/'plan'
            k.m.save(path, value, exclusive=True)
            with self.assertRaisesRegex(ValueError, 'response lost'):
                k.apply(api, kube, value, path, k.m.digest(value), 30)
            self.assertEqual(api.writes, [])
            self.assertEqual(kube.writes, ['patch', 'create'])
            result = k.apply(api, kube, value, path, k.m.digest(value), 30)
            self.assertTrue(result['confirmed'])
            self.assertEqual(kube.writes, ['patch', 'create', 'create'])
            k.apply(api, kube, value, path, k.m.digest(value), 30)
            self.assertEqual(kube.writes, ['patch', 'create', 'create'])
            self.assertIn(('PersistentVolumeClaim', 'lost'), kube.objects)
            self.assertEqual(api.current['voters'], [1, 3, 4])
            del kube.objects['PersistentVolumeClaim', 'fresh']
            with self.assertRaisesRegex(ValueError, 'refusing to recreate'):
                k.apply(api, kube, value, path, k.m.digest(value), 30)
            self.assertEqual(kube.writes, ['patch', 'create', 'create'])

    def test_unreachability_stale_evidence_and_foreign_identity_fail_before_writes(self):
        for key, bad in [('volume_loss_confirmed', False), ('isolation_confirmed', False), ('deployment_uid', 'foreign'), ('pvc_uid', 'foreign'), ('confirmed_at', '2000-01-01T00:00:00Z')]:
            with self.subTest(key=key):
                api, kube, proof = fixture.Cluster(), Kubernetes(), evidence()
                proof[key] = bad
                with self.assertRaises(ValueError): k.build(api, kube, 2, 4, 'loss', 'old', 'lost', proof, bundle())
                self.assertEqual(kube.writes, [])
                self.assertEqual(api.writes, [])

    def test_preflight_scope_drift_and_new_object_identity_block_mutation(self):
        api, kube = fixture.Cluster(), Kubernetes()
        value = k.build(api, kube, 2, 4, 'loss', 'old', 'lost', evidence(), bundle())
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'plan'
            api.current['catalog'] = 'different'
            with self.assertRaisesRegex(ValueError, 'scope or topology'):
                k.apply(api, kube, value, path, k.m.digest(value), 30)
            self.assertEqual(kube.writes, [])
            api.current['catalog'] = 'authorized'
            kube.objects['Deployment', 'old']['metadata']['uid'] = 'different'
            with self.assertRaisesRegex(ValueError, 'identity changed'):
                k.apply(api, kube, value, path, k.m.digest(value), 30)
            self.assertEqual(kube.writes, [])

    def test_rejects_reused_storage_and_ambiguous_data_mount(self):
        for mutate in [lambda value: value['items'][0]['spec'].update(volumeName='old-pv'),
                       lambda value: value['items'][1]['spec']['template']['spec']['containers'][0]['args'].extend(['--id', '3']),
                       lambda value: value['items'][1]['spec']['template']['spec']['containers'][0]['volumeMounts'][0].update(mountPath='/other')]:
            value = bundle(); mutate(value)
            with self.assertRaises(ValueError): k.replacement_objects(value, 'controllers', 4, 'lost')


if __name__ == '__main__': unittest.main()
