import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('replace_member', Path(__file__).with_name('replace-member.py'))
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)


class Cluster:
    origins = ['http://127.0.0.1:8080']

    def __init__(self):
        self.current = {'enabled': True, 'cluster_id': '4096', 'member_id': '1', 'leader_id': '1',
                        'configuration': 'initial', 'catalog': 'authorized', 'catalog_size': 5,
                        'max_id': '3', 'voters': [1, 2, 3], 'learners': []}
        self.writes = []
        self.drop = True

    def request(self, path, body=None, origin=None):
        if body is None:
            return json.loads(json.dumps(self.current))
        self.writes.append(body)
        q = json.loads(body)
        if q['expected_configuration'] != self.current['configuration']:
            raise ValueError('stale configuration')
        member = int(q['member_id'])
        if q['action'] == 'add_learner':
            self.current['learners'] = [member]
            self.current['max_id'] = str(member)
        elif q['action'] == 'promote':
            self.current['learners'] = []
            self.current['voters'].append(member)
        else:
            self.current['voters'].remove(member)
        self.current['configuration'] = q['action']
        if self.drop:
            self.drop = False
            raise OSError('response lost after commit')
        return {'submitted': True}


class ReplacementTests(unittest.TestCase):
    def test_lost_response_is_reconciled_without_repeating_membership_writes(self):
        api = Cluster()
        value = r.plan(api, 2, 4, 'lost volume replacement')
        self.assertEqual(api.writes, [])
        with tempfile.TemporaryDirectory() as directory, patch.object(r.time, 'sleep'):
            path = Path(directory)/'replacement.json'
            r.m.save(path, value, exclusive=True)
            result = r.apply(api, value, path, r.m.digest(value))
            self.assertTrue(result['confirmed'])
            self.assertEqual(api.current['voters'], [1, 3, 4])
            self.assertEqual(len(api.writes), 3)
            r.apply(api, value, path, r.m.digest(value))
            self.assertEqual(len(api.writes), 3)

    def test_rejects_identity_reuse_concurrent_changes_and_foreign_clusters(self):
        api = Cluster()
        with self.assertRaises(ValueError): r.plan(api, 2, 3, 'reuse')
        with self.assertRaises(ValueError): r.plan(api, 1, 4, 'current leader')
        with self.assertRaises(ValueError): r.plan(api, 2, 6, 'unknown slot')
        value = r.plan(api, 2, 4, 'replacement')
        for key, bad in [('cluster_id', 'another'), ('catalog', 'another'), ('voters', [1, 2])]:
            with self.subTest(key=key), tempfile.TemporaryDirectory() as directory:
                saved = api.current[key]
                api.current[key] = bad
                with self.assertRaises(ValueError):
                    r.apply(api, value, Path(directory)/'plan.json', r.m.digest(value))
                self.assertEqual(api.writes, [])
                api.current[key] = saved


if __name__ == '__main__': unittest.main()
