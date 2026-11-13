#!/usr/bin/env python3
"""Review and reconcile a learner-first replacement using a fresh Raft identity."""
import argparse
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import sys
import time
import urllib.error
import uuid

spec = importlib.util.spec_from_file_location('release_management', Path(__file__).with_name('manage-release.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
FORMAT = 'djob-member-replacement/v1'


def leader(api):
    for origin in api.origins:
        try:
            current = api.request('cluster/members', origin=origin)
            if current.get('enabled') and current.get('member_id') == current.get('leader_id'):
                api.origins = [origin]+[item for item in api.origins if item != origin]
                return origin, current
        except (OSError, ValueError):
            continue
    raise ValueError('No configured controller established current leader membership')


def plan(api, old, new, reason):
    _, current = leader(api)
    if old not in current['voters'] or old == int(current['leader_id']):
        raise ValueError('Select an existing voter that is no longer the current leader')
    if new <= int(current['max_id']) or new > current['catalog_size'] or current['learners']:
        raise ValueError('Replacement needs a fresh higher catalog slot and no other pending learners')
    if not reason.strip() or len(reason.encode()) > 4096:
        raise ValueError('Provide an audited reason within 4096 UTF-8 bytes')
    return {'format': FORMAT, 'id': uuid.uuid4().hex, 'old': old, 'new': new, 'reason': reason,
            'snapshot': current, 'created_at': m.now()}


def apply(api, value, path, approval, timeout=300):
    checksum = m.digest(value)
    if value.get('format') != FORMAT or approval != checksum:
        raise ValueError('Review and approve the exact replacement plan SHA256')
    fd = os.open(str(path)+'.lock', os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state_path = str(path)+'.state'
        state = json.loads(m.private_read(state_path)) if Path(state_path).exists() else {'digest': checksum, 'steps': []}
        if state.get('digest') != checksum:
            raise ValueError('Replacement journal differs from its reviewed plan')
        original = value['snapshot']
        voters = sorted(original['voters'])
        replacement = value['new']
        shapes = [(voters, []), (voters, [replacement]), (sorted(voters+[replacement]), []),
                  (sorted([v for v in voters if v != value['old']]+[replacement]), [])]
        deadline = time.monotonic()+timeout
        for index, action in enumerate(['add_learner', 'promote', 'remove']):
            if len(state['steps']) > index and state['steps'][index].get('confirmed'):
                continue
            while True:
                if time.monotonic() >= deadline:
                    raise ValueError('Replacement timed out; keep the plan and journal, inspect the learner, then resume apply')
                try:
                    origin, current = leader(api)
                except ValueError:
                    time.sleep(1)
                    continue
                if current['cluster_id'] != original['cluster_id'] or current['catalog'] != original['catalog'] or current.get('scope') != original.get('scope'):
                    raise ValueError('Cluster identity, scope or preauthorized peer catalog changed')
                shape = (sorted(current['voters']), sorted(current.get('learners') or []))
                retained = len(state['steps']) > index
                if retained and shape == shapes[index+1]:
                    state['steps'][index]['confirmed'] = True
                    m.save(state_path, state)
                    break
                if shape != shapes[index]:
                    raise ValueError('Concurrent membership change; reconcile the retained plan before proceeding')
                if not retained:
                    command = {'request_id': 'member-replacement/'+value['id']+'/'+str(index),
                               'reason': value['reason'], 'expected_configuration': current['configuration'],
                               'action': action, 'member_id': str(value['old'] if action == 'remove' else replacement)}
                    state['steps'].append({'wire': m.canonical(command).decode(), 'confirmed': False})
                    m.save(state_path, state)  # Persist intent before the first network write.
                wire = state['steps'][index]['wire']
                q = json.loads(wire)
                expected = {'request_id': 'member-replacement/'+value['id']+'/'+str(index),
                            'reason': value['reason'], 'expected_configuration': current['configuration'],
                            'action': action, 'member_id': str(value['old'] if action == 'remove' else replacement)}
                if q != expected:
                    raise ValueError('Retained membership envelope differs from the reviewed operation')
                try:
                    api.request('cluster/members', wire.encode(), origin=origin)
                except urllib.error.HTTPError as error:
                    code = error.code
                    error.close()
                    if code not in (408, 409, 412, 429) and code < 500:
                        raise ValueError('Membership request returned HTTP '+str(code)+'; inspect before resuming') from error
                except OSError:
                    pass  # A lost response is reconciled against quorum-confirmed configuration.
                time.sleep(1)
        state['confirmed'] = True
        m.save(state_path, state)
        return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['plan', 'review', 'apply'])
    parser.add_argument('--controllers', default='http://127.0.0.1:8080')
    parser.add_argument('--credential-file'); parser.add_argument('--ca-file')
    parser.add_argument('--plan-file', required=True)
    parser.add_argument('--old-member', type=int); parser.add_argument('--new-member', type=int)
    parser.add_argument('--reason'); parser.add_argument('--approve')
    parser.add_argument('--timeout', type=int, default=300)
    args = parser.parse_args()
    if args.action == 'review':
        value = json.loads(m.private_read(args.plan_file))
        print(json.dumps(value, indent=2)); print('SHA256:', m.digest(value)); return
    if not args.credential_file: parser.error('--credential-file is required')
    api = m.API(args.controllers, args.credential_file, args.ca_file)
    if args.action == 'plan':
        if not args.old_member or not args.new_member or not args.reason:
            parser.error('plan requires --old-member, --new-member and --reason')
        value = plan(api, args.old_member, args.new_member, args.reason)
        m.save(args.plan_file, value, exclusive=True)
        print('Review', args.plan_file, 'SHA256', m.digest(value))
    else:
        if not 5 <= args.timeout <= 3600: parser.error('--timeout must be 5..3600 seconds')
        value = json.loads(m.private_read(args.plan_file))
        apply(api, value, args.plan_file, args.approve, args.timeout)
        print('Replacement confirmed:', value['old'], '->', value['new'])


if __name__ == '__main__':
    try: main()
    except (ValueError, OSError) as error:
        print(error, file=sys.stderr); sys.exit(1)
