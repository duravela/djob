#!/usr/bin/env python3
"""Reviewable worker-release promotion/rollback plans with exact durable retries."""
import argparse
import copy
import datetime as dt
import fcntl
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import ssl
import stat
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid

FORMAT = 'djob-release-plan/v1'

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()

def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()

def now():
    return dt.datetime.now(dt.timezone.utc).isoformat().replace('+00:00', 'Z')

def revision(value):
    if not isinstance(value, str) or not value.isdecimal() or not 0 < int(value) < 2**64:
        raise ValueError('Record has no exact uint64 revision')
    return int(value)

def private_read(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode) or os.fstat(stream.fileno()).st_mode & 0o077:
            raise ValueError('Plan, state and credential files must have owner-only permissions')
        raw = stream.read(16 * 1024 * 1024 + 1)
        if len(raw) > 16 * 1024 * 1024:
            raise ValueError('File exceeds 16 MiB')
        return raw

def save(path, value, exclusive=False):
    path = Path(path)
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(json.dumps(value, indent=2, ensure_ascii=False).encode() + b'\n')
            stream.flush(); os.fsync(stream.fileno())
        if exclusive:
            os.link(temporary, path)  # An existing reviewed plan is never overwritten.
            temporary.unlink()
        else:
            os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(directory)
        finally: os.close(directory)
    finally:
        if temporary.exists(): temporary.unlink()

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs): return None

class API:
    def __init__(self, origins, credential_file, ca_file=None):
        self.origins = []
        for raw in origins.split(','):
            u = urllib.parse.urlsplit(raw.strip())
            try: local = ipaddress.ip_address(u.hostname).is_loopback
            except ValueError: local = False
            if not u.hostname or u.username or u.password or u.path not in ('', '/') or u.query or u.fragment or (u.scheme != 'https' and not (u.scheme == 'http' and local)):
                raise ValueError('Controllers require HTTPS origins, except literal loopback')
            self.origins.append(raw.strip().rstrip('/'))
        header = private_read(credential_file).decode().strip()
        if not header.startswith('Authorization: Bearer ') or '\n' in header or '\r' in header:
            raise ValueError('Credential file must contain one Authorization: Bearer header')
        self.authorization = header.removeprefix('Authorization: ')
        self.client = urllib.request.build_opener(NoRedirect(), urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=ca_file)))

    def request(self, path, body=None, origin=None):
        raw = None if body is None else (body if isinstance(body, bytes) else canonical(body))
        request = urllib.request.Request((origin or self.origins[0]) + '/api/jobs/v1/' + path, data=raw, headers={'Authorization': self.authorization, 'Content-Type': 'application/json'})
        with self.client.open(request, timeout=15) as response:
            data = response.read(8 * 1024 * 1024 + 1)
            if len(data) > 8 * 1024 * 1024: raise ValueError('Controller reply exceeds 8 MiB')
            return json.loads(data)

    def get(self, tenant, kind, identity):
        value = self.request('get', {'reference': {'kind': kind, 'tenant_id': tenant, 'id': identity}})
        if value.get('reference') != {'kind': kind, 'tenant_id': tenant, 'id': identity}:
            # Protobuf zero fields may be emitted; compare identity components.
            ref = value.get('reference', {})
            if (ref.get('kind'), ref.get('tenant_id'), ref.get('id')) != (kind, tenant, identity): raise ValueError('Record scope mismatch')
        return value

    def all(self, tenant, kind):
        token, seen, records = '', set(), []
        for _ in range(512):
            q = {'kind': kind, 'tenant_id': tenant, 'page_size': 32}
            if token: q['page_token'] = token
            page = self.request('list', q)
            for row in page.get('records', []):
                ref = row.get('reference', {})
                if ref.get('kind') != kind or ref.get('tenant_id') != tenant: raise ValueError('List scope mismatch')
                records.append(row)
            token = page.get('next_page_token', '')
            if not token: return records
            if token in seen: raise ValueError('Repeated page token')
            seen.add(token)
        raise ValueError('Inspection exceeds 512 pages; narrow the tenant configuration')

    def deployment(self, tenant, identity):
        value = self.request('worker-deployments/get', {'tenant_id': tenant, 'id': identity})
        if value.get('tenant_id') != tenant or value.get('id') != identity or not value.get('published'): raise ValueError('Expected a published release in this tenant')
        return value


def matches(definition, release):
    return definition.get('worker_release') == release or definition.get('worker_release_weights', {}).get(release, 0) > 0

def specification(definition):
    value = copy.deepcopy(definition); value.pop('revision', None)
    return value

def move(definition, source, target):
    result = specification(definition)
    weights = result.get('worker_release_weights', {})
    if weights:
        amount = weights.pop(source, 0)
        weights[target] = weights.get(target, 0) + amount
    else:
        result['worker_release'] = target
    return result

def operation(kind, value, expected, before=None):
    return {'kind': kind, 'id': value['id'], 'expected_revision': str(expected), 'value': value, 'before': before}

def plan(api, tenant, source, target, jobs=None, triggers=None, canary_run=None, allow_unqualified=False):
    if source == target: raise ValueError('Choose distinct releases')
    origin, destination = api.deployment(tenant, source), api.deployment(tenant, target)
    if destination['state'] != 'active' or destination['spec']['replicas'] == 0: raise ValueError('Target release must accept new runs')
    if not canary_run and not allow_unqualified: raise ValueError('Provide --canary-run or explicitly approve --allow-unqualified')
    if canary_run: verify_canary(api,tenant,target,canary_run,destination)
    identity = uuid.uuid4().hex
    selected_jobs, selected_triggers, operations, planned = [], [], [], {}
    requirements = {}
    selected = jobs is not None or triggers is not None
    for row in api.all(tenant, 'DEFINITION'):
        id = row['reference']['id']
        if selected and (not jobs or id not in jobs): continue
        full = api.get(tenant, 'DEFINITION', id)['definition']
        if matches(full, source): selected_jobs.append(full)
    if jobs and set(jobs) != {v['id'] for v in selected_jobs}: raise ValueError('Selected jobs must exist and reference the source release')
    for row in api.all(tenant, 'TRIGGER'):
        id = row['reference']['id']
        if selected and (not triggers or id not in triggers): continue
        full = api.get(tenant, 'TRIGGER', id)['trigger']
        if matches(full['job_definition'], source): selected_triggers.append(full)
    if triggers and set(triggers) != {v['definition']['id'] for v in selected_triggers}: raise ValueError('Selected schedules must exist and reference the source release')
    for before in selected_jobs:
        changed = move(before, source, target)
        operations.append(operation('DEFINITION', changed, revision(before['revision']), before))
        planned[changed['id']] = (changed, revision(before['revision']) + 1)
        requirements[changed['id']] = changed
    for before in selected_triggers:
        trigger = copy.deepcopy(before['definition']); definition_id = trigger['definition_id']
        if definition_id in planned:
            _, version = planned[definition_id]
        else:
            current = api.get(tenant, 'DEFINITION', definition_id)['definition']
            if matches(current, target) and not matches(current, source):
                version = revision(current['revision'])
                requirements[definition_id] = specification(current)
            else:
                clone = move(before['job_definition'], source, target)
                clone['id'] = 'migration-' + identity[:12] + '-' + hashlib.sha256(trigger['id'].encode()).hexdigest()[:12]
                operations.append(operation('DEFINITION', clone, 0))
                requirements[clone['id']] = clone
                definition_id, version = clone['id'], 1
        trigger['definition_id'], trigger['definition_revision'] = definition_id, str(version)
        operations.append(operation('TRIGGER', trigger, revision(before['revision']), before))
    return {'format': FORMAT, 'id': identity, 'tenant': tenant, 'source': source, 'target': target, 'created_at': now(), 'source_snapshot': origin, 'target_snapshot': destination, 'operations': operations, 'required_definitions': list(requirements.values()), 'canary_run':canary_run, 'allow_unqualified':allow_unqualified}

def rollback(api, original, state):
    if state.get('plan_digest') != digest(original) or len(state.get('operations', [])) != len(original['operations']): raise ValueError('Progress does not match the reviewed plan')
    if any(item['status'] in ('submitting', 'uncertain') for item in state['operations']): raise ValueError('Resolve uncertain submissions with an exact retry before planning rollback')
    tenant, source, target = original['tenant'], original['target'], original['source']
    destination = api.deployment(tenant, target)
    if destination['state'] == 'retired': raise ValueError('Permanent retirement is irreversible; deploy another release before rollback')
    result = {'format': FORMAT, 'id': uuid.uuid4().hex, 'tenant': tenant, 'source': source, 'target': target, 'created_at': now(), 'source_snapshot': api.deployment(tenant, source), 'target_snapshot': destination, 'operations': [], 'allow_unqualified':original.get('allow_unqualified',False)}
    confirmed = [op for op, item in zip(original['operations'], state['operations']) if item['status'] == 'confirmed']
    restorable = any(op['kind'] == 'TRIGGER' or op['kind'] == 'DEFINITION' and op['before'] is not None for op in confirmed)
    version = revision(destination['revision'])
    if restorable and destination['spec']['replicas'] == 0:
        result['operations'].append(operation('DEPLOYMENT', {'id': target, 'worker_id': destination['worker_id'], 'action': 'scale', 'replicas': 1, 'reason': 'Resume reviewed release rollback'}, version)); version += 1
    if restorable and destination['state'] == 'retiring':
        result['operations'].append(operation('DEPLOYMENT', {'id': target, 'worker_id': destination['worker_id'], 'action': 'resume', 'reason': 'Resume reviewed release rollback'}, version))
    restored = {}
    for op in confirmed:
        if op['kind'] != 'DEFINITION' or op['before'] is None: continue
        current = api.get(tenant, 'DEFINITION', op['id'])['definition']
        if revision(current['revision']) != int(op['expected_revision']) + 1: raise ValueError('Job changed after promotion; replan instead of overwriting it')
        old = specification(op['before']); expected = revision(current['revision'])
        result['operations'].append(operation('DEFINITION', old, expected, current)); restored[old['id']] = (old, expected + 1)
    for op in confirmed:
        if op['kind'] != 'TRIGGER': continue
        current = api.get(tenant, 'TRIGGER', op['id'])['trigger']
        if revision(current['revision']) != int(op['expected_revision']) + 1: raise ValueError('Schedule changed after promotion; replan instead of overwriting it')
        old = op['before']; trigger = copy.deepcopy(old['definition']); template = specification(old['job_definition'])
        candidate = restored.get(template['id'])
        if candidate and candidate[0] == template:
            definition_id, version = template['id'], candidate[1]
        else:
            # A dedicated copy restores a schedule without rewriting an unrelated current job.
            template['id'] = 'rollback-' + result['id'][:12] + '-' + hashlib.sha256(op['id'].encode()).hexdigest()[:12]
            result['operations'].append(operation('DEFINITION', template, 0)); definition_id, version = template['id'], 1
        trigger['definition_id'], trigger['definition_revision'] = definition_id, str(version)
        result['operations'].append(operation('TRIGGER', trigger, revision(current['revision']), current))
    return result


def verify_canary(api, tenant, target, run_id, deployment):
    run=api.get(tenant,'RUN',run_id)['run']
    if run.get('status')!='succeeded' or run.get('definition',{}).get('worker_release')!=target:
        raise ValueError('Canary must be a succeeded run pinned to the target release')
    try:
        created=dt.datetime.fromisoformat(run['created_at'].replace('Z','+00:00'))
        finished=dt.datetime.fromisoformat(run['last_transition'].replace('Z','+00:00'))
        changed=dt.datetime.fromisoformat(deployment['changed_at'].replace('Z','+00:00'))
        age=(dt.datetime.now(dt.timezone.utc)-finished).total_seconds()
        if created<changed or not -5<=age<=1800: raise ValueError('Canary is stale for the current target deployment')
    except (KeyError,TypeError,OverflowError) as error:
        raise ValueError('Canary has no valid execution timestamps') from error
    return run

def qualify(api,args):
    path=Path(args.plan_file)
    if not args.approve:
        if not args.tenant or not args.target or not args.definition or len(args.definition)!=1:
            raise ValueError('qualify requires --tenant, --target and one dedicated --definition')
        target=api.deployment(args.tenant,args.target)
        definition=api.get(args.tenant,'DEFINITION',args.definition[0])['definition']
        if definition.get('worker_release')!=args.target or definition.get('worker_release_weights'):
            raise ValueError('Canary definition must be pinned exclusively to the target release')
        identity=uuid.uuid4().hex
        request={'tenant_id':args.tenant,'command':{'request_id':'canary/'+identity,'at':now()},'launch':{'definition_id':definition['id'],'definition_revision':definition['revision'],'run_id':'canary-'+identity}}
        value={'format':'djob-canary/v1','tenant':args.tenant,'target':args.target,'target_snapshot':target,'request':request}
        save(path,value,exclusive=True);print('Review canary',path,'SHA256',digest(value));return
    value=json.loads(private_read(path))
    if value.get('format')!='djob-canary/v1' or args.approve!=digest(value):raise ValueError('Review the canary and approve its exact SHA256')
    # Reuse the same locked, journaled submission protocol as promotion writes.
    q=value['request'];launch=q['launch']; state_path=str(path)+'.state'
    fd=os.open(str(path)+'.lock',os.O_WRONLY|os.O_CREAT|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        state=json.loads(private_read(state_path)) if Path(state_path).exists() else {'digest':digest(value),'status':'waiting'}
        if state.get('digest')!=digest(value):raise ValueError('Canary progress does not match its plan')
        if state['status']=='rejected':raise ValueError('Canary was rejected; review a new plan')
        uncertain=state['status'] in ('uncertain','submitting')
        if uncertain and not args.retry_uncertain:raise ValueError('Canary requires explicit --retry-uncertain')
        if state['status']!='confirmed':
            if state['status']=='waiting':
                current=api.deployment(value['tenant'],value['target'])
                if current['state']!='active' or current['generation']!=value['target_snapshot']['generation'] or current['spec']['replicas']==0:raise ValueError('Canary target changed or stopped')
            state['status']='submitting';save(state_path,state)
            try:
                ack=api.request('mutate',canonical(q));ref=ack.get('reference',{})
                if ack.get('request_id')!=q['command']['request_id'] or ref.get('kind')!='RUN' or ref.get('id')!=launch['run_id'] or ref.get('tenant_id')!=value['tenant']:raise ValueError('Canary acknowledgement mismatch')
                state['status']='confirmed';state['acknowledgement']=ack
            except urllib.error.HTTPError as error:
                error.close()
                state['status']='uncertain' if uncertain or error.code>=500 or error.code==408 else 'rejected'
                state['http_status']=error.code;save(state_path,state)
                raise ValueError('Canary returned HTTP '+str(error.code)+'; retained state: '+state['status']) from error
            except Exception:
                state['status']='uncertain';save(state_path,state);raise
            save(state_path,state)
        print('Canary run:',launch['run_id'],'Inspect its terminal result before selecting it with --canary-run.')


def fresh_observation(value):
    if not isinstance(value, str): return False
    try:
        observed = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
        age = (dt.datetime.now(dt.timezone.utc) - observed).total_seconds()
    except (ValueError, TypeError, OverflowError):
        return False
    return -5 <= age <= 30


def preflight(api, plan, allow_unobserved):
    tenant, target = plan['tenant'], plan['target']
    current = api.deployment(tenant, target)
    if current['state'] != 'active' or current['spec']['replicas'] == 0: raise ValueError('Target release stopped accepting new runs')
    if current['spec']['image'] != plan['target_snapshot']['spec']['image'] or current['generation'] != plan['target_snapshot']['generation']: raise ValueError('Target generation changed; rebuild the reviewed plan')
    if plan.get('canary_run'): verify_canary(api,tenant,target,plan['canary_run'],current)
    elif not plan.get('allow_unqualified'): raise ValueError('A successful target canary is required; review a new plan or explicitly approve --allow-unqualified')
    if allow_unobserved: return
    available, workers = False, []
    for origin in api.origins:
        try:
            sample = api.request('worker-deployments/status', {'tenant_id': tenant, 'id': target}, origin)
            runtime = sample.get('runtime', {})
            if sample.get('tenant_id') != tenant or sample.get('id') != target: raise ValueError('Runtime observation scope mismatch')
            available |= sample.get('observed', False) and fresh_observation(runtime.get('observed_at')) and runtime.get('state') == 'available' and runtime.get('generation') == current['generation'] and runtime.get('revision') == current['revision'] and runtime.get('available_replicas', 0) > 0
            token, seen = '', set()
            for _ in range(512):
                query = {'tenant_id': tenant, 'page_size': 16}
                if token: query['page_token'] = token
                inventory = api.request('workers?' + urllib.parse.urlencode(query), origin=origin)
                if inventory.get('tenant_id') != tenant: raise ValueError('Worker observation scope mismatch')
                if not fresh_observation(inventory.get('observed_at')): raise ValueError('Worker observation is stale')
                workers.extend(w for w in inventory.get('workers', []) if w.get('deployment_id') == target and w.get('deployment_generation') == current['generation'] and w.get('state') == 'connected' and w.get('tenant_id') == tenant)
                token = inventory.get('next_page_token', '')
                if not token: break
                if token in seen: raise ValueError('Repeated worker page')
                seen.add(token)
            else: raise ValueError('Worker observation exceeds 512 pages')
        except (urllib.error.URLError, ValueError, KeyError):
            continue
    if not available or not workers: raise ValueError('Target availability and connected workers could not be established; inspect it or explicitly approve --allow-unobserved')
    for d in plan.get('required_definitions', [op['value'] for op in plan['operations'] if op['kind'] == 'DEFINITION']):
        runtime = d.get('runtime')
        if not runtime: runtime = api.get(tenant, 'GROUP', d['group_id'])['group']['runtime']
        binding = api.get(tenant, 'BINDING', d['binding_id'])['binding'] if d.get('binding_id') else None
        if not any(runtime in w.get('runtimes', []) and {'name': d['handler'], 'version': d['handler_version']} in w.get('handlers', []) and (not binding or {'name': binding['driver'], 'version': binding['driver_version']} in w.get('drivers', [])) for w in workers):
            raise ValueError('No connected target worker advertises the complete capabilities for job ' + d['id'])


def readiness(api, value):
    if value.get('format') != FORMAT:
        raise ValueError('Readiness requires a reviewed release plan')
    report = {'format': 'djob-release-readiness/v1', 'observed_at': now(), 'plan_digest': digest(value),
              'tenant': value['tenant'], 'target': value['target'], 'checks': []}
    def check(name, inspect):
        try:
            inspect()
            report['checks'].append({'name': name, 'passed': True})
        except (ValueError, KeyError, TypeError, OSError) as error:
            # Never copy controller response bodies or credentials into reports.
            message = str(error) if isinstance(error, ValueError) else 'Inspection unavailable or malformed'
            report['checks'].append({'name': name, 'passed': False, 'reason': message})
    check('target_and_canary', lambda: preflight(api, value, True))
    check('runtime_and_worker_capabilities', lambda: preflight(api, value, False))
    def revisions():
        for op in value['operations']:
            if op['expected_revision'] == '0':
                continue  # Creation conflicts are decided by the controller at apply.
            record = api.deployment(value['tenant'], op['id']) if op['kind'] == 'DEPLOYMENT' else api.get(value['tenant'], op['kind'], op['id'])[op['kind'].lower()]
            if record['revision'] != op['expected_revision']:
                raise ValueError('A reviewed record revision changed; reconcile progress or rebuild the plan')
    check('reviewed_revisions', revisions)
    report['ready'] = all(item['passed'] for item in report['checks'])
    return report


def apply(api, plan, path, approval, retry_uncertain=False, allow_unobserved=False):
    checksum = digest(plan)
    if plan.get('format') != FORMAT or approval != checksum: raise ValueError('Review the plan and supply its exact --approve SHA256')
    lock_fd = os.open(str(path) + '.lock', os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(lock_fd, 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state_path = str(path) + '.state'
        state = json.loads(private_read(state_path)) if Path(state_path).exists() else {'plan_digest': checksum, 'operations': [{'status': 'waiting'} for _ in plan['operations']]}
        if state.get('plan_digest') != checksum or len(state['operations']) != len(plan['operations']): raise ValueError('Progress does not match this plan')
        healthy = False
        for index, (op, item) in enumerate(zip(plan['operations'], state['operations'])):
            if item['status'] == 'confirmed': continue
            if item['status'] == 'rejected': raise ValueError('A reviewed step was rejected; inspect it and create a new plan or rollback')
            uncertain = item['status'] in ('uncertain', 'submitting')
            if uncertain and not retry_uncertain: raise ValueError('An uncertain step requires explicit --retry-uncertain; its exact envelope is retained')
            # Exact retries must be able to recover a receipt even after observations changed.
            if not uncertain and op['kind'] != 'DEPLOYMENT' and not healthy:
                preflight(api, plan, allow_unobserved); healthy = True
            if 'wire' not in item:
                command = {'request_id': 'release-plan/' + plan['id'] + '/' + str(index), 'at': now()}
                if op['kind'] == 'DEPLOYMENT':
                    q = {'tenant_id': plan['tenant'], 'command': command, 'expected_revision': op['expected_revision'], **op['value']}
                else:
                    field = 'put_definition' if op['kind'] == 'DEFINITION' else 'put_trigger'
                    q = {'tenant_id': plan['tenant'], 'command': command, field: {'expected_revision': op['expected_revision'], 'definition': op['value']}}
                item['wire'] = canonical(q).decode()
            wire = json.loads(item['wire']); metadata = wire.pop('command', {})
            if op['kind'] == 'DEPLOYMENT':
                expected = {'tenant_id': plan['tenant'], 'expected_revision': op['expected_revision'], **op['value']}
            else:
                field = 'put_definition' if op['kind'] == 'DEFINITION' else 'put_trigger'
                expected = {'tenant_id': plan['tenant'], field: {'expected_revision': op['expected_revision'], 'definition': op['value']}}
            if wire != expected or metadata.get('request_id') != 'release-plan/' + plan['id'] + '/' + str(index) or not metadata.get('at'):
                raise ValueError('Retained envelope does not match the reviewed plan')
            item['status'] = 'submitting'; save(state_path, state)
            endpoint = 'worker-deployments/mutate' if op['kind'] == 'DEPLOYMENT' else 'mutate'
            try:
                ack = api.request(endpoint, item['wire'].encode())
                q = json.loads(item['wire']); ref = ack.get('deployment') if op['kind'] == 'DEPLOYMENT' else ack.get('reference', {})
                matched = ack.get('request_id') == q['command']['request_id'] and ref.get('tenant_id') == plan['tenant'] and ref.get('id') == op['id']
                matched &= (ref.get('revision') if op['kind'] == 'DEPLOYMENT' else ack.get('revision')) == str(int(op['expected_revision']) + 1)
                if op['kind'] != 'DEPLOYMENT': matched &= ref.get('kind') == op['kind']
                if not matched: raise ValueError('Acknowledgement does not match the exact reviewed step')
                item['status'], item['acknowledgement'] = 'confirmed', ack
            except urllib.error.HTTPError as error:
                error.close()
                item['status'] = 'uncertain' if uncertain or error.code >= 500 or error.code == 408 else 'rejected'
                item['http_status'] = error.code; save(state_path, state)
                raise ValueError('Step ' + str(index + 1) + ' returned HTTP ' + str(error.code) + '; retained state: ' + item['status']) from error
            except Exception:
                item['status'] = 'uncertain'; save(state_path, state); raise
            save(state_path, state)
            print(str(index + 1) + '/' + str(len(plan['operations'])) + ' confirmed ' + op['kind'] + ' ' + op['id'])
        return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['plan', 'review', 'readiness', 'apply', 'rollback', 'qualify'])
    parser.add_argument('--controllers', default='http://127.0.0.1:8080')
    parser.add_argument('--credential-file'); parser.add_argument('--ca-file')
    parser.add_argument('--canary-run'); parser.add_argument('--allow-unqualified', action='store_true')
    parser.add_argument('--tenant'); parser.add_argument('--source'); parser.add_argument('--target')
    parser.add_argument('--definition', action='append'); parser.add_argument('--trigger', action='append')
    parser.add_argument('--plan-file', required=True); parser.add_argument('--output')
    parser.add_argument('--approve'); parser.add_argument('--retry-uncertain', action='store_true'); parser.add_argument('--allow-unobserved', action='store_true')
    args = parser.parse_args()
    if args.action == 'review':
        value = json.loads(private_read(args.plan_file))
        if value.get('format') == 'djob-canary/v1':
            print(json.dumps(value,indent=2)); print('SHA256:',digest(value)); return
        print('Tenant:', value['tenant'], 'Release:', value['source'], '->', value['target'])
        for index, op in enumerate(value['operations']): print(index + 1, op['kind'], op['id'], 'expected revision', op['expected_revision'])
        print('SHA256:', digest(value)); return
    if not args.credential_file: parser.error('--credential-file is required')
    api = API(args.controllers, args.credential_file, args.ca_file)
    if args.action == 'readiness':
        report = readiness(api, json.loads(private_read(args.plan_file)))
        print(json.dumps(report, indent=2))
        if not report['ready']: sys.exit(2)
        return
    if args.action == 'qualify':
        qualify(api,args); return
    if args.action == 'plan':
        if not all([args.tenant, args.source, args.target]): parser.error('plan requires --tenant, --source and --target')
        value = plan(api, args.tenant, args.source, args.target, args.definition, args.trigger,args.canary_run,args.allow_unqualified)
        save(args.plan_file, value, exclusive=True); print('Review', args.plan_file, 'SHA256', digest(value))
    elif args.action == 'apply':
        value = json.loads(private_read(args.plan_file)); apply(api, value, args.plan_file, args.approve, args.retry_uncertain, args.allow_unobserved)
    else:
        if not args.output: parser.error('rollback requires --output for a separately reviewed plan')
        original = json.loads(private_read(args.plan_file)); state = json.loads(private_read(args.plan_file + '.state'))
        value = rollback(api, original, state)
        value['canary_run']=args.canary_run;value['allow_unqualified']=args.allow_unqualified
        save(args.output, value, exclusive=True); print('Review rollback', args.output, 'SHA256', digest(value))

if __name__ == '__main__':
    try: main()
    except (ValueError, OSError, KeyError, urllib.error.URLError) as error:
        print('Release operation stopped:', str(error), file=sys.stderr); sys.exit(1)
