import test from 'node:test';
import assert from 'node:assert/strict';
import {deploymentRecord, buildDeploymentMutation, deploymentRuntimeView, deploymentSessionsView, deploymentAccessView} from './deployments.js';

const image = 'ghcr.io/acme/worker@sha256:' + 'a'.repeat(64);
const fields = () => new Map(Object.entries({id: 'line-one', worker_id: 'worker-one', pool: 'default', image, config_secret: 'worker-config', replicas: '2', cpu_millis: '100', memory_mib: '128', drain_seconds: '45', args: '["--worker-config","/var/run/djob/credentials/worker.json"]', reason: 'Deploy company capability'}));
test('deployment mutations keep revisions exact and worker identity immutable', () => {
  const created = buildDeploymentMutation(fields(), null, 'alpha', 'put').worker_deployment;
  assert.equal(created.expected_revision, '0'); assert.equal(created.spec.replicas, 2);
  const record = deploymentRecord({tenant_id: 'alpha', id: 'line-one', worker_id: 'worker-one', revision: '9007199254740993', generation: '2', state: 'active', spec: created.spec}, 'alpha');
  const changed = fields(); changed.set('worker_id', 'other-worker'); changed.set('pool', 'other-pool'); changed.set('replicas', '0');
  const scaled = buildDeploymentMutation(changed, record, 'alpha', 'scale').worker_deployment;
  assert.equal(scaled.expected_revision, '9007199254740993'); assert.equal(scaled.worker_id, 'worker-one'); assert.equal(scaled.replicas, 0); assert.equal(scaled.spec, undefined);
  const update = buildDeploymentMutation(changed, record, 'alpha', 'put').worker_deployment;
  assert.equal(update.spec.pool, 'default');
  assert.throws(() => deploymentRecord(record.worker_deployment, 'beta'));
});
test('deployments require immutable images, bounded arguments and resources', () => {
  for (const [key, value] of [['image', 'ghcr.io/acme/worker:latest'], ['replicas', '33'], ['cpu_millis', 'NaN'], ['memory_mib', '-1'], ['config_secret', '../secret'], ['args', '{"password":"secret"}']]) {
    const f = fields(); f.set(key, value); assert.throws(() => buildDeploymentMutation(f, null, 'alpha', 'put'));
  }
  const record = deploymentRecord({tenant_id: 'alpha', id: 'line-one', worker_id: 'worker-one', revision: '3', generation: '2', state: 'retired', spec: {image, replicas: 0}}, 'alpha');
  assert.throws(() => buildDeploymentMutation(fields(), record, 'alpha', 'put'));
});

test('runtime availability uses available Pods and keeps missing observations unknown', () => {
  const value = {desired_replicas: 2, available_replicas: 1, ready_replicas: 2, updated_replicas: 2, replicas: 4, terminating_replicas: 1};
  assert.deepEqual(deploymentRuntimeView(value).rows, [['Available / desired Pods', '1 / 2'], ['Ready Pods', '2'], ['Updated Pods', '2'], ['Current replicas', '4'], ['Terminating Pods', '1']]);
  assert.equal(deploymentRuntimeView({desired_replicas: 2, ready_replicas: 2}).rows[0][1], 'Not reported / 2');
  assert.equal(deploymentRuntimeView({terminating_replicas: 0}).rows[4][1], '0');
  for (const invalid of [-1, 0.5, '2', 4294967296, null]) assert.equal(deploymentRuntimeView({replicas: invalid}).rows[3][1], 'Not reported');
});

test('runtime failures give operator guidance without rendering controller messages', () => {
  assert.match(deploymentRuntimeView({state: 'error', error: 'configuration_conflict'}).advice, /admission policies/);
  assert.match(deploymentRuntimeView({error: 'replica_failure'}).advice, /could not be created/);
  assert.match(deploymentRuntimeView({error: 'progress_deadline_exceeded'}).advice, /stopped making progress/);
  assert.match(deploymentRuntimeView({state: 'stopping'}).advice, /cleanup and resource reservations/);
  assert.equal(deploymentRuntimeView({error: '<script>secret</script>'}).advice, '');
});

test('runtime retry timing remains separate from the age of its last observation', () => {
  const view = deploymentRuntimeView({state: 'error', error: 'unavailable', available_replicas: 0, desired_replicas: 2, replicas: 0, observed_at: '2026-11-11T10:00:00Z', retry_after: '2026-11-11T10:00:30Z'});
  assert.match(view.advice, /retries automatically/);
  assert.deepEqual(view.rows[0], ['Available / desired Pods', 'Not reported / 2']);
  assert.deepEqual(view.rows[3], ['Current replicas', 'Not reported']);
  assert.deepEqual(view.rows.at(-1), ['Automatic retry after (UTC)', '2026-11-11 10:00:30 UTC']);
  assert.equal(deploymentRuntimeView({retry_after: 'private-invalid-date'}).rows.at(-1)[1], 'Not reported');
  assert.equal(deploymentRuntimeView({}).rows.length, 5);
});

test('deletion remains visible even when the last replica counts look available', () => {
  const view = deploymentRuntimeView({state: 'deleting', available_replicas: 2, ready_replicas: 2, desired_replicas: 0, deletion_requested_at: '2026-11-11T12:34:56Z'});
  assert.match(view.advice, /waits for its removal/);
  assert.match(view.advice, /finalizers/);
  assert.deepEqual(view.rows.at(-1), ['Deletion requested (UTC)', '2026-11-11 12:34:56 UTC']);
  assert.equal(deploymentRuntimeView({state: 'deleting', deletion_requested_at: 'private-bad-timestamp'}).rows.at(-1)[1], 'Not reported');
});

test('platform rollout pauses stay distinct from worker shutdown and deletion', () => {
  for (const state of ['paused', 'stopped', 'stopping', 'retired']) {
    const view = deploymentRuntimeView({state, rollout_paused: true, desired_replicas: 2, available_replicas: 2});
    assert.deepEqual(view.rows.at(-1), ['Kubernetes rollout', 'Paused']);
    assert.deepEqual(view.rows[0], ['Available / desired Pods', '2 / 2']);
    assert.match(view.advice, /resume the Deployment in Kubernetes/);
    assert.match(view.advice, /Existing workers may still execute jobs/);
  }
  const deleting = deploymentRuntimeView({state: 'deleting', rollout_paused: true});
  assert.match(deleting.advice, /finalizers/);
  assert.doesNotMatch(deleting.advice, /resume the Deployment/);
  assert.equal(deploymentRuntimeView({rollout_paused: 'true'}).rows.length, 5);
});

test('runtime samples become stale while the deployment dialog remains open', () => {
  const now = Date.parse('2026-11-11T10:00:00Z');
  const sample = {state: 'available', observed_at: '2026-11-11T10:00:00Z'};
  assert.equal(deploymentRuntimeView(sample, now).stale, false);
  assert.equal(deploymentRuntimeView(sample, now).expiresAt, now + 10001);
  assert.equal(deploymentRuntimeView(sample, now + 10001).stale, true);
  assert.equal(deploymentRuntimeView(sample, now + 10001).expiresAt, null);
  assert.equal(deploymentRuntimeView(sample, now - 5001).stale, true);
});

const sessionTime = Date.parse('2026-11-11T10:00:00Z');
const deployment = {tenant_id: 'alpha', id: 'line-one', worker_id: 'company-worker', revision: '4', generation: '3', spec: {replicas: 2}};
function accessSource(changes = {}, statusChanges = {}) {
  return {status: {tenant_id: 'alpha', id: 'line-one', revision: '4', worker_access: {
    worker_id: 'company-worker', observed: true, observed_at: new Date(sessionTime).toISOString(), revision: '9007199254740993', state: 'enabled', ...changes,
  }, ...statusChanges}};
}

test('deployment access observations preserve enrollment revisions and expire without another read', () => {
  const source = accessSource();
  const view = deploymentAccessView([source, source], deployment, sessionTime);
  assert.equal(view.complete, true);
  assert.deepEqual(view.rows, [['Access for new claims', 'enabled'], ['Enrollment revision', '9007199254740993']]);
  assert.equal(view.advice, '');
  assert.equal(view.expiresAt, sessionTime + 10001);
  const stale = deploymentAccessView([source], deployment, sessionTime + 10001);
  assert.equal(stale.rows[0][1], 'Not established');
  assert.equal(stale.expiresAt, null);
});

test('missing, foreign, stale or disagreeing access observations never establish enabled access', () => {
  for (const bad of [
    null, {error: 'private-marker'}, {status: {}}, accessSource({observed: false}),
    accessSource({worker_id: 'another-worker'}), accessSource({state: 'unexpected'}),
    accessSource({revision: '0'}), accessSource({revision: '01'}), accessSource({revision: 1}), accessSource({revision: '18446744073709551616'}),
    accessSource({observed_at: new Date(sessionTime - 10001).toISOString()}), accessSource({observed_at: new Date(sessionTime + 5001).toISOString()}),
    accessSource({}, {tenant_id: 'beta'}), accessSource({}, {id: 'another-deployment'}), accessSource({}, {revision: '5'}),
    accessSource({revision: '9007199254740994'}), accessSource({state: 'draining'}),
  ]) {
    const view = deploymentAccessView([accessSource(), bad], deployment, sessionTime);
    assert.equal(view.complete, false);
    assert.equal(view.rows[0][1], 'Not established');
    assert.match(view.advice, /missing, stale or disagree/);
    assert.doesNotMatch(view.advice, /private-marker/);
  }
  assert.equal(deploymentAccessView([], deployment, sessionTime).rows[0][1], 'Not established');
});

test('drained and revoked access explain blocked claims independently of process availability', () => {
  for (const state of ['draining', 'revoked']) {
    const source = accessSource({state});
    const view = deploymentAccessView([source], deployment, sessionTime);
    assert.equal(view.rows[0][1], state);
    assert.match(view.advice, state === 'draining' ? /new claims are refused/ : /new worker identity and deployment/);
    const partial = deploymentAccessView([source, {error: 'unreachable'}], deployment, sessionTime);
    assert.equal(partial.rows[0][1], 'Not established');
    assert.match(partial.advice, new RegExp(state));
  }
});
function sessionSource(controllerID = '1', changes = {}, statusChanges = {}) {
  return {origin: 'https://controller.example', status: {tenant_id: 'alpha', id: 'line-one', revision: '4', worker_sessions: {
    controller_id: controllerID, instance_id: 'process-' + controllerID, observed_at: new Date(sessionTime).toISOString(), generation: '3', enabled: true,
    connected: 0, registered: 0, draining: 0, relocating: 0, stale: 0, unavailable: 0, other_generations: 0, ...changes,
  }, ...statusChanges}};
}

test('deployment connections aggregate controllers and keep other generations separate', () => {
  const view = deploymentSessionsView([sessionSource('9007199254740993', {connected: 1, draining: 2}), sessionSource('2', {connected: 1, other_generations: 3})], deployment, sessionTime);
  assert.equal(view.complete, true);
  assert.deepEqual(view.rows[0], ['Connected sessions · generation 3', '2']);
  assert.deepEqual(view.rows[1], ['Sessions on other generations', '3']);
  assert.deepEqual(view.details[1], ['Draining sessions', '2']);
  assert.equal(view.advice, '');
  assert.equal(view.expiresAt, sessionTime + 10001);
  assert.equal(deploymentSessionsView([sessionSource('1', {connected: 2})], deployment, sessionTime + 10001).expiresAt, null);
  assert.match(deploymentSessionsView([sessionSource()], deployment, sessionTime).advice, /No current-generation worker is connected/);
  assert.equal(deploymentSessionsView([sessionSource()], {...deployment, spec: {replicas: 0}}, sessionTime).advice, '');
});

test('missing, stale, disabled or mismatched deployment samples cannot establish zero connections', () => {
  for (const bad of [
    {error: 'unreachable'}, {status: {}}, null,
    sessionSource('2', {generation: '2'}), sessionSource('2', {}, {revision: '3'}),
    sessionSource('2', {}, {tenant_id: 'beta'}), sessionSource('2', {}, {id: 'another'}),
    sessionSource('2', {connected: -1}), sessionSource('2', {connected: '1'}),
    sessionSource('2', {connected: 1, enabled: false}), sessionSource('2', {observed_at: new Date(sessionTime - 10001).toISOString()}),
    sessionSource('2', {observed_at: new Date(sessionTime + 5001).toISOString()}),
  ]) {
    const view = deploymentSessionsView([sessionSource(), bad], deployment, sessionTime);
    assert.equal(view.complete, false);
    assert.equal(view.rows[0][1], 'Not established');
    assert.match(view.advice, /partial/);
  }
  assert.equal(deploymentSessionsView([sessionSource('1', {enabled: false})], deployment, sessionTime).complete, true);
  const partial = deploymentSessionsView([sessionSource('1', {connected: 2}), {error: 'unreachable'}], deployment, sessionTime);
  assert.equal(partial.rows[0][1], 'At least 2');
  const missing = deploymentSessionsView([], deployment, sessionTime);
  assert.equal(missing.rows[0][1], 'Not established');
  assert.match(missing.advice, /No current worker-session observation/);
});

test('controller aliases and process restarts cannot multiply deployment connection counts', () => {
  const first = sessionSource('1', {connected: 2});
  const second = sessionSource('1', {connected: 1, observed_at: new Date(sessionTime + 1).toISOString()});
  assert.equal(deploymentSessionsView([first, first], deployment, sessionTime).rows[0][1], '2');
  assert.equal(deploymentSessionsView([first, second], deployment, sessionTime).rows[0][1], '1');
  second.status.worker_sessions.instance_id = 'restarted';
  const restarted = deploymentSessionsView([first, second], deployment, sessionTime);
  assert.equal(restarted.complete, false);
  assert.equal(restarted.rows[0][1], 'At least 1');
  const conflicting = deploymentSessionsView([first, sessionSource('1')], deployment, sessionTime);
  assert.equal(conflicting.complete, false);
});

test('busy published releases allow increases but reject restarts and replica reductions', () => {
 const value={tenant_id:'alpha',id:'v1',worker_id:'w1',revision:'2',generation:'1',state:'active',published:true,outstanding_runs:'1',spec:{image,config_secret:'worker-config',replicas:2}};
 for(const action of ['restart','retire','scale']) { const f=fields();f.set('replicas','1');assert.throws(()=>buildDeploymentMutation(f,{worker_deployment:value},'alpha',action),/draining/); }
 const f=fields();f.set('replicas','3');assert.equal(buildDeploymentMutation(f,{worker_deployment:value},'alpha','scale').worker_deployment.replicas,3);
});
