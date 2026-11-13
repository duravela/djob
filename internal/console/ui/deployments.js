import {integer, displayTime} from './model.js';

const identity = value => typeof value === 'string' && /^[A-Za-z0-9_.-]{1,64}$/.test(value);

const sessionCounts = ['connected', 'registered', 'draining', 'relocating', 'stale', 'unavailable', 'other_generations'];

// Enrollment policy and process availability are independent. Replica lag,
// missing fields and stale samples cannot establish enabled access.
export function deploymentAccessView(sources, record, now = Date.now()) {
  const samples = [];
  let complete = Array.isArray(sources) && sources.length > 0 && sources.length <= 16;
  for (const source of Array.isArray(sources) ? sources.slice(0, 16) : []) {
    const status = source?.status, sample = status?.worker_access;
    let valid = !source?.error && status?.tenant_id === record.tenant_id && status.id === record.id && status.revision === record.revision
      && sample?.observed === true && identity(sample.worker_id) && sample.worker_id === record.worker_id && ['enabled', 'draining', 'revoked'].includes(sample.state)
      && typeof sample.revision === 'string' && typeof sample.observed_at === 'string' && Number.isFinite(Date.parse(sample.observed_at))
      && now - Date.parse(sample.observed_at) <= 10000 && Date.parse(sample.observed_at) - now <= 5000;
    try { integer(sample?.revision, true); } catch { valid = false; }
    if (!valid) { complete = false; continue; }
    samples.push(sample);
  }
  complete = complete && new Set(samples.map(sample => sample.state + '/' + sample.revision)).size === 1;
  let advice = '';
  const state = complete ? samples[0].state : null;
  if (!complete) advice = 'Worker-access observations are missing, stale or disagree. Access for new claims is not established. Refresh and inspect the worker access record.';
  if (state === 'revoked' || !complete && samples.some(sample => sample.state === 'revoked')) advice += (advice ? ' ' : '') + 'Worker access is reported revoked. This identity cannot be resumed; replacement requires a new worker identity and deployment.';
  else if (state === 'draining' || !complete && samples.some(sample => sample.state === 'draining')) advice += (advice ? ' ' : '') + 'Worker access is reported draining, so new claims are refused. Resume worker access when appropriate. Existing conversations retain their normal lease and cleanup rules.';
  return {complete, state, advice, expiresAt: samples.length ? Math.min(...samples.map(sample => Date.parse(sample.observed_at) + 10001)) : null, rows: [
    ['Access for new claims', state || 'Not established'], ['Enrollment revision', complete ? samples[0].revision : 'Not established'],
  ]};
}

// Samples count sessions, not distinct processes or execution capacity. Aliases
// of one controller must not multiply its counts. Missing samples stay unknown.
export function deploymentSessionsView(sources, record, now = Date.now()) {
  const observations = new Map();
  let complete = Array.isArray(sources) && sources.length > 0 && sources.length <= 16;
  for (const source of Array.isArray(sources) ? sources.slice(0, 16) : []) {
    const status = source?.status, sample = status?.worker_sessions;
    let valid = !source?.error && status?.tenant_id === record.tenant_id && status.id === record.id && status.revision === record.revision
      && sample?.generation === record.generation && typeof sample.enabled === 'boolean'
      && typeof sample.instance_id === 'string' && sample.instance_id.length > 0 && sample.instance_id.length <= 128
      && typeof sample.observed_at === 'string' && Number.isFinite(Date.parse(sample.observed_at))
      && now - Date.parse(sample.observed_at) <= 10000 && Date.parse(sample.observed_at) - now <= 5000
      && sessionCounts.every(key => Number.isSafeInteger(sample[key]) && sample[key] >= 0 && sample[key] <= 0xffffffff)
      && (sample.enabled || sessionCounts.every(key => sample[key] === 0));
    try { if (typeof sample?.controller_id !== 'string') valid = false; else integer(sample.controller_id, true); }
    catch { valid = false; }
    if (!valid) { complete = false; continue; }
    const previous = observations.get(sample.controller_id);
    if (previous && (previous.instance_id !== sample.instance_id || Date.parse(previous.observed_at) === Date.parse(sample.observed_at) && sessionCounts.some(key => previous[key] !== sample[key]))) complete = false;
    if (!previous || Date.parse(sample.observed_at) > Date.parse(previous.observed_at)) observations.set(sample.controller_id, sample);
  }
  complete = complete && observations.size > 0;
  const counts = Object.fromEntries(sessionCounts.map(key => [key, [...observations.values()].reduce((sum, sample) => sum + sample[key], 0)]));
  const show = n => complete ? String(n) : n > 0 ? `At least ${n}` : 'Not established';
  let advice = '';
  if (!observations.size) advice = 'No current worker-session observation. Refresh and check the configured controllers.';
  else if (!complete) advice = 'Worker-session observations are partial. Unreadable, stale or mismatched controllers may have additional sessions.';
  else if (record.spec.replicas > 0 && counts.connected === 0) advice = 'No current-generation worker is connected to the observed controllers. Check worker configuration, credentials and logs.';
  const expiresAt = observations.size ? Math.min(...[...observations.values()].map(sample => Date.parse(sample.observed_at) + 10001)) : null;
  return {complete, connected: counts.connected, advice, expiresAt, rows: [
    ['Connected sessions · generation ' + record.generation, show(counts.connected)],
    ['Sessions on other generations', show(counts.other_generations)],
    ['Controller observations', observations.size + ' current' + (complete ? '' : ' · partial')],
  ], details: [
    ['Registered, not polling', show(counts.registered)], ['Draining sessions', show(counts.draining)],
    ['Relocating sessions', show(counts.relocating)], ['Stale worker contact', show(counts.stale)],
    ['Unavailable for new work', show(counts.unavailable)],
  ]};
}

export function deploymentRuntimeView(value, now = Date.now()) {
  const reported = n => Number.isSafeInteger(n) && n >= 0 && n <= 0xffffffff ? String(n) : 'Not reported';
  const observed = n => value.state === 'error' ? 'Not reported' : reported(n);
  let advice = '';
  switch (value.error) {
    case 'replica_failure': advice = 'Worker Pods could not be created. Inspect the Kubernetes Deployment events for quota or workload admission failures.'; break;
    case 'rollout_failed': advice = 'Kubernetes reports a rollout failure. Inspect the Deployment events before editing or restarting it.'; break;
    case 'progress_deadline_exceeded': advice = 'The rollout stopped making progress. Inspect the worker Pods for image, configuration or startup failures, then edit or restart the deployment after fixing the cause.'; break;
    case 'forbidden': advice = 'The worker manager cannot access this Kubernetes Deployment. Check its namespace and service-account permissions.'; break;
    case 'pool_unavailable': advice = 'This runtime pool is not configured on the observing controller.'; break;
    case 'unavailable': advice = 'The Kubernetes API is unavailable. Check runtime connectivity and TLS configuration; djob retries automatically.'; break;
    case 'state_unavailable': advice = 'Current deployment state could not be read. Check the controller’s health and storage.'; break;
    case 'immutable_config_required': advice = 'Provision an immutable, protected versioned configuration Secret and grant the manager read access.'; break;
    case 'immutable_config_replaced': advice = 'The configuration Secret UID changed. Restore the original protected configuration or publish a new release with a new Secret name.'; break;
    case 'configuration_conflict': advice = 'Kubernetes changed settings owned by djob. Inspect workload admission policies for conflicting worker identity, image, resources, credentials or security settings.'; break;
  }
  if (!advice && value.state === 'stopping') advice = 'Pods are still stopping. Existing conversation cleanup and resource reservations remain active.';
  if (value.state === 'paused' || value.rollout_paused === true && ['stopped', 'stopping', 'retired'].includes(value.state)) advice += (advice ? ' ' : '') + 'Rollouts are paused by the platform. Have a platform operator review and resume the Deployment in Kubernetes. Existing workers may still execute jobs; Start or Restart does not remove this rollout hold.';
  if (value.state === 'deleting') advice = 'Kubernetes is deleting this workload. djob waits for its removal before reconciling the current desired record. Inspect finalizers if deletion stalls.';
  const rows = [
    ['Available / desired Pods', observed(value.available_replicas) + ' / ' + reported(value.desired_replicas)],
    ['Ready Pods', observed(value.ready_replicas)], ['Updated Pods', observed(value.updated_replicas)],
    ['Current replicas', observed(value.replicas)], ['Terminating Pods', observed(value.terminating_replicas)],
  ];
  if (value.rollout_paused === true) rows.push(['Kubernetes rollout', 'Paused']);
  const timestamp = at => typeof at === 'string' && Number.isFinite(Date.parse(at)) ? displayTime(at) : 'Not reported';
  if (value.retry_after !== undefined) rows.push(['Automatic retry after (UTC)', timestamp(value.retry_after)]);
  if (value.deletion_requested_at !== undefined) rows.push(['Deletion requested (UTC)', timestamp(value.deletion_requested_at)]);
  const observedAt = typeof value.observed_at === 'string' ? Date.parse(value.observed_at) : NaN;
  const stale = !Number.isFinite(observedAt) || now - observedAt > 10000 || observedAt - now > 5000;
  return {advice, rows, stale, expiresAt: stale ? null : observedAt + 10001};
}

function count(value, label, min, max) {
  if (!/^\d+$/.test(String(value))) throw new Error(`${label} must be a whole number.`);
  const n = Number(value);
  if (!Number.isSafeInteger(n) || n < min || n > max) throw new Error(`${label} must be between ${min} and ${max}.`);
  return n;
}
export function deploymentRecord(value, tenant, id = '') {
  if (!value || value.tenant_id !== tenant || !identity(value.id) || !identity(value.worker_id) || id && value.id !== id
    || !['active', 'retiring', 'retired'].includes(value.state) || !value.spec || typeof value.spec.image !== 'string'
    || typeof value.revision !== 'string' || typeof value.generation !== 'string') throw new Error('Controller deployment does not match the requested identity.');
  integer(value.revision, true); integer(value.generation, true);
  count(value.spec.replicas, 'Replicas', 0, 32);
  return {reference: {kind: 'WORKER_DEPLOYMENT', tenant_id: tenant, id: value.id}, worker_deployment: value};
}
export function buildDeploymentMutation(fields, record, tenant, action) {
  if (!['put', 'scale', 'restart', 'retire', 'publish', 'retiring', 'resume'].includes(action)) throw new Error('Unknown deployment action.');
  const f = name => String(fields.get(name) ?? '').trim(), value = record?.worker_deployment;
  if (record) { deploymentRecord(value, tenant); if (value.state === 'retired') throw new Error('Retired deployments cannot be reused.'); }
  const q = {tenant_id: tenant, id: value?.id || f('id'), worker_id: value?.worker_id || f('worker_id'), action, expected_revision: value ? integer(value.revision, true) : '0', reason: f('reason')};
  if (!identity(q.id) || !identity(q.worker_id)) throw new Error('Use deployment and worker IDs of 1–64 letters, digits, dots, underscores or hyphens.');
  if (!q.reason || new TextEncoder().encode(q.reason).length > 1024) throw new Error('Provide a reason of at most 1024 UTF-8 bytes.');
  if (action === 'put') {
    const pool = value?.spec.pool || f('pool') || 'default', image = f('image'), config_secret = f('config_secret'), pull_secret = f('pull_secret');
    if (!/^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/.test(pool) || pool.length > 63) throw new Error('Use a Kubernetes runtime pool name of at most 63 lowercase letters, digits or hyphens.');
    if (image.length > 512 || !/^[a-z0-9][a-z0-9._:/-]+@sha256:[a-f0-9]{64}$/.test(image)) throw new Error('Pin the image as registry/repository@sha256 followed by its 64-character digest.');
    const secret = name => name.length <= 253 && name.split('.').every(part => /^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/.test(part) && part.length <= 63);
    if (!secret(config_secret) || pull_secret && !secret(pull_secret)) throw new Error('Use valid Kubernetes Secret names.');
    let args = [];
    try { args = JSON.parse(f('args') || '[]'); } catch { throw new Error('Arguments must be a JSON array of strings.'); }
    if (!Array.isArray(args) || args.length > 32 || args.some(arg => typeof arg !== 'string' || arg.includes('\0') || new TextEncoder().encode(arg).length > 1024) || args.reduce((n, arg) => n + new TextEncoder().encode(arg).length, 0) > 4096) throw new Error('Use at most 32 arguments totaling 4096 UTF-8 bytes.');
    q.spec = {pool, image, config_secret, ...(pull_secret ? {pull_secret} : {}), args,
      replicas: count(f('replicas'), 'Replicas', 0, 32), cpu_millis: count(f('cpu_millis'), 'CPU millicores', 10, 64000),
      memory_mib: count(f('memory_mib'), 'Memory MiB', 32, 65536), drain_seconds: count(f('drain_seconds'), 'Shutdown grace', 10, 600)};
  } else if (!value) throw new Error('Open an existing deployment before changing its lifecycle.');
  else if (action === 'scale') q.replicas = count(f('replicas'), 'Replicas', 0, 32);
  if (value?.published && value.outstanding_runs !== '0' && (action === 'restart' || action === 'retire' || action === 'scale' && q.replicas < value.spec.replicas || action === 'put' && q.spec.replicas < value.spec.replicas)) throw new Error('Finish draining outstanding release runs before restarting or reducing replicas.');
  return {worker_deployment: q};
}
