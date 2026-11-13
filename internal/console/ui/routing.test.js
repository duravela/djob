import test from 'node:test';
import assert from 'node:assert/strict';
import {readRoute, recordReference, referenceHref, routeHref, mutationTarget} from './routing.js';

const pages = {jobs: {kind: 'DEFINITION'}, schedules: {kind: 'TRIGGER'}, runs: {kind: 'RUN'}, groups: {kind: 'GROUP'}, bindings: {kind: 'BINDING'}, grants: {kind: 'GRANT'}, enrollments: {kind: 'WORKER_ENROLLMENT'}, resources: {kind: 'RESOURCE', global: true}, tenants: {kind: 'TENANT', global: true}, workers: {readonly: true}, usage: {readonly: true, kinds: ['LIMITS', 'USAGE', 'CONFIGURATION_LIMITS', 'CONFIGURATION_USAGE']}};
const reader = {role: 'reader', tenant_id: 'alpha'};
const platform = {role: 'operator', tenant_id: ''};

test('record links round trip exact Unicode IDs, reserved characters and surrounding spaces', () => {
  for (const id of ['run/with?reserved&=#%+chars', 'Grüße 世界', ' id with spaces ', '9007199254740993']) {
    const ref = {kind: 'RUN', tenant_id: 'alpha', id};
    const href = referenceHref(ref, '', pages);
    const parsed = readRoute(href, reader, pages);
    assert.deepEqual(parsed.reference, ref);
    assert.equal(parsed.error, '');
    assert.equal(parsed.page, 'runs');
  }
  assert.equal(routeHref('jobs', 'alpha'), '#/jobs?tenant=alpha');
});

test('foreign tenant and platform links never become reads of a same-named local record', () => {
  for (const href of ['#/runs?tenant=beta&id=shared-id', '#/resources?id=shared-id', '#/constructor?id=shared-id', '#/workers?tenant=alpha&id=shared-id']) {
    const parsed = readRoute(href, reader, pages);
    assert.equal(parsed.reference, null);
    assert.notEqual(parsed.error, '');
    assert.equal(parsed.tenant, 'alpha');
  }
  assert.deepEqual(readRoute('#/runs?tenant=beta&id=shared-id', platform, pages).reference, {kind: 'RUN', tenant_id: 'beta', id: 'shared-id'});
});

test('global records and workspace-scoped records use their own reference rules', () => {
  for (const kind of ['DEFINITION', 'TRIGGER', 'RUN', 'GROUP', 'BINDING', 'GRANT', 'WORKER_ENROLLMENT']) {
    const ref = {kind, tenant_id: 'beta', id: 'record'};
    assert.deepEqual(readRoute(referenceHref(ref, 'alpha', pages), platform, pages).reference, ref);
  }
  const resource = {kind: 'RESOURCE', tenant_id: '', id: 'shared-bus'};
  assert.deepEqual(readRoute(referenceHref(resource, 'alpha', pages), platform, pages).reference, resource);
  assert.equal(readRoute(referenceHref(resource, 'alpha', pages), platform, pages).tenant, 'alpha');
  const tenant = {kind: 'TENANT', tenant_id: 'beta'};
  assert.deepEqual(readRoute(referenceHref(tenant, 'alpha', pages), platform, pages).reference, tenant);
  assert.equal(readRoute('#/runs?id=record', platform, pages).reference, null);
  assert.equal(referenceHref({kind: 'ACTION', tenant_id: 'alpha', sequence: '1'}, 'alpha', pages), null);
});

test('malformed, ambiguous and oversized links do not get normalized into different identities', () => {
  for (const href of ['#/runs?tenant=alpha&tenant=beta&id=x', '#/runs?tenant=alpha&id=a&id=b', '#/runs?tenant=alpha&id=%FF', '#/runs?tenant=alpha&id=%', '#/runs?tenant=alpha&id=', '#/runs?tenant=alpha&url=https://elsewhere&id=x', '#/?tenant=alpha&id=x', '#/runs?tenant=alpha&id=' + '界'.repeat(86)]) {
    const parsed = readRoute(href, reader, pages);
    assert.equal(parsed.reference, null, href);
    assert.notEqual(parsed.error, '', href);
  }
  assert.throws(() => recordReference('constructor', 'alpha', 'x', pages));
  assert.throws(() => recordReference('runs', 'alpha', '\ud800', pages));
  assert.equal(readRoute('', reader, pages).page, 'jobs');
  assert.equal(readRoute('', platform, pages).page, 'tenants');
});

test('uncertain writes expose only a read target and preserve their exact envelope', () => {
  const request = {tenant_id: 'alpha', command: {request_id: 'retained', at: '2026-11-11T00:00:00.123456789Z'}, launch: {definition_id: 'job', definition_revision: '9007199254740993', run_id: 'generated-run'}};
  const original = JSON.stringify(request);
  assert.deepEqual(mutationTarget(request), {kind: 'RUN', tenant_id: 'alpha', id: 'generated-run'});
  assert.equal(JSON.stringify(request), original);
  for (const [operation, value, expected] of [
    ['put_definition', {definition: {id: 'job'}}, {kind: 'DEFINITION', tenant_id: 'alpha', id: 'job'}],
    ['purge', {run_id: 'done'}, {kind: 'DELETION', tenant_id: 'alpha', id: 'done'}],
    ['resolve_run', {run_id: 'unknown'}, {kind: 'RUN', tenant_id: 'alpha', id: 'unknown'}],
    ['resolve_resource', {resource_id: 'bus'}, {kind: 'RESOURCE', tenant_id: '', id: 'bus'}],
    ['put_resource', {spec: {id: 'bus'}}, {kind: 'RESOURCE', tenant_id: '', id: 'bus'}],
    ['put_tenant', {}, {kind: 'TENANT', tenant_id: 'alpha'}],
    ['put_limits', {limits: {}}, {kind: 'LIMITS', tenant_id: 'alpha'}],
    ['put_configuration_limits', {limits: {}}, {kind: 'CONFIGURATION_LIMITS', tenant_id: 'alpha'}],
    ['put_trigger', {definition: {id: 'daily'}}, {kind: 'TRIGGER', tenant_id: 'alpha', id: 'daily'}],
  ]) assert.deepEqual(mutationTarget({[operation]: value}, 'alpha'), expected);
  const enrollment = {tenant_id: 'alpha', worker_id: 'company-one', action: 'rotate', expected_revision: '9007199254740993', credential_hash: 'abc', command: {request_id: 'retained-enrollment'}};
  const exact = JSON.stringify(enrollment);
  assert.deepEqual(mutationTarget(enrollment), {kind: 'WORKER_ENROLLMENT', tenant_id: 'alpha', id: 'company-one'});
  assert.equal(JSON.stringify(enrollment), exact);
});

test('tenant singleton results link to usage without inventing record identifiers', () => {
  for (const kind of pages.usage.kinds) {
    const href = referenceHref({kind, tenant_id: 'alpha'}, 'beta', pages);
    assert.equal(href, '#/usage?tenant=alpha');
    assert.deepEqual(readRoute(href, reader, pages), {page: 'usage', tenant: 'alpha', reference: null, error: ''});
    assert.equal(referenceHref({kind}, 'alpha', pages), null);
  }
  assert.match(readRoute('#/usage?tenant=beta', reader, pages).error, /another tenant/);
  assert.match(readRoute('#/usage?tenant=alpha&id=anything', reader, pages).error, /no durable record/);
});

test('release inspection links preserve the tenant, selected release and run across reload', () => {
  const href = routeHref('runs', 'alpha', 'waiting/123', 'worker-v1');
  assert.equal(href, '#/runs?tenant=alpha&id=waiting%2F123&worker_release=worker-v1');
  const parsed = readRoute(href, reader, pages);
  assert.equal(parsed.workerRelease, 'worker-v1');
  assert.deepEqual(parsed.reference, {kind: 'RUN', tenant_id: 'alpha', id: 'waiting/123'});
  assert.equal(referenceHref(parsed.reference, 'alpha', pages, 'worker-v1'), href);
  for (const bad of ['#/jobs?tenant=alpha&worker_release=v1', '#/runs?tenant=alpha&worker_release=', '#/runs?tenant=alpha&worker_release=v1&worker_release=v2', '#/runs?tenant=beta&worker_release=v1', '#/runs?tenant=alpha&worker_release=bad%2Fid']) {
    const invalid = readRoute(bad, reader, pages);
    assert.notEqual(invalid.error, ''); assert.equal(invalid.reference, null); assert.equal(invalid.workerRelease, undefined);
  }
});

test('release lifecycle receipts link to the deployment rather than its enrollment', () => {
  for (const action of ['publish', 'retiring', 'resume']) {
    assert.deepEqual(mutationTarget({tenant_id: 'alpha', id: 'release-v1', worker_id: 'worker-v1', action}), {kind: 'WORKER_DEPLOYMENT', tenant_id: 'alpha', id: 'release-v1'});
  }
  assert.deepEqual(mutationTarget({tenant_id: 'alpha', worker_id: 'worker-v1', action: 'resume'}), {kind: 'WORKER_ENROLLMENT', tenant_id: 'alpha', id: 'worker-v1'});
});
