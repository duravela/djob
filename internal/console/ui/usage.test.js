import test from 'node:test';
import assert from 'node:assert/strict';
import {limitPolicies, validateUsageRecord, limitRows, buildLimitMutation, UsageInspector} from './usage.js';

const record = (kind, value = {}, tenant = 'alpha') => ({reference: {kind, tenant_id: tenant}, [kind.toLowerCase()]: value});

test('omitted protobuf zeros require a read record and comparisons preserve uint64 precision', () => {
  assert.equal(validateUsageRecord(record('LIMITS'), 'LIMITS', 'alpha').limits.revision, undefined);
  let [row] = limitRows('limits', null, null);
  assert.equal(row.used, null); assert.equal(row.ceiling, null); assert.equal(row.comparison, 'Not read');
  [row] = limitRows('limits', {}, null);
  assert.equal(row.ceiling, '0'); assert.equal(row.used, null); assert.equal(row.comparison, 'Unlimited');
  [row] = limitRows('limits', null, {});
  assert.equal(row.used, '0'); assert.equal(row.comparison, 'Not read');
  for (const [used, comparison] of [['9007199254740992', 'Within limit'], ['9007199254740993', 'At limit'], ['9007199254740994', 'Above limit']]) {
    [row] = limitRows('limits', {max_pending_runs: '9007199254740993'}, {pending_runs: used});
    assert.equal(row.used, used); assert.equal(row.comparison, comparison);
  }
});

test('usage responses reject wrong scope, missing records and malformed counters', () => {
  for (const value of [record('LIMITS', {}, 'beta'), record('USAGE'), {reference: {kind: 'LIMITS', tenant_id: 'alpha'}}, record('LIMITS', []), record('LIMITS', {revision: null}), record('LIMITS', {max_pending_runs: 9007199254740992}), record('LIMITS', {max_pending_runs: '18446744073709551616'}), record('LIMITS', {max_pending_runs: '-1'})]) {
    assert.throws(() => validateUsageRecord(value, 'LIMITS', 'alpha'));
  }
  for (const policy of Object.values(limitPolicies)) {
    for (const kind of [policy.kind, policy.usage]) assert.deepEqual(validateUsageRecord(record(kind), kind, 'alpha'), record(kind));
  }
});

test('policy replacement keeps every field, exact expected revision and the draft read snapshot', () => {
  for (const [key, spec] of Object.entries(limitPolicies)) {
    const before = record(spec.kind, {revision: '9007199254740993'});
    const original = JSON.stringify(before);
    const fields = new Map(spec.fields.map(([name], i) => ['max_' + name, i ? '0' : '18446744073709551615']));
    const request = buildLimitMutation(key, fields, before, 'alpha');
    assert.equal(request.tenant_id, 'alpha');
    assert.equal(request['put_' + key].expected_revision, '9007199254740993');
    assert.deepEqual(request['put_' + key].limits, Object.fromEntries(fields));
    assert.equal(JSON.stringify(before), original);
    for (const invalid of ['', '01', '-1', '1.5', '18446744073709551616']) {
      fields.set('max_' + spec.fields[0][0], invalid);
      assert.throws(() => buildLimitMutation(key, fields, before, 'alpha'));
    }
    assert.throws(() => buildLimitMutation(key, fields, null, 'alpha'));
    assert.throws(() => buildLimitMutation(key, fields, before, 'beta'));
  }
});

test('usage sources fail independently and retry only the failed tenant-scoped singleton', async () => {
  const calls = [];
  let fail = true;
  const inspector = new UsageInspector('alpha', {active: () => true, root: () => null, query: async (method, body) => {
    calls.push({method, body});
    if (body.reference.kind === 'USAGE' && fail) throw Object.assign(new Error('replica unavailable'), {controller: 'https://two'});
    return {value: record(body.reference.kind), controller: 'https://one'};
  }});
  await inspector.load();
  assert.equal(inspector.sources.USAGE.record, null);
  assert.equal(inspector.sources.USAGE.error, 'replica unavailable');
  assert.equal(inspector.sources.USAGE.controller, 'https://two');
  assert.deepEqual(inspector.policy('limits'), record('LIMITS'));
  assert.ok(inspector.sources.CONFIGURATION_USAGE.record);
  fail = false;
  await inspector.loadRecord('USAGE');
  assert.ok(inspector.sources.USAGE.record); assert.equal(inspector.sources.USAGE.error, '');
  assert.equal(calls.length, 5);
  assert.ok(calls.every(call => call.method === 'get' && call.body.reference.tenant_id === 'alpha'));
  assert.deepEqual(calls.map(call => call.body.reference.kind), ['LIMITS', 'USAGE', 'CONFIGURATION_LIMITS', 'CONFIGURATION_USAGE', 'USAGE']);
});

test('a late or foreign usage read never populates an inactive workspace', async () => {
  let resolve, active = true;
  const inspector = new UsageInspector('alpha', {active: () => active, root: () => null, query: () => new Promise(done => { resolve = done; })});
  const reading = inspector.loadRecord('LIMITS');
  active = false;
  resolve({value: record('LIMITS'), controller: 'https://one'});
  await reading;
  assert.equal(inspector.sources.LIMITS.record, null);
  assert.throws(() => inspector.policy('limits'));
  active = true;
  inspector.query = async () => ({value: record('LIMITS', {}, 'beta'), controller: 'https://one'});
  await inspector.loadRecord('LIMITS');
  assert.equal(inspector.sources.LIMITS.record, null);
  assert.match(inspector.sources.LIMITS.error, /requested tenant/);
});
