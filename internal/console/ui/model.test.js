import test from 'node:test';
import assert from 'node:assert/strict';
import {buildMutation, decodeBytes, durationSeconds, encodeBytes, escapeHTML, integer, workerEntries, workerMatches, inventoryPartial} from './model.js';

test('binary inputs round trip without changing Unicode, JSON numbers, or opaque bytes', () => {
  for (const text of ['Grüße 世界', '{"reading":9007199254740993}', '']) {
    const encoded = encodeBytes(text, 'text');
    assert.equal(decodeBytes(encoded).text, text);
  }
  assert.deepEqual(decodeBytes('/wA='), {encoding: 'base64', text: '/wA='});
  assert.equal(encodeBytes('{"id":9007199254740993}', 'json'), btoa('{"id":9007199254740993}'));
  assert.throws(() => encodeBytes('bad json', 'json'));
  assert.throws(() => encodeBytes('/x==', 'base64'));
});

test('optimistic revision and run identities retain uint64 precision', () => {
  const fields = new Map(Object.entries({id: 'job', runtime: 'steps/v1', handler: 'hash', handler_version: '1', payload: 'Hello', encoding: 'text'}));
  const q = buildMutation('definition', fields, {definition: {revision: '9007199254740993'}}, 'alpha');
  assert.equal(q.put_definition.expected_revision, '9007199254740993');
  assert.equal(integer('18446744073709551615'), '18446744073709551615');
  assert.throws(() => integer('18446744073709551616'));
  assert.throws(() => integer('1e3'));
});

test('once and event triggers use separate activation specifications', () => {
  const fields = new Map(Object.entries({id: 'once', definition_id: 'hash', definition_revision: '1', max_pending: '32', mode: 'once', start_at: '2026-11-11T12:00:00', missed_policy: 'coalesce'}));
  const once = buildMutation('trigger', fields, null, 'alpha').put_trigger;
  assert.equal(once.definition.timing.start_at, '2026-11-11T12:00:00.000Z');
  assert.equal(once.definition.timing.kind, 'once');
  assert.equal(once.expected_revision, '0');
  assert.equal(once.definition.event, undefined);
  fields.set('mode', 'event'); fields.set('source', 'meter-events');
  const event = buildMutation('trigger', fields, null, 'alpha').put_trigger.definition;
  assert.deepEqual(event.event, {source: 'meter-events'});
  assert.equal(event.timing, undefined);
});

test('interval duration stays exact and bounded replay cannot exceed queue capacity', () => {
  const f = new Map(Object.entries({id: 'daily', definition_id: 'job', definition_revision: '1', max_pending: '2', mode: 'interval', start_at: '2026-11-11T12:00', interval: '86400', missed_policy: 'replay', catch_up: '2'}));
  assert.equal(buildMutation('trigger', f, null, 'alpha').put_trigger.definition.timing.interval_ns, '86400000000000');
  f.set('catch_up', '3');
  assert.throws(() => buildMutation('trigger', f, null, 'alpha'));
});

test('controller data is escaped before HTML rendering', () => {
  assert.equal(escapeHTML('<img src=x onerror="alert(1)">'), '&lt;img src=x onerror=&quot;alert(1)&quot;&gt;');
});

test('editing schedules preserves fractional durations and unchanged nanosecond anchors', () => {
  const f = new Map(Object.entries({id: 'precise', definition_id: 'job', definition_revision: '1', max_pending: '2', mode: 'interval', start_at: '2026-11-11T12:00:00.123', interval: durationSeconds('1000000001'), missed_policy: 'coalesce'}));
  const record = {trigger: {revision: '9', definition: {timing: {start_at: '2026-11-11T12:00:00.123456789Z'}}}};
  const q = buildMutation('trigger', f, record, 'alpha').put_trigger;
  assert.equal(q.definition.timing.interval_ns, '1000000001');
  assert.equal(q.definition.timing.start_at, '2026-11-11T12:00:00.123456789Z');
  assert.equal(q.expected_revision, '9');
});

test('compatibility requires exact handler, driver, tenant and runtime on the same worker', () => {
  const required = {tenant_id: 'alpha', runtime: 'steps/v1', handler: 'meter', handler_version: '2', driver: 'mqtt', driver_version: '1'};
  const worker = {tenant_id: 'alpha', runtimes: ['steps/v1'], handlers: [{name: 'meter', version: '2'}], drivers: [{name: 'mqtt', version: '1'}], state: 'draining'};
  assert.equal(workerMatches(worker, required), true, 'code presence is independent of execution admission');
  for (const change of [{tenant_id: 'beta'}, {runtimes: ['other']}, {handlers: [{name: 'meter', version: '1'}]}, {drivers: []}]) {
    assert.equal(workerMatches({...worker, ...change}, required), false);
  }
  const split = [{...worker, drivers: []}, {...worker, handlers: []}];
  assert.equal(split.some(w => workerMatches(w, required)), false);
  assert.equal(workerMatches({...worker, drivers: []}, {...required, driver: ''}), true, 'compute jobs need no driver');
});

test('inventory deduplicates controller aliases and keeps partial observations explicit', () => {
  const first = {origin: 'https://one', inventory: {controller_id: '9007199254740993', instance_id: 'process-1', workers: [{id: 'session-1'}, {id: 'session-2'}]}};
  assert.equal(workerEntries([first, {...first, origin: 'https://alias'}]).length, 2);
  assert.equal(workerEntries([first, {...first, inventory: {...first.inventory, instance_id: 'process-2'}}]).length, 4);
  assert.equal(inventoryPartial([first]), false);
  for (const partial of [{error: 'unreachable'}, {...first, inventory: {...first.inventory, next_page_token: 'more'}}, {...first, inventory: {...first.inventory, workers: [{capabilities_truncated: true}]}}]) {
    assert.equal(inventoryPartial([first, partial]), true);
  }
  assert.equal(inventoryPartial([]), true);
});

test('weighted release policies validate explicit basis points and reject ambiguous routing', async () => {
 const {releaseWeights} = await import('./model.js');
 assert.deepEqual(releaseWeights('{}','v1'),{});
 assert.deepEqual(releaseWeights('{"v1":9000,"v2":1000}'), {v1:9000,v2:1000});
 for(const value of ['[]','null','{"v1":9999}','{"v1":0}','{"v1":10000.5}','{"":10000}']) assert.throws(()=>releaseWeights(value));
 assert.throws(()=>releaseWeights('{"v1":10000}','v2'));
});
