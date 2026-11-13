import test from 'node:test';
import assert from 'node:assert/strict';
import {HistoryInspector, historyEntries, mergeHistoryPage, timestampNanos, recordedTime, recordedInterval, runOverview, payloadPanel} from './history.js';

const at = '2026-11-11T12:00:00.123456789Z';
function record(kind, id, fields = {}, tenant = 'alpha', run = 'run-1') {
  const reference = {kind, tenant_id: tenant, run_id: run, ...(kind === 'ACTION' ? {sequence: id} : {id})};
  const value = kind === 'ATTEMPT' ? {id, number: '1', started_at: at, status: 'completed'} : kind === 'ACTION' ? {sequence: id, authorized_at: at, operation: 'request', status: 'unknown'} : {id, at, key: 'reply'};
  return {reference, [kind.toLowerCase()]: {...value, run_id: run, ...fields}};
}
function inspector(query) {
  return new HistoryInspector({reference: {tenant_id: 'alpha'}, run: {id: 'run-1'}}, {query, root: () => null, active: () => true});
}

test('recorded time and intervals preserve nanoseconds, offsets and pre-epoch instants', () => {
  assert.equal(timestampNanos('1969-12-31T23:59:59.999999999Z'), -1n);
  assert.equal(timestampNanos(at), timestampNanos('2026-11-11T13:00:00.123456789+01:00'));
  assert.equal(recordedTime(at), '2026-11-11 12:00:00.123456789 UTC');
  assert.equal(recordedInterval(at, '2026-11-11T12:00:00.123456790Z'), '1 ns');
  assert.equal(recordedInterval(at, '2026-11-11T12:00:00.123458289Z'), '1.5 µs');
  assert.equal(recordedInterval(at, '2026-11-11T12:00:01.123456790Z'), '1.000000001 s');
  assert.equal(recordedInterval(at, ''), 'No recorded interval');
  assert.equal(recordedInterval(at, '2026-11-11T12:00:00.123456788Z'), 'End precedes start in recorded time');
  assert.equal(timestampNanos('not a date'), null);
  assert.equal(timestampNanos('2026-11-11T12:00:00.1234567890Z'), null);
});

test('timeline orders sub-millisecond records and uint64 sequences without inventing transition times', () => {
  const sources = {
    ATTEMPT: {records: [record('ATTEMPT', 'attempt', {started_at: '2026-11-11T12:00:00.123456788Z'})]},
    ACTION: {records: [record('ACTION', '9007199254740993'), record('ACTION', '9007199254740992')]},
    EVENT: {records: [record('EVENT', 'signal', {at: ''})]},
  };
  const entries = historyEntries(sources);
  assert.deepEqual(entries.map(v => v.title), ['Attempt 1', 'Action 9007199254740992', 'Action 9007199254740993', 'Signal signal']);
  assert.equal(entries[1].state, 'unknown');
  assert.equal(entries[1].timeLabel, 'Authorized');
  assert.equal(entries[1].end, undefined, 'unknown does not imply an action completion timestamp');
  assert.equal(entries[3].at, '', 'missing times stay missing');
  assert.equal(historyEntries(sources, 'ACTION').length, 2);
});

test('history pages update duplicates while rejecting cross-run, cross-tenant and rounded identities', () => {
  const first = record('ACTION', '9007199254740993');
  const update = record('ACTION', '9007199254740993', {status: 'succeeded'});
  const next = record('ACTION', '9007199254740994');
  const merged = mergeHistoryPage([first], {records: [update, next]}, 'ACTION', 'alpha', 'run-1');
  assert.equal(merged.length, 2);
  assert.equal(merged[0].action.status, 'succeeded');
  for (const bad of [record('ACTION', '1', {}, 'beta'), record('ACTION', '1', {}, 'alpha', 'other'), record('EVENT', '1'), record('ACTION', 9007199254740993), record('ACTION', '1', {sequence: '2'})]) {
    assert.throws(() => mergeHistoryPage([], {records: [bad]}, 'ACTION', 'alpha', 'run-1'));
  }
  for (const bad of [false, [], {records: false}, {records: Array(33).fill(first)}, {next_page_token: false}, {next_page_token: 0}, {records: [], next_page_token: 'x'.repeat(4097)}]) {
    assert.throws(() => mergeHistoryPage([], bad, 'ACTION', 'alpha', 'run-1'));
  }
  assert.deepEqual(mergeHistoryPage([], {}, 'ACTION', 'alpha', 'run-1'), []);
});

test('failed continuation retains loaded history and retries the same page explicitly', async () => {
  let calls = 0;
  const pages = [];
  const h = inspector(async (method, query) => {
    assert.equal(method, 'list'); pages.push(query.page_token);
    if (++calls === 2) throw new Error('temporary controller failure');
    return {controller: 'https://member-1', value: {records: [record('EVENT', calls === 1 ? 'first' : 'second')], next_page_token: calls === 1 ? 'cursor' : ''}};
  });
  await h.loadPage('EVENT');
  await h.loadPage('EVENT', true);
  assert.equal(h.sources.EVENT.records.length, 1);
  assert.equal(h.sources.EVENT.next, 'cursor');
  assert.match(h.listHTML(), /History is partial/);
  assert.match(h.listHTML(), /temporary controller failure/);
  await h.loadPage('EVENT', true);
  assert.equal(h.sources.EVENT.records.length, 2);
  assert.equal(h.sources.EVENT.next, '');
  assert.deepEqual(pages, ['', 'cursor', 'cursor']);
});

test('late record responses cannot restore a closed selection or replace another run', async () => {
  let resolve;
  const h = inspector(() => new Promise(done => { resolve = done; }));
  const r = record('ACTION', '1');
  const pending = h.select(r);
  h.handle('history-back', {dataset: {}});
  resolve({value: r, controller: 'https://member-1'});
  await pending;
  assert.equal(h.selected, null);
  let active = true;
  h.active = () => active;
  const closed = h.select(r);
  active = false;
  resolve({value: r, controller: 'https://member-1'});
  await closed;
  assert.equal(h.selected.full, false);
});

test('summary payloads, uncertain actions and untrusted record text retain their meaning', () => {
  const run = {definition: {id: 'job', revision: '9007199254740993', handler: 'handler', handler_version: '1'}, status: 'unknown', summary: true, error: '<img src=x onerror=alert(1)>'};
  const overview = runOverview(run);
  assert.match(overview, /external outcome is uncertain/);
  assert.match(overview, /Payloads are omitted/);
  assert.doesNotMatch(overview, /<summary>Checkpoint state|<summary>Result|<img/);
  const h = inspector(async () => {});
  h.selected = {record: record('ACTION', '1', {error: '<script>bad()</script>', attempt_id: 'attempt-1'}), full: true};
  const detail = h.detailHTML();
  assert.match(detail, /uncertain external outcome/);
  assert.match(detail, /Open owning attempt/);
  assert.match(detail, /Action result/);
  assert.doesNotMatch(detail, /<script>/);
  assert.match(payloadPanel('Signal data', '/wA='), /base64/);
});
