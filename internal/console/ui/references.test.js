import test from 'node:test';
import assert from 'node:assert/strict';
import {ReleaseReferences} from './references.js';
const definition = (id, release, version = '1') => ({reference: {kind: 'DEFINITION', tenant_id: 'alpha', id}, definition: {id, revision: version, worker_release: release}});
const trigger = (id, release, extra = {}) => ({reference: {kind: 'TRIGGER', tenant_id: 'alpha', id}, trigger: {revision: '9007199254740993', definition: {id, definition_id: 'job', definition_revision: '1', paused: true}, job_definition: {id: 'job', revision: '1', worker_release: release}, ...extra}});
const inspector = query => new ReleaseReferences('alpha', 'v1', {query, render() {}, active: () => true});

test('schedule references use their pinned templates independently of current definitions', async () => {
  const view = inspector(async (_, q) => ({value: {records: q.kind === 'DEFINITION' ? [definition('job', 'v2'), definition('ordinary', '')] : [trigger('paused-v1', 'v1'), trigger('completed-v1', 'v1', {completed: true}), trigger('other', 'v2')]}, controller: 'replica'}));
  await view.load();
  assert.equal(view.sources.DEFINITION.rows.size, 0);
  const rows = [...view.sources.TRIGGER.rows.values()];
  assert.deepEqual(rows.map(row => [row.reference.id, row.revision, row.state]), [['paused-v1', '9007199254740993', 'Paused'], ['completed-v1', '9007199254740993', 'Completed']]);
  const html = view.html(ref => ref.id);
  assert.match(html, /All pages read/);
  assert.match(html, /No references observed in these readings/);
  assert.match(html, /job@1/);
  assert.match(html, /Replica reads can lag/);
});

test('bounded pagination and independent retries retain earlier results and exact tenant scope', async () => {
  const calls = []; let failed = false;
  const view = inspector(async (method, q) => {
    calls.push({...q}); assert.equal(method, 'list'); assert.equal(q.tenant_id, 'alpha'); assert.equal(q.page_size, 32); assert.equal(q.worker_release, undefined);
    if (q.kind === 'TRIGGER') return {value: {}};
    if (!q.page_token) return {value: {records: [definition('first', 'v1')], next_page_token: 'opaque-page'}};
    if (!failed) { failed = true; throw new Error('Read unavailable'); }
    return {value: {records: [definition('second', 'v1')]}};
  });
  await view.load();
  assert.match(view.html(ref => ref.id), /Partial inspection/);
  await view.loadPage('DEFINITION');
  assert.equal(view.sources.DEFINITION.next, 'opaque-page');
  assert.equal(view.sources.DEFINITION.rows.size, 1);
  assert.match(view.html(ref => ref.id), /Read unavailable/);
  await view.loadPage('DEFINITION');
  assert.deepEqual([...view.sources.DEFINITION.rows.keys()], ['first', 'second']);
  assert.equal(view.sources.DEFINITION.checked, 2);
  assert.equal(view.sources.DEFINITION.error, '');
  assert.equal(calls.filter(q => q.kind === 'TRIGGER').length, 1);
  await view.loadPage('DEFINITION');
  assert.equal(calls.length, 4);
});

test('scope, identity and pinned-revision mismatches reject a whole page without exposing its records', async () => {
  const badDefinition = definition('foreign', 'v1'); badDefinition.reference.tenant_id = 'beta';
  const badTrigger = trigger('wrong-template', 'v1'); badTrigger.trigger.job_definition.revision = '2';
  const rounded = definition('rounded', 'v1', 9007199254740992);
  for (const bad of [badDefinition, badTrigger, rounded]) {
    const kind = bad.reference.kind;
    const view = inspector(async () => ({value: {records: [kind === 'TRIGGER' ? trigger('good', 'v1') : definition('good', 'v1'), bad], next_page_token: 'next'}}));
    await view.loadPage(kind);
    assert.equal(view.sources[kind].rows.size, 0);
    assert.equal(view.sources[kind].checked, 0);
    assert.equal(view.sources[kind].next, '');
    assert.ok(view.sources[kind].error);
    assert.doesNotMatch(view.html(ref => ref.id), /All pages read/);
  }
});

test('repeated pagination tokens remain partial and do not double count rows', async () => {
  const view = inspector(async (_, q) => ({value: {records: [definition(q.page_token ? 'second' : 'first', 'v1')], next_page_token: 'same'}}));
  await view.loadPage('DEFINITION'); await view.loadPage('DEFINITION');
  assert.equal(view.sources.DEFINITION.checked, 1);
  assert.equal(view.sources.DEFINITION.rows.size, 1);
  assert.match(view.sources.DEFINITION.error, /repeated/);
});

test('late reads cannot populate a closed or replaced release inspection', async () => {
  let done, active = true, renders = 0;
  const view = new ReleaseReferences('alpha', 'v1', {query: () => new Promise(resolve => { done = resolve; }), render() { renders++; }, active: () => active});
  const loading = view.loadPage('DEFINITION');
  active = false;
  done({value: {records: [definition('late', 'v1')]}});
  await loading;
  assert.equal(view.sources.DEFINITION.rows.size, 0);
  assert.equal(renders, 1);
});

test('malformed empty pages cannot establish that a release has no references', async () => {
  for (const value of [null, [], {records: null}, {records: {}}, {next_page_token: null}]) {
    const view = inspector(async () => ({value}));
    await view.loadPage('DEFINITION');
    assert.equal(view.sources.DEFINITION.loaded, false);
    assert.ok(view.sources.DEFINITION.error);
    assert.doesNotMatch(view.html(ref => ref.id), /No references observed/);
  }
});
