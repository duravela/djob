import test from 'node:test';
import assert from 'node:assert/strict';
import {canResolveRun, canResolveResource, buildRecoveryMutation} from './recovery.js';

const run = {reference: {kind: 'RUN', tenant_id: 'alpha', id: 'job'}, run: {id: 'job', status: 'unknown', step: '9007199254740993'}};
const resource = {reference: {kind: 'RESOURCE', id: 'bus'}, resource: {spec: {id: 'bus'}, state: 'blocked', epoch: '18446744073709551615'}};
const platform = {role: 'operator'}, operator = {role: 'operator', tenant_id: 'alpha'};
const fields = (extra = {}) => new Map(Object.entries({reason: ' Independently reconciled ', attested: 'on', status: 'failed', ...extra}));

test('recovery actions require the authorized operator and an eligible exact snapshot', () => {
  for (const role of ['reader', 'writer']) {
    assert.equal(canResolveRun({role}, run), false);
    assert.equal(canResolveResource({role}, resource), false);
  }
  assert.equal(canResolveRun(platform, run), true);
  assert.equal(canResolveRun(operator, run), true);
  assert.equal(canResolveRun({...operator, tenant_id: 'beta'}, run), false);
  assert.equal(canResolveResource(operator, resource), false);
  assert.equal(canResolveResource(platform, resource), true);
  for (const patch of [{step: 9007199254740992}, {step: '18446744073709551615'}, {status: 'running'}, {owner_id: 'worker'}, {id: 'other'}]) {
    assert.equal(canResolveRun(platform, {...run, run: {...run.run, ...patch}}), false);
  }
  for (const patch of [{epoch: 1}, {epoch: '0'}, {state: 'owned'}, {spec: {id: 'other'}}]) {
    assert.equal(canResolveResource(platform, {...resource, resource: {...resource.resource, ...patch}}), false);
  }
});

test('review binds the retained snapshot and byte result, ignoring edited identity and checkpoint fields', () => {
  const q = buildRecoveryMutation('resolve_run', fields({run_id: 'other', expected_step: '8', tenant_id: 'beta', encoding: 'json', payload: '{"n":9007199254740993}'}), run);
  assert.deepEqual(q, {tenant_id: 'alpha', resolve_run: {run_id: 'job', expected_step: '9007199254740993', status: 'failed', reason: 'Independently reconciled', result: Buffer.from('{"n":9007199254740993}').toString('base64')}});
  assert.deepEqual(buildRecoveryMutation('resolve_resource', fields({resource_id: 'other', expected_epoch: '2'}), resource), {resolve_resource: {resource_id: 'bus', expected_epoch: '18446744073709551615', quiescent: true, reason: 'Independently reconciled'}});
  const zero = {...run, run: {...run.run}}; delete zero.run.step;
  assert.equal(buildRecoveryMutation('resolve_run', fields(), zero).resolve_run.expected_step, '0');
});

test('recovery requires independent attestation, a terminal outcome and a bounded UTF-8 reason', () => {
  for (const extra of [{attested: ''}, {reason: ' '}, {reason: '界'.repeat(1366)}, {status: 'unknown'}, {encoding: 'json', payload: '{'}, {encoding: 'base64', payload: '??'}]) {
    assert.throws(() => buildRecoveryMutation('resolve_run', fields(extra), run));
  }
  assert.throws(() => buildRecoveryMutation('resolve_resource', fields({attested: ''}), resource));
  assert.equal(buildRecoveryMutation('resolve_resource', fields({reason: '界'.repeat(1365)}), resource).resolve_resource.reason.length, 1365);
});

test('history deletion binds exact known terminal checkpoint and completion cutoff', async()=>{
 const {canPurgeRun,buildPurgeMutation}=await import('./recovery.js');
 const record={reference:{kind:'RUN',tenant_id:'alpha',id:'done'},run:{id:'done',status:'failed',step:'9007199254740993',last_transition:'2026-11-12T12:00:00.123456789Z'}};
 assert.equal(canPurgeRun(operator,record),true);assert.equal(canPurgeRun({role:'writer',tenant_id:'alpha'},record),false);
 const q=buildPurgeMutation(fields(),record);assert.equal(q.purge.expected_step,'9007199254740993');assert.equal(q.purge.finished_before,record.run.last_transition);assert.equal(q.purge.batch_size,128);
 assert.equal(canPurgeRun(platform,{...record,run:{...record.run,status:'unknown'}}),false);
 assert.equal(canPurgeRun(platform,{...record,run:{...record.run,owner_id:'still-cleaning'}}),false);
 assert.throws(()=>buildPurgeMutation(fields({attested:''}),record));
 const audit={reference:{kind:'DELETION',tenant_id:'alpha',id:'done'},deletion:{run_id:'done',status:'failed',step:'9007199254740993',finished_at:record.run.last_transition}};
 assert.equal(canPurgeRun(platform,audit),true);assert.equal(canPurgeRun(platform,{...audit,deletion:{...audit.deletion,cleanup_complete:true}}),false);
});
