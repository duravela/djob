import test from 'node:test';
import assert from 'node:assert/strict';
import {enrollmentRecord, buildEnrollmentMutation} from './enrollments.js';

const value = {tenant_id: 'alpha', worker_id: 'one', subject: 'company/deployment', revision: '9007199254740993', state: 'enabled'};

test('worker enrollment reads validate tenant, identity, state and exact revisions', () => {
  assert.equal(enrollmentRecord(value, 'alpha', 'one').worker_enrollment.revision, '9007199254740993');
  for (const changed of [{tenant_id: 'beta'}, {worker_id: 'two'}, {state: 'online'}, {revision: 2}, {revision: '18446744073709551616'}, {credential_hash: 'secret'}, {token: 'secret'}]) assert.throws(() => enrollmentRecord({...value, ...changed}, 'alpha', 'one'));
});

test('enrollment actions retain read revisions and generate no browser credential', () => {
  const fields = new Map([['worker_id', 'new-worker'], ['subject', 'company/deployment'], ['reason', 'Roll out private handler']]);
  const creating = buildEnrollmentMutation(fields, null, 'alpha', 'enroll').worker_enrollment;
  assert.equal(creating.expected_revision, '0'); assert.equal(creating.subject, 'company/deployment');
  assert.equal(creating.credential_hash, undefined); assert.equal(creating.token, undefined);
  const record = enrollmentRecord(value, 'alpha');
  for (const action of ['rotate', 'drain', 'resume', 'revoke']) {
    const request = buildEnrollmentMutation(fields, record, 'alpha', action).worker_enrollment;
    assert.equal(request.expected_revision, value.revision); assert.equal(request.worker_id, 'one'); assert.equal(request.action, action);
    assert.equal(request.subject, undefined); assert.equal(request.credential_hash, undefined);
  }
  assert.throws(() => buildEnrollmentMutation(fields, enrollmentRecord({...value, state: 'revoked'}, 'alpha'), 'alpha', 'resume'));
  assert.throws(() => buildEnrollmentMutation(fields, record, 'beta', 'drain'));
  fields.set('worker_id', 'worker/invalid'); assert.throws(() => buildEnrollmentMutation(fields, null, 'alpha', 'enroll'));
});
