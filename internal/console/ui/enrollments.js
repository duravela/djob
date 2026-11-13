import {integer} from './model.js';

export function enrollmentRecord(value, tenant, id) {
  if (!value || value.tenant_id !== tenant || typeof value.worker_id !== 'string' || !/^[A-Za-z0-9_.-]{1,64}$/.test(value.worker_id)
    || id && value.worker_id !== id || typeof value.subject !== 'string' || !/^[!-~]{1,128}$/.test(value.subject)
    || !['enabled', 'draining', 'revoked'].includes(value.state) || typeof value.revision !== 'string' || value.token || value.credential_hash) {
    throw new Error('Controller enrollment does not match the requested worker identity.');
  }
  integer(value.revision, true);
  return {reference: {kind: 'WORKER_ENROLLMENT', tenant_id: tenant, id: value.worker_id}, worker_enrollment: value};
}

export function buildEnrollmentMutation(fields, record, tenant, action) {
  if (!['enroll', 'rotate', 'drain', 'resume', 'revoke'].includes(action)) throw new Error('Unknown worker enrollment action.');
  const f = name => String(fields.get(name) ?? '').trim();
  const q = {tenant_id: tenant, worker_id: record?.worker_enrollment.worker_id || f('worker_id'), action, expected_revision: record ? integer(record.worker_enrollment.revision, true) : '0', reason: f('reason')};
  if (!/^[A-Za-z0-9_.-]{1,64}$/.test(q.worker_id)) throw new Error('Use a worker ID of 1 to 64 letters, digits, dots, underscores or hyphens.');
  if (action === 'enroll') {
    if (record) throw new Error('A worker identity can only be enrolled once.');
    q.subject = f('subject');
    if (!/^[!-~]{1,128}$/.test(q.subject)) throw new Error('Use an audit subject of 1 to 128 visible ASCII characters.');
  } else {
    enrollmentRecord(record?.worker_enrollment, tenant, q.worker_id);
    if (record.worker_enrollment.state === 'revoked') throw new Error('Revoked worker identities cannot be reused.');
  }
  if (!q.reason || new TextEncoder().encode(q.reason).length > 1024) throw new Error('Provide a reason of at most 1024 UTF-8 bytes.');
  return {worker_enrollment: q};
}
