import {integer, encodeBytes} from './model.js';

function checkpoint(value, positive = false) {
  if (typeof value !== 'string') throw new Error('Reload the record to obtain an exact checkpoint.');
  return integer(value, positive);
}
function runSnapshot(record) {
  const reference = record?.reference, run = record?.run;
  if (reference?.kind !== 'RUN' || !reference.tenant_id || !reference.id || run?.id !== reference.id || run.status !== 'unknown' || run.owner_id) throw new Error('Open an unknown run with no current execution owner.');
  const step = checkpoint(run.step ?? '0');
  if (step === '18446744073709551615') throw new Error('The run checkpoint cannot advance.');
  return {reference, run, step};
}
function resourceSnapshot(record) {
  const reference = record?.reference, resource = record?.resource;
  if (reference?.kind !== 'RESOURCE' || reference.tenant_id || !reference.id || resource?.spec?.id !== reference.id || resource.state !== 'blocked') throw new Error('Open a blocked resource before confirming quiescence.');
  return {reference, resource, epoch: checkpoint(resource.epoch, true)};
}
export function canResolveRun(identity, record) {
  try {
    const {reference} = runSnapshot(record);
    return identity?.role === 'operator' && (!identity.tenant_id || identity.tenant_id === reference.tenant_id);
  } catch { return false; }
}
export function canResolveResource(identity, record) {
  try { resourceSnapshot(record); return identity?.role === 'operator' && !identity.tenant_id; }
  catch { return false; }
}

export function buildRecoveryMutation(kind, fields, record) {
  const reason = String(fields.get('reason') ?? '').trim();
  if (!reason || new TextEncoder().encode(reason).length > 4096) throw new Error('Provide the evidence and reason within 4096 UTF-8 bytes.');
  if (fields.get('attested') !== 'on') throw new Error('Confirm the independent verification before preparing this recovery.');
  if (kind === 'resolve_run') {
    const {reference, step} = runSnapshot(record);
    const status = String(fields.get('status') ?? '');
    if (!['succeeded', 'failed', 'cancelled'].includes(status)) throw new Error('Choose the independently confirmed terminal outcome.');
    return {tenant_id: reference.tenant_id, resolve_run: {run_id: reference.id, expected_step: step, status, reason, result: encodeBytes(String(fields.get('payload') ?? ''), String(fields.get('encoding') ?? 'text'))}};
  }
  if (kind === 'resolve_resource') {
    const {reference, epoch} = resourceSnapshot(record);
    return {resolve_resource: {resource_id: reference.id, expected_epoch: epoch, quiescent: true, reason}};
  }
  throw new Error('Unsupported recovery action.');
}

function purgeSnapshot(record) {
 const ref=record?.reference, deletion=ref?.kind==='DELETION', value=deletion?record.deletion:record?.run;
 if (!['RUN','DELETION'].includes(ref?.kind) || !ref.tenant_id || !ref.id || !value || (deletion?value.run_id:value.id)!==ref.id || !['succeeded','failed','cancelled'].includes(value.status) || value.owner_id || deletion&&value.cleanup_complete) throw new Error('Open a completed, ownerless run or an unfinished deletion audit.');
 const cutoff=deletion?value.finished_at:value.last_transition;
 if(typeof cutoff!=='string'||!Number.isFinite(Date.parse(cutoff))) throw new Error('Read the exact completion cutoff before deleting history.');
 return {ref,step:checkpoint(value.step??'0'),cutoff};
}
export function canPurgeRun(identity,record) {try {const {ref}=purgeSnapshot(record);return identity?.role==='operator'&&(!identity.tenant_id||identity.tenant_id===ref.tenant_id);} catch {return false;}}
export function buildPurgeMutation(fields,record) {
 const {ref,step,cutoff}=purgeSnapshot(record),reason=String(fields.get('reason')??'').trim();
 if(fields.get('attested')!=='on'||!reason||new TextEncoder().encode(reason).length>4096)throw new Error('Confirm permanent history deletion and provide an audited reason within 4096 UTF-8 bytes.');
 return {tenant_id:ref.tenant_id,purge:{run_id:ref.id,expected_step:step,finished_before:cutoff,batch_size:128,reason}};
}
