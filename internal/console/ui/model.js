export function escapeHTML(value) {
  return String(value ?? '').replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));
}

export function encodeBytes(value, encoding) {
  if (encoding === 'base64') {
    if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) throw new Error('Use padded standard base64 without whitespace.');
    if (btoa(atob(value)) !== value) throw new Error('Base64 must use canonical padding.');
    return value;
  }
  if (encoding === 'json') JSON.parse(value);
  let binary = '';
  for (const byte of new TextEncoder().encode(value)) binary += String.fromCharCode(byte);
  return btoa(binary);
}

export function decodeBytes(value) {
  if (!value) return {encoding: 'text', text: ''};
  try {
    const text = new TextDecoder('utf-8', {fatal: true}).decode(Uint8Array.from(atob(value), c => c.charCodeAt(0)));
    try { JSON.parse(text); return {encoding: 'json', text}; } catch { return {encoding: 'text', text}; }
  } catch { return {encoding: 'base64', text: value}; }
}

export function releaseWeights(text, single = '') {
  if (!String(text || '').trim()) return {};
  let weights;
  try { weights = JSON.parse(text); } catch { throw new Error('Release weights must be a JSON object of basis points.'); }
  if (!weights || typeof weights !== 'object' || Array.isArray(weights) || Object.keys(weights).length > 8) throw new Error('Choose one release or at most eight weighted releases.');
  const entries = Object.entries(weights);
  if(entries.length && single)throw new Error('Choose one release or a weighted release policy.');
  if (entries.length && (entries.some(([id, weight]) => !/^[A-Za-z0-9_.-]{1,64}$/.test(id) || !Number.isInteger(weight) || weight <= 0 || weight > 10000) || entries.reduce((sum, [,weight]) => sum + weight, 0) !== 10000)) throw new Error('Positive release weights must sum to 10000 basis points.');
  return weights;
}

export function integer(value, positive = false) {
  const text = String(value);
  if (!/^(0|[1-9][0-9]*)$/.test(text) || BigInt(text) > 18446744073709551615n || positive && BigInt(text) === 0n) throw new Error('Use a whole number within the unsigned 64-bit range.');
  return text;
}

function nanoseconds(seconds, positive = true) {
  const parts = /^(0|[1-9][0-9]*)(?:\.([0-9]{1,9}))?$/.exec(String(seconds));
  if (!parts) throw new Error('Use seconds with at most nine decimal places.');
  const n = BigInt(parts[1]) * 1000000000n + BigInt((parts[2] || '').padEnd(9, '0'));
  if (positive && n === 0n || n > 9223372036854775807n) throw new Error('Use a positive duration within Go’s supported range.');
  return n.toString();
}

export function durationSeconds(value) {
  if (!value) return '';
  const n = BigInt(value);
  const fraction = (n % 1000000000n).toString().padStart(9, '0').replace(/0+$/, '');
  return (n / 1000000000n).toString() + (fraction ? '.' + fraction : '');
}

export function resourceIDs(value) {
  return [...new Set(value.split(/[,\n]/).map(s => s.trim()).filter(Boolean))];
}

// Keep protobuf uint64 values as decimal strings, including optimistic revisions.
// Parsing JSON input validates it without reserializing or rounding its numbers.
export function buildMutation(kind, fields, record, tenant) {
  const f = name => String(fields.get(name) ?? '').trim();
  const raw = name => String(fields.get(name) ?? '');
  const revision = String(record?.[kind]?.revision ?? '0');
  const q = {tenant_id: tenant};
  const resources = resourceIDs(f('resources'));
  if (kind === 'definition') q.put_definition = {expected_revision: revision, definition: {
    id: f('id'), group_id: f('group_id'), runtime: f('runtime'), handler: f('handler'), handler_version: f('handler_version'),
    binding_id: f('binding_id'), worker_release: f('worker_release'), worker_release_weights: releaseWeights(raw('worker_release_weights'), f('worker_release')), resources, input: encodeBytes(raw('payload'), f('encoding')),
  }};
  else if (kind === 'group') q.put_group = {expected_revision: revision, group: {id: f('id'), runtime: f('runtime'), resources, max_running: integer(f('max_running'))}};
  else if (kind === 'binding') q.put_binding = {expected_revision: revision, binding: {id: f('id'), driver: f('driver'), driver_version: f('driver_version'), resources, config: encodeBytes(raw('payload'), f('encoding'))}};
  else if (kind === 'tenant') q.put_tenant = {expected_revision: revision, max_running: integer(f('max_running'))}, q.tenant_id = f('id');
  else if (kind === 'resource') {
    q.tenant_id = '';
    q.put_resource = {expected_revision: revision, spec: {id: f('id'), duplex: f('duplex'), capacity: 1, channel_id: f('channel_id')}};
  } else if (kind === 'grant') q.put_grant = {expected_revision: revision, resource_id: f('id'), allowed: fields.get('allowed') === 'on'};
  else if (kind === 'launch') q.launch = {definition_id: f('definition_id'), definition_revision: integer(f('definition_revision'), true), run_id: f('run_id')};
  else if (kind === 'cancel') q.cancel = {run_id: f('run_id'), reason: f('reason')};
  else if (kind === 'trigger') {
    const max = Number(integer(f('max_pending'), true));
    if (max > 128) throw new Error('Pending runs per trigger must be in 1..128.');
    const definition = {id: f('id'), definition_id: f('definition_id'), definition_revision: integer(f('definition_revision'), true), max_pending: max, paused: fields.get('paused') === 'on'};
    const mode = f('mode');
    if (mode === 'event') definition.event = {source: f('source')};
    else {
      if (!['once', 'interval', 'cron'].includes(mode)) throw new Error('Choose a supported trigger kind.');
      const start = new Date(f('start_at') + 'Z');
      if (!Number.isFinite(start.getTime())) throw new Error('Choose a valid UTC start time.');
      const originalStart = record?.trigger?.definition?.timing?.start_at;
      const startAt = originalStart && new Date(originalStart).getTime() === start.getTime() ? originalStart : start.toISOString();
      definition.timing = {kind: mode, start_at: startAt, missed_policy: f('missed_policy')};
      if (mode === 'interval') definition.timing.interval_ns = nanoseconds(f('interval'));
      if (mode === 'cron') Object.assign(definition.timing, {cron: f('cron'), time_zone: f('time_zone')});
      if (f('missed_policy') === 'skip') definition.timing.misfire_grace_ns = nanoseconds(f('misfire_grace'));
      if (f('missed_policy') === 'replay') {
        const catchUp = Number(integer(f('catch_up'), true));
        if (catchUp > max) throw new Error('Catch-up limit cannot exceed the trigger’s pending limit.');
        definition.timing.catch_up_limit = catchUp;
      }
    }
    q.put_trigger = {expected_revision: revision, definition};
  } else throw new Error('Unsupported form.');
  return q;
}

export function displayTime(value) {
  if (!value) return 'None';
  const date = new Date(value);
  return Number.isFinite(date.getTime()) ? date.toISOString().replace('T', ' ').replace(/\.\d{3}Z$/, ' UTC') : String(value);
}

// Each entry describes one worker session on one controller process. Aliases
// of a controller are deduplicated; different sessions are never combined.
export function workerEntries(sources) {
  const seen = new Set();
  const entries = [];
  for (const source of sources) {
    if (source.error || !source.inventory) continue;
    const v = source.inventory;
    for (const worker of v.workers || []) {
      const key = JSON.stringify([v.controller_id, v.instance_id, worker.id]);
      if (seen.has(key)) continue;
      seen.add(key);
      entries.push({...worker, origin: source.origin, controller_id: v.controller_id, observed_at: worker.observed_at || v.observed_at});
    }
  }
  return entries;
}

export function workerMatches(worker, required) {
  const has = (versions, name, version) => (versions || []).some(v => v.name === name && v.version === version);
  return (required.worker_release ? worker.deployment_id === required.worker_release : Object.keys(required.worker_release_weights || {}).length ? Object.hasOwn(required.worker_release_weights, worker.deployment_id) : true) && worker.tenant_id === required.tenant_id && (worker.runtimes || []).includes(required.runtime)
    && has(worker.handlers, required.handler, required.handler_version)
    && (!required.driver || has(worker.drivers, required.driver, required.driver_version));
}

export function inventoryPartial(sources) {
  return !sources.length || sources.some(s => s.error || !s.inventory || s.inventory.next_page_token || (s.inventory.workers || []).some(w => w.capabilities_truncated));
}
