import {escapeHTML as e, integer} from './model.js';

export const limitPolicies = {
  limits: {kind: 'LIMITS', usage: 'USAGE', title: 'Run admission & retained data', edit: 'Edit run limits', fields: [
    ['pending_runs', 'Pending runs', 'Ready, running, waiting and unknown runs.'],
    ['stored_runs', 'Stored runs', 'All retained runs, including terminal outcomes.'],
    ['inbox_events', 'Inbox events', 'Retained conversation signals, including consumed events.'],
    ['inbox_bytes', 'Inbox bytes', 'Raw payload bytes of retained conversation signals.'],
    ['activations', 'Event activations', 'Retained accepted event-trigger activations.'],
    ['activation_bytes', 'Activation bytes', 'Raw event payload bytes, counted once per unique activation.'],
  ]},
  configuration_limits: {kind: 'CONFIGURATION_LIMITS', usage: 'CONFIGURATION_USAGE', title: 'Configuration', edit: 'Edit configuration limits', fields: [
    ['groups', 'Groups', 'Current tenant groups.'],
    ['definitions', 'Job definitions', 'Current tenant job definitions.'],
    ['bindings', 'Bindings', 'Current tenant driver bindings.'],
    ['triggers', 'Schedules', 'Current calendar and event triggers, including paused and completed triggers.'],
    ['payload_bytes', 'Configuration payload bytes', 'Raw inputs and binding configs, including the independent copies pinned by each trigger.'],
  ]},
};

// Omitted protobuf scalar fields mean zero only within a successfully read
// record. Missing/failed records remain unknown, never an unlimited policy.
function uint64(value) {
  if (value === undefined) return '0';
  if (typeof value !== 'string' && !(typeof value === 'number' && Number.isSafeInteger(value))) throw new Error('Invalid unsigned 64-bit value.');
  return integer(value);
}

export function validateUsageRecord(record, kind, tenant) {
  const policy = Object.entries(limitPolicies).find(([, p]) => p.kind === kind || p.usage === kind);
  const field = kind.toLowerCase();
  if (!policy || record?.reference?.kind !== kind || record.reference.tenant_id !== tenant
    || record.reference.id && record.reference.id !== tenant || !record[field] || typeof record[field] !== 'object' || Array.isArray(record[field])) {
    throw new Error('The response does not match the requested tenant usage record.');
  }
  const [, spec] = policy;
  const limits = kind === spec.kind;
  if (limits) uint64(record[field].revision);
  for (const [name] of spec.fields) uint64(record[field][limits ? 'max_' + name : name]);
  return record;
}

export function limitRows(key, limits, usage) {
  const spec = limitPolicies[key];
  if (!spec) throw new Error('Unknown limits policy.');
  return spec.fields.map(([name, label, description]) => {
    const used = usage ? uint64(usage[name]) : null;
    const ceiling = limits ? uint64(limits['max_' + name]) : null;
    let comparison = 'Not read', severity = '';
    if (ceiling === '0') comparison = 'Unlimited';
    else if (ceiling !== null && used !== null) {
      const a = BigInt(used), b = BigInt(ceiling);
      comparison = a > b ? 'Above limit' : a === b ? 'At limit' : 'Within limit';
      severity = a >= b ? 'limit-full' : '';
    }
    return {name, label, description, used, ceiling, comparison, severity};
  });
}

export function buildLimitMutation(key, fields, record, tenant) {
  const spec = limitPolicies[key];
  if (!spec) throw new Error('Unknown limits policy.');
  validateUsageRecord(record, spec.kind, tenant);
  const limits = Object.fromEntries(spec.fields.map(([name]) => ['max_' + name, integer(String(fields.get('max_' + name) ?? '').trim())]));
  return {tenant_id: tenant, ['put_' + key]: {expected_revision: uint64(record[key].revision), limits}};
}

export class UsageInspector {
  constructor(tenant, {query, root, active, editable}) {
    this.tenant = tenant; this.query = query; this.root = root; this.active = active; this.editable = editable;
    this.sources = Object.fromEntries(Object.values(limitPolicies).flatMap(p => [p.kind, p.usage]).map(kind => [kind, {record: null, controller: '', error: '', loading: false}]));
  }

  load() { return Promise.all(Object.keys(this.sources).map(kind => this.loadRecord(kind))); }

  async loadRecord(kind) {
    const source = this.sources[kind];
    if (!this.active() || !source || source.loading) return;
    source.loading = true; source.record = null; source.error = ''; source.controller = ''; this.render();
    try {
      const {value, controller} = await this.query('get', {reference: {kind, tenant_id: this.tenant}});
      if (!this.active()) return;
      source.controller = controller;
      source.record = validateUsageRecord(value, kind, this.tenant);
    } catch (err) { if (this.active()) { source.error = err.message; source.controller = err.controller || source.controller; } }
    finally { source.loading = false; if (this.active()) this.render(); }
  }

  policy(key) {
    if (!Object.hasOwn(limitPolicies, key)) throw new Error('Unknown limits policy.');
    const record = this.sources[limitPolicies[key].kind].record;
    if (!record) throw new Error('Read the limits policy before editing it.');
    return record;
  }

  sourceHTML(kind, label) {
    const source = this.sources[kind];
    return `<div><div><strong>${e(label)}</strong><span>${source.loading ? 'Reading…' : source.record ? 'Read' : 'Not read'}</span>${source.controller ? '<p class="hint">Responding controller: <code>' + e(source.controller) + '</code></p>' : ''}${source.error ? '<p class="error" role="alert">' + e(source.error) + '</p>' : ''}</div>${source.error ? `<button type="button" id="retry-${kind}" data-action="usage-retry" data-kind="${kind}">Retry read</button>` : ''}</div>`;
  }

  render() {
    if (!this.active()) return;
    const root = this.root();
    if (!root) return;
    const focusID = root.contains(document.activeElement) ? document.activeElement.id : '';
    root.innerHTML = '<p class="notice">Limits and usage are separate replica reads. Comparisons describe those readings; controllers decide whether to admit each operation.</p>'
      + Object.entries(limitPolicies).map(([key, spec]) => {
        const policy = this.sources[spec.kind].record, usage = this.sources[spec.usage].record;
        const limits = policy?.[key];
        const rows = limitRows(key, limits, usage?.[spec.usage.toLowerCase()]);
        const revision = limits ? uint64(limits.revision) : null;
        return `<section class="usage-section" aria-labelledby="usage-${key}"><div class="usage-heading"><div><h2 id="usage-${key}">${e(spec.title)}</h2><p class="hint">${revision === null ? 'Limits policy not read.' : revision === '0' ? 'No configured policy · revision 0 · all limits unlimited.' : 'Limits revision ' + e(revision)}</p></div>${this.editable ? `<button type="button" data-action="usage-edit" data-key="${key}" ${policy ? '' : 'disabled'}>${e(spec.edit)}</button>` : ''}</div>`
          + `<div class="table-scroll" tabindex="0" role="region" aria-label="${e(spec.title)} usage"><table aria-label="${e(spec.title)}"><thead><tr><th scope="col">Metric</th><th scope="col">Observed use</th><th scope="col">Limit</th><th scope="col">Compared readings</th></tr></thead><tbody>${rows.map(row => `<tr><th scope="row">${e(row.label)}<p class="hint">${e(row.description)}</p></th><td>${e(row.used ?? 'Not read')}</td><td>${e(row.ceiling === '0' ? 'Unlimited' : row.ceiling ?? 'Not read')}</td><td><span class="${row.severity}">${e(row.comparison)}</span></td></tr>`).join('')}</tbody></table></div>`
          + '<div class="usage-sources">' + this.sourceHTML(spec.kind, 'Limits policy') + this.sourceHTML(spec.usage, 'Usage counters') + '</div></section>';
      }).join('')
      + '<p class="hint">Zero means unlimited. Limits cover logical records and raw payload bytes; they do not measure free disk space. Tenant/group concurrency, trigger backlog bounds and shared-resource admission apply separately.</p>'
      + '<p class="hint">Lowering a limit below observed usage restricts growth in that metric. Existing work can still progress. Completion retains run records; consuming a signal retains its inbox bytes; pausing a schedule retains its configuration and pinned payloads.</p>';
    if (focusID) root.querySelector('#' + CSS.escape(focusID))?.focus({preventScroll: true});
  }
}
