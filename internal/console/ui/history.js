import {escapeHTML as e, decodeBytes} from './model.js';

export const historyKinds = ['ATTEMPT', 'ACTION', 'EVENT'];
const labels = {TIMELINE: 'Timeline', ATTEMPT: 'Attempts', ACTION: 'Actions', EVENT: 'Signals'};
const singular = {ATTEMPT: 'Attempt', ACTION: 'Action', EVENT: 'Signal'};
const code = value => `<code>${e(value || 'None')}</code>`;
const button = (action, label, attributes = '') => `<button type="button" data-action="${action}" ${attributes}>${e(label)}</button>`;
const facts = values => '<dl class="facts">' + values.map(([label, value]) => `<div><dt>${e(label)}</dt><dd>${value}</dd></div>`).join('') + '</dl>';
const json = value => JSON.stringify(value, null, 2);
const badge = value => `<span class="status ${['succeeded', 'failed', 'unknown', 'running', 'waiting', 'completed', 'cancelled'].includes(value) ? value : ''}">${e(value || 'Not recorded')}</span>`;

function parsedTime(value) {
  if (typeof value !== 'string') return null;
  const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$/.exec(value);
  if (!match) return null;
  const milliseconds = Date.parse(match[1] + match[3]);
  return Number.isFinite(milliseconds) ? {milliseconds, fraction: match[2] || ''} : null;
}

export function timestampNanos(value) {
  const parsed = parsedTime(value);
  return parsed ? BigInt(parsed.milliseconds) * 1000000n + BigInt(parsed.fraction.padEnd(9, '0')) : null;
}

export function recordedTime(value) {
  const parsed = parsedTime(value);
  if (!parsed) return value ? String(value) : 'Not recorded';
  return new Date(parsed.milliseconds).toISOString().slice(0, 19).replace('T', ' ') + (parsed.fraction ? '.' + parsed.fraction : '') + ' UTC';
}

export function recordedInterval(start, end) {
  const a = timestampNanos(start), b = timestampNanos(end);
  if (a === null || b === null) return 'No recorded interval';
  if (b < a) return 'End precedes start in recorded time';
  const n = b - a;
  for (const [unit, scale, digits] of [['s', 1000000000n, 9], ['ms', 1000000n, 6], ['µs', 1000n, 3], ['ns', 1n, 0]]) {
    if (n < scale && scale !== 1n) continue;
    const fraction = digits ? (n % scale).toString().padStart(digits, '0').replace(/0+$/, '') : '';
    return (n / scale).toString() + (fraction ? '.' + fraction : '') + ' ' + unit;
  }
}

function uint(value) {
  const text = String(value ?? '');
  if (typeof value === 'number' && !Number.isSafeInteger(value) || text.length > 20 || !/^(0|[1-9][0-9]*)$/.test(text) || BigInt(text) > 18446744073709551615n) throw new Error('Invalid history sequence.');
  return text;
}
function recordID(record) {
  if (record.reference.kind === 'ACTION') return uint(record.reference.sequence);
  const id = record.reference.id;
  if (typeof id !== 'string' || !id) throw new Error('Invalid history record ID.');
  return id;
}
function checkRecords(records, kind, tenant, run) {
  if (!Array.isArray(records) || records.length > 32) throw new Error('Invalid history page.');
  for (const record of records) {
    const ref = record?.reference, value = record?.[kind.toLowerCase()];
    if (!ref || ref.kind !== kind || ref.tenant_id !== tenant || ref.run_id !== run || !value || value.run_id !== run || !recordID(record)) throw new Error('History response does not match this run.');
    if (kind === 'ACTION' && (uint(value.sequence) !== recordID(record) || recordID(record) === '0')) throw new Error('Action identity does not match its record.');
    if (kind !== 'ACTION' && value.id !== recordID(record)) throw new Error('History identity does not match its record.');
    if (kind === 'ATTEMPT') uint(value.number);
  }
}

export function mergeHistoryPage(previous, page, kind, tenant, run) {
  if (!page || typeof page !== 'object' || Array.isArray(page)) throw new Error('Invalid history page.');
  const records = page.records ?? [];
  checkRecords(records, kind, tenant, run);
  if (typeof (page.next_page_token ?? '') !== 'string' || (page.next_page_token ?? '').length > 4096) throw new Error('Invalid history continuation token.');
  const merged = new Map(previous.map(record => [recordID(record), record]));
  for (const record of records) merged.set(recordID(record), record);
  return [...merged.values()];
}

function describe(record) {
  const kind = record.reference.kind, value = record[kind.toLowerCase()];
  if (kind === 'ATTEMPT') return {title: 'Attempt ' + value.number, at: value.started_at, timeLabel: 'Started', end: value.finished_at, state: value.status, context: value.owner_id};
  if (kind === 'ACTION') return {title: 'Action ' + value.sequence, at: value.authorized_at, timeLabel: 'Authorized', end: value.finished_at, state: value.status, context: value.operation};
  return {title: 'Signal ' + value.id, at: value.at, timeLabel: 'Recorded', end: value.consumed_at, state: value.consumed_at ? 'consumed' : 'queued', context: value.key};
}

export function historyEntries(sources, view = 'TIMELINE') {
  const entries = [];
  for (const kind of historyKinds) {
    if (view !== 'TIMELINE' && view !== kind) continue;
    (sources[kind]?.records || []).forEach((record, index) => entries.push({kind, index, record, ...describe(record)}));
  }
  return entries.sort((a, b) => {
    const at = timestampNanos(a.at), bt = timestampNanos(b.at);
    if (at !== bt) return at === null ? 1 : bt === null ? -1 : at < bt ? -1 : 1;
    if (a.kind !== b.kind) return historyKinds.indexOf(a.kind) - historyKinds.indexOf(b.kind);
    if (a.kind === 'ACTION' || a.kind === 'ATTEMPT') {
      const field = a.kind === 'ACTION' ? 'sequence' : 'number';
      const av = BigInt(uint(a.record[a.kind.toLowerCase()][field])), bv = BigInt(uint(b.record[b.kind.toLowerCase()][field]));
      if (av !== bv) return av < bv ? -1 : 1;
    }
    return String(recordID(a.record)).localeCompare(String(recordID(b.record)), 'en');
  });
}

export function payloadPanel(label, value) {
  const decoded = decodeBytes(value);
  return `<details><summary>${e(label)} · ${e(decoded.encoding)}</summary><pre>${e(decoded.text || '(empty)')}</pre></details>`;
}

export function runOverview(run) {
  const explanation = {
    ready: 'Waiting for an eligible worker claim. Code versions, quotas and resource admission can affect execution.',
    running: 'A worker owns the recorded attempt. Inspect action records for external operation outcomes.',
    waiting: 'The run is waiting for a matching signal or its recorded wake time. Resource reservations may remain held.',
    unknown: 'The external outcome is uncertain. Inspect action history and follow operator recovery procedures before releasing resources.',
    succeeded: 'The run recorded successful completion.', failed: 'The run recorded a failure.', cancelled: 'The run recorded cancellation.',
  }[run.status] || 'Inspect the recorded state and execution history.';
  let content = `<p class="${run.status === 'unknown' ? 'warning' : 'notice'}">${badge(run.status)} ${e(explanation)}</p>`;
  if (run.error) content += `<section class="run-error" aria-label="Recorded run error"><h3>Recorded error</h3><pre>${e(run.error)}</pre></section>`;
  if (run.summary) content += '<p class="notice">Payloads are omitted from this run summary. Empty fields do not establish an empty input, checkpoint or result.</p>';
  content += facts([
    ['Job revision', code(run.definition.id + '@' + run.definition.revision)], ['Runtime', code(run.runtime)],
    ['Attempt count', e(run.attempt_count)], ['Action count', e(run.action_count)], ['Step', e(run.step)],
    ['Worker release', code(run.definition?.worker_release || 'Capability routing')], ['Required resources', e((run.resources || []).join(', ') || 'None')], ['Created', e(recordedTime(run.created_at))], ['Last transition', e(recordedTime(run.last_transition))],
  ]);
  if (run.status === 'waiting' || run.wait_key || run.wake_at) content += '<h3>Wait state</h3>' + facts([['Signal key', code(run.wait_key)], ['Wake at', e(recordedTime(run.wake_at))]]);
  if (run.cancel_requested) content += '<p class="warning">Cancellation requested' + (run.cancel_reason ? ': ' + e(run.cancel_reason) : '.') + ' The request does not establish physical quiescence.</p>';
  content += '<details><summary>Execution ownership</summary>' + facts([['Current attempt', code(run.attempt_id)], ['Last attempt', code(run.last_attempt_id)], ['Owner', code(run.owner_id)], ['Lease until', e(recordedTime(run.lease_until))], ['Pending action sequence', code(run.pending_action_sequence === '0' ? '' : run.pending_action_sequence)]]) + '</details>';
  const origin = run.activation ? 'Event activation' : run.trigger_id ? 'Scheduled occurrence' : 'Direct launch';
  content += '<details><summary>Launch context · ' + e(origin) + '</summary>' + facts([['Trigger', code(run.trigger_id ? run.trigger_id + '@' + run.trigger_revision : '')], ['Scheduled at', e(recordedTime(run.scheduled_at))]]);
  if (run.activation) {
    const a = run.activation;
    content += facts([['Source', code(a.source)], ['Event ID', code(a.event_id)], ['Occurred at', e(recordedTime(a.occurred_at))], ['Accepted at', e(recordedTime(a.accepted_at))]]);
    if (!run.summary && !a.summary && !a.data_deleted) content += payloadPanel('Activation data', a.data);
    else content += '<p class="hint">Activation payload is omitted or deleted.</p>';
  }
  content += '</details><details><summary>Pinned execution configuration</summary>' + facts([
    ['Handler', code(run.definition.handler + '@' + run.definition.handler_version)], ['Group revision', code(run.group ? run.group.id + '@' + run.group.revision : '')],
    ['Binding revision', code(run.binding ? run.binding.id + '@' + run.binding.revision : '')], ['Driver', code(run.binding ? run.binding.driver + '@' + run.binding.driver_version : '')],
  ]);
  if (run.summary) content += '<p class="hint">Pinned payloads are omitted from this summary.</p>';
  else content += payloadPanel('Job input', run.definition.input) + (run.binding ? payloadPanel('Binding configuration', run.binding.config) : '');
  content += '</details>';
  if (!run.summary) content += payloadPanel('Checkpoint state', run.state) + payloadPanel('Result', run.result);
  return content;
}

export class HistoryInspector {
  constructor(record, {query, root, active}) {
    this.tenant = record.reference.tenant_id; this.run = record.run.id;
    this.query = query; this.root = root; this.active = active; this.view = 'TIMELINE'; this.selected = null;
    this.sources = Object.fromEntries(historyKinds.map(kind => [kind, {records: [], next: '', loading: false, loaded: false, error: '', controller: ''}]));
  }

  load() { return Promise.all(historyKinds.map(kind => this.loadPage(kind))); }

  async loadPage(kind, append = false) {
    if (!historyKinds.includes(kind)) return;
    const source = this.sources[kind];
    if (!this.active() || !source || source.loading || append && !source.next) return;
    source.loading = true; source.error = ''; this.render();
    try {
      const {value, controller} = await this.query('list', {kind, tenant_id: this.tenant, run_id: this.run, page_size: 32, page_token: append ? source.next : ''});
      if (!this.active()) return;
      const records = mergeHistoryPage(append ? source.records : [], value, kind, this.tenant, this.run);
      if (append && value.next_page_token && value.next_page_token === source.next) throw new Error('History page did not advance. Refresh the run to start again.');
      source.records = records; source.next = value.next_page_token || ''; source.loaded = true; source.controller = controller;
    } catch (err) { if (this.active()) { source.error = err.message; source.controller = err.controller || source.controller; } }
    finally { source.loading = false; if (this.active()) this.render(); }
  }

  async select(record) {
    if (!this.active()) return;
    const selection = this.selected = {record, full: false, loading: true, error: '', controller: ''};
    this.render('history-record-title');
    try {
      const {value, controller} = await this.query('get', {reference: record.reference});
      if (!this.active() || this.selected !== selection) return;
      checkRecords([value], record.reference.kind, this.tenant, this.run);
      if (recordID(record) !== recordID(value)) throw new Error('History record identity does not match the request.');
      selection.record = value; selection.full = true; selection.controller = controller;
    } catch (err) { if (this.active() && this.selected === selection) selection.error = err.message; }
    finally { selection.loading = false; if (this.active() && this.selected === selection) this.render(); }
  }

  handle(action, node) {
    if (action === 'history-view' && Object.hasOwn(labels, node.dataset.kind)) {
      this.view = node.dataset.kind; this.selected = null; this.render();
    }
    if (action === 'history-page') return this.loadPage(node.dataset.kind, !!this.sources[node.dataset.kind]?.loaded);
    if (action === 'history-record') {
      const record = this.sources[node.dataset.kind]?.records[Number(node.dataset.index)];
      if (record) return this.select(record);
    }
    if (action === 'history-back') { this.selected = null; this.render('history-heading'); }
    if (action === 'history-record-refresh' && this.selected && !this.selected.loading) return this.select(this.selected.record);
    if (action === 'history-related-attempt') {
      const id = this.selected?.record.action?.attempt_id;
      if (id) return this.select({reference: {kind: 'ATTEMPT', tenant_id: this.tenant, run_id: this.run, id}, attempt: {id, run_id: this.run}});
    }
  }

  render(focusID) {
    if (!this.active()) return;
    const target = this.root();
    if (!target) return;
    const navigate = !!focusID;
    const focused = target.contains(document.activeElement) ? document.activeElement : null;
    const focusedAction = focused?.dataset.action, focusedKind = focused?.dataset.kind, focusedIndex = focused?.dataset.index;
    focusID ||= focused?.id;
    target.innerHTML = this.selected ? this.detailHTML() : this.listHTML();
    if (focusID) {
      const heading = target.querySelector('#' + CSS.escape(focusID));
      heading?.focus({preventScroll: true});
      if (navigate && heading) {
        const dialog = target.closest('dialog'), header = dialog?.querySelector('.dialog-heading');
        if (dialog && header) dialog.scrollTop += heading.getBoundingClientRect().top - dialog.getBoundingClientRect().top - header.getBoundingClientRect().height - 16;
      }
    }
    else if (focusedAction) [...target.querySelectorAll('[data-action]')].find(node => node.dataset.action === focusedAction && node.dataset.kind === focusedKind && node.dataset.index === focusedIndex)?.focus({preventScroll: true});
  }

  listHTML() {
    const entries = historyEntries(this.sources, this.view);
    const sources = Object.entries(this.sources), loading = sources.some(([, s]) => s.loading);
    const partial = sources.some(([, s]) => !s.loaded || s.error || s.next);
    let content = '<h3 id="history-heading" tabindex="-1">Execution history</h3><p class="hint">Loaded records ordered by recorded time. Each entry shows its observed state. Replica reads may lag; later pages can add earlier entries.</p>';
    content += '<div class="actions history-views" aria-label="History views">' + Object.entries(labels).map(([kind, label]) => button('history-view', label, `data-kind="${kind}" aria-pressed="${this.view === kind}"`)).join('') + '</div>';
    if (partial && !loading) content += '<p class="warning">History is partial. Load remaining pages or retry failed sources below.</p>';
    if (this.view === 'TIMELINE') content += `<ol class="history-timeline" aria-label="Loaded execution timeline">${entries.map(entry => `<li><div><span class="history-time-label">${e(entry.timeLabel)}</span><time>${e(recordedTime(entry.at))}</time></div><div>${button('history-record', entry.title, `data-kind="${entry.kind}" data-index="${entry.index}" class="link"`)} ${badge(entry.state)}<p>${code(entry.context)}</p>${entry.end ? '<p class="hint">' + e(entry.kind === 'EVENT' ? 'Consumed' : 'Finished') + ': ' + e(recordedTime(entry.end)) + '</p>' : ''}</div></li>`).join('')}</ol>`;
    else content += `<div class="table-scroll" tabindex="0" role="region" aria-label="Scrollable ${e(labels[this.view].toLowerCase())}"><table aria-label="${e(singular[this.view])} records"><thead><tr><th scope="col">Record</th><th scope="col">Observed state</th><th scope="col">${e(this.view === 'ATTEMPT' ? 'Owner' : this.view === 'ACTION' ? 'Operation' : 'Signal key')}</th><th scope="col">Recorded interval</th></tr></thead><tbody>${entries.map(entry => `<tr><th scope="row">${button('history-record', entry.title, `data-kind="${entry.kind}" data-index="${entry.index}" class="link"`)}</th><td>${badge(entry.state)}</td><td>${code(entry.context)}</td><td>${e(recordedInterval(entry.at, entry.end))}</td></tr>`).join('') || '<tr><td colspan="4">No loaded records in this view.</td></tr>'}</tbody></table></div>`;
    if (!entries.length && this.view === 'TIMELINE') content += '<p class="hint">' + (loading ? 'Loading records…' : partial ? 'Available reads cannot establish the complete history.' : 'No attempt, action or signal records were returned in these reads.') + '</p>';
    return content + '<div class="history-sources" aria-label="History sources">' + sources.map(([kind, s]) => `<div><div><strong>${e(labels[kind])}</strong><span>${s.loading ? 'Loading…' : s.records.length + ' loaded'}</span>${s.controller ? '<span class="history-controller">Last response: ' + code(s.controller) + '</span>' : ''}${s.error ? '<p class="source-error" role="alert">' + e(s.error) + '</p>' : ''}</div>${s.next || s.error ? button('history-page', s.error ? 'Retry ' + labels[kind].toLowerCase() : 'Load more ' + labels[kind].toLowerCase(), `data-kind="${kind}" aria-disabled="${s.loading}"`) : ''}</div>`).join('') + '</div>';
  }

  detailHTML() {
    const selection = this.selected, record = selection.record, kind = record.reference.kind, value = record[kind.toLowerCase()];
    const description = kind === 'ATTEMPT' && !value.number ? {title: 'Attempt ' + record.reference.id} : describe(record);
    let content = '<div class="detail-actions">' + button('history-back', 'Back to history') + button('history-record-refresh', selection.loading ? 'Loading…' : 'Refresh record', selection.loading ? 'disabled' : '') + '</div><h3 id="history-record-title" tabindex="-1">' + e(description.title) + '</h3>';
    if (selection.error) content += '<p class="error" role="alert">' + e(selection.error) + '</p><p class="hint">The current record could not be fetched. The retained record context is shown below.</p>';
    if (selection.loading) content += '<p class="hint" role="status">Loading current record…</p>';
    content += facts([['Run', code(this.run)], ['Tenant', code(this.tenant)], ['Observed state', badge(value.status || (kind === 'EVENT' ? value.consumed_at ? 'consumed' : 'queued' : ''))]]);
    if (kind === 'ATTEMPT') content += facts([['Attempt ID', code(value.id)], ['Number', e(value.number || 'Not recorded')], ['Owner', code(value.owner_id)], ['Started', e(recordedTime(value.started_at))], ['Finished', e(recordedTime(value.finished_at))], ['Recorded interval', e(recordedInterval(value.started_at, value.finished_at))]]);
    if (kind === 'ACTION') {
      content += facts([['Sequence', code(value.sequence)], ['Operation', code(value.operation)], ['Attempt', code(value.attempt_id)], ['Authorized', e(recordedTime(value.authorized_at))], ['Finished', e(recordedTime(value.finished_at))], ['Recorded interval', e(recordedInterval(value.authorized_at, value.finished_at))]]);
      if (value.attempt_id) content += button('history-related-attempt', 'Open owning attempt');
      if (value.status === 'unknown') content += '<p class="warning">This action has an uncertain external outcome. Authorization and elapsed time do not establish completion.</p>';
    }
    if (kind === 'EVENT') content += facts([['Signal ID', code(value.id)], ['Signal key', code(value.key)], ['Recorded', e(recordedTime(value.at))], ['Consumed', e(recordedTime(value.consumed_at))], ['Recorded interval', e(recordedInterval(value.at, value.consumed_at))]]);
    if (value.error) content += '<h4>Recorded error</h4><pre>' + e(value.error) + '</pre>';
    if (selection.full) for (const [field, label] of kind === 'ACTION' ? [['input', 'Action input'], ['result', 'Action result']] : kind === 'EVENT' ? [['data', 'Signal data']] : []) content += payloadPanel(label, value[field]);
    if (selection.controller) content += '<p class="hint">Read from ' + code(selection.controller) + '. Intervals reflect recorded timestamps.</p>';
    return content + '<details><summary>' + (selection.full ? 'Raw record' : 'Retained record context') + '</summary><pre>' + e(json(record)) + '</pre></details>';
  }
}
