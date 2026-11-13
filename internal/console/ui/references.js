import {escapeHTML as e, integer, releaseWeights} from './model.js';

const kinds = {DEFINITION: 'Jobs', TRIGGER: 'Schedules'};
function revision(value) {
  if (typeof value !== 'string') throw new Error('The reference has no exact revision.');
  return integer(value, true);
}
function referenceRow(record, kind, tenant) {
  const ref = record?.reference, value = record?.[kind === 'DEFINITION' ? 'definition' : 'trigger'];
  if (ref?.kind !== kind || ref.tenant_id !== tenant || typeof ref.id !== 'string' || !ref.id || !value) throw new Error('A release reference does not match the requested scope.');
  const template = kind === 'DEFINITION' ? value : value.job_definition;
  const id = kind === 'DEFINITION' ? value.id : value.definition?.id;
  if (id !== ref.id || !template || typeof template.id !== 'string' || !template.id
    || template.worker_release !== undefined && typeof template.worker_release !== 'string') throw new Error('A release reference is missing its pinned definition.');
  const version = revision(value.revision), definitionRevision = revision(template.revision);
  if (kind === 'TRIGGER' && (template.id !== value.definition.definition_id || definitionRevision !== revision(value.definition.definition_revision))) throw new Error('The schedule does not match its pinned definition.');
  return {reference: {kind, tenant_id: tenant, id}, revision: version, release: template.worker_release || '', weights: releaseWeights(JSON.stringify(template.worker_release_weights ?? {}), template.worker_release || ''), definitionID: template.id, definitionRevision,
    state: kind === 'DEFINITION' ? 'Current definition' : value.completed ? 'Completed' : value.definition.paused ? 'Paused' : 'Active'};
}

// Scan each configuration collection explicitly. Trigger templates, rather than
// current job definitions, determine which release future activations target.
export class ReleaseReferences {
  constructor(tenant, release, {query, render, active}) {
    this.tenant = tenant; this.release = release; this.query = query; this.render = render; this.active = active;
    this.sources = Object.fromEntries(Object.keys(kinds).map(kind => [kind, {rows: new Map(), checked: 0, loaded: false, loading: false, next: '', seen: new Set(), error: '', controller: ''}]));
  }
  load() { return Promise.all(Object.keys(kinds).map(kind => this.loadPage(kind))); }
  async loadPage(kind) {
    const source = this.sources[kind];
    if (!source || !this.active() || source.loading || source.loaded && !source.next) return;
    source.loading = true; source.error = ''; this.render();
    try {
      const {value, controller} = await this.query('list', {kind, tenant_id: this.tenant, page_size: 32, ...(source.next ? {page_token: source.next} : {})});
      if (!this.active()) return;
      source.controller = controller;
      const records = value?.records === undefined ? [] : value.records, next = value?.next_page_token === undefined ? '' : value.next_page_token;
      if (!value || typeof value !== 'object' || Array.isArray(value) || !Array.isArray(records) || records.length > 32 || typeof next !== 'string' || next && (next === source.next || source.seen.has(next))) throw new Error('Invalid or repeated release reference page; reload the inspection.');
      const rows = records.map(record => referenceRow(record, kind, this.tenant));
      for (const row of rows) {
        if (row.release === this.release || Object.hasOwn(row.weights, this.release)) source.rows.set(row.reference.id, row);
        else source.rows.delete(row.reference.id);
      }
      source.checked += rows.length; source.loaded = true;
      if (source.next) source.seen.add(source.next);
      source.next = next;
    } catch (err) { if (this.active()) { source.error = err.message; source.controller = err.controller || source.controller; } }
    finally { source.loading = false; if (this.active()) this.render(); }
  }
  html(link) {
    return '<p>Current jobs and pinned schedule snapshots that refer to this release. Updating a job does not update its schedules. Paused and completed schedules are included.</p>'
      + '<p class="notice">Replica reads can lag and change between pages. This inspection does not count existing runs or authorize stopping a release.</p>'
      + Object.entries(kinds).map(([kind, label]) => {
        const source = this.sources[kind], rows = [...source.rows.values()];
        const coverage = source.loaded && !source.next && !source.error ? 'All pages read' : 'Partial inspection';
        return `<section class="history" aria-label="${label} referencing release"><h3>${label}</h3><p>${coverage} · ${source.checked} records examined · ${rows.length} matching references loaded</p>`
          + (source.controller ? '<p class="hint">Last responding controller: <code>' + e(source.controller) + '</code></p>' : '')
          + (source.error ? '<p class="error" role="alert">' + e(source.error) + '</p>' : '')
          + (rows.length ? `<div class="table-scroll" tabindex="0" role="region" aria-label="${label} references"><table aria-label="${label} references"><thead><tr><th scope="col">Record</th><th scope="col">Revision</th><th scope="col">Pinned job revision</th><th scope="col">State</th></tr></thead><tbody>` + rows.map(row => '<tr><th scope="row">' + link(row.reference) + '</th><td>' + e(row.revision) + '</td><td>' + e(row.definitionID + '@' + row.definitionRevision) + '</td><td>' + e(row.state) + '</td></tr>').join('') + '</tbody></table></div>' : '<p>' + (source.loaded && !source.next && !source.error ? 'No references observed in these readings.' : 'No matching references loaded; unread or failed pages may contain more.') + '</p>')
          + (source.loading ? '<p role="status">Reading…</p>' : source.error || !source.loaded || source.next ? `<button type="button" id="references-${kind}" data-action="release-references-more" data-kind="${kind}">${source.error ? 'Retry' : 'Load more'} ${label.toLowerCase()}</button>` : '') + '</section>';
      }).join('');
  }
}
