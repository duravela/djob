import {escapeHTML as e, buildMutation, decodeBytes, displayTime, durationSeconds, workerEntries, workerMatches, inventoryPartial} from './model.js';
import {buildCompanyMutation} from './forms.js';
import {ReleaseReferences} from './references.js';
import {HistoryInspector, runOverview} from './history.js';
import {canResolveRun, canResolveResource, buildRecoveryMutation, canPurgeRun, buildPurgeMutation} from './recovery.js';
import {routeHref as recordURL, readRoute, recordReference, referenceHref, mutationTarget} from './routing.js';
import {UsageInspector, limitPolicies, buildLimitMutation} from './usage.js';
import {enrollmentRecord, buildEnrollmentMutation} from './enrollments.js';
import {deploymentRecord, buildDeploymentMutation, deploymentRuntimeView, deploymentSessionsView, deploymentAccessView} from './deployments.js';

const API = '/api/console/v1/';
const app = document.getElementById('app');
const dialog = document.getElementById('dialog');
const pages = {
  cluster: {label: 'Cluster', global: true, readonly: true, description: 'Quorum-confirmed membership and reviewed controller replacement.'},
  jobs: {label: 'Jobs', kind: 'DEFINITION', field: 'definition', description: 'Versioned definitions of the work your handlers perform.'},
  schedules: {label: 'Schedules', kind: 'TRIGGER', field: 'trigger', description: 'One-shot, recurring and event triggers, pinned to a job revision.'},
  deletions: {label: 'Deleted history', kind: 'DELETION', field: 'deletion', readonly: true, description: 'Permanent deletion audits and bounded cleanup progress. Run identities cannot be reused.'},
  runs: {label: 'Runs', kind: 'RUN', field: 'run', description: 'Accepted work, execution progress and recorded outcomes.'},
  workers: {label: 'Workers', readonly: true, description: 'Tenant-scoped observations of worker code registered with configured controllers.'},
  deployments: {label: 'Deployments', kind: 'WORKER_DEPLOYMENT', field: 'worker_deployment', description: 'Company worker images, desired replicas and graceful lifecycle through Kubernetes.'},
  enrollments: {label: 'Worker access', kind: 'WORKER_ENROLLMENT', field: 'worker_enrollment', description: 'Enrolled company workers, credential lifecycle and permission to start conversations.'},
  usage: {label: 'Usage & limits', readonly: true, kinds: ['LIMITS', 'USAGE', 'CONFIGURATION_LIMITS', 'CONFIGURATION_USAGE'], description: 'Tenant admission budgets and observed logical storage usage.'},
  groups: {label: 'Groups', kind: 'GROUP', field: 'group', description: 'Shared runtime defaults, resource requirements and concurrency limits.'},
  bindings: {label: 'Bindings', kind: 'BINDING', field: 'binding', description: 'Driver versions, target configuration and required resources.'},
  grants: {label: 'Resource access', kind: 'GRANT', field: 'grant', description: 'This tenant’s permission to use canonical shared resources.'},
  resources: {label: 'Resources', kind: 'RESOURCE', field: 'resource', global: true, description: 'Shared targets and the authority retained across conversations.'},
  tenants: {label: 'Tenants', kind: 'TENANT', field: 'tenant', global: true, description: 'Tenant workspaces and cluster-wide concurrent-run limits.'},
};
const state = {session: null, tenant: '', page: 'jobs', workerRelease: '', records: [], workerSources: [], next: '', filter: '', loading: false, generation: 0, editor: null, detail: null, history: null, references: null, routeRecord: null, routeError: '', listError: '', failedReference: null};
let ignoredClosures = 0;
const platform = () => state.session?.identity.role === 'operator' && !state.session.identity.tenant_id;
const writable = () => state.session?.identity.role !== 'reader';
const mayEdit = kind => writable() && (!['binding', 'resource', 'grant', 'tenant', 'limits', 'configuration_limits', 'worker_enrollment', 'worker_deployment'].includes(kind) || platform());
const json = value => JSON.stringify(value, null, 2);
const button = (action, label, attributes = '', primary = false) => `<button type="button" data-action="${action}" ${attributes} class="${primary ? 'primary' : ''}">${e(label)}</button>`;
const code = value => `<code>${e(value || 'None')}</code>`;
const status = value => {
  const known = ['ready', 'running', 'waiting', 'succeeded', 'failed', 'cancelled', 'unknown', 'available', 'owned', 'blocked', 'paused', 'completed', 'active', 'connected', 'registered', 'draining', 'relocating', 'stale', 'unavailable', 'stopped'];
  return `<span class="status ${known.includes(value) ? value : ''}">${e(value || 'None')}</span>`;
};
function routeHref(page, tenant = state.tenant, id = '') {
  return recordURL(page, tenant, id, page === 'runs' && tenant === state.tenant ? state.workerRelease : '');
}
function navigate(page, tenant = state.tenant, id = '') {
  return followRecordLink(routeHref(page, tenant, id));
}
function followRecordLink(href) {
  if (location.hash === href) return route();
  location.hash = href;
}
function closeDialog(preserveRoute = false) {
  clearTimeout(state.deploymentTimer);
  state.deploymentTimer = null;
  if (!dialog.open) return;
  // A queued close event must not clear a record opened by later navigation.
  ignoredClosures++;
  if (!preserveRoute) dialogClosed();
  dialog.close();
}
function recordLink(reference, label, className = 'button') {
  const href = referenceHref(reference, state.tenant, pages, state.workerRelease);
  return href ? `<a class="${className}" data-record-link href="${e(href)}">${e(label)}</a>` : '';
}
const reviewButton = () => state.session?.pending ? button('pending', 'Open review') : '';
async function request(path, body, method = 'POST', withController = false) {
  const session = state.session;
  const headers = {};
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (session) headers['X-Djob-CSRF'] = session.csrf;
  const response = await fetch(API + path, {method, headers, credentials: 'same-origin', body: body === undefined ? undefined : JSON.stringify(body)});
  const controller = response.headers.get('X-Djob-Controller');
  if (controller && session && state.session === session) {
    session.controller = controller;
    const label = document.querySelector('.connection code');
    if (label) label.textContent = controller;
  }
  let value;
  try { value = await response.json(); } catch { throw new Error('The console response could not be read. Reload to recover any pending review.'); }
  if (!response.ok) {
    const error = new Error(value.error || value.message || 'Request failed (' + response.status + ').');
    error.status = response.status;
    error.controller = controller;
    throw error;
  }
  return withController ? {value, controller: controller || ''} : value;
}
const query = (method, body) => request('query/' + method, body);
function alertIn(id, message) {
  const node = document.getElementById(id);
  if (node) { node.textContent = message; node.hidden = !message; }
}
function showError(error) {
  const id = dialog.open ? 'dialog-error' : 'page-error';
  alertIn(id, error.message || String(error));
}

function signIn() {
 request('oidc/config',undefined,'GET').then(config=>{const target=document.getElementById('sso-login');if(target)target.hidden=!config.enabled;}).catch(()=>{});
  clearTimeout(draftTimer); draftDirty = false; draftQueue = Promise.resolve();
  state.generation++; state.records = []; state.workerSources = []; state.detail = null; state.history = null; state.usage = null; state.detailRequest = null;
  state.session = null; state.releasePlan=null; state.workerRelease = '';
  state.routeRecord = null; state.routeError = ''; state.listError = ''; state.failedReference = null;
  closeDialog(true);
  app.innerHTML = `<main id="main" class="signin" tabindex="-1">
    <div class="wordmark">djob<span>console</span></div>
    <div class="signin-panel"><h1>Open your workspace</h1>
      <p>Use a djob API credential to manage jobs within its existing permissions.</p>
      <p id="sso-login" hidden><a class="button" href="/api/console/v1/oidc/start">Sign in with SSO</a></p>
      <form id="login-form">
        <label for="credential">API credential</label>
        <input id="credential" name="credential" type="password" required minlength="32" maxlength="8192" autocomplete="off" spellcheck="false" autofocus>
        <p class="hint">Tenant reader, writer, operator, or platform operator.</p>
        <p id="login-error" class="error" role="alert" hidden></p>
        <button class="primary" type="submit">Sign in</button>
      </form>
    </div>
    <p class="signin-note">The console manages intent. Controllers retain durable state. Workers execute your code.</p>
  </main>`;
}
async function resume() {
  try { state.session = await request('session', undefined, 'GET'); await route(); restoreEditorDraft(); }
  catch (err) { signIn(); if (err.status !== 401) alertIn('login-error', err.message); }
}
function renderShell() {
  const p = pages[state.page];
  const identity = state.session.identity;
  const nav = Object.entries(pages).filter(([, config]) => !config.global || platform()).map(([key, config]) =>
    `<a href="${e(routeHref(key))}" ${key === state.page ? 'aria-current="page"' : ''}>${e(config.label)}</a>`).join('');
  const canCreate = !p.readonly && state.page !== 'runs' && mayEdit(p.field) && (p.global || state.tenant);
  app.innerHTML = `<div class="layout">
    <aside class="sidebar"><a class="wordmark" href="${e(routeHref('jobs'))}">djob<span>console</span></a>
      <div class="workspace"><span class="label">Tenant workspace</span>${platform()
        ? `<form id="tenant-form"><label class="sr-only" for="tenant-select">Tenant ID</label><input id="tenant-select" name="tenant" value="${e(state.tenant)}" placeholder="Enter tenant ID" required maxlength="256"><button type="submit">Open</button></form>`
        : `<strong>${e(identity.tenant_id)}</strong>`}</div>
      <nav aria-label="Management">${nav}</nav>
      <div class="account"><strong>${e(identity.subject)}</strong><span>${e(identity.role)}${platform() ? ' · platform' : ''}</span>${button('signout', 'Sign out')}</div>
    </aside>
    <main id="main" class="workspace-main" tabindex="-1">
      <div class="connection"><span>Controller</span><code>${e(state.session.controller)}</code></div>
      <div id="pending-banner"></div>
      <header class="page-heading"><div><h1>${e(p.label)}</h1><p>${e(p.description)}</p></div>
        <div class="actions">${button('refresh', 'Refresh')}${state.page==='deployments'&&platform()&&state.tenant?button('release-plan-form','Promote or roll back'):''}${state.page === 'workers' && platform() && state.tenant ? '<a class="button" data-record-link href="' + e(routeHref('enrollments')) + '">Manage worker access</a>' : ''}${state.page === 'jobs' && writable() ? button('company-forms', 'New from company form') : ''}${canCreate ? button('create', state.page === 'deployments' ? 'New deployment' : state.page === 'enrollments' ? 'Enroll worker' : 'New ' + ({definition: 'job', trigger: 'schedule', grant: 'grant'}[p.field] || p.field), '', true) : ''}</div>
      </header>
      ${state.workerRelease ? '<section class="history" aria-label="Worker release filter"><p>Outstanding runs for release ' + code(state.workerRelease) + '. Includes queued, running, waiting and unknown work.</p><a class="button" data-record-link href="' + e(recordURL('deployments', state.tenant, state.workerRelease)) + '">Inspect release</a> <a class="button" data-record-link href="' + e(recordURL('runs', state.tenant)) + '">All runs</a></section>' : ''}
      <p id="page-error" class="error" role="alert" hidden></p>
      ${!p.global && !state.tenant ? `<section class="empty"><h2>Choose a tenant workspace</h2><p>Open a tenant by ID, or provision one from Tenants.</p><a class="button" href="${e(routeHref('tenants', ''))}">View tenants</a></section>`
        : state.page === 'usage' ? '<div id="usage-browser"></div>'
        : `${p.kind ? '<form id="record-lookup" class="record-lookup"><label for="record-id">Open by ID</label><input id="record-id" name="record_id" required maxlength="256" placeholder="Exact record ID" autocomplete="off" spellcheck="false"><button type="submit">Open record</button></form>' : ''}<div class="toolbar"><label for="filter">Filter loaded records</label><input id="filter" type="search" placeholder="ID, handler, or status" value="${e(state.filter)}"><span id="load-status" role="status"></span></div><div id="records"></div>`}
      <footer class="workspace-footer">${state.page === 'cluster' ? 'Membership reads confirm quorum. Progress is a local leader observation; refresh before acting.' : state.page === 'workers' ? 'Worker observations are local to each configured controller. Refresh after registrations change.' : 'Reads reflect the selected replica and may lag. Refresh to check again.'}</footer>
    </main>
  </div>`;
  renderPendingBanner();
}
async function route() {
  if (!state.session) return;
  closeDialog(true);
  state.history = null; state.usage = null; state.detail = null; state.detailRequest = null; state.failedReference = null;
  const destination = readRoute(window.location.hash, state.session.identity, pages);
  const preserveList = state.page !== 'cluster' && state.page === destination.page && state.tenant === destination.tenant && state.workerRelease === (destination.workerRelease || '') && !state.loading && !!document.getElementById('records');
  state.page = destination.page; state.tenant = destination.tenant; state.workerRelease = destination.workerRelease || '';
  state.routeRecord = destination.reference; state.routeError = destination.error;
  if (!preserveList) { state.records = []; state.workerSources = []; state.next = ''; state.filter = ''; state.listError = ''; }
  state.loading = false; state.generation++;
  document.title = pages[state.page].label + ' · djob console';
  renderShell();
  alertIn('page-error', [state.routeError, state.listError].filter(Boolean).join(' '));
  if (pages[state.page].global || state.tenant) {
    const listing = preserveList ? Promise.resolve(renderTable()) : loadList();
    const opening = state.routeRecord ? details({reference: state.routeRecord}) : Promise.resolve();
    await Promise.all([listing, opening]);
  }
}
async function loadList(append = false) {
  if (state.page === 'cluster') return loadCluster();
  if (state.page === 'workers') return loadWorkers();
  if (state.page === 'usage') return loadUsage();
  const generation = ++state.generation;
  state.loading = true; state.listError = ''; renderTable(); alertIn('page-error', state.routeError);
  try {
    const p = pages[state.page];
    const data = state.page === 'deployments'
      ? await request('worker-deployments/list', {tenant_id: state.tenant, page_token: append ? state.next : ''})
      : state.page === 'enrollments'
      ? await request('worker-enrollments/list', {tenant_id: state.tenant, page_token: append ? state.next : ''})
      : await query('list', {kind: p.kind, tenant_id: p.global ? '' : state.tenant, page_size: 32, page_token: append ? state.next : '', ...(state.workerRelease ? {worker_release: state.workerRelease} : {})});
    if (generation !== state.generation) return;
    let records = data.records || [];
    if (state.workerRelease && (!Array.isArray(records) || records.some(record => record.reference?.tenant_id !== state.tenant || record.run?.definition?.worker_release !== state.workerRelease || (['succeeded', 'failed', 'cancelled'].includes(record.run?.status) && !record.run.owner_id)))) throw new Error('Controller returned a run outside this outstanding release scope.');
    if (state.page === 'enrollments') {
      if (data.tenant_id !== state.tenant || !Array.isArray(data.enrollments) || data.enrollments.length > 32 || data.next_page_token && !/^[A-Za-z0-9_.-]{1,64}$/.test(data.next_page_token)) throw new Error('Invalid worker enrollment page.');
      records = data.enrollments.map(v => enrollmentRecord(v, state.tenant));
    }
    if (state.page === 'deployments') {
      if (data.tenant_id !== state.tenant || !Array.isArray(data.deployments) || data.deployments.length > 32 || data.next_page_token && !/^[A-Za-z0-9_.-]{1,64}$/.test(data.next_page_token)) throw new Error('Invalid worker deployment page.');
      records = data.deployments.map(v => deploymentRecord(v, state.tenant));
    }
    state.records = append ? state.records.concat(records) : records;
    state.next = data.next_page_token || '';
  } catch (err) { if (generation === state.generation) { state.listError = err.message; alertIn('page-error', [state.routeError, err.message].filter(Boolean).join(' ')); } }
  finally { if (generation === state.generation) { state.loading = false; renderTable(); } }
}
async function loadUsage() {
  if (!state.tenant) return;
  const generation = ++state.generation, session = state.session;
  const inspector = new UsageInspector(state.tenant, {
    query: (method, body) => request('query/' + method, body, 'POST', true),
    root: () => document.getElementById('usage-browser'),
    active: () => state.usage === inspector && state.session === session && state.generation === generation,
    editable: platform(),
  });
  state.usage = inspector;
  await inspector.load();
}
function renderTable() {
  if (state.page === 'cluster') return;
  if (state.page === 'workers') return renderWorkers();
  if (state.page === 'usage') return state.usage?.render();
  const target = document.getElementById('records');
  if (!target) return;
  const p = pages[state.page];
  const records = state.records.map((record, index) => ({record, index})).filter(({record}) => json(record).toLowerCase().includes(state.filter.toLowerCase()));
  const configurations = {
    jobs: ['Job', 'Handler', 'Group', 'Binding', 'Revision'],
    schedules: ['Schedule', 'Job / revision', 'Activation', 'Status', 'Next due (UTC)'],
    deletions: ['Run', 'Outcome', 'Cleanup', 'Records deleted', 'Deleted (UTC)'],
    runs: ['Run', 'Job', 'Status', 'Attempts', 'Created (UTC)'],
    groups: ['Group', 'Runtime', 'Required resources', 'Max running', 'Revision'],
    bindings: ['Binding', 'Driver', 'Required resources', 'Revision'],
    grants: ['Resource', 'Access', 'Revision'],
    resources: ['Resource', 'Status', 'Duplex', 'Owner tenant', 'Epoch'],
    tenants: ['Tenant', 'Max running', 'Revision', 'Workspace'],
    deployments: ['Deployment', 'Worker', 'Desired replicas', 'Image', 'Revision'],
    enrollments: ['Worker', 'Audit subject', 'Admission', 'Revision', 'Changed (UTC)'],
  };
  const rows = records.map(({record: r, index}) => {
    const v = r[p.field];
    const id = r.reference.id || r.reference.tenant_id;
    let cells;
    if (state.page === 'jobs') cells = [code(v.handler + '@' + v.handler_version), e(v.group_id || 'None'), e(v.binding_id || 'None'), e(v.revision)];
    if (state.page === 'schedules') cells = [code(v.definition.definition_id + '@' + v.definition.definition_revision), e(v.definition.event ? 'event · ' + v.definition.event.source : v.definition.timing.kind), status(v.completed ? 'completed' : v.definition.paused ? 'paused' : 'active'), e(displayTime(v.next_at))];
    if (state.page === 'deletions') cells = [status(v.status), e(v.cleanup_complete ? 'Complete' : 'Pending'), e(v.records_deleted || '0'), e(displayTime(v.deleted_at))];
    if (state.page === 'runs') cells = [code(v.definition.id), status(v.status), e(v.attempt_count), e(displayTime(v.created_at))];
    if (state.page === 'groups') cells = [code(v.runtime), e((v.resources || []).join(', ') || 'None'), e(v.max_running), e(v.revision)];
    if (state.page === 'bindings') cells = [code(v.driver + '@' + v.driver_version), e((v.resources || []).join(', ') || 'None'), e(v.revision)];
    if (state.page === 'grants') cells = [e(v.allowed ? 'Allowed' : 'Denied'), e(v.revision)];
    if (state.page === 'resources') cells = [status(v.state), e(v.spec.duplex), e(v.owner_tenant_id || 'None'), e(v.epoch)];
    if (state.page === 'tenants') cells = [e(v.max_running), e(v.revision), button('open-tenant', 'Open workspace', `data-id="${e(r.reference.tenant_id)}"`)];
    if (state.page === 'deployments') cells = [code(v.worker_id), e(v.state === 'retired' ? 'Retired' : v.spec.replicas), code(v.spec.image), e(v.revision)];
    if (state.page === 'enrollments') cells = [code(v.subject), e(v.state), e(v.revision), e(displayTime(v.changed_at))];
    return `<tr><th scope="row">${recordLink(r.reference, id, 'record-link') || code(id)}</th>${cells.map(cell => '<td>' + cell + '</td>').join('')}</tr>`;
  }).join('');
  const statusNode = document.getElementById('load-status');
  statusNode.textContent = state.loading ? 'Loading…' : state.records.length + ' loaded';
  if (!state.records.length && !state.loading) {
    target.innerHTML = state.listError ? '<section class="empty"><h2>No records loaded</h2><p>The list could not be read. Retry with Refresh or open a known record by ID.</p></section>' : state.workerRelease ? '<section class="empty"><h2>No outstanding runs</h2><p>This replica reports no remaining runs for this release. Refresh the release record before final retirement.</p></section>' : `<section class="empty"><h2>No ${e(p.label.toLowerCase())} yet</h2><p>${state.page === 'runs' ? 'Launch a job or configure a trigger. Accepted work will appear here.' : state.page === 'jobs' ? 'Create a definition, then launch it or attach a schedule.' : 'Records will appear here after they are configured.'}</p></section>`;
  } else {
    target.innerHTML = `<div class="table-scroll" tabindex="0" role="region" aria-label="Scrollable records"><table aria-label="${e(p.label)}" aria-busy="${state.loading}"><thead><tr>${configurations[state.page].map(c => '<th scope="col">' + e(c) + '</th>').join('')}</tr></thead><tbody>${rows || '<tr><td colspan="6">No loaded records match this filter.</td></tr>'}</tbody></table></div><div class="pagination"><span>${records.length} shown / ${state.records.length} loaded</span>${state.next ? button('more', state.loading ? 'Loading…' : 'Load next page', state.loading ? 'disabled' : '') : ''}</div>`;
  }
}
async function loadWorkers(index) {
  if (index !== undefined && state.loading) return;
  const generation = ++state.generation;
  const tenant = state.tenant;
  state.loading = true; renderWorkers(); alertIn('page-error', state.routeError);
  try {
    const previous = state.workerSources.find(s => s.index === index);
    const data = await request('workers', {tenant_id: tenant, ...(index === undefined ? {} : {controller_index: index, page_token: previous?.inventory?.next_page_token || ''})});
    if (generation !== state.generation) return;
    const incoming = data.controllers.map(source => {
      if (!source.inventory) return source;
      const inventory = {...source.inventory, workers: (source.inventory.workers || []).map(w => ({...w, observed_at: source.inventory.observed_at}))};
      if (previous?.inventory && previous.inventory.instance_id === inventory.instance_id && previous.inventory.controller_id === inventory.controller_id) {
        const updates = new Map(previous.inventory.workers.map(w => [w.id, w]));
        for (const w of inventory.workers) updates.set(w.id, w);
        inventory.workers = [...updates.values()];
      }
      return {...source, inventory};
    });
    state.workerSources = index === undefined ? incoming : state.workerSources.map(s => incoming.find(update => update.index === s.index) || s);
  } catch (err) { if (generation === state.generation) alertIn('page-error', [state.routeError, err.message].filter(Boolean).join(' ')); }
  finally { if (generation === state.generation) { state.loading = false; renderWorkers(); } }
}
function capabilityNames(versions, limit = 3) {
  const all = versions || [];
  return all.length ? all.slice(0, limit).map(v => code(v.name + '@' + v.version)).join('<br>') + (all.length > limit ? `<br><span class="hint">+${all.length - limit} more</span>` : '') : '<span class="hint">None reported</span>';
}
function workerTable(entries, interactive = false) {
  return `<div class="table-scroll" tabindex="0" role="region" aria-label="Scrollable worker observations"><table class="worker-table" aria-label="Worker observations"><thead><tr>${['Worker', 'Mode / state', 'Handlers', 'Drivers', 'Local tasks', 'Controller'].map(label => '<th scope="col">' + e(label) + '</th>').join('')}</tr></thead><tbody>${entries.map(w => `<tr><th scope="row">${interactive ? '<button type="button" class="link" data-action="worker-detail" data-index="' + w.index + '">' + e(w.worker_id) + '</button>' : code(w.worker_id)}</th><td>${e(w.mode)}<br>${status(w.state)}</td><td>${capabilityNames(w.handlers)}</td><td>${capabilityNames(w.drivers)}</td><td>${e(w.local_tasks)}</td><td>${code(w.controller_id)}<br><span class="hint">${e(w.origin)}</span></td></tr>`).join('') || '<tr><td colspan="6">No loaded observations match this filter.</td></tr>'}</tbody></table></div>`;
}
function sourceSummary(sources, pagination = false) {
  return `<details class="inventory-sources" open><summary>Controller observations</summary><ul>${sources.map(s => `<li><div>${code(s.origin)}<p class="${s.error ? 'source-error' : 'hint'}">${s.error ? e(s.error) : 'Controller ' + e(s.inventory.controller_id) + ' · latest page ' + e(displayTime(s.inventory.observed_at))}</p></div>${pagination && s.inventory?.next_page_token ? button('workers-more', 'Load next page', 'data-index="' + s.index + '" ' + (state.loading ? 'disabled' : '')) : ''}</li>`).join('')}</ul></details>`;
}
function renderWorkers() {
  const target = document.getElementById('records');
  if (!target) return;
  const all = workerEntries(state.workerSources);
  const filtered = all.map((w, index) => ({...w, index})).filter(w => json(w).toLowerCase().includes(state.filter.toLowerCase()));
  document.getElementById('load-status').textContent = state.loading ? 'Observing…' : all.length + ' observed';
  const partial = inventoryPartial(state.workerSources);
  target.innerHTML = '<p class="notice">Registrations report code presence. Claims, quotas, resource reservations and uncertain outcomes still govern execution. Local task counts belong to this tenant and can include pending claims.</p>'
    + (state.workerSources.length && partial ? '<p class="warning">Observations are partial. Check controller errors, load remaining pages, and inspect any truncated capabilities before drawing conclusions.</p>' : '')
    + (all.length ? workerTable(filtered, true) : `<section class="empty"><h2>${state.loading ? 'Observing workers…' : 'No workers observed'}</h2><p>${partial ? 'The available observations cannot establish which workers are registered. Check the sources below and refresh.' : 'No worker registrations were reported for this tenant by the configured controllers.'}</p></section>`)
    + (state.workerSources.length ? sourceSummary(state.workerSources, true) : '')
    + '<p class="hint">Only configured controllers are queried. Pages are sampled at different times; refresh to replace earlier observations.</p>';
}
function workerDetails(index) {
  const worker = workerEntries(state.workerSources)[index];
  if (!worker) return;
  openModal(worker.worker_id, keyValues([['Tenant', code(worker.tenant_id)], ['Mode', e(worker.mode)], ...(worker.deployment_id ? [['Deployment', code(worker.deployment_id)], ['Deployment generation', code(worker.deployment_generation)]] : []), ['State', status(worker.state)], ['Runtime', code((worker.runtimes || []).join(', '))], ['Local execution tasks', e(worker.local_tasks)], ['Controller', code(worker.controller_id)], ['Origin', code(worker.origin)], ['Observed', e(displayTime(worker.observed_at))], ['Last worker contact', e(displayTime(worker.last_seen))], ['Credential subject', code(worker.subject)]])
    + (worker.capabilities_truncated ? '<p class="warning">This registration’s capability list is truncated. Omitted code may still be present.</p>' : '')
    + '<h3>Handlers</h3><div class="capabilities">' + capabilityNames(worker.handlers, 64) + '</div><h3>Drivers</h3><div class="capabilities">' + capabilityNames(worker.drivers, 64) + '</div>', true);
}
async function checkWorkers(node) {
  const detail = state.detail;
  const generation = state.generation;
  const session = state.session;
  const ticket = {};
  state.detailRequest = ticket;
  node.disabled = true;
  try {
    const tenant = detail.reference.tenant_id;
    let definition, binding, runtime, context;
    if (detail.run || detail.trigger) {
      const pinned = detail.run || detail.trigger;
      definition = detail.run ? pinned.definition : pinned.job_definition;
      binding = pinned.binding; runtime = pinned.runtime;
      context = 'Pinned execution dependencies from this ' + (detail.run ? 'run' : 'schedule') + '.';
    } else {
      definition = detail.definition;
      runtime = definition.runtime;
      if (!runtime && definition.group_id) runtime = (await query('get', {reference: {kind: 'GROUP', tenant_id: tenant, id: definition.group_id}})).group.runtime;
      if (definition.binding_id) binding = (await query('get', {reference: {kind: 'BINDING', tenant_id: tenant, id: definition.binding_id}})).binding;
      context = 'Current definition and dependency revisions from replica reads. A launch pins the revisions resolved by its committed command.';
    }
    const required = {tenant_id: tenant, runtime, handler: definition.handler, handler_version: definition.handler_version, worker_release: definition.worker_release, worker_release_weights: definition.worker_release_weights, driver: binding?.driver, driver_version: binding?.driver_version};
    const data = await request('workers', {tenant_id: tenant});
    if (generation !== state.generation || session !== state.session || state.detailRequest !== ticket) return;
    const sources = data.controllers;
    const matching = workerEntries(sources).filter(w => workerMatches(w, required));
    openModal('Worker compatibility', '<p>' + e(context) + '</p>'
      + keyValues([['Runtime', code(required.runtime)], ['Handler', code(required.handler + '@' + required.handler_version)], ['Worker release', code(required.worker_release || 'Capability routing')], ['Driver', required.driver ? code(required.driver + '@' + required.driver_version) : 'None required']])
      + '<h3>' + (matching.length ? 'Matching code observed' : 'No matching worker observed') + '</h3>'
      + '<p class="hint">Runtime, handler version and required driver version must be present on the same worker. A match does not reserve capacity or authorize execution; check the reported state.</p>'
      + (inventoryPartial(sources) ? '<p class="warning">Observations are partial. Unreachable controllers, unloaded pages or truncated capabilities may contain additional matches.</p>' : '')
      + (matching.length ? workerTable(matching) : '<p class="hint">Definitions can be saved before their code is deployed. Missing observations do not block job submission.</p>')
      + sourceSummary(sources) + '<div class="dialog-actions">' + button('back-detail', 'Back to record') + button('open-workers', 'Open workers') + '</div>', true);
  } catch (err) {
    if (generation === state.generation && session === state.session && state.detailRequest === ticket) throw err;
  } finally { node.disabled = false; }
}
function openModal(title, content, wide = false) {
  state.detailRequest = null; state.references = null;
  closeDialog(true);
  dialog.classList.toggle('wide', wide);
  dialog.innerHTML = `<header class="dialog-heading"><h2 id="dialog-title">${e(title)}</h2>${button('close', 'Close', 'aria-label="Close dialog"')}</header><div class="dialog-body"><p id="dialog-error" class="error" role="alert" hidden></p>${content}</div>`;
  dialog.showModal();
  dialog.scrollTop = 0;
}
const field = (name, label, value = '', options = {}) => `<div class="field"><label for="f-${name}">${e(label)}</label><input id="f-${name}" name="${name}" type="${options.type || 'text'}" value="${e(value)}" ${options.required ? 'required' : ''} ${options.readonly ? 'readonly' : ''} ${options.numeric ? 'inputmode="numeric" pattern="[0-9]+" maxlength="20"' : options.type === 'number' ? 'min="0" step="1"' : options.type === 'datetime-local' ? 'step="any"' : 'maxlength="' + (name === 'resources' ? '8192' : name === 'image' ? '512' : '256') + '"'} ${options.autofocus ? 'autofocus' : ''}>${options.help ? '<p class="hint">' + e(options.help) + '</p>' : ''}</div>`;
const select = (name, label, value, options) => `<div class="field"><label for="f-${name}">${e(label)}</label><select id="f-${name}" name="${name}">${options.map(([v, label]) => `<option value="${e(v)}" ${v === value ? 'selected' : ''}>${e(label)}</option>`).join('')}</select></div>`;
const checkbox = (name, label, checked) => `<label class="checkbox"><input name="${name}" type="checkbox" ${checked ? 'checked' : ''}>${e(label)}</label>`;
function bytesForm(value, label) {
  const decoded = decodeBytes(value);
  return select('encoding', label + ' format', decoded.encoding, [['text', 'UTF-8 text'], ['json', 'JSON (UTF-8)'], ['base64', 'Base64 bytes']]) +
    `<div class="field"><label for="f-payload">${e(label)}</label><textarea id="f-payload" name="payload" rows="7" spellcheck="false">${e(decoded.text)}</textarea><p class="hint">Stored as bytes. JSON is validated without changing the input.</p></div>`;
}
function enrollmentEditor(action, record = null) {
  if (!platform()) return;
  const value = record?.worker_enrollment;
  const descriptions = {
    enroll: 'Enroll a stable worker identity and create a private credential. The company worker advertises its installed handler and driver versions when it connects.',
    rotate: 'Create a replacement credential. Current sessions drain safely; start the worker with the new private configuration. Rotation keeps the audit identity.',
    drain: 'Prevent new sessions and new conversations. Current conversations can finish and clean up before their sessions end.',
    resume: 'Permit fresh sessions and new conversations. Start drained workers again with their existing private configuration.',
    revoke: 'Permanently revoke this identity and prevent new conversations. Current conversations drain safely. This action does not establish physical quiescence or release gateway reservations.',
  };
  const labels = {enroll: 'Enroll worker', rotate: 'Rotate worker credential', drain: 'Drain worker', resume: 'Resume worker admission', revoke: 'Revoke worker access'};
  state.editor = {kind: 'worker_enrollment', action, record, tenant: state.tenant};
  openModal(labels[action], `<p>${e(descriptions[action])}</p><form id="mutation-form"><p class="form-context">Tenant: ${e(state.tenant)}${value ? ' · expected revision ' + e(value.revision) : ''}</p>`
    + field('worker_id', 'Worker ID', value?.worker_id || '', {required: true, readonly: !!record, help: '1–64 letters, digits, dots, underscores or hyphens. Matches worker.Config.WorkerID.'})
    + (action === 'enroll' ? field('subject', 'Audit subject', '', {required: true, help: 'Stable deployment identity, for example acme/measurement-workers.'}) : '')
    + field('reason', 'Reason', '', {required: true})
    + '<div class="dialog-actions">' + button('close', 'Cancel') + '<button type="submit" class="primary">Review request</button></div></form>');
}
function deploymentEditor(action, record = null, defaults = {}) {
  if (!platform()) return;
  const value = record?.worker_deployment, spec = value?.spec || defaults;
  if (value?.state === 'retired') return;
  state.editor = {kind: 'worker_deployment', action, record, tenant: state.tenant};
  const titles = {put: value ? 'Edit worker deployment' : 'New worker deployment', scale: defaults.replicas === 0 ? 'Stop worker deployment' : 'Scale worker deployment', restart: 'Restart worker deployment', retire: 'Retire worker deployment', publish: 'Publish worker release', retiring: 'Begin release retirement', resume: 'Resume new runs'};
  let fields = '';
  if (action === 'put') fields = field('id', 'Deployment ID', value?.id || '', {required: true, readonly: !!value})
    + field('worker_id', 'Enrolled worker ID', value?.worker_id || defaults.worker_id || '', {required: true, readonly: !!value, help: 'Use a dedicated enrolled identity for this deployment.'})
    + field('pool', 'Kubernetes runtime pool', spec.pool || 'default', {required: true, readonly: !!value})
    + field('image', 'Container image with digest', spec.image || '', {required: true, help: 'registry/repository@sha256:<64 lowercase hexadecimal characters>'})
    + field('config_secret', 'Worker configuration Secret', spec.config_secret || '', {required: true, help: 'Existing Secret containing worker.json and its referenced TLS/profile files.'})
    + field('pull_secret', 'Image pull Secret (optional)', spec.pull_secret || '')
    + field('replicas', 'Desired replicas', spec.replicas ?? 1, {required: true, type: 'number'})
    + field('cpu_millis', 'CPU per instance (millicores)', spec.cpu_millis || 100, {required: true, type: 'number'})
    + field('memory_mib', 'Memory per instance (MiB)', spec.memory_mib || 128, {required: true, type: 'number'})
    + field('drain_seconds', 'Pod shutdown grace (seconds)', spec.drain_seconds || 45, {required: true, type: 'number', help: 'Must cover the worker’s drain and cleanup timeouts.'})
    + '<div class="field"><label for="f-args">Arguments (JSON array)</label><textarea id="f-args" name="args" rows="3" spellcheck="false">' + e(json(spec.args || [])) + '</textarea><p class="hint">Empty arguments use --worker-config /var/run/djob/credentials/worker.json. Arguments are durable configuration; keep credentials in the Secret.</p></div>';
  if (action === 'scale') fields = field('replicas', 'Desired replicas', defaults.replicas ?? (value.spec.replicas || 1), {required: true, type: 'number', readonly: defaults.replicas === 0});
  const descriptions = {publish: 'Publish this deployment ID as a worker release. Its image, arguments and configuration references become immutable. Use immutable versioned configuration Secrets. Published workers serve only jobs pinned to this release.', retiring: 'Stop accepting new runs for this release. Keep its workers running for queued jobs, retries and resumptions. This does not interrupt existing conversations.', resume: 'Allow new runs to select this release again.'};
  const description = descriptions[action] || (action === 'retire' ? 'Retiring stops new work and keeps the deployment identity and audit history permanently reserved.' : 'Kubernetes manages process replacement. Active conversations retain their ordinary lease and cleanup rules; stopping a process does not resolve an uncertain external effect.');
  openModal(titles[action], '<p>' + e(description) + '</p><form id="mutation-form"><p class="form-context">Tenant: ' + e(state.tenant) + (value ? ' · expected revision ' + e(value.revision) : '') + '</p>' + fields + field('reason', 'Reason', '', {required: true}) + '<div class="dialog-actions">' + button('close', 'Cancel') + '<button type="submit" class="primary">Review request</button></div></form>');
}
async function deploymentRuntime(reference, ticket) {
  const target = document.getElementById('deployment-runtime');
  const sessions = document.getElementById('deployment-sessions');
  const access = document.getElementById('deployment-access');
  try {
    const data = await request('worker-deployments/status', {tenant_id: reference.tenant_id, id: reference.id});
    if (state.detailRequest !== ticket || !target?.isConnected) return;
    if (data.tenant_id !== reference.tenant_id || data.id !== reference.id || !Array.isArray(data.controllers) || data.controllers.length > 16) throw new Error('Invalid runtime observation.');
    const observations = data.controllers.filter(source => source.status?.observed && source.status.runtime && source.status.runtime.revision === state.detail.worker_deployment.revision && Number.isFinite(Date.parse(source.status.runtime.observed_at))).sort((a, b) => Date.parse(b.status.runtime.observed_at) - Date.parse(a.status.runtime.observed_at));
    const source = observations[0], partial = data.controllers.some(v => v.error);
    const renderObservations = () => {
      if (state.detailRequest !== ticket || !sessions?.isConnected || !target?.isConnected || !access?.isConnected) return;
      clearTimeout(state.deploymentTimer);
      const connected = deploymentSessionsView(data.controllers, state.detail.worker_deployment);
      const expanded = sessions.querySelector('details')?.open;
      const expirations = connected.expiresAt === null ? [] : [connected.expiresAt];
      const policy = deploymentAccessView(data.controllers, state.detail.worker_deployment);
      if (policy.expiresAt !== null) expirations.push(policy.expiresAt);
      access.innerHTML = '<h3>Worker access</h3>' + (policy.advice ? '<p class="warning">' + e(policy.advice) + '</p>' : '')
        + keyValues(policy.rows.map(([label, value]) => [label, e(value)]))
        + '<p class="hint">Enrollment policy is observed locally. Enabled access does not verify Pod credentials or grant execution permission.</p>'
        + '<a class="button" data-record-link href="' + e(routeHref('enrollments', reference.tenant_id, state.detail.worker_deployment.worker_id)) + '">Inspect worker access</a>';
      const connectionAdvice = connected.complete && connected.connected === 0 && policy.complete && ['draining', 'revoked'].includes(policy.state) ? '' : connected.advice;
      sessions.innerHTML = '<h3>Worker connections</h3>' + (connectionAdvice ? '<p class="warning">' + e(connectionAdvice) + '</p>' : '')
        + keyValues(connected.rows.map(([label, value]) => [label, e(value)]))
        + '<details' + (expanded ? ' open' : '') + '><summary>Current-generation session states</summary>' + keyValues(connected.details.map(([label, value]) => [label, e(value)])) + '</details>'
        + '<p class="hint">Sessions are local connection observations, not distinct Pods or permission to execute jobs. Other generations may still be finishing conversations. The Workers view shows installed capabilities.</p>';
      if (!source) target.textContent = 'No current runtime observation yet. A configured Kubernetes controller must reconcile this desired state.' + (partial ? ' Some controllers could not be read.' : '');
      else {
        const v = source.status.runtime, view = deploymentRuntimeView(v);
        if (view.expiresAt !== null) expirations.push(view.expiresAt);
        target.innerHTML = (view.advice ? '<p class="warning">' + e(view.advice) + '</p>' : '') + keyValues([['Runtime observation', e(v.state) + (view.stale ? ' · stale' : '')], ...view.rows.map(([label, value]) => [label, e(value)]), ['Kubernetes target', code(v.namespace + '/' + v.name)], ['Observed (UTC)', e(displayTime(v.observed_at))], ['Source controller', code(source.origin)], ...(v.error ? [['Runtime error', e(v.error)]] : [])]) + '<p class="hint">Pod availability is an observation. The Workers view shows registered capabilities; committed claims govern execution.' + (partial ? ' Some controllers could not be read.' : '') + '</p>';
      }
      state.deploymentTimer = expirations.length ? setTimeout(renderObservations, Math.max(1, Math.min(...expirations) - Date.now())) : null;
    };
    renderObservations();
  } catch (err) { if (state.detailRequest === ticket && target?.isConnected) { target.textContent = err.message; sessions.textContent = 'Worker-session observations unavailable.'; access.textContent = 'Worker-access observations unavailable.'; } }
}
async function releaseReferences() {
  const record = state.detail?.worker_deployment;
  if (!record?.published || record.tenant_id !== state.tenant) return;
  const session = state.session, generation = state.generation;
  openModal('Release references', keyValues([['Tenant', code(record.tenant_id)], ['Worker release', code(record.id)]]) + '<div id="release-references"></div><div class="dialog-actions">' + button('release-references', 'Reload references') + button('back-detail', 'Back to release') + '</div>', true);
  const ticket = Symbol('release references');
  state.detailRequest = ticket;
  const root = () => dialog.querySelector('#release-references');
  const inspector = new ReleaseReferences(record.tenant_id, record.id, {
    query: (method, body) => request('query/' + method, body, 'POST', true),
    active: () => state.references === inspector && state.session === session && state.generation === generation && state.detailRequest === ticket && dialog.open && !!root(),
    render: () => {
      const target = root();
      if (!target) return;
      const focusID = target.contains(document.activeElement) ? document.activeElement.id : '';
      target.innerHTML = inspector.html(ref => recordLink(ref, ref.id));
      if (focusID) target.querySelector('#' + CSS.escape(focusID))?.focus({preventScroll: true});
    },
  });
  state.references = inspector;
  await inspector.load();
}
function releasePlanForm(rollback=false) {
 if(!platform()||!state.tenant)return;
 state.editor=null;
 openModal(rollback?'Prepare rollback plan':'Prepare promotion plan','<p>Inspect all matching jobs and pinned schedules. Planning performs reads only. Every write will use the existing exact request review. Use a dedicated canary job with isolated resources and representative assertions before promotion.</p><form id="release-plan-form" data-rollback="'+rollback+'">'+(!rollback?field('source','Source release','',{required:true})+field('target','Target release','',{required:true}):'')+field('canary','Successful target canary run ID','')+'<label class="checkbox"><input name="allow_unqualified" type="checkbox">I explicitly accept proceeding without a successful target canary.</label><label class="checkbox"><input name="allow_unobserved" type="checkbox">I explicitly accept proceeding without observed target availability and capabilities.</label><div class="dialog-actions">'+button('close','Cancel')+button('release-plan-current','Open retained plan')+'<button class="primary" type="submit">Build reviewed plan</button></div></form>');
}
function showReleasePlan(plan) {
 state.releasePlan=plan;state.editor=null;
 const steps=plan.steps||[],confirmed=steps.filter(step=>step.confirmed).length;
 openModal('Release plan',keyValues([['Tenant',code(plan.tenant_id)],['Migration',code(plan.source+' → '+plan.target)],['Confirmed',e(confirmed+' / '+steps.length)],['Plan digest',code(plan.digest)],['Canary',code(plan.canary_run||'Explicitly waived')]])+'<p>Review all original snapshots and proposed mutations below. Each next step is prepared for individual confirmation. Existing runs retain their accepted release.</p><div class="dialog-actions">'+(confirmed<steps.length?button('release-plan-next','Review next step','',true):'')+(confirmed?button('release-plan-rollback','Prepare rollback'):'')+button('release-plan-form','New promotion')+button('release-plan-download','Download plan')+button('close','Close')+'</div>'+steps.map((step,i)=>'<details><summary>'+e((step.confirmed?'Confirmed: ':'Pending: ')+(i+1)+' '+step.kind+' '+step.id)+'</summary><pre>'+e(json(step))+'</pre></details>').join(''));
}
async function currentReleasePlan(){showReleasePlan(await request('release-plans/current',{}));}
async function gatewayDetails(id) {
 if(!platform())return;
 const session=state.session,generation=state.generation,ticket=Symbol('gateway');state.detailRequest=ticket;
 let response;
 try { response=await request('gateways/get',{id}); } catch(error) {
  if(session!==state.session||generation!==state.generation||state.detailRequest!==ticket||!dialog.open)return;
  throw error;
 }
 if(session!==state.session||generation!==state.generation||state.detailRequest!==ticket||!dialog.open)return;
 const gateway=response.channel;
 if(gateway?.id!==id||typeof gateway.epoch!=='string')throw new Error('Gateway authority response does not match the requested ID.');
 state.gateway=gateway;
 openModal('Gateway authority',keyValues([['Gateway',code(id)],['State',status(gateway.state)],['Epoch',code(gateway.epoch)],['Owner',code(gateway.owner_id)],['Lease until',e(displayTime(gateway.lease_until))],['Active attempt',code(gateway.active_attempt_id)]])+'<p>Logical expiry blocks access; it does not establish physical quiescence. A gateway resolution does not resolve a generic resource or job outcome.</p><div class="dialog-actions">'+(['owned','draining'].includes(gateway.state)?button('gateway-expire','Expire observed lease'):'')+(gateway.state==='blocked'?button('gateway-resolve','Confirm gateway quiescence'):'')+button('back-detail','Back to resource')+'</div>');
}
function gatewayEditor(operation) {
 if(!platform()||!state.gateway)return;
 const g=state.gateway;state.editor={kind:'gateway',record:g,operation,tenant:state.tenant};
 openModal(operation==='resolve'?'Confirm gateway quiescence':'Expire gateway lease','<p>'+e(operation==='resolve'?'Independently verify previous physical access and downstream device activity have stopped. This preserves the gateway epoch and records your attestation.':'Expire only the observed logical lease. A renewed lease or different epoch rejects this request; expiry cannot authorize another worker or prove physical quiescence.')+'</p><form id="mutation-form">'+field('id','Gateway ID',g.id,{readonly:true})+field('expected_epoch','Expected gateway epoch',g.epoch,{readonly:true})+(operation==='resolve'?'<div class="field"><label for="f-reason">Evidence and reason</label><textarea id="f-reason" name="reason" required maxlength="4096"></textarea></div>':'')+'<label class="checkbox"><input name="attested" type="checkbox" required>'+e(operation==='resolve'?'I independently verified physical quiescence.':'I understand expiry blocks access and can mark active execution unknown.')+'</label><div class="dialog-actions">'+button('close','Cancel')+'<button type="submit" class="primary">Review request</button></div></form>');
}
async function companyForms() {
 const generation=state.generation,session=state.session,data=await request('forms',{tenant_id:state.tenant});
 if(session!==state.session||generation!==state.generation)return;
 if(data.tenant_id!==state.tenant||!Array.isArray(data.forms))throw new Error('Company form scope does not match the workspace.');state.companyForms=data.forms;
 openModal('Company job forms',data.forms.length?'<p>Templates fix handler and binding configuration. Review the generated job definition before saving.</p>'+data.forms.map((profile,index)=>button('company-form',profile.label,'data-index="'+index+'"')).join(''):'<p>No company forms are configured for this tenant.</p>');
}
function companyEditor(profile) {
 if(!writable()||profile?.tenant_id!==state.tenant)return;
 state.editor={kind:'company_definition',record:profile,tenant:state.tenant};
 const fields=profile.fields.map(f=>f.type==='boolean'?select('company_'+f.name,f.label,f.default||'', [['','Choose'],['true','True'],['false','False']]):'<div class="field"><label for="f-company_'+e(f.name)+'">'+e(f.label)+'</label><textarea id="f-company_'+e(f.name)+'" name="company_'+e(f.name)+'" rows="2" maxlength="65536" '+(f.required?'required':'')+'>'+e(f.default||'')+'</textarea><p class="hint">'+e(f.type)+'</p></div>').join('');
 openModal(profile.label,'<p>Form revision '+code(profile.revision)+'. Values become opaque job input; workers retain responsibility for domain validation.</p><form id="mutation-form">'+field('id','Job ID','',{required:true})+keyValues([['Handler',code(profile.handler+'@'+profile.handler_version)],['Binding',code(profile.binding_id)],['Worker release',code(profile.worker_release)]])+fields+'<div class="dialog-actions">'+button('close','Cancel')+'<button type="submit" class="primary">Review request</button></div></form>');
}
function purgeEditor(record) {
 if(!canPurgeRun(state.session?.identity,record))return;
 state.editor={kind:'purge',record,tenant:state.tenant};
 openModal('Delete retained run history','<p>This permanently removes retained run payloads and history in bounded batches. The run ID, deletion audit and recovery attestations remain. Backups, WAL and old snapshots can retain payload copies.</p><form id="mutation-form">'+field('run_id','Run ID',record.reference.id,{readonly:true})+field('expected_step','Expected checkpoint',(record.run||record.deletion).step||'0',{readonly:true})+'<div class="field"><label for="f-reason">Evidence and reason</label><textarea name="reason" id="f-reason" required maxlength="4096"></textarea></div><label class="checkbox"><input name="attested" type="checkbox" required>I understand that retained run history will be permanently removed.</label><div class="dialog-actions">'+button('close','Cancel')+'<button type="submit" class="primary">Review request</button></div></form>');
}
function recoveryEditor(kind, record) {
  const run = kind === 'resolve_run';
  if (!(run ? canResolveRun(state.session?.identity, record) : canResolveResource(state.session?.identity, record))) return;
  const reference = record.reference, value = run ? record.run : record.resource;
  const title = run ? 'Resolve unknown outcome' : 'Confirm resource quiescence';
  const explanation = run
    ? 'Record an independently confirmed terminal outcome for this exact checkpoint. This does not replay an action or release blocked resources. Historical action outcomes remain unchanged.'
    : 'Confirm independently that prior access and device activity have stopped for this blocked epoch. This releases the resource; it does not determine the job outcome. Live gateway authority must be resolved first when required.';
  let fields = field(run ? 'run_id' : 'resource_id', run ? 'Run ID' : 'Resource ID', reference.id, {readonly: true});
  fields += field(run ? 'expected_step' : 'expected_epoch', run ? 'Expected checkpoint' : 'Expected resource epoch', run ? value.step ?? '0' : value.epoch, {readonly: true});
  if (run) fields += select('status', 'Confirmed outcome', '', [['', 'Choose confirmed outcome'], ['succeeded', 'Succeeded'], ['failed', 'Failed'], ['cancelled', 'Cancelled']]) + bytesForm('', 'Verified result (optional)');
  fields += '<div class="field"><label for="f-reason">Evidence and reason</label><textarea id="f-reason" name="reason" rows="3" maxlength="4096" required></textarea><p class="hint">Describe the independent verification. Recorded with the authenticated operator identity.</p></div>';
  fields += '<label class="checkbox"><input name="attested" type="checkbox" required>' + e(run ? 'I independently verified this terminal outcome.' : 'I independently verified that prior access and device activity have stopped.') + '</label>';
  state.editor = {kind, record, tenant: state.tenant};
  openModal(title, '<p>' + e(explanation) + '</p><form id="mutation-form"><p class="form-context">' + e(run ? 'Tenant: ' + reference.tenant_id : 'Global resource recovery') + '</p>' + fields + '<div class="dialog-actions">' + button('close', 'Cancel') + '<button type="submit" class="primary">Review request</button></div></form>');
}

function editor(kind, record = null, defaults = {}) {
  if (['resolve_run', 'resolve_resource'].includes(kind)) return recoveryEditor(kind, record);
  if (kind === 'worker_enrollment') return enrollmentEditor('enroll');
  if (kind === 'worker_deployment') return deploymentEditor('put', record, defaults);
  if (!mayEdit(kind) && !['launch', 'cancel', 'trigger'].includes(kind) || !writable()) return;
  const v = record?.[kind] || defaults;
  const identity = v.id || (kind === 'trigger' ? v.definition?.id : '') || v.spec?.id || v.resource_id || record?.reference?.tenant_id || '';
  const editing = Boolean(record);
  const policy = Object.hasOwn(limitPolicies, kind) ? limitPolicies[kind] : null;
  let form = '';
  if (!['launch', 'cancel'].includes(kind) && !policy) form += field('id', kind === 'grant' ? 'Resource ID' : 'ID', identity, {required: true, readonly: editing, autofocus: !editing});
  if (policy) form += '<p class="notice">This replaces every limit in this policy. Zero means unlimited. Lowering a limit restricts growth in that metric; existing work can continue.</p><div class="form-grid">' + policy.fields.map(([name, label, help]) => field('max_' + name, label + ' limit', v['max_' + name] ?? '0', {required: true, numeric: true, help})).join('') + '</div>';
  if (kind === 'definition') form += `<div class="form-grid">${field('handler', 'Handler', v.handler, {required: true, help: 'Exact name deployed to your workers.'})}${field('handler_version', 'Handler version', v.handler_version || '1', {required: true})}${field('runtime', 'Runtime', v.runtime ?? 'steps/v1', {help: 'Leave empty to inherit the group runtime.'})}${field('group_id', 'Group ID', v.group_id)}${field('binding_id', 'Binding ID', v.binding_id)}${field('worker_release', 'Worker release (optional)', v.worker_release, {help: 'Published deployment ID. The run keeps this release across waits and retries; leave empty for ordinary capability routing.'})}</div>` + '<div class="field"><label for="f-worker_release_weights">Release weights (optional)</label><textarea id="f-worker_release_weights" name="worker_release_weights" rows="3">' + e(Object.keys(v.worker_release_weights || {}).length ? json(v.worker_release_weights) : '') + '</textarea><p class="hint">JSON basis points summing to 10000, for example {&quot;v1&quot;:9000,&quot;v2&quot;:1000}. Leave the single release field empty. Schedules pin this policy; existing runs retain their chosen release.</p></div>' + bytesForm(v.input, 'Input');
  if (kind === 'group') form += `<div class="form-grid">${field('runtime', 'Runtime', v.runtime || 'steps/v1')}${field('max_running', 'Max concurrent runs', v.max_running || '1', {required: true, help: 'Zero leaves this quota unlimited.'})}</div>`;
  if (kind === 'binding') form += `<div class="form-grid">${field('driver', 'Driver', v.driver, {required: true, help: 'Exact name deployed to your workers.'})}${field('driver_version', 'Driver version', v.driver_version || '1', {required: true})}</div>` + bytesForm(v.config, 'Configuration');
  if (['definition', 'binding', 'group'].includes(kind)) form += field('resources', 'Required resource IDs', (v.resources || []).join(', '), {help: 'Comma-separated canonical IDs. Shared buses must resolve to the same resource.'});
  if (kind === 'tenant') form += field('max_running', 'Max concurrent runs', v.max_running || '1', {required: true, help: 'Enforced across controllers and workers. Zero is unlimited.'});
  if (kind === 'resource') form += select('duplex', 'Duplex profile', v.spec?.duplex || 'none', [['none', 'Not applicable'], ['half', 'Half duplex'], ['full', 'Full duplex']]) + '<p class="hint">Current resource profiles reserve one conversation, including related commands and waits.</p>' + field('channel_id', 'Existing gateway channel ID (optional)', v.spec?.channel_id, {help: 'Use only when this resource shares authority with the gateway-specific scheduler.'});
  if (kind === 'grant') form += checkbox('allowed', 'Allow this tenant to use the resource', v.allowed ?? true);
  if (kind === 'launch') form += field('definition_id', 'Job ID', v.definition_id, {required: true, readonly: true}) + field('definition_revision', 'Job revision', v.definition_revision, {required: true, readonly: true}) + field('run_id', 'Run ID (optional)', '', {help: 'Leave empty to generate a unique identity during review. The launch queues durable work.'});
  if (kind === 'cancel') form += field('run_id', 'Run ID', v.run_id, {required: true, readonly: true}) + field('reason', 'Reason', '', {required: true, help: 'Cancellation records intent. Active conversations retain resources through cleanup.'});
  if (kind === 'trigger') {
    const d = v.definition || v;
    const timing = d.timing || {};
    const mode = d.event ? 'event' : timing.kind || 'once';
    const seconds = durationSeconds;
    form += `<div class="form-grid">${field('definition_id', 'Job ID', d.definition_id, {required: true})}${field('definition_revision', 'Job revision', d.definition_revision || '', {required: true, help: 'Pins this exact revision for future occurrences.'})}</div>`;
    form += select('mode', 'Activation', mode, [['once', 'Once'], ['interval', 'Anchored interval'], ['cron', 'Cron'], ['event', 'Event source']]);
    form += `<div data-mode="calendar">${field('start_at', 'Start at (UTC)', timing.start_at ? new Date(timing.start_at).toISOString().slice(0, -1) : new Date(Date.now() + 60000).toISOString().slice(0, -1), {type: 'datetime-local', required: true})}</div>`;
    form += `<div data-mode="interval">${field('interval', 'Interval (seconds)', seconds(timing.interval_ns) || '60', {required: true})}</div>`;
    form += `<div data-mode="cron" class="form-grid">${field('cron', 'Cron expression (five fields)', timing.cron || '0 * * * *', {required: true})}${field('time_zone', 'IANA time zone', timing.time_zone || 'UTC', {required: true})}</div>`;
    form += `<div data-mode="event">${field('source', 'Event source', d.event?.source, {required: true, help: 'Producers activate this trigger through the API with durable event identities.'})}</div>`;
    form += `<div data-mode="calendar">${select('missed_policy', 'Missed occurrences', timing.missed_policy || 'coalesce', [['coalesce', 'Coalesce to the latest occurrence'], ['skip', 'Skip outside the grace window'], ['replay', 'Replay a bounded catch-up window']])}</div>`;
    form += `<div data-mode="calendar" data-policy="skip">${field('misfire_grace', 'Grace window (seconds)', seconds(timing.misfire_grace_ns) || '1', {required: true})}</div>`;
    form += `<div data-mode="calendar" data-policy="replay">${field('catch_up', 'Maximum catch-up runs', timing.catch_up_limit || '32', {required: true})}</div>`;
    form += field('max_pending', 'Maximum pending runs', d.max_pending || '32', {required: true, help: '1..128 per trigger. Ready, running, waiting and unknown work occupy slots.'}) + checkbox('paused', 'Start paused', d.paused);
  }
  state.editor = {kind, record, defaults, tenant: state.tenant};
  const label = {definition: 'job', trigger: 'schedule', grant: 'resource grant', limits: 'run limits', configuration_limits: 'configuration limits'}[kind] || kind;
  openModal(kind === 'launch' ? 'Launch job' : kind === 'cancel' ? 'Request cancellation' : (editing ? 'Edit ' : 'New ') + label,
    `<form id="mutation-form"><p class="form-context">${e(kind === 'resource' ? 'Global resource catalog' : 'Tenant: ' + (kind === 'tenant' ? identity || 'new tenant' : state.tenant))}${editing ? ' · expected revision ' + e(v.revision ?? '0') : ''}</p>${form}<div class="dialog-actions">${button('close', 'Cancel')}<button type="submit" class="primary">Review request</button></div></form>`);
  syncConditionalFields();
}
function syncConditionalFields() {
  const form = document.getElementById('mutation-form');
  if (!form) return;
  const mode = form.elements.namedItem('mode')?.value;
  const policy = form.elements.namedItem('missed_policy')?.value;
  for (const section of form.querySelectorAll('[data-mode]')) {
    const visible = (section.dataset.mode === 'calendar' ? mode !== 'event' : mode === section.dataset.mode) && (!section.dataset.policy || policy === section.dataset.policy);
    section.hidden = !visible;
    for (const input of section.querySelectorAll('input, select')) input.disabled = !visible;
  }
}
function keyValues(values) {
  return '<dl class="facts">' + values.map(([label, value]) => '<div><dt>' + e(label) + '</dt><dd>' + value + '</dd></div>').join('') + '</dl>';
}
function bytesPanel(label, value) {
  const decoded = decodeBytes(value);
  return `<details><summary>${e(label)} · ${e(decoded.encoding)}</summary><pre>${e(decoded.text || '(empty)')}</pre></details>`;
}
async function details(record) {
  const generation = state.generation;
  const session = state.session;
  const reference = record.reference;
  if (!dialog.open) openModal('Loading record', `<p role="status">Reading ${code(reference.id || reference.tenant_id)}…</p>${reviewButton()}`);
  const ticket = {};
  state.detailRequest = ticket;
  let full;
  try {
    try { full = reference.kind === 'WORKER_DEPLOYMENT' ? deploymentRecord(await request('worker-deployments/get', {tenant_id: reference.tenant_id, id: reference.id}), reference.tenant_id, reference.id) : reference.kind === 'WORKER_ENROLLMENT' ? enrollmentRecord(await request('worker-enrollments/get', {tenant_id: reference.tenant_id, worker_id: reference.id}), reference.tenant_id, reference.id) : await query('get', {reference}); }
    catch (err) { if (err.status === 429 && ['RUN', 'TRIGGER'].includes(reference.kind)) full = await query('get', {reference, summary: true}); else throw err; }
    if (reference.kind === 'RUN' && state.workerRelease && full.run?.definition?.worker_release !== state.workerRelease) throw new Error('This run belongs to another worker release. Open All runs to inspect it.');
    const actual = full?.reference;
    if (!actual || actual.kind !== reference.kind || (actual.tenant_id || '') !== (reference.tenant_id || '') || reference.kind !== 'TENANT' && actual.id !== reference.id || !full[reference.kind.toLowerCase()]) throw new Error('Controller record does not match the requested identity.');
  } catch (err) {
    if (generation !== state.generation || session !== state.session || state.detailRequest !== ticket) return;
    state.detail = null; state.history = null; state.failedReference = reference;
    openModal('Record unavailable', `<p class="error" role="alert">${e(err.message)}</p><p>Requested record: ${code(reference.id || reference.tenant_id)}</p><p class="hint">Replica reads can lag. A missing record does not establish the outcome of an uncertain mutation.</p><div class="dialog-actions">${button('retry-record', 'Retry read')}${reviewButton()}<a class="button" data-record-link href="${e(routeHref(state.page))}">Back to list</a></div>`, true);
    return;
  }
  if (generation !== state.generation || session !== state.session || state.detailRequest !== ticket) return;
  state.detail = full; state.history = null;
  const kind = reference.kind.toLowerCase();
  const value = full[kind];
  let actions = '';
  if (mayEdit(kind) && !['run', 'worker_enrollment'].includes(kind) && !(kind === 'worker_deployment' && (value.state === 'retired' || value.published))) actions += button('edit-detail', 'Edit');
  if (kind === 'worker_enrollment' && platform() && value.state !== 'revoked') {
    actions += button('enrollment-action', 'Rotate credential', 'data-operation="rotate"')
      + button('enrollment-action', value.state === 'enabled' ? 'Drain worker' : 'Resume admission', 'data-operation="' + (value.state === 'enabled' ? 'drain' : 'resume') + '"')
      + button('enrollment-action', 'Revoke access', 'data-operation="revoke"');
  }
  if (kind === 'worker_deployment' && platform() && value.state !== 'retired') {
    const busy = value.published && value.outstanding_runs !== '0';
    if (!busy || !value.spec.replicas) actions += button('deployment-action', value.spec.replicas ? 'Stop' : 'Start', 'data-operation="scale"' + (value.spec.replicas ? ' data-replicas="0"' : ''));
    actions += button('deployment-action', 'Scale', 'data-operation="scale"') + (value.spec.replicas && !busy ? button('deployment-action', 'Restart instances', 'data-operation="restart"') : '');
    if (value.published) actions += button('deployment-action', value.state === 'retiring' ? 'Resume new runs' : 'Begin retirement', 'data-operation="' + (value.state === 'retiring' ? 'resume' : 'retiring') + '"');
    else actions += button('deployment-action', 'Publish release', 'data-operation="publish"');
    if (!busy) actions += button('deployment-action', 'Retire', 'data-operation="retire"');
  }
  if (kind === 'worker_enrollment' && platform() && value.state === 'enabled') actions += button('deployment-create', 'Create deployment');
  if (kind === 'definition' && writable()) actions += button('schedule-detail', 'Schedule') + button('launch-detail', 'Launch now', '', true);
  if (canPurgeRun(state.session.identity,full)) actions += button('purge-run', kind === 'deletion' ? 'Continue cleanup' : 'Delete retained history');
  if (kind === 'run' && canResolveRun(state.session.identity, full)) actions += button('resolve-run', 'Resolve unknown outcome');
  if (kind === 'resource' && platform() && value.spec.channel_id) actions += button('gateway-inspect','Inspect gateway authority');
  if (kind === 'resource' && canResolveResource(state.session.identity, full)) actions += button('resolve-resource', 'Confirm quiescence');
  if (kind === 'run' && writable() && !['succeeded', 'failed', 'cancelled', 'unknown'].includes(value.status) && !value.cancel_requested) actions += button('cancel-detail', 'Request cancellation');
  if (kind === 'trigger' && writable() && !value.completed) actions += button('pause-detail', value.definition.paused ? 'Resume schedule' : 'Pause schedule');
  if (kind === 'tenant') actions += button('tenant-detail', 'Open workspace', '', true);
  if (['definition', 'run', 'trigger'].includes(kind)) actions += button('check-workers', 'Check workers');
  actions += recordLink(full.reference, 'Record link');
  actions += reviewButton();
  let content = `<div class="detail-actions">${button('refresh-detail', 'Refresh')}${actions}</div>`;
  if (kind === 'worker_enrollment') content += keyValues([['Tenant', code(value.tenant_id)], ['Audit subject', code(value.subject)], ['Admission', e(value.state)], ['Revision', e(value.revision)], ['Changed by', code(value.actor)], ['Reason', e(value.reason)]]) + '<p class="hint">Enrollment is durable access policy. Current worker sessions and installed capabilities appear on Workers. Draining sessions finish through normal resource cleanup.</p><a class="button" data-record-link href="' + e(routeHref('workers')) + '">Observe workers</a>';
  if (kind === 'worker_deployment' && value.published) content += '<section class="history" aria-label="Worker release"><h3>Worker release</h3>' + keyValues([['Release ID', code(value.id)], ['New-run admission', value.state === 'active' && value.spec.replicas ? 'Open' : 'Closed'], ['Outstanding release runs', e(value.outstanding_runs ?? 'Unknown')]]) + (value.state === 'retiring' ? '<p>Keep these workers running for existing jobs and resumptions. Stop or retire after all outstanding runs are resolved.</p>' : '') + '<a class="button" data-record-link href="' + e(recordURL('runs', value.tenant_id, '', value.id)) + '">Inspect outstanding runs</a>' + button('release-references', 'Inspect job and schedule references') + '</section>';
  if (kind === 'worker_deployment') content += '<section id="deployment-access" class="history" aria-label="Worker access observations" aria-live="polite">Loading worker access…</section><section id="deployment-runtime" class="history" aria-label="Kubernetes runtime observation" aria-live="polite">Loading runtime observation…</section><section id="deployment-sessions" class="history" aria-label="Worker connection observations" aria-live="polite">Loading worker connections…</section><a class="button" data-record-link href="' + e(routeHref('workers')) + '">Observe workers</a>' + '<h3>Deployment configuration</h3>' + keyValues([['Worker identity', code(value.worker_id)], ['Record state', e(value.state)], ['Worker release', value.published ? code(value.id) : 'Unpublished deployment'], ['Desired replicas', e(value.spec.replicas)], ['Revision / generation', e(value.revision + ' / ' + value.generation)], ['Image', code(value.spec.image)], ['Runtime pool', code(value.spec.pool)], ['Configuration Secret', code(value.spec.config_secret)], ['Changed by', code(value.actor)], ['Reason', e(value.reason)]]);
  if (kind === 'run') {
    content += runOverview(value) + (platform() && value.resources?.length ? '<p>Inspect required resources: ' + value.resources.map(id => recordLink({kind: 'RESOURCE', tenant_id: '', id}, id)).join(' ') + '</p>' : '') + '<section id="history-browser" class="history" aria-label="Run execution history"></section>';
  } else if (kind === 'deletion') {
    content += keyValues([['Outcome',status(value.status)],['Cleanup',e(value.cleanup_complete?'Complete':'Pending')],['Records deleted',e(value.records_deleted||'0')],['Operator',code(value.actor)],['Reason',e(value.reason)],['Deleted (UTC)',e(displayTime(value.deleted_at))]]);
  } else if (kind === 'definition') {
    content += keyValues([['Revision', e(value.revision)], ['Handler', code(value.handler + '@' + value.handler_version)], ['Worker release', code(value.worker_release || 'Capability routing')], ['Runtime', code(value.runtime)], ['Group', code(value.group_id)], ['Binding', code(value.binding_id)], ['Required resources', e((value.resources || []).join(', ') || 'None')]]) + bytesPanel('Input', value.input);
  } else if (kind === 'trigger') {
    content += keyValues([['Revision', e(value.revision)], ['Job revision', code(value.definition.definition_id + '@' + value.definition.definition_revision)], ['Status', status(value.completed ? 'completed' : value.definition.paused ? 'paused' : 'active')], ['Next due', e(displayTime(value.next_at))], ['Pending limit', e(value.definition.max_pending)]]);
  } else if (kind === 'resource') {
    content += keyValues([['State', status(value.state)], ['Epoch', e(value.epoch)], ['Owner tenant', code(value.owner_tenant_id)], ['Run', code(value.run_id)], ['Lease until', e(displayTime(value.lease_until))]]);
    if (value.state === 'blocked') content += '<p class="warning">This resource remains blocked until a platform operator independently establishes physical quiescence and records an audited confirmation.</p>';
  }
  content += `<details class="raw-record"><summary>Raw record</summary><pre>${e(json(full))}</pre></details>`;
  openModal(reference.id || reference.tenant_id, content, true);
  if (kind === 'worker_deployment') { state.detailRequest = ticket; deploymentRuntime(reference, ticket); }
  if (kind === 'run') {
    const inspector = new HistoryInspector(full, {
      query: (method, body) => request('query/' + method, body, 'POST', true),
      root: () => dialog.querySelector('#history-browser'),
      active: () => state.history === inspector && state.session === session && state.generation === generation && dialog.open && !!dialog.querySelector('#history-browser'),
    });
    state.history = inspector;
    await inspector.load();
  }
}
function renderPendingBanner() {
  const target = document.getElementById('pending-banner');
  const pending = state.session?.pending;
  if (target) target.innerHTML = pending ? `<div class="pending-notice"><div><strong>${pending.state === 'uncertain' ? 'Mutation outcome uncertain' : pending.state === 'prepared' ? 'Changes ready for review' : pending.state === 'submitting' ? 'Submission in progress' : 'Mutation ' + e(pending.state)}</strong><code>${e(pending.request_id)}</code></div>${button('pending', 'Open review')}</div>` : '';
}
function showPending() {
  const p = state.session.pending;
  if (!p) return;
  const texts = {
    prepared: ['Review changes', 'This request has not been submitted. Confirm to send the exact envelope below.'],
    submitting: ['Submitting request', 'The console is awaiting the controller response. Check status before taking another action.'],
    confirmed: ['Change confirmed', 'The controller acknowledged durable application of this request. Reads on another replica may lag.'],
    rejected: ['Request rejected', 'The controller refused this request. Review its response before editing or replacing it.'],
    uncertain: ['Outcome not confirmed', 'Retain this request identity. Retry sends the same envelope, including its timestamp and run ID.'],
  };
  const [title, message] = texts[p.state];
  const response = p.error || p.response?.error || p.response?.message;
  let actions = button('download', 'Download request');
  if (['prepared', 'uncertain'].includes(p.state)) actions += button('commit', p.state === 'uncertain' ? 'Retry same request' : 'Confirm and submit', '', true);
  if (p.state === 'submitting') actions += button('check-pending', 'Check status');
  const target = p.response?.deployment ? {kind: 'WORKER_DEPLOYMENT', tenant_id: p.response.deployment.tenant_id, id: p.response.deployment.id} : p.response?.enrollment ? {kind: 'WORKER_ENROLLMENT', tenant_id: p.response.enrollment.tenant_id, id: p.response.enrollment.worker_id} : p.response?.reference;
  if (['confirmed', 'uncertain'].includes(p.state)) actions += recordLink(p.state === 'confirmed' ? target : mutationTarget(p.request, state.session.identity.tenant_id), p.state === 'confirmed' ? 'Open result' : 'Inspect target');
  if (p.state === 'confirmed' && p.worker_config_available) actions += button('worker-config', 'Download worker configuration', '', true);
  if (p.state === 'confirmed' && (p.request.resolve_run || p.request.resolve_resource || p.request.quiescent === true)) actions += button('recovery-audit', 'Inspect recovery audit');
  if (platform()&&state.releasePlan) actions+=button('release-plan-current','Open release plan');
  if (['prepared', 'confirmed', 'rejected'].includes(p.state)) actions += button('discard', p.state === 'prepared' ? 'Discard draft' : 'Done');
  openModal(title, `<p>${e(message)}</p>${p.worker_config_available ? '<p class="warning">Save the private worker configuration after confirmation. Its secret is available only in this console session until this review is cleared or expires. The request envelope contains only its hash. Workers require HTTPS and an enabled worker endpoint; add ca_file for a private controller CA and protect the downloaded file with mode 0600.</p>' : ''}${response ? '<p class="error" role="alert">' + e(response) + '</p>' : ''}${keyValues([['Tenant', code(p.request.tenant_id || 'global')], ['Request ID', code(p.request_id)]])}<div class="dialog-actions">${actions}</div><details open><summary>Exact controller request</summary><pre>${e(json(p.request))}</pre></details>${p.response ? '<details open><summary>Controller response</summary><pre>' + e(json(p.response)) + '</pre></details>' : ''}`, true);
  renderPendingBanner();
}
async function prepare(body) {
  clearTimeout(draftTimer);
  if (document.getElementById('mutation-form')) await saveEditorDraft();
  await draftQueue;
  state.session.pending = body.gateway ? await request('mutations/gateway', body.gateway) : body.worker_deployment ? await request('mutations/worker-deployment', body.worker_deployment) : body.worker_enrollment ? await request('mutations/worker-enrollment', body.worker_enrollment) : await request('mutations', body);
  state.session.draft = null; state.session.draft_revision = state.session.pending.draft_revision || state.session.draft_revision;
  draftDirty = false;
  showPending();
}
async function act(action, node) {
  if(action==='cluster-advance') {await request('cluster/advance',{approval:state.cluster.plan.digest});return loadCluster();}
  if(action==='release-plan-form')return releasePlanForm();
  if(action==='release-plan-rollback')return releasePlanForm(true);
  if(action==='release-plan-current')return currentReleasePlan();
  if(action==='release-plan-next') {const pending=await request('release-plans/next',{approval:state.releasePlan.digest});if(pending.complete)return currentReleasePlan();state.session.pending=pending;showPending();return;}
  if(action==='release-plan-download'){const blob=new Blob([json(state.releasePlan)],{type:'application/json'}),url=URL.createObjectURL(blob),link=document.createElement('a');link.href=url;link.download='release-plan.json';link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);return;}
  if (action === 'close') return closeDialog();
  if (action === 'retry-record' && state.failedReference) return details({reference: state.failedReference});
  if (action === 'refresh') return loadList();
  if (action === 'release-references') return releaseReferences();
  if (action === 'release-references-more') return state.references?.loadPage(node.dataset.kind);
  if (action === 'usage-retry') return state.usage?.loadRecord(node.dataset.kind);
  if (action === 'usage-edit' && platform() && state.usage) return state.session.pending ? showPending() : editor(node.dataset.key, state.usage.policy(node.dataset.key));
  if (action === 'more' && !state.loading) return loadList(true);
  if (action === 'workers-more') return loadWorkers(Number(node.dataset.index));
  if (action === 'worker-detail') return workerDetails(Number(node.dataset.index));
  if (action === 'check-workers') return checkWorkers(node);
  if (action === 'back-detail') return details(state.detail);
  if (action === 'recovery-audit') {
    const pending = state.session?.pending, session = state.session, generation = state.generation;
    if (pending?.state !== 'confirmed' || !(pending.request.resolve_run || pending.request.resolve_resource || pending.request.quiescent === true)) return;
    const tenant = pending.request.resolve_run ? pending.request.tenant_id : '';
    const ticket = Symbol('recovery audit');
    state.detailRequest = ticket;
    const active = () => state.session === session && session.pending === pending && state.generation === generation && state.detailRequest === ticket && dialog.open;
    let audit;
    try { audit = pending.request.quiescent===true ? await request('gateways/audit',{id:pending.request_id}) : await query('get', {reference: {kind: 'RESOLUTION', tenant_id: tenant, id: pending.request_id}}); }
    catch (err) { if (active()) throw err; return; }
    if (!active()) return;
    if (pending.request.quiescent!==true && (audit.reference?.kind !== 'RESOLUTION' || (audit.reference.tenant_id || '') !== tenant || audit.reference.id !== pending.request_id || !audit.resolution)) throw new Error('Recovery audit does not match this request.');
    if(pending.request.quiescent===true && (audit.resolution?.request_id!==pending.request_id || audit.resolution.channel_id!==pending.request.id || !audit.resolution.quiescent))throw new Error('Gateway audit does not match the reviewed request.');
    openModal('Recovery audit', '<p>Durable operator attestation for this request. An outcome resolution and physical quiescence are separate records.</p>' + keyValues([['Operator', code(audit.resolution.actor)], ['Reason', e(audit.resolution.reason)], ['Recorded (UTC)', e(displayTime(audit.resolution.at))]]) + '<pre>' + e(json(audit.resolution)) + '</pre>' + button('pending', 'Back to review'), true);
    return;
  }
  if (action === 'open-workers') return navigate('workers');
  if (action === 'company-forms') return state.session.pending ? showPending() : companyForms();
  if (action === 'company-form') return companyEditor(state.companyForms[Number(node.dataset.index)]);
  if (action === 'create') {
    if (state.session.pending) return showPending();
    return editor(pages[state.page].field);
  }
  if (action === 'enrollment-action' && platform()) return state.session.pending ? showPending() : enrollmentEditor(node.dataset.operation, state.detail);
  if (action === 'open-tenant') return navigate('jobs', node.dataset.id);
  if (action === 'pending') return showPending();
  if (action === 'check-pending') { state.session = await request('session', undefined, 'GET'); return showPending(); }
  if (action === 'commit') {
    node.disabled = true;
    const id = state.session.pending.request_id;
    try { state.session.pending = await request('mutations/commit', {request_id: id}); showPending(); }
    catch (err) { state.session = await request('session', undefined, 'GET'); showPending(); throw err; }
    return;
  }
  if (action === 'discard' && state.session.pending?.state === 'confirmed' && state.session.pending.worker_config_available) return openModal('Close enrollment review?', '<p>Save the private worker configuration before closing this review. Closing removes this console’s copy of the credential. An enrolled worker stays authorized until its identity is drained or revoked.</p><div class="dialog-actions">' + button('worker-config', 'Download worker configuration', '', true) + button('pending', 'Keep review') + button('discard-secret', 'Close and clear credential') + '</div>');
  if (action === 'discard' || action === 'discard-secret') {
    const session = state.session, pending = session.pending, generation = state.generation, modal = dialog.firstElementChild;
    await request('mutations/current', {request_id: pending.request_id}, 'DELETE');
    if (state.session !== session || session.pending !== pending) return;
    session.pending = null; renderPendingBanner();
    if (state.generation !== generation || dialog.firstElementChild !== modal) return;
    closeDialog(); return loadList();
  }
  if (action === 'worker-config') {
    const cfg = await request('mutations/worker-config', {request_id: state.session.pending.request_id});
    const url = URL.createObjectURL(new Blob([json(cfg) + '\n'], {type: 'application/json'}));
    const link = document.createElement('a'); link.href = url; link.download = cfg.worker_id + '.worker.json'; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000); return;
  }
  if (action === 'download') {
    const blob = new Blob([json(state.session.pending.request) + '\n'], {type: 'application/json'});
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url; link.download = 'djob-' + state.session.pending.request_id.replaceAll('/', '-') + '.json';
    link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000); return;
  }
  if (action === 'signout' || action === 'signout-confirm') {
    if (action === 'signout' && state.session.pending?.state === 'confirmed' && state.session.pending.worker_config_available) return openModal('Save worker configuration before signing out', '<p>Signing out removes this console’s copy of the private worker credential. The worker enrollment remains authorized.</p><div class="dialog-actions">' + button('worker-config', 'Download worker configuration', '', true) + button('pending', 'Keep review') + button('signout-confirm', 'Sign out anyway') + '</div>');
    if (action === 'signout' && state.session.pending && !['prepared', 'confirmed', 'rejected'].includes(state.session.pending.state)) {
      return openModal('End this console session?', '<p>The mutation outcome remains uncertain. Download its request before signing out. Ending this session removes its retained retry envelope and does not cancel the controller run.</p><div class="dialog-actions">' + button('download', 'Download request') + button('pending', 'Open mutation review') + button('signout-confirm', 'Sign out anyway') + '</div>');
    }
    await request('session', {}, 'DELETE'); return signIn();
  }
  if (action.startsWith('history-')) return state.history?.handle(action, node);
  const d = state.detail;
  if (action === 'refresh-detail') return details(d);
  if (action === 'deployment-action') return state.session.pending ? showPending() : deploymentEditor(node.dataset.operation, d, Object.hasOwn(node.dataset, 'replicas') ? {replicas: Number(node.dataset.replicas)} : {});
  if (action === 'deployment-create') return state.session.pending ? showPending() : deploymentEditor('put', null, {worker_id: d.worker_enrollment.worker_id});
  if (action === 'edit-detail') {
    if (state.session.pending) return showPending();
    return editor(d.reference.kind.toLowerCase(), d);
  }
  if (action === 'launch-detail' || action === 'schedule-detail') {
    if (state.session.pending) return showPending();
    const defaults = {definition_id: d.definition.id, definition_revision: d.definition.revision};
    return editor(action === 'launch-detail' ? 'launch' : 'trigger', null, defaults);
  }
  if (action === 'gateway-inspect') return gatewayDetails(d.resource.spec.channel_id);
  if (action === 'gateway-resolve' || action === 'gateway-expire') return state.session.pending ? showPending() : gatewayEditor(action==='gateway-resolve'?'resolve':'expire');
  if (action === 'purge-run') return state.session.pending ? showPending() : purgeEditor(d);
  if (action === 'resolve-run') return state.session.pending ? showPending() : recoveryEditor('resolve_run', d);
  if (action === 'resolve-resource') return state.session.pending ? showPending() : recoveryEditor('resolve_resource', d);
  if (action === 'cancel-detail') return state.session.pending ? showPending() : editor('cancel', null, {run_id: d.run.id});
  if (action === 'pause-detail') {
    if (state.session.pending) return showPending();
    return prepare({tenant_id: d.reference.tenant_id, pause_trigger: {id: d.trigger.definition.id, expected_revision: d.trigger.revision, paused: !d.trigger.definition.paused}});
  }
  if (action === 'tenant-detail') return navigate('jobs', d.reference.tenant_id);
}

document.addEventListener('click', async event => {
  const skip = event.target.closest('.skip-link');
  if (skip) { event.preventDefault(); document.getElementById('main')?.focus(); return; }
  const link = event.target.closest('a[data-record-link]');
  if (link && event.button === 0 && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey) {
    event.preventDefault();
    try { await followRecordLink(link.getAttribute('href')); } catch (err) { showError(err); }
    return;
  }
  const node = event.target.closest('[data-action]');
  if (!node) return;
  try { await act(node.dataset.action, node); } catch (err) { showError(err); }
});
document.addEventListener('input', event => {
  if (event.target.id === 'filter') { state.filter = event.target.value; renderTable(); }
});
document.addEventListener('change', event => {
  if (['mode', 'missed_policy'].includes(event.target.name)) syncConditionalFields();
});
document.addEventListener('submit', async event => {
  const form = event.target;
  const formID = form.getAttribute('id');
  if (!['login-form', 'tenant-form', 'mutation-form', 'record-lookup'].includes(formID)) return;
  event.preventDefault();
  const submit = form.querySelector('button[type="submit"]');
  submit.disabled = true;
  try {
    if (formID === 'login-form') {
      const input = form.elements.namedItem('credential');
      const token = input.value;
      input.value = '';
      try { state.session = await request('session', {token}); await route(); }
      catch (err) { alertIn('login-error', err.message); }
    } else if (formID === 'tenant-form') navigate('jobs', form.elements.namedItem('tenant').value.trim());
    else if (formID === 'record-lookup') {
      const reference = recordReference(state.page, state.tenant, form.elements.namedItem('record_id').value, pages);
      await followRecordLink(referenceHref(reference, state.tenant, pages, state.workerRelease));
    }
    else {
      alertIn('dialog-error', '');
      const context = state.editor;
      if(context.kind==='company_definition') await prepare(buildCompanyMutation(context.record,new FormData(form),context.tenant));
      else if(context.kind==='gateway') {const fields=new FormData(form);if(fields.get('attested')!=='on')throw new Error('Confirm the gateway recovery attestation.');await prepare({gateway:{operation:context.operation,id:context.record.id,expected_epoch:context.record.epoch,reason:String(fields.get('reason')||'').trim(),quiescent:context.operation==='resolve'}});}
      else if (context.kind === 'purge') await prepare(buildPurgeMutation(new FormData(form),context.record));
      else if (['resolve_run', 'resolve_resource'].includes(context.kind)) await prepare(buildRecoveryMutation(context.kind, new FormData(form), context.record));
      else if (context.kind === 'worker_deployment') await prepare(buildDeploymentMutation(new FormData(form), context.record, context.tenant, context.action));
      else if (context.kind === 'worker_enrollment') await prepare(buildEnrollmentMutation(new FormData(form), context.record, context.tenant, context.action));
      else {
        const build = Object.hasOwn(limitPolicies, context.kind) ? buildLimitMutation : buildMutation;
        await prepare(build(context.kind, new FormData(form), context.record, context.tenant));
      }
    }
  } catch (err) { showError(err); }
  finally { submit.disabled = false; }
});
function dialogClosed() {
  clearTimeout(draftTimer);
  if (document.getElementById('mutation-form') && state.session && !state.session.pending) {
    draftDirty = false;
    saveEditorDraft(null).catch(showError);
  }
  clearTimeout(state.deploymentTimer);
  state.deploymentTimer = null;
  state.detailRequest = null; state.references = null; state.history = null; state.detail = null; state.failedReference = null;
  if (state.routeRecord) {
    state.routeRecord = null;
    window.history.replaceState(null, '', routeHref(state.page));
  }
}
dialog.addEventListener('close', () => {
  if (ignoredClosures) { ignoredClosures--; return; }
  dialogClosed();
});
// Drafts are saved in the authenticated session, never localStorage.
let draftTimer, draftDirty = false, draftQueue = Promise.resolve();
function draftStatus(message) {
  const form = document.getElementById('mutation-form');
  if (!form) return;
  let status = form.querySelector('[data-draft-status]');
  if (!status) {
    status = document.createElement('p');
    status.dataset.draftStatus = '';
    status.setAttribute('role', 'status');
    form.append(status);
  }
  status.textContent = message;
}
function captureEditorDraft() {
  const form = document.getElementById('mutation-form');
  if (!form || !state.editor) return null;
  const fields = {};
  for (const element of form.elements) {
    if (!element.name) continue;
    fields[element.name] = element.type === 'checkbox' ? element.checked : element.value;
  }
  return {context: state.editor, fields};
}
function saveEditorDraft(value = captureEditorDraft()) {
  const session = state.session;
  if (!session) return Promise.resolve();
  const save = draftQueue.catch(() => {}).then(async () => {
    if (state.session !== session) return;
    draftStatus('Saving draft…');
    const response = await request('draft', {expected_revision: session.draft_revision || '0', draft: value}, 'PUT');
    if (state.session !== session) return;
    session.draft_revision = response.revision; session.draft = value;
    // A newer input may have arrived while this request was in flight.
    if (JSON.stringify(captureEditorDraft()) === JSON.stringify(value)) {
      draftDirty = false; draftStatus('Draft saved');
    }
  });
  draftQueue = save;
  return save;
}
function restoreEditorDraft() {
  const saved = state.session?.draft, context = saved?.context;
  if (!context || state.session.pending || context.tenant !== state.tenant || !saved.fields) return;
  const kinds = ['definition', 'binding', 'group', 'resource', 'grant', 'tenant', 'trigger', 'launch', 'cancel', 'limits', 'configuration_limits', 'worker_enrollment', 'worker_deployment', 'resolve_run', 'resolve_resource', 'purge', 'company_definition', 'gateway'];
  if (!kinds.includes(context.kind) || !writable()) return;
  if (context.kind === 'worker_enrollment') enrollmentEditor(context.action, context.record);
  else if (context.kind === 'worker_deployment') deploymentEditor(context.action, context.record, context.defaults);
  else if(context.kind==='gateway') {if(!platform())return;state.gateway=context.record;gatewayEditor(context.operation);}
  else if(context.kind==='company_definition') companyEditor(context.record);
  else if(context.kind==='purge') purgeEditor(context.record);
  else editor(context.kind, context.record, context.defaults);
  const form = document.getElementById('mutation-form');
  if (!form) return;
  for (const [name, value] of Object.entries(saved.fields)) {
    const element = form.elements.namedItem(name);
    if (!element || element instanceof RadioNodeList) continue;
    if (element.type === 'checkbox') element.checked = Boolean(value);
    else if (typeof value === 'string') element.value = value;
  }
  syncConditionalFields(); draftDirty = false; draftStatus('Draft restored');
}
function scheduleEditorDraft(event) {
  if (!event.target.closest('#mutation-form') || !state.session) return;
  draftDirty = true; draftStatus('Draft has unsaved changes'); clearTimeout(draftTimer);
  draftTimer = setTimeout(() => saveEditorDraft().catch(error => {
    draftStatus('Draft could not be saved'); showError(error);
  }), 500);
}
document.addEventListener('submit',async event=>{
 if(event.target.id!=='release-plan-form')return;event.preventDefault();
 const form=event.target,fields=new FormData(form),session=state.session;
 try{const plan=await request(form.dataset.rollback==='true'?'release-plans/rollback':'release-plans/build',{tenant_id:state.tenant,source:String(fields.get('source')||''),target:String(fields.get('target')||''),canary:String(fields.get('canary')||''),allow_unqualified:fields.get('allow_unqualified')==='on',allow_unobserved:fields.get('allow_unobserved')==='on'});if(state.session===session&&document.getElementById('release-plan-form')===form)showReleasePlan(plan);}catch(error){if(state.session===session&&document.getElementById('release-plan-form')===form)showError(error);}
});
document.addEventListener('input', scheduleEditorDraft);
document.addEventListener('change', scheduleEditorDraft);
window.addEventListener('beforeunload', event => {
  if (draftDirty) { event.preventDefault(); event.returnValue = ''; }
});
window.addEventListener('hashchange', () => route().catch(showError));
resume();

// Coordinate refresh within this tab; shared-session CAS coordinates replicas.
let identityHeartbeat=false;
setInterval(async()=>{
 const session=state.session;if(!session?.sso||identityHeartbeat)return;
 identityHeartbeat=true;
 try {
  const due=session.oidc_refresh_at&&Date.parse(session.oidc_refresh_at)<=Date.now();
  const current=await request(due?'oidc/refresh':'session',due?{}:undefined,due?'POST':'GET');
  if(state.session===session){state.session={...session,...current};}
 } catch(error){if(state.session===session){if(error.status===401)signIn();else showError(error);}}
 finally{identityHeartbeat=false;}
},20000);

async function loadCluster() {
 const generation=state.generation;
 try {
  const value=await request('cluster/status',{});
  if(state.page!=='cluster'||generation!==state.generation)return;
  state.cluster=value;
  const m=value.membership,p=value.plan;
  const progress=(m.progress||[]).map(row=>'<tr><td>'+e(row.member_id)+'</td><td>'+e(row.match)+'</td><td>'+e(row.recent_active?'Recently active':'No recent activity')+'</td><td>'+e(row.recent_active&&BigInt(row.match)>=BigInt(m.commit)?'Caught up at observation':'Catching up')+'</td></tr>').join('');
  document.querySelector('.toolbar').hidden=true;
  document.getElementById('records').innerHTML=keyValues([['Cluster',code(m.cluster_id)],['Leader',code(m.leader_id)],['Voters',e(m.voters.join(', '))],['Learners',e(m.learners.join(', ')||'None')],['Configuration',code(m.configuration)]])+
   '<p>Refresh to update progress. Promotion checks catch-up again on the leader. Unreachability does not prove volume loss or physical isolation.</p><div class="table-scroll"><table><thead><tr><th>Member</th><th>Matched index</th><th>Activity</th><th>Catch-up</th></tr></thead><tbody>'+progress+'</tbody></table></div>'+
   (p?'<section class="history"><h2>Retained replacement plan</h2>'+keyValues([['Old voter',code(p.old)],['New member',code(p.new)],['Reason',e(p.reason)],['Reviewed digest',code(p.digest)],['State',e(p.complete?'Complete':p.intent?'Reconcile '+p.intent.action:'Ready for first step')]])+'<details><summary>Reviewed topology and exact intent</summary><pre>'+e(json(p))+'</pre></details>'+(p.complete?'':button('cluster-advance','Approve digest and advance one stage'))+'</section>':'')+
   (!p||p.complete?'<form id="cluster-plan-form"><h2>Plan controller replacement</h2><p>Provision and isolate infrastructure separately. This plan adds a fresh learner, promotes it after catch-up, then removes the old nonleader voter. Review each stage before advancing.</p>'+field('old','Old nonleader voter ID','',{required:true})+field('new','Fresh higher catalog ID','',{required:true})+field('reason','Audited reason','',{required:true})+'<button type="submit">Build plan for review</button></form>':'');
  alertIn('page-error','');
 }catch(error){if(state.page==='cluster'&&generation===state.generation)showError(error);}
}
document.addEventListener('submit',async event=>{
 if(event.target.id!=='cluster-plan-form')return;
 event.preventDefault();const form=event.target,session=state.session,fields=new FormData(form),submit=form.querySelector('[type="submit"]');submit.disabled=true;
 try{await request('cluster/plan',{old:String(fields.get('old')),new:String(fields.get('new')),reason:String(fields.get('reason'))});if(state.session===session&&state.page==='cluster')await loadCluster();}catch(error){if(state.session===session)showError(error);}finally{submit.disabled=false;}
});
