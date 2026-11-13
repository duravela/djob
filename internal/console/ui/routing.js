// Record URLs contain identifiers only. Every read still passes through the
// controller's credential and tenant checks.
function identifier(value) {
  if (typeof value !== 'string' || value.trim() === '') return false;
  try { encodeURIComponent(value); } catch { return false; }
  return new TextEncoder().encode(value).length <= 256;
}

export function routeHref(page, tenant = '', id = '', workerRelease = '') {
  const query = new URLSearchParams();
  if (tenant) query.set('tenant', tenant);
  if (id) query.set('id', id);
  if (workerRelease) query.set('worker_release', workerRelease);
  return '#/' + page + (query.size ? '?' + query.toString() : '');
}

export function recordReference(page, tenant, id, pages) {
  const kind = Object.hasOwn(pages, page) ? pages[page].kind : '';
  if (!kind) throw new Error('This view has no durable record identifiers.');
  if (!identifier(id)) throw new Error('Use an exact record ID containing 1 to 256 UTF-8 bytes.');
  if (kind === 'TENANT') return {kind, tenant_id: id};
  if (kind === 'RESOURCE') return {kind, tenant_id: '', id};
  if (!identifier(tenant)) throw new Error('Choose a tenant workspace before opening a record.');
  return {kind, tenant_id: tenant, id};
}

export function referenceHref(reference, workspace, pages, workerRelease = '') {
  if (!reference || typeof reference.kind !== 'string') return null;
  const entry = Object.entries(pages).find(([, config]) => config.kind === reference.kind || config.kinds?.includes(reference.kind));
  if (!entry) return null;
  const [page, config] = entry;
  if (config.kinds) return identifier(reference.tenant_id) ? routeHref(page, reference.tenant_id) : null;
  const id = reference.kind === 'TENANT' ? reference.tenant_id : reference.id;
  const tenant = config.global ? workspace : reference.tenant_id;
  try { recordReference(page, tenant, id, pages); }
  catch { return null; }
  return routeHref(page, tenant, id, reference.kind === 'RUN' ? workerRelease : '');
}

export function readRoute(hash, identity, pages) {
  const platform = identity.role === 'operator' && !identity.tenant_id;
  const fallback = platform ? 'tenants' : 'jobs';
  const result = {page: fallback, tenant: identity.tenant_id || '', reference: null, error: ''};
  try {
    if (typeof hash !== 'string' || hash.length > 4096) throw new Error('Invalid console link.');
    const raw = hash.replace(/^#\/?/, '');
    const separator = raw.indexOf('?');
    const page = separator < 0 ? raw : raw.slice(0, separator);
    const search = separator < 0 ? '' : raw.slice(separator + 1);
    // URLSearchParams alone replaces malformed UTF-8. Reject it rather than
    // accidentally opening a different, replacement-character identifier.
    decodeURIComponent(search.replace(/\+/g, ' '));
    const query = new URLSearchParams(search);
    for (const key of query.keys()) {
      if (!['tenant', 'id', 'worker_release'].includes(key) || query.getAll(key).length !== 1) throw new Error('Use each console link parameter once.');
    }
    const tenant = query.get('tenant') || '';
    if (tenant && !identifier(tenant)) throw new Error('Invalid tenant in console link.');
    result.tenant = identity.tenant_id || tenant;
    if (page) {
      if (!Object.hasOwn(pages, page)) throw new Error('Unknown console view in this link.');
      if (pages[page].global && !platform) throw new Error('This credential cannot open a platform-only view.');
      result.page = page;
    }
    if (identity.tenant_id && tenant && tenant !== identity.tenant_id) throw new Error('This link targets another tenant. Sign in with a credential for that tenant.');
    if (query.has('worker_release')) {
      const release = query.get('worker_release');
      if (result.page !== 'runs' || !result.tenant || !/^[A-Za-z0-9_.-]{1,64}$/.test(release)) throw new Error('Select a valid worker release in the Runs view.');
      result.workerRelease = release;
    }
    if (query.has('id')) {
      if (!page) throw new Error('A record link requires an explicit console view.');
      result.reference = recordReference(result.page, result.tenant, query.get('id'), pages);
    }
  } catch (err) { result.reference = null; delete result.workerRelease; result.error = err instanceof URIError ? 'Invalid encoding in console link.' : err.message; }
  return result;
}

export function mutationTarget(request, tenant = '') {
  if (!request || typeof request !== 'object') return null;
  const scope = request.tenant_id || tenant;
  if (request.worker_id && request.id && ['put', 'scale', 'restart', 'retire', 'publish', 'retiring', 'resume'].includes(request.action)) return {kind: 'WORKER_DEPLOYMENT', tenant_id: scope, id: request.id};
  if (request.worker_id && ['enroll', 'rotate', 'drain', 'resume', 'revoke'].includes(request.action)) return {kind: 'WORKER_ENROLLMENT', tenant_id: scope, id: request.worker_id};
  if (request.purge) return {kind: 'DELETION', tenant_id: scope, id: request.purge.run_id};
  if (request.resolve_run) return {kind: 'RUN', tenant_id: scope, id: request.resolve_run.run_id};
  if (request.resolve_resource) return {kind: 'RESOURCE', tenant_id: '', id: request.resolve_resource.resource_id};
  if (request.put_limits) return {kind: 'LIMITS', tenant_id: scope};
  if (request.put_configuration_limits) return {kind: 'CONFIGURATION_LIMITS', tenant_id: scope};
  if (request.put_tenant) return {kind: 'TENANT', tenant_id: scope};
  if (request.put_resource) return {kind: 'RESOURCE', tenant_id: '', id: request.put_resource.spec?.id};
  for (const [operation, kind, field] of [['put_group', 'GROUP', 'group'], ['put_definition', 'DEFINITION', 'definition'], ['put_binding', 'BINDING', 'binding']]) {
    if (request[operation]) return {kind, tenant_id: scope, id: request[operation][field]?.id};
  }
  for (const [operation, kind, field] of [['put_grant', 'GRANT', 'resource_id'], ['launch', 'RUN', 'run_id'], ['cancel', 'RUN', 'run_id'], ['pause_trigger', 'TRIGGER', 'id']]) {
    if (request[operation]) return {kind, tenant_id: scope, id: request[operation][field]};
  }
  if (request.put_trigger) return {kind: 'TRIGGER', tenant_id: scope, id: request.put_trigger.definition?.id};
  return null;
}
