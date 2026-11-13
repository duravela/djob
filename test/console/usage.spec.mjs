import {test, expect} from '@playwright/test';

const controllerURL = () => 'http://127.0.0.1:' + process.env.DJOB_CONSOLE_TEST_HTTP_PORT;
const usagePath = tenant => '/#/usage?' + new URLSearchParams({tenant});
async function mutate(page, tenant, id, operation, value, expected = 200, role = 'PLATFORM') {
  const response = await page.request.post(controllerURL() + '/api/jobs/v1/mutate', {headers: {Authorization: 'Bearer ' + process.env['DJOB_CONSOLE_' + role + '_TOKEN']}, data: {command: {request_id: 'usage-fixture/' + id, at: new Date().toISOString()}, tenant_id: tenant, [operation]: value}});
  expect(response.status(), await response.text()).toBe(expected);
  return response.json();
}
async function get(page, tenant, kind, id) {
  const response = await page.request.post(controllerURL() + '/api/jobs/v1/get', {headers: {Authorization: 'Bearer ' + process.env.DJOB_CONSOLE_PLATFORM_TOKEN}, data: {reference: {kind, tenant_id: tenant, ...(id ? {id} : {})}}});
  expect(response.ok(), await response.text()).toBeTruthy();
  return response.json();
}
async function seed(page, tenant) {
  await mutate(page, tenant, tenant, 'put_tenant', {max_running: '4'});
  await mutate(page, tenant, tenant + '-job', 'put_definition', {definition: {id: 'queued', runtime: 'steps/v1', handler: 'not-deployed', handler_version: '1'}});
  await mutate(page, tenant, tenant + '-run', 'launch', {definition_id: 'queued', definition_revision: '1', run_id: 'pending-one'});
}
async function signIn(page, tenant, role = 'PLATFORM') {
  await page.goto(usagePath(tenant));
  await page.getByLabel('API credential', {exact: true}).fill(process.env['DJOB_CONSOLE_' + role + '_TOKEN']);
  await page.getByRole('button', {name: 'Sign in', exact: true}).click();
  await expect(page.getByRole('heading', {name: 'Usage & limits', exact: true})).toBeVisible();
}
async function review(page) {
  await page.getByRole('button', {name: 'Review request', exact: true}).click();
  await expect(page.getByRole('heading', {name: 'Review changes', exact: true})).toBeVisible();
  return JSON.parse(await page.getByRole('dialog').locator('pre').innerText());
}
async function confirm(page, confirmed = true) {
  await page.getByRole('button', {name: 'Confirm and submit', exact: true}).click();
  await expect(page.getByRole('heading', {name: confirmed ? 'Change confirmed' : 'Request rejected', exact: true})).toBeVisible();
}
async function done(page) {
  await page.getByRole('button', {name: 'Done', exact: true}).click();
  await expect(page.getByRole('dialog')).not.toBeVisible();
}

test('platform reviews quota replacement, sees admission failures and resolves a revision conflict', async ({page}, testInfo) => {
  const tenant = 'usage-platform';
  await seed(page, tenant);
  await signIn(page, tenant);
  const runTable = page.getByRole('table', {name: 'Run admission & retained data', exact: true});
  const pending = runTable.getByRole('row').filter({has: page.getByRole('rowheader', {name: /^Pending runs/})});
  await expect(pending.getByRole('cell').nth(0)).toHaveText('1');
  await expect(pending.getByRole('cell').nth(1)).toHaveText('Unlimited');
  await page.getByRole('button', {name: 'Edit run limits', exact: true}).click();
  await page.getByLabel('Pending runs limit', {exact: true}).fill('1');
  await page.getByLabel('Stored runs limit', {exact: true}).fill('18446744073709551615');
  const first = await review(page);
  expect(first.put_limits.expected_revision ?? '0').toBe('0');
  expect(first.put_limits.limits.max_stored_runs).toBe('18446744073709551615');
  expect((await get(page, tenant, 'LIMITS')).limits.revision ?? '0').toBe('0');
  await page.reload();
  await page.getByRole('button', {name: 'Open review', exact: true}).click();
  expect(JSON.parse(await page.getByRole('dialog').locator('pre').innerText())).toEqual(first);
  await confirm(page);
  await page.getByRole('link', {name: 'Open result', exact: true}).click();
  await expect(page.getByRole('dialog')).not.toBeVisible();
  await expect(pending.getByRole('cell').nth(1)).toHaveText('1');
  await expect(pending).toContainText('At limit');
  await page.getByRole('button', {name: 'Open review', exact: true}).click();
  await done(page);
  await mutate(page, tenant, 'run-refused', 'launch', {definition_id: 'queued', definition_revision: '1', run_id: 'pending-two'}, 429);

  await page.getByRole('button', {name: 'Edit run limits', exact: true}).click();
  await page.getByLabel('Pending runs limit', {exact: true}).fill('2');
  const stale = await review(page);
  expect(stale.put_limits.expected_revision).toBe('1');
  await mutate(page, tenant, 'concurrent-policy', 'put_limits', {expected_revision: '1', limits: {max_pending_runs: '3'}});
  await confirm(page, false);
  const retained = JSON.parse(await page.getByRole('dialog').locator('pre').first().innerText());
  expect(retained).toEqual(stale);
  expect((await get(page, tenant, 'LIMITS')).limits.revision).toBe('2');
  await done(page);
  await page.getByRole('button', {name: 'Edit run limits', exact: true}).click();
  await expect(page.getByLabel('Pending runs limit', {exact: true})).toHaveValue('3');
  await expect(page.getByLabel('Stored runs limit', {exact: true})).toHaveValue('0');
  await page.getByLabel('Pending runs limit', {exact: true}).fill('2');
  const updated = await review(page);
  expect(updated.put_limits.expected_revision).toBe('2');
  expect(updated.command.request_id).not.toBe(stale.command.request_id);
  await confirm(page); await done(page);
  await mutate(page, tenant, 'run-admitted', 'launch', {definition_id: 'queued', definition_revision: '1', run_id: 'pending-two'});

  await page.getByRole('button', {name: 'Edit configuration limits', exact: true}).click();
  await page.getByLabel('Job definitions limit', {exact: true}).fill('1');
  const configuration = await review(page);
  expect(configuration.put_configuration_limits.limits.max_definitions).toBe('1');
  await confirm(page); await done(page);
  await mutate(page, tenant, 'definition-refused', 'put_definition', {definition: {id: 'second', runtime: 'steps/v1', handler: 'not-deployed', handler_version: '1'}}, 429);
  await expect(pending.getByRole('cell').nth(0)).toHaveText('2');
  await expect(pending).toContainText('At limit');
  const configurationTable = page.getByRole('table', {name: 'Configuration', exact: true});
  await expect(configurationTable.getByRole('row').filter({has: page.getByRole('rowheader', {name: /^Job definitions/})})).toContainText('At limit');
  await page.screenshot({path: testInfo.outputPath('desktop-usage-limits.png'), fullPage: true});
  await page.setViewportSize({width: 390, height: 844});
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
  await page.screenshot({path: testInfo.outputPath('mobile-usage-limits.png'), fullPage: true});
});

test('tenant readers and writers inspect usage without policy editing authority', async ({page}) => {
  const references = [];
  page.on('request', request => { if (request.url().endsWith('/query/get')) references.push(request.postDataJSON().reference); });
  await signIn(page, 'alpha', 'READER');
  await expect(page.getByRole('table', {name: 'Configuration', exact: true})).not.toContainText('Not read');
  await expect(page.getByRole('button', {name: /^Edit .*limits$/})).toHaveCount(0);
  await expect(page.getByLabel('Filter loaded records', {exact: true})).toHaveCount(0);
  await expect(page.getByLabel('Open by ID', {exact: true})).toHaveCount(0);
  expect(references).toHaveLength(4);
  expect(references.every(reference => reference.tenant_id === 'alpha')).toBeTruthy();
  await page.getByRole('button', {name: 'Sign out', exact: true}).click();
  await signIn(page, 'alpha', 'WRITER');
  await expect(page.getByRole('table', {name: 'Run admission & retained data', exact: true})).not.toContainText('Not read');
  await expect(page.getByRole('button', {name: /^Edit .*limits$/})).toHaveCount(0);
  const session = await (await page.request.get('/api/console/v1/session')).json();
  const denied = await page.request.post('/api/console/v1/mutations', {headers: {'X-Djob-CSRF': session.csrf, Origin: new URL(page.url()).origin}, data: {tenant_id: 'alpha', put_limits: {limits: {max_pending_runs: '1'}}}});
  expect(denied.status()).toBe(403);
});

test('partial usage reads remain unknown and late readings cannot replace another workspace', async ({page}) => {
  const tenant = 'usage-partial', other = 'usage-other';
  await seed(page, tenant);
  await mutate(page, tenant, 'partial-limits', 'put_limits', {limits: {max_pending_runs: '5'}});
  await mutate(page, other, other, 'put_tenant', {max_running: '2'});
  let fail = true, delay = false, release, observed;
  const delayed = new Promise(resolve => { observed = resolve; });
  const releaseRead = new Promise(resolve => { release = resolve; });
  const calls = [];
  await page.route('**/api/console/v1/query/get', async route => {
    const reference = route.request().postDataJSON().reference;
    calls.push(reference);
    if (reference.kind === 'LIMITS' && fail) return route.fulfill({status: 503, json: {error: 'Limits policy unavailable'}});
    if (reference.kind === 'USAGE' && reference.tenant_id === tenant && delay) {
      const response = await route.fetch(); observed(); await releaseRead;
      await route.fulfill({response}); return;
    }
    await route.continue();
  });
  await signIn(page, tenant);
  const runTable = page.getByRole('table', {name: 'Run admission & retained data', exact: true});
  const pending = runTable.getByRole('row').filter({has: page.getByRole('rowheader', {name: /^Pending runs/})});
  await expect(page.getByRole('alert')).toContainText('Limits policy unavailable');
  await expect(pending.getByRole('cell').nth(0)).toHaveText('1');
  await expect(pending.getByRole('cell').nth(1)).toHaveText('Not read');
  await expect(page.getByRole('button', {name: 'Edit run limits', exact: true})).toBeDisabled();
  await expect(page.getByRole('button', {name: 'Edit configuration limits', exact: true})).toBeEnabled();
  expect(calls).toHaveLength(4);
  fail = false;
  await page.getByRole('button', {name: 'Retry read', exact: true}).click();
  await expect(pending.getByRole('cell').nth(1)).toHaveText('5');
  await expect(pending).toContainText('Within limit');
  expect(calls).toHaveLength(5);
  delay = true;
  await page.getByRole('button', {name: 'Refresh', exact: true}).click(); await delayed;
  await page.getByLabel('Tenant ID', {exact: true}).fill(other);
  await page.locator('#tenant-form').getByRole('button', {name: 'Open', exact: true}).click();
  await page.getByRole('link', {name: 'Usage & limits', exact: true}).click();
  await expect(pending.getByRole('cell').nth(0)).toHaveText('0');
  await expect(pending.getByRole('cell').nth(1)).toHaveText('Unlimited');
  const lateResponse = page.waitForResponse(response => response.url().endsWith('/query/get') && response.request().postDataJSON().reference.kind === 'USAGE' && response.request().postDataJSON().reference.tenant_id === tenant);
  release();
  await (await lateResponse).finished();
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  await expect(pending.getByRole('cell').nth(0)).toHaveText('0');
  await expect(page.getByRole('alert')).toHaveCount(0);
  await expect(page.getByLabel('Tenant ID', {exact: true})).toHaveValue(other);
});
