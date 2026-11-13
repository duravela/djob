import {test, expect} from '@playwright/test';

const controllerURL = () => 'http://127.0.0.1:' + process.env.DJOB_CONSOLE_TEST_HTTP_PORT;
const runLink = (id, tenant = 'alpha') => '/#/runs?' + new URLSearchParams({tenant, id});
async function mutate(page, id, operation, value, tenant = 'alpha', role = 'WRITER') {
  const response = await page.request.post(controllerURL() + '/api/jobs/v1/mutate', {headers: {Authorization: 'Bearer ' + process.env['DJOB_CONSOLE_' + role + '_TOKEN']}, data: {command: {request_id: 'routing-fixture/' + id, at: new Date().toISOString()}, tenant_id: tenant, [operation]: value}});
  expect(response.ok(), await response.text()).toBeTruthy();
}
async function definition(page, id, tenant = 'alpha', role = 'WRITER') {
  await mutate(page, id, 'put_definition', {definition: {id, runtime: 'steps/v1', handler: 'not-deployed-' + tenant, handler_version: '1'}}, tenant, role);
}
async function launch(page, id, definitionID, tenant = 'alpha', role = 'WRITER') {
  await mutate(page, 'launch-' + id, 'launch', {definition_id: definitionID, definition_revision: '1', run_id: id}, tenant, role);
}
async function signIn(page, path = '/', role = 'READER') {
  await page.goto(path);
  await page.getByLabel('API credential', {exact: true}).fill(process.env['DJOB_CONSOLE_' + role + '_TOKEN']);
  await page.getByRole('button', {name: 'Sign in', exact: true}).click();
  await expect(page.getByRole('navigation', {name: 'Management'})).toBeVisible();
}
async function openID(page, id) {
  await page.getByLabel('Open by ID', {exact: true}).fill(id);
  await page.getByRole('button', {name: 'Open record', exact: true}).click();
}

test('record lookup, reload and browser history work beyond the loaded list', async ({page, context}, testInfo) => {
  await definition(page, 'links-pagination');
  for (let i = 0; i < 40; i++) await launch(page, 'links-' + String(i).padStart(3, '0'), 'links-pagination');
  const id = 'zz-run /?&=#%+ Grüße 世界';
  await launch(page, id, 'links-pagination');
  await signIn(page);
  await page.getByRole('link', {name: 'Runs', exact: true}).click();
  const records = page.locator('#records');
  await expect(records.locator('tbody tr')).toHaveCount(32);
  await expect(records.getByRole('link', {name: id, exact: true})).toHaveCount(0);
  await page.getByLabel('Filter loaded records', {exact: true}).fill('links-00');
  const before = await records.innerText();
  await openID(page, id);
  await expect(page.getByRole('dialog').getByRole('heading', {name: id, exact: true})).toBeVisible();
  const bookmark = page.url();
  expect(new URLSearchParams(new URL(bookmark).hash.split('?')[1]).get('id')).toBe(id);
  await page.goBack();
  await expect(page.getByRole('dialog')).not.toBeVisible();
  await expect(page.getByLabel('Filter loaded records', {exact: true})).toHaveValue('links-00');
  expect(await records.innerText()).toBe(before);
  await page.goForward();
  await expect(page.getByRole('dialog').getByRole('heading', {name: id, exact: true})).toBeVisible();
  await page.reload();
  await expect(page.getByRole('dialog').getByRole('heading', {name: id, exact: true})).toBeVisible();
  expect(page.url()).toBe(bookmark);
  const otherTab = await context.newPage();
  await otherTab.goto(bookmark);
  await expect(otherTab.getByRole('dialog').getByRole('heading', {name: id, exact: true})).toBeVisible();
  await otherTab.close();
  await page.getByRole('button', {name: 'Close dialog', exact: true}).click();
  await expect.poll(() => new URLSearchParams(new URL(page.url()).hash.split('?')[1]).has('id')).toBeFalsy();
  await page.getByRole('link', {name: 'Jobs', exact: true}).click();
  await page.getByRole('link', {name: 'links-pagination', exact: true}).click();
  await expect(page.getByRole('dialog').getByRole('heading', {name: 'links-pagination', exact: true})).toBeVisible();
  await page.screenshot({path: testInfo.outputPath('desktop-linked-record.png'), fullPage: true});
  await page.getByRole('button', {name: 'Close dialog', exact: true}).click();
  await page.setViewportSize({width: 390, height: 844});
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
  await page.screenshot({path: testInfo.outputPath('mobile-record-lookup.png'), fullPage: true});
  await page.locator('.skip-link').focus();
  const beforeSkip = page.url();
  await page.keyboard.press('Enter');
  await expect(page.locator('#main')).toBeFocused();
  expect(page.url()).toBe(beforeSkip);
});

test('a foreign-tenant link cannot silently open the same ID in the signed-in tenant', async ({page}) => {
  await mutate(page, 'beta', 'put_tenant', {max_running: '2'}, 'beta', 'PLATFORM');
  for (const tenant of ['alpha', 'beta']) {
    await definition(page, 'links-shared-definition', tenant, 'PLATFORM');
    await launch(page, 'links-shared-id', 'links-shared-definition', tenant, 'PLATFORM');
  }
  const opened = [];
  page.on('request', request => {
    if (request.url().endsWith('/query/get')) opened.push(request.postDataJSON().reference);
  });
  await signIn(page, runLink('links-shared-id', 'beta'));
  await expect(page.locator('#page-error')).toContainText('This link targets another tenant.');
  await expect(page.getByRole('dialog')).not.toBeVisible();
  expect(opened).toEqual([]);
  await openID(page, 'links-shared-id');
  await expect(page.getByRole('dialog').getByRole('heading', {name: 'links-shared-id', exact: true})).toBeVisible();
  expect(opened.every(reference => reference.tenant_id === 'alpha')).toBeTruthy();
  await page.getByText('Pinned execution configuration', {exact: true}).click();
  await expect(page.getByRole('dialog')).toContainText('not-deployed-alpha@1');
  await page.getByRole('button', {name: 'Close dialog', exact: true}).click();
  await page.goto('/#/resources?id=links-shared-id');
  await expect(page.locator('#page-error')).toContainText('platform-only');
  await expect(page.getByRole('dialog')).not.toBeVisible();
  await page.getByRole('button', {name: 'Sign out', exact: true}).click();
  await signIn(page, runLink('links-shared-id', 'beta'), 'PLATFORM');
  await expect(page.getByRole('dialog').getByRole('heading', {name: 'links-shared-id', exact: true})).toBeVisible();
  await page.getByText('Pinned execution configuration', {exact: true}).click();
  await expect(page.getByRole('dialog')).toContainText('not-deployed-beta@1');
});

test('known records remain reachable when listing fails and late reads cannot reopen a closed target', async ({page}) => {
  await definition(page, 'links-late-definition');
  let failList = true;
  await page.route('**/api/console/v1/query/list', async route => {
    if (route.request().postDataJSON().kind === 'RUN' && failList) {
      failList = false; await route.fulfill({status: 503, json: {error: 'Injected unavailable run list'}}); return;
    }
    await route.continue();
  });
  await signIn(page, runLink('links-created-after-read'));
  const dialog = page.getByRole('dialog');
  await expect(dialog.getByRole('heading', {name: 'Record unavailable', exact: true})).toBeVisible();
  await expect(dialog).toContainText('does not establish the outcome of an uncertain mutation');
  await launch(page, 'links-created-after-read', 'links-late-definition');
  await dialog.getByRole('button', {name: 'Retry read', exact: true}).click();
  await expect(dialog.getByRole('heading', {name: 'links-created-after-read', exact: true})).toBeVisible();
  await page.getByRole('button', {name: 'Close dialog', exact: true}).click();
  await expect(page.getByRole('heading', {name: 'No records loaded', exact: true})).toBeVisible();
  await page.getByRole('button', {name: 'Refresh', exact: true}).click();
  await expect(page.locator('#records tbody tr')).toHaveCount(32);
  await launch(page, 'links-delay-read', 'links-late-definition');
  let release, received;
  const gate = new Promise(done => { release = done; });
  const held = new Promise(done => { received = done; });
  await page.route('**/api/console/v1/query/get', async route => {
    if (route.request().postDataJSON().reference.id === 'links-delay-read') {
      const response = await route.fetch(); received(); await gate; await route.fulfill({response}); return;
    }
    await route.continue();
  });
  await openID(page, 'links-delay-read');
  await held;
  await expect(dialog.getByRole('heading', {name: 'Loading record', exact: true})).toBeVisible();
  await page.keyboard.press('Escape');
  await openID(page, 'links-created-after-read');
  await expect(dialog.getByRole('heading', {name: 'links-created-after-read', exact: true})).toBeVisible();
  const late = page.waitForResponse(response => response.url().endsWith('/query/get') && response.request().postDataJSON().reference.id === 'links-delay-read');
  release(); await late;
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  await expect(dialog.getByRole('heading', {name: 'links-created-after-read', exact: true})).toBeVisible();
});

test('inspect an uncertain target and open its confirmed result without replacing the retry envelope', async ({page}) => {
  await definition(page, 'links-uncertain-definition');
  await signIn(page, '/', 'WRITER');
  const origin = new URL(page.url()).origin;
  const session = await (await page.request.get(origin + '/api/console/v1/session')).json();
  const response = await page.request.post(origin + '/api/console/v1/mutations', {headers: {Origin: origin, 'X-Djob-CSRF': session.csrf}, data: {tenant_id: 'alpha', launch: {definition_id: 'links-uncertain-definition', definition_revision: '1'}}});
  expect(response.ok()).toBeTruthy();
  const prepared = await response.json();
  // UI fault injection; the Go suite separately loses real HTTP acknowledgements.
  await page.route('**/api/console/v1/session', async route => {
    const response = await route.fetch(), data = await response.json();
    data.pending.state = 'uncertain'; await route.fulfill({response, json: data});
  }, {times: 1});
  let commits = 0;
  page.on('request', request => { if (request.url().endsWith('/mutations/commit')) commits++; });
  await page.reload();
  await page.getByRole('button', {name: 'Open review', exact: true}).click();
  const dialog = page.getByRole('dialog');
  const envelope = await dialog.locator('pre').innerText();
  await dialog.getByRole('link', {name: 'Inspect target', exact: true}).click();
  await expect(dialog.getByRole('heading', {name: 'Record unavailable', exact: true})).toBeVisible();
  expect(commits).toBe(0);
  const retained = await (await page.request.get(origin + '/api/console/v1/session')).json();
  expect(retained.pending.request_id).toBe(prepared.request_id);
  expect(retained.pending.request).toEqual(prepared.request);
  await dialog.getByRole('button', {name: 'Open review', exact: true}).click();
  await expect(dialog.locator('pre')).toHaveText(envelope);
  await dialog.getByRole('button', {name: 'Retry same request', exact: true}).click();
  await expect(dialog.getByRole('heading', {name: 'Change confirmed', exact: true})).toBeVisible();
  await dialog.getByRole('link', {name: 'Open result', exact: true}).click();
  await expect(dialog.getByRole('heading', {name: prepared.request.launch.run_id, exact: true})).toBeVisible();
  expect(commits).toBe(1);
  await dialog.getByRole('button', {name: 'Open review', exact: true}).click();
  await dialog.getByRole('button', {name: 'Done', exact: true}).click();
  await expect(dialog).not.toBeVisible();
});
