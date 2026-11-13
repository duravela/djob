import {test, expect} from '@playwright/test';
const controller = () => 'http://127.0.0.1:' + process.env.DJOB_CONSOLE_TEST_HTTP_PORT;
const target = () => 'http://127.0.0.1:' + process.env.DJOB_CONSOLE_TEST_TARGET_PORT;
const bytes = value => Buffer.from(JSON.stringify(value)).toString('base64');
async function mutate(page, id, operation, value) {
  const response = await page.request.post(controller() + '/api/jobs/v1/mutate', {headers: {Authorization: 'Bearer ' + process.env.DJOB_CONSOLE_PLATFORM_TOKEN}, data: {tenant_id: operation === 'put_resource' ? '' : 'alpha', command: {request_id: 'recovery-fixture/' + id, at: new Date().toISOString()}, [operation]: value}});
  expect(response.ok(), await response.text()).toBeTruthy();
}
async function getRun(page, id) {
  const response = await page.request.post(controller() + '/api/jobs/v1/get', {headers: {Authorization: 'Bearer ' + process.env.DJOB_CONSOLE_READER_TOKEN}, data: {reference: {kind: 'RUN', tenant_id: 'alpha', id}}});
  expect(response.ok()).toBeTruthy(); return (await response.json()).run;
}
async function unknown(page, id) {
  await mutate(page, id + '-binding', 'put_binding', {binding: {id, driver: 'http', driver_version: '1', config: bytes({operations: [{name: 'execute', method: 'POST', url: target() + '/effects/uncertain'}]})}});
  await mutate(page, id + '-definition', 'put_definition', {definition: {id, runtime: 'steps/v1', handler: 'invoke', handler_version: '1', binding_id: id, input: bytes({command: 'synthetic'})}});
  await mutate(page, id + '-launch', 'launch', {definition_id: id, definition_revision: '1', run_id: id});
  await expect.poll(async () => (await getRun(page, id)).status).toBe('unknown');
}
async function signIn(page, id, role = 'PLATFORM') {
  await page.goto('/#/runs?tenant=alpha&id=' + id);
  await page.getByLabel('API credential', {exact: true}).fill(process.env['DJOB_CONSOLE_' + role + '_TOKEN']);
  await page.getByRole('button', {name: 'Sign in', exact: true}).click();
  await expect(page.getByRole('dialog').getByRole('heading', {name: id, exact: true})).toBeVisible();
}
async function prepare(page) {
  await page.getByRole('button', {name: 'Resolve unknown outcome', exact: true}).click();
  await expect(page.getByLabel('Expected checkpoint', {exact: true})).toHaveAttribute('readonly', '');
  await page.getByLabel('Confirmed outcome', {exact: true}).selectOption('failed');
  await page.getByLabel('Evidence and reason', {exact: true}).fill('Device log independently confirms the command failed.');
  await page.getByLabel('Verified result (optional)', {exact: true}).fill('Reconciled device log');
  await page.getByLabel('I independently verified this terminal outcome.', {exact: true}).check();
  await page.getByRole('button', {name: 'Review request', exact: true}).click();
  await expect(page.getByRole('heading', {name: 'Review changes', exact: true})).toBeVisible();
  return JSON.parse(await page.getByRole('dialog').locator('pre').first().innerText());
}

test('operator resolves a real unknown outcome and inspects its audit without replaying the effect', async ({page, browser}, testInfo) => {
  const id = 'recovery-verified';
  await unknown(page, id);
  const before = await getRun(page, id);
  const counts = await (await page.request.get(target() + '/counts')).json();
  const reader = await browser.newContext();
  const readerPage = await reader.newPage();
  await signIn(readerPage, id, 'READER');
  await expect(readerPage.getByRole('button', {name: 'Resolve unknown outcome', exact: true})).toHaveCount(0);
  await expect(readerPage.getByRole('button', {name: 'Request cancellation', exact: true})).toHaveCount(0);
  await reader.close();
  await signIn(page, id);
  const q = await prepare(page);
  expect(q.resolve_run.expected_step || '0').toBe(before.step || '0');
  expect(q.resolve_run.status).toBe('failed');
  const explanation = await page.getByRole('dialog').locator('.dialog-body > p').nth(1).boundingBox();
  const header = await page.getByRole('dialog').locator('.dialog-heading').boundingBox();
  expect(explanation.y).toBeGreaterThanOrEqual(header.y + header.height);
  await page.screenshot({path: testInfo.outputPath('desktop-recovery-review.png'), fullPage: true});
  await page.getByRole('button', {name: 'Confirm and submit', exact: true}).click();
  await expect(page.getByRole('heading', {name: 'Change confirmed', exact: true})).toBeVisible();
  let release, received;
  const gate = new Promise(done => { release = done; });
  const held = new Promise(done => { received = done; });
  await page.route('**/api/console/v1/query/get', async route => {
    if (route.request().postDataJSON().reference.kind !== 'RESOLUTION') return route.continue();
    const response = await route.fetch(); received();
    await gate; await route.fulfill({response});
  }, {times: 1});
  await page.getByRole('button', {name: 'Inspect recovery audit', exact: true}).click();
  await held;
  await page.getByRole('button', {name: 'Close dialog', exact: true}).click();
  await page.getByRole('button', {name: 'Open review', exact: true}).click();
  const late = page.waitForResponse(response => response.url().endsWith('/query/get') && response.request().postDataJSON().reference.kind === 'RESOLUTION');
  release(); await late;
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  await expect(page.getByRole('heading', {name: 'Change confirmed', exact: true})).toBeVisible();
  await page.getByRole('button', {name: 'Inspect recovery audit', exact: true}).click();
  await expect(page.getByRole('heading', {name: 'Recovery audit', exact: true})).toBeVisible();
  await expect(page.getByRole('dialog')).toContainText('console/platform');
  await expect(page.getByRole('dialog')).toContainText(q.resolve_run.reason);
  await expect(page.getByRole('dialog')).toContainText(q.command.request_id);
  await page.setViewportSize({width: 390, height: 844});
  expect(await page.getByRole('dialog').evaluate(node => node.scrollWidth <= node.clientWidth)).toBeTruthy();
  await page.screenshot({path: testInfo.outputPath('mobile-recovery-audit.png'), fullPage: true});
  await page.getByRole('button', {name: 'Back to review', exact: true}).click();
  await page.getByRole('button', {name: 'Done', exact: true}).click();
  const after = await getRun(page, id);
  expect(after.status).toBe('failed');
  expect(BigInt(after.step)).toBe(BigInt(before.step || '0') + 1n);
  expect(after.result).toBe(Buffer.from('Reconciled device log').toString('base64'));
  expect((await (await page.request.get(target() + '/counts')).json())['/effects/uncertain']).toBe(counts['/effects/uncertain']);
  const action = await page.request.post(controller() + '/api/jobs/v1/get', {headers: {Authorization: 'Bearer ' + process.env.DJOB_CONSOLE_READER_TOKEN}, data: {reference: {kind: 'ACTION', tenant_id: 'alpha', run_id: id, sequence: '1'}}});
  expect((await action.json()).action.status).toBe('unknown');

});

test('a stale reviewed recovery is rejected without rebasing its checkpoint', async ({page}) => {
  const id = 'recovery-stale';
  await unknown(page, id);
  await signIn(page, id);
  const q = await prepare(page);
  await mutate(page, id + '-elsewhere', 'resolve_run', {...q.resolve_run, status: 'cancelled', reason: 'Independently reconciled by another operator'});
  await page.getByRole('button', {name: 'Confirm and submit', exact: true}).click();
  await expect(page.getByRole('heading', {name: 'Request rejected', exact: true})).toBeVisible();
  expect(JSON.parse(await page.getByRole('dialog').locator('pre').first().innerText())).toEqual(q);
  expect((await getRun(page, id)).status).toBe('cancelled');
});


test('resource recovery binds a global epoch and rejects a stale blocked snapshot', async ({page}) => {
  const id = 'recovery-resource-stale';
  await mutate(page, id, 'put_resource', {spec: {id, duplex: 'half', capacity: 1}});
  await page.route('**/api/console/v1/query/get', async route => {
    const reference = route.request().postDataJSON().reference;
    if (reference.kind === 'RESOURCE' && reference.id === id) {
      // A lagging replica can still report an old blocked epoch.
      await route.fulfill({json: {reference: {kind: 'RESOURCE', id}, resource: {spec: {id, duplex: 'half', capacity: 1}, state: 'blocked', epoch: '9007199254740993'}}});
      return;
    }
    await route.continue();
  });
  await page.goto('/#/resources?tenant=alpha&id=' + id);
  await page.getByLabel('API credential', {exact: true}).fill(process.env.DJOB_CONSOLE_PLATFORM_TOKEN);
  await page.getByRole('button', {name: 'Sign in', exact: true}).click();
  await page.getByRole('button', {name: 'Confirm quiescence', exact: true}).click();
  await expect(page.getByLabel('Expected resource epoch', {exact: true})).toHaveValue('9007199254740993');
  await expect(page.getByLabel('Expected resource epoch', {exact: true})).toHaveAttribute('readonly', '');
  await page.getByLabel('Evidence and reason', {exact: true}).fill('Independently verified prior access and device activity have stopped.');
  await page.getByLabel('I independently verified that prior access and device activity have stopped.', {exact: true}).check();
  await page.getByRole('button', {name: 'Review request', exact: true}).click();
  const q = JSON.parse(await page.getByRole('dialog').locator('pre').first().innerText());
  expect(q.tenant_id).toBeUndefined();
  expect(q.resolve_resource).toEqual({resource_id: id, expected_epoch: '9007199254740993', reason: 'Independently verified prior access and device activity have stopped.', quiescent: true});
  await page.getByRole('button', {name: 'Confirm and submit', exact: true}).click();
  await expect(page.getByRole('heading', {name: 'Request rejected', exact: true})).toBeVisible();
  expect(JSON.parse(await page.getByRole('dialog').locator('pre').first().innerText())).toEqual(q);
  const actual = await page.request.post(controller() + '/api/jobs/v1/get', {headers: {Authorization: 'Bearer ' + process.env.DJOB_CONSOLE_PLATFORM_TOKEN}, data: {reference: {kind: 'RESOURCE', id}}});
  const resource = (await actual.json()).resource;
  expect(resource.state).toBe('available');
  expect(resource.epoch || '0').toBe('0');
});
