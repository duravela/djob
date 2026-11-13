import {test, expect} from '@playwright/test';

const controllerURL = () => 'http://127.0.0.1:' + process.env.DJOB_CONSOLE_TEST_HTTP_PORT;
const targetURL = () => 'http://127.0.0.1:' + process.env.DJOB_CONSOLE_TEST_TARGET_PORT;
const bytes = value => Buffer.from(typeof value === 'string' ? value : JSON.stringify(value)).toString('base64');

async function mutate(page, id, operation, value, platform = false) {
  const response = await page.request.post(controllerURL() + '/api/jobs/v1/mutate', {
    headers: {Authorization: 'Bearer ' + process.env['DJOB_CONSOLE_' + (platform ? 'PLATFORM' : 'WRITER') + '_TOKEN']},
    data: {command: {request_id: 'history-fixture/' + id, at: new Date().toISOString()}, tenant_id: 'alpha', [operation]: value},
  });
  expect(response.ok(), await response.text()).toBeTruthy();
}
async function waitRun(page, id, status) {
  await expect.poll(async () => {
    const response = await page.request.post(controllerURL() + '/api/jobs/v1/get', {headers: {Authorization: 'Bearer ' + process.env.DJOB_CONSOLE_READER_TOKEN}, data: {reference: {kind: 'RUN', tenant_id: 'alpha', id}}});
    return response.ok() ? (await response.json()).run.status : String(response.status());
  }).toBe(status);
}
async function signIn(page) {
  await page.goto('/');
  await page.getByLabel('API credential', {exact: true}).fill(process.env.DJOB_CONSOLE_READER_TOKEN);
  await page.getByRole('button', {name: 'Sign in', exact: true}).click();
  await page.getByRole('link', {name: 'Runs', exact: true}).click();
}
async function binding(page, id, path) {
  await mutate(page, id + '-binding', 'put_binding', {binding: {id, driver: 'http', driver_version: '1', config: bytes({operations: [{name: 'request', method: 'POST', url: targetURL() + path}, {name: 'execute', method: 'POST', url: targetURL() + path}]})}}, true);
}

test('inspect a real request/reply conversation while retaining run context', async ({page}, testInfo) => {
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await binding(page, 'history-http', '/effects/history-conversation');
  await mutate(page, 'conversation-definition', 'put_definition', {definition: {id: 'history-conversation', runtime: 'steps/v1', handler: 'request-reply', handler_version: '1', binding_id: 'history-http', input: bytes({correlation_key: 'history-reply', payload: bytes({command: 'probe'}), timeout_ns: 120000000000})}});
  await mutate(page, 'conversation-launch', 'launch', {definition_id: 'history-conversation', definition_revision: '1', run_id: 'history-conversation'});
  await waitRun(page, 'history-conversation', 'waiting');
  await mutate(page, 'unrelated-signal', 'signal', {run_id: 'history-conversation', event_id: 'a-unrelated', key: 'other-key', data: bytes({ignored: true})});
  await signIn(page);
  await page.getByRole('link', {name: 'history-conversation', exact: true}).click();
  const dialog = page.getByRole('dialog');
  const history = page.locator('#history-browser');
  await expect(dialog).toContainText('The run is waiting for a matching signal');
  await expect(dialog.getByRole('heading', {name: 'Wait state', exact: true})).toBeVisible();
  await expect(dialog).toContainText('history-reply');
  await mutate(page, 'reply-signal', 'signal', {run_id: 'history-conversation', event_id: 'z-reply', key: 'history-reply', data: bytes({answer: 42})});
  await waitRun(page, 'history-conversation', 'succeeded');
  await dialog.getByRole('button', {name: 'Refresh', exact: true}).click();
  await expect(history.getByRole('button', {name: 'Action 1', exact: true})).toBeVisible();
  await expect(dialog).toContainText('The run recorded successful completion.');
  await history.getByRole('button', {name: 'Action 1', exact: true}).click();
  await expect(history.getByRole('heading', {name: 'Action 1', exact: true})).toBeVisible();
  await expect(history.getByText('Recorded interval', {exact: true})).toBeVisible();
  await history.getByText('Action input · json', {exact: true}).click();
  await expect(history).toContainText('"command":"probe"');
  await history.getByText('Action result · json', {exact: true}).click();
  await expect(history).toContainText('"accepted":true');
  await history.getByRole('button', {name: 'Open owning attempt', exact: true}).click();
  await expect(history.getByRole('heading', {name: /^Attempt \d+$/})).toBeVisible();
  await expect(dialog.getByRole('heading', {name: 'history-conversation', exact: true})).toBeVisible();
  await history.getByRole('button', {name: 'Back to history', exact: true}).click();
  await history.getByRole('button', {name: 'Signals', exact: true}).click();
  const signals = history.getByRole('table', {name: 'Signal records', exact: true});
  await expect(signals.locator('tbody tr')).toHaveCount(2);
  await expect(signals).toContainText('consumed');
  await expect(signals).toContainText('queued');
  await history.getByRole('button', {name: 'Signal z-reply', exact: true}).click();
  await history.getByText('Signal data · json', {exact: true}).click();
  await expect(history).toContainText('"answer":42');
  await history.getByRole('button', {name: 'Refresh record', exact: true}).click();
  await expect(history.getByRole('button', {name: 'Refresh record', exact: true})).toBeEnabled();
  const counts = await (await page.request.get(targetURL() + '/counts')).json();
  expect(counts['/effects/history-conversation']).toBe(1);
  await history.getByRole('button', {name: 'Back to history', exact: true}).click();
  await history.getByRole('button', {name: 'Timeline', exact: true}).click();
  await page.screenshot({path: testInfo.outputPath('desktop-history.png'), fullPage: true});
  await page.setViewportSize({width: 390, height: 844});
  expect(await dialog.evaluate(node => node.scrollWidth <= node.clientWidth)).toBeTruthy();
  await page.screenshot({path: testInfo.outputPath('mobile-history.png'), fullPage: true});
  await page.getByRole('button', {name: 'Close dialog', exact: true}).click();
  await mutate(page, 'failure-definition', 'put_definition', {definition: {id: 'history-failure', runtime: 'steps/v1', handler: 'invoke', handler_version: '1'}});
  await mutate(page, 'failure-launch', 'launch', {definition_id: 'history-failure', definition_revision: '1', run_id: 'history-failure'});
  await waitRun(page, 'history-failure', 'failed');
  await page.getByRole('button', {name: 'Refresh', exact: true}).click();
  await page.getByRole('link', {name: 'history-failure', exact: true}).click();
  await expect(dialog.getByRole('region', {name: 'Recorded run error', exact: true})).toContainText('invoke requires a binding');
  expect(errors).toEqual([]);
});

test('history pagination, source failures and late responses preserve the selected run', async ({page}) => {
  for (const id of ['history-pages', 'history-other']) {
    await mutate(page, id + '-definition', 'put_definition', {definition: {id, runtime: 'steps/v1', handler: 'not-deployed', handler_version: '1'}});
    await mutate(page, id + '-launch', 'launch', {definition_id: id, definition_revision: '1', run_id: id});
  }
  for (let i = 0; i < 35; i++) {
    const id = 'signal-' + String(i).padStart(2, '0');
    await mutate(page, 'pages-' + id, 'signal', {run_id: 'history-pages', event_id: id, key: 'reply', data: bytes({i})});
  }
  let failActions = true;
  await page.route('**/api/console/v1/query/list', async route => {
    const q = route.request().postDataJSON();
    if (q.run_id === 'history-pages' && q.kind === 'ACTION' && failActions) {
      failActions = false;
      await route.fulfill({status: 503, json: {error: 'Injected unavailable action history'}}); return;
    }
    await route.continue();
  });
  await signIn(page);
  await page.getByRole('link', {name: 'history-pages', exact: true}).click();
  const history = page.locator('#history-browser');
  await expect(history).toContainText('Injected unavailable action history');
  await expect(history).toContainText('History is partial.');
  await history.getByRole('button', {name: 'Retry actions', exact: true}).click();
  await expect(history.getByText('Injected unavailable action history', {exact: true})).toHaveCount(0);
  await history.getByRole('button', {name: 'Load more signals', exact: true}).click();
  await expect(history).not.toContainText('History is partial.');
  await history.getByRole('button', {name: 'Signals', exact: true}).click();
  await expect(history.getByRole('table', {name: 'Signal records', exact: true}).locator('tbody tr')).toHaveCount(35);
  let release;
  const gate = new Promise(done => { release = done; });
  let received;
  const held = new Promise(done => { received = done; });
  await page.route('**/api/console/v1/query/get', async route => {
    const ref = route.request().postDataJSON().reference;
    if (ref.kind === 'EVENT' && ref.id === 'signal-34') {
      const response = await route.fetch(); received();
      await gate; await route.fulfill({response}); return;
    }
    await route.continue();
  });
  await history.getByRole('button', {name: 'Signal signal-34', exact: true}).click();
  await held;
  const recordHeading = history.getByRole('heading', {name: 'Signal signal-34', exact: true});
  await expect(recordHeading).toBeFocused();
  const heading = await recordHeading.boundingBox();
  const header = await page.getByRole('dialog').locator('.dialog-heading').boundingBox();
  expect(heading.y).toBeGreaterThanOrEqual(header.y + header.height);
  await page.getByRole('button', {name: 'Close dialog', exact: true}).click();
  await page.getByRole('link', {name: 'history-other', exact: true}).click();
  const late = page.waitForResponse(response => response.url().endsWith('/query/get') && response.request().postDataJSON().reference.id === 'signal-34');
  release(); await late;
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  await expect(page.getByRole('dialog').getByRole('heading', {name: 'history-other', exact: true})).toBeVisible();
  await expect(history.getByRole('heading', {name: 'Signal signal-34', exact: true})).toHaveCount(0);
});

test('uncertain outcomes and summary payload omissions remain explicit', async ({page}) => {
  await binding(page, 'history-uncertain-http', '/effects/uncertain');
  await mutate(page, 'uncertain-definition', 'put_definition', {definition: {id: 'history-unknown', runtime: 'steps/v1', handler: 'invoke', handler_version: '1', binding_id: 'history-uncertain-http', input: bytes({command: 'synthetic'})}});
  await mutate(page, 'uncertain-launch', 'launch', {definition_id: 'history-unknown', definition_revision: '1', run_id: 'history-unknown'});
  await waitRun(page, 'history-unknown', 'unknown');
  let summaries = 0;
  await page.route('**/api/console/v1/query/get', async route => {
    const q = route.request().postDataJSON();
    if (q.reference.kind === 'RUN' && q.reference.id === 'history-unknown') {
      if (!q.summary) { await route.fulfill({status: 429, json: {error: 'Injected full-record response budget'}}); return; }
      summaries++;
    }
    await route.continue();
  });
  await signIn(page);
  await page.getByRole('link', {name: 'history-unknown', exact: true}).click();
  const dialog = page.getByRole('dialog');
  const history = page.locator('#history-browser');
  await expect(dialog).toContainText('The external outcome is uncertain.');
  await expect(dialog).toContainText('Payloads are omitted from this run summary.');
  await expect(dialog.getByText('Checkpoint state · text', {exact: true})).toHaveCount(0);
  await history.getByRole('button', {name: 'Action 1', exact: true}).click();
  await expect(history).toContainText('This action has an uncertain external outcome.');
  await expect(history.getByRole('button', {name: 'Refresh record', exact: true})).toBeEnabled();
  expect(summaries).toBe(1);
  expect((await (await page.request.get(targetURL() + '/counts')).json())['/effects/uncertain']).toBe(1);
});
