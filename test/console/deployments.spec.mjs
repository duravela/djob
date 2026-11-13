import {test, expect} from '@playwright/test';
import {createHash} from 'node:crypto';

async function seedWorker(page, tenant) {
  const controller = 'http://127.0.0.1:' + process.env.DJOB_CONSOLE_TEST_HTTP_PORT;
  const auth = {Authorization: 'Bearer ' + process.env.DJOB_CONSOLE_PLATFORM_TOKEN};
  let response = await page.request.post(controller + '/api/jobs/v1/mutate', {headers: auth, data: {command: {request_id: tenant, at: new Date().toISOString()}, tenant_id: tenant, put_tenant: {max_running: '4'}}});
  expect(response.ok(), await response.text()).toBeTruthy();
  response = await page.request.post(controller + '/api/jobs/v1/worker-enrollments/mutate', {headers: auth, data: {command: {request_id: tenant + '/worker', at: new Date().toISOString()}, tenant_id: tenant, worker_id: 'company-worker', action: 'enroll', expected_revision: '0', subject: 'company/worker', reason: 'Browser deployment fixture', credential_hash: createHash('sha256').update(tenant).digest('hex')}});
  expect(response.ok(), await response.text()).toBeTruthy();
  return {controller, auth};
}

test('manages desired worker images and replicas through reviewed mutations', async ({page}, testInfo) => {
  const tenant = 'deployment-browser-' + testInfo.workerIndex;
  await seedWorker(page, tenant);
  await page.goto('/#/deployments?' + new URLSearchParams({tenant}));
  await page.getByLabel('API credential', {exact: true}).fill(process.env.DJOB_CONSOLE_PLATFORM_TOKEN);
  await page.getByRole('button', {name: 'Sign in', exact: true}).click();
  await expect(page.getByRole('heading', {name: 'Deployments', exact: true})).toBeVisible();
  await page.getByRole('button', {name: 'New deployment', exact: true}).click();
  await page.getByLabel('Deployment ID', {exact: true}).fill('line-one');
  await page.getByLabel('Enrolled worker ID', {exact: true}).fill('company-worker');
  await page.getByLabel('Container image with digest', {exact: true}).fill('ghcr.io/company/worker@sha256:' + 'a'.repeat(64));
  await page.getByLabel('Worker configuration Secret', {exact: true}).fill('company-worker-config');
  await page.getByLabel('Desired replicas', {exact: true}).fill('2');
  async function commit(reason) {
    await page.getByLabel('Reason', {exact: true}).fill(reason);
    await page.getByRole('button', {name: 'Review request', exact: true}).click();
    await expect(page.getByRole('heading', {name: 'Review changes', exact: true})).toBeVisible();
    await page.getByRole('button', {name: 'Confirm and submit', exact: true}).click();
    await expect(page.getByRole('heading', {name: 'Change confirmed', exact: true})).toBeVisible();
    await page.getByRole('button', {name: 'Done', exact: true}).click();
    await expect(page.getByRole('dialog')).not.toBeVisible();
  }
  await commit('Launch two company workers');
  await page.getByRole('link', {name: 'line-one', exact: true}).click();
  await expect(page.getByRole('dialog')).toContainText('company-worker-config');
  await expect(page.getByRole('dialog')).toContainText('No current runtime observation');
  await page.screenshot({path: testInfo.outputPath('desktop-worker-deployment.png'), fullPage: false});
  await page.getByRole('button', {name: 'Stop', exact: true}).click();
  await expect(page.getByLabel('Desired replicas', {exact: true})).toHaveValue('0');
  await commit('Drain and stop workers');
  await page.getByRole('link', {name: 'line-one', exact: true}).click();
  await expect(page.getByRole('button', {name: 'Start', exact: true})).toBeVisible();
  await page.getByRole('button', {name: 'Start', exact: true}).click();
  await page.getByLabel('Desired replicas', {exact: true}).fill('1');
  await commit('Start one worker');
  await page.getByRole('link', {name: 'line-one', exact: true}).click();
  await page.getByRole('button', {name: 'Restart instances', exact: true}).click();
  await commit('Reload the worker configuration Secret');
  await page.getByRole('link', {name: 'line-one', exact: true}).click();
  await page.setViewportSize({width: 390, height: 844});
  await page.screenshot({path: testInfo.outputPath('mobile-worker-deployment.png'), fullPage: false});
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
  await page.getByRole('button', {name: 'Retire', exact: true}).click();
  await commit('Retire the deployment identity');
  await page.getByRole('link', {name: 'line-one', exact: true}).click();
  await expect(page.getByRole('button', {name: 'Edit', exact: true})).toHaveCount(0);
  await expect(page.getByRole('button', {name: 'Start', exact: true})).toHaveCount(0);
});


test('shows rollout availability and actionable Kubernetes failure observations', async ({page}, testInfo) => {
  const tenant = 'rollout-browser-' + testInfo.workerIndex;
  const {controller, auth} = await seedWorker(page, tenant);
  const response = await page.request.post(controller + '/api/jobs/v1/worker-deployments/mutate', {headers: auth, data: {
    command: {request_id: tenant + '/deployment', at: new Date().toISOString()}, tenant_id: tenant, id: 'line-one', worker_id: 'company-worker', action: 'put', expected_revision: '0', reason: 'Rollout diagnostic fixture',
    spec: {pool: 'default', image: 'ghcr.io/company/worker@sha256:' + 'a'.repeat(64), config_secret: 'company-worker-config', replicas: 2},
  }});
  expect(response.ok(), await response.text()).toBeTruthy();
  let observed = {state: 'pending', desired_replicas: 2, replicas: 4, ready_replicas: 2, available_replicas: 1, updated_replicas: 2, terminating_replicas: 1};
  await page.route('**/api/console/v1/worker-deployments/status', route => route.fulfill({json: {
    tenant_id: tenant, id: 'line-one', controllers: [{index: 0, origin: controller, status: {tenant_id: tenant, id: 'line-one', revision: '1', configured: true, observed: true,
      runtime: {revision: '1', generation: '1', pool: 'default', namespace: 'djob-workers', name: 'djob-worker-test', observed_at: new Date().toISOString(), ...observed},
    }}],
  }}));
  await page.goto('/#/deployments?' + new URLSearchParams({tenant}));
  await page.getByLabel('API credential', {exact: true}).fill(process.env.DJOB_CONSOLE_PLATFORM_TOKEN);
  await page.getByRole('button', {name: 'Sign in', exact: true}).click();
  await page.getByRole('link', {name: 'line-one', exact: true}).click();
  const runtime = page.locator('#deployment-runtime');
  await expect(runtime).toContainText('1 / 2');
  await expect(runtime.getByText('Current replicas', {exact: true}).locator('..')).toContainText('4');
  await expect(runtime.getByText('Terminating Pods', {exact: true}).locator('..')).toContainText('1');
  observed = {...observed, state: 'paused', rollout_paused: true};
  await page.getByRole('dialog').getByRole('button', {name: 'Refresh', exact: true}).click();
  await expect(runtime.getByText('paused', {exact: true})).toBeVisible();
  await expect(runtime.getByText('Kubernetes rollout', {exact: true}).locator('..')).toContainText('Paused');
  await expect(runtime).toContainText('resume the Deployment in Kubernetes');
  await expect(runtime).toContainText('Existing workers may still execute jobs');
  await expect(runtime).toContainText('1 / 2');
  await page.screenshot({path: testInfo.outputPath('desktop-paused-rollout.png'), fullPage: false});
  const viewport = page.viewportSize();
  await page.setViewportSize({width: 390, height: 844});
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
  await page.screenshot({path: testInfo.outputPath('mobile-paused-rollout.png'), fullPage: false});
  await page.setViewportSize(viewport);
  observed = {...observed, state: 'deleting', deletion_requested_at: '2026-11-11T12:34:56Z'};
  await page.getByRole('dialog').getByRole('button', {name: 'Refresh', exact: true}).click();
  await expect(runtime).toContainText('waits for its removal');
  await expect(runtime.getByText('Deletion requested (UTC)', {exact: true}).locator('..')).toContainText('2026-11-11 12:34:56 UTC');
  await expect(runtime.getByText('deleting', {exact: true})).toBeVisible();
  observed = {state: 'blocked', error: 'replica_failure', desired_replicas: 2, replicas: 0, ready_replicas: 0, available_replicas: 0, updated_replicas: 0};
  await page.getByRole('dialog').getByRole('button', {name: 'Refresh', exact: true}).click();
  await expect(runtime).toContainText('Worker Pods could not be created.');
  await expect(runtime.getByText('Deletion requested (UTC)', {exact: true})).toHaveCount(0);
  await expect(runtime.getByText('Kubernetes rollout', {exact: true})).toHaveCount(0);
  await expect(runtime).toContainText('0 / 2');
  await expect(runtime.getByText('Terminating Pods', {exact: true}).locator('..')).toContainText('Not reported');
  observed = {...observed, state: 'stalled', error: 'progress_deadline_exceeded', observed_at: new Date(Date.now() - 60_000).toISOString()};
  await page.getByRole('dialog').getByRole('button', {name: 'Refresh', exact: true}).click();
  await expect(runtime).toContainText('The rollout stopped making progress.');
  await expect(runtime).toContainText('stalled · stale');
  observed = {...observed, state: 'error', error: 'unavailable', retry_after: new Date(Date.now() + 30_000).toISOString()};
  await page.getByRole('dialog').getByRole('button', {name: 'Refresh', exact: true}).click();
  await expect(runtime).toContainText('djob retries automatically.');
  await expect(runtime).toContainText('Automatic retry after (UTC)');
  await expect(runtime).toContainText('error · stale');
  await expect(runtime.getByText('Available / desired Pods', {exact: true}).locator('..')).toContainText('Not reported / 2');
  await expect(runtime.getByText('Current replicas', {exact: true}).locator('..')).toContainText('Not reported');
  await page.screenshot({path: testInfo.outputPath('desktop-rollout-failure.png'), fullPage: false});
  await page.setViewportSize({width: 390, height: 844});
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
  await page.screenshot({path: testInfo.outputPath('mobile-rollout-failure.png'), fullPage: false});
});

test('deployment details distinguish worker access from desired process state', async ({page}, testInfo) => {
  await page.clock.install();
  const tenant = 'deployment-access-' + testInfo.workerIndex;
  const {controller, auth} = await seedWorker(page, tenant);
  const response = await page.request.post(controller + '/api/jobs/v1/worker-deployments/mutate', {headers: auth, data: {
    command: {request_id: tenant + '/deployment', at: new Date().toISOString()}, tenant_id: tenant, id: 'line-one', worker_id: 'company-worker', action: 'put', expected_revision: '0', reason: 'Worker access observations fixture',
    spec: {pool: 'default', image: 'ghcr.io/company/worker@sha256:' + 'a'.repeat(64), config_secret: 'company-worker-config', replicas: 2},
  }});
  expect(response.ok(), await response.text()).toBeTruthy();
  await page.goto('/#/deployments?' + new URLSearchParams({tenant}));
  await page.getByLabel('API credential', {exact: true}).fill(process.env.DJOB_CONSOLE_PLATFORM_TOKEN);
  await page.getByRole('button', {name: 'Sign in', exact: true}).click();
  await page.getByRole('link', {name: 'line-one', exact: true}).click();
  const access = page.locator('#deployment-access');
  const state = access.getByText('Access for new claims', {exact: true}).locator('..');
  await expect(state).toContainText('enabled');
  await expect(page.locator('#deployment-runtime')).toContainText('No current runtime observation');
  for (const [action, revision, expected] of [['drain', '1', 'draining'], ['resume', '2', 'enabled'], ['revoke', '3', 'revoked']]) {
    const changed = await page.request.post(controller + '/api/jobs/v1/worker-enrollments/mutate', {headers: auth, data: {
      command: {request_id: tenant + '/' + action, at: new Date().toISOString()}, tenant_id: tenant, worker_id: 'company-worker', action, expected_revision: revision, reason: 'Observe worker access independently',
    }});
    expect(changed.ok(), await changed.text()).toBeTruthy();
    await page.getByRole('dialog').getByRole('button', {name: 'Refresh', exact: true}).click();
    await expect(state).toContainText(expected);
    if (action === 'drain') await expect(access).toContainText('new claims are refused');
    if (action === 'revoke') await expect(access).toContainText('new worker identity and deployment');
    if (action !== 'resume') await expect(page.locator('#deployment-sessions')).not.toContainText('Check worker configuration, credentials and logs');
    await expect(page.getByRole('dialog').getByText('Desired replicas', {exact: true}).locator('..')).toContainText('2');
  }
  await page.screenshot({path: testInfo.outputPath('desktop-deployment-access.png'), fullPage: false});
  await page.setViewportSize({width: 390, height: 844});
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
  await page.screenshot({path: testInfo.outputPath('mobile-deployment-access.png'), fullPage: false});
  await page.clock.fastForward(10001);
  await expect(state).toContainText('Not established');
  await page.getByRole('link', {name: 'Inspect worker access', exact: true}).click();
  await expect(page.getByRole('dialog').getByText('Admission', {exact: true}).locator('..')).toContainText('revoked');
  await expect(page).toHaveURL(new RegExp('enrollments.*company-worker'));
});

test('distinguishes available Pods from current-generation worker connections', async ({page}, testInfo) => {
  await page.clock.install();
  const tenant = 'connections-browser-' + testInfo.workerIndex;
  const {controller, auth} = await seedWorker(page, tenant);
  const response = await page.request.post(controller + '/api/jobs/v1/worker-deployments/mutate', {headers: auth, data: {
    command: {request_id: tenant + '/deployment', at: new Date().toISOString()}, tenant_id: tenant, id: 'line-one', worker_id: 'company-worker', action: 'put', expected_revision: '0', reason: 'Connection observations fixture',
    spec: {pool: 'default', image: 'ghcr.io/company/worker@sha256:' + 'a'.repeat(64), config_secret: 'company-worker-config', replicas: 2},
  }});
  expect(response.ok(), await response.text()).toBeTruthy();
  const first = {controller_id: '9007199254740993', instance_id: 'first-process', generation: '1', enabled: true, observed_at: new Date().toISOString(), connected: 0, registered: 0, draining: 0, relocating: 0, stale: 0, unavailable: 0, other_generations: 1};
  const source = (index, sample) => ({index, origin: controller + '/alias-' + index, status: {tenant_id: tenant, id: 'line-one', revision: '1', configured: true, observed: true,
    worker_sessions: sample, runtime: {revision: '1', generation: '1', pool: 'default', namespace: 'djob-workers', name: 'djob-worker-test', observed_at: new Date().toISOString(), state: 'available', desired_replicas: 2, replicas: 2, ready_replicas: 2, available_replicas: 2, updated_replicas: 2},
  }});
  const second = {...first, controller_id: '2', instance_id: 'second-process', other_generations: 0};
  let sources = [source(0, first), source(1, second), source(2, first)];
  let observations = 0;
  await page.route('**/api/console/v1/worker-deployments/status', route => { observations++; return route.fulfill({json: {tenant_id: tenant, id: 'line-one', controllers: sources}}); });
  await page.goto('/#/deployments?' + new URLSearchParams({tenant}));
  await page.getByLabel('API credential', {exact: true}).fill(process.env.DJOB_CONSOLE_PLATFORM_TOKEN);
  await page.getByRole('button', {name: 'Sign in', exact: true}).click();
  await page.getByRole('link', {name: 'line-one', exact: true}).click();
  const sessions = page.locator('#deployment-sessions');
  await expect(page.locator('#deployment-runtime').getByText('available', {exact: true})).toBeVisible();
  await expect(sessions).toContainText('No current-generation worker is connected');
  const current = sessions.getByText('Connected sessions · generation 1', {exact: true}).locator('..');
  await expect(current).toContainText('0');
  await expect(sessions.getByText('Sessions on other generations', {exact: true}).locator('..')).toContainText('1');
  await expect(sessions.getByText('Controller observations', {exact: true}).locator('..')).toContainText('2 current');
  sources = [source(0, {...first, connected: 1}), {index: 1, origin: controller, error: 'unreachable'}, source(2, {...first, connected: 1})];
  await page.getByRole('dialog').getByRole('button', {name: 'Refresh', exact: true}).click();
  await expect(sessions).toContainText('Worker-session observations are partial');
  await expect(current).toContainText('At least 1');
  await sessions.scrollIntoViewIfNeeded();
  await page.screenshot({path: testInfo.outputPath('desktop-worker-connections.png'), fullPage: false});
  await page.setViewportSize({width: 390, height: 844});
  await sessions.scrollIntoViewIfNeeded();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
  await page.screenshot({path: testInfo.outputPath('mobile-worker-connections.png'), fullPage: false});
  const readsBeforeExpiry = observations;
  await page.clock.fastForward(11000);
  await expect(sessions).toContainText('No current worker-session observation');
  await expect(current).toContainText('Not established');
  await expect(page.locator('#deployment-runtime')).toContainText('available · stale');
  expect(observations).toBe(readsBeforeExpiry);
});

test('publishes a pinned release and keeps retiring workers until queued jobs finish', async ({page}, testInfo) => {
  const tenant = 'release-browser-' + testInfo.workerIndex;
  const {controller, auth} = await seedWorker(page, tenant);
  let nonce = 0;
  async function api(path, body, ok = true) {
    const response = await page.request.post(controller + '/api/jobs/v1/' + path, {headers: auth, data: {command: {request_id: tenant + '/release/' + (++nonce), at: new Date().toISOString()}, tenant_id: tenant, ...body}});
    expect(response.ok(), await response.text()).toBe(ok);
    return response;
  }
  await api('worker-deployments/mutate', {id: 'release-v1', worker_id: 'company-worker', action: 'put', expected_revision: '0', reason: 'Publish first worker release', spec: {image: 'ghcr.io/company/worker@sha256:' + 'a'.repeat(64), config_secret: 'company-config-v1', replicas: 1}});
  await page.goto('/#/deployments?' + new URLSearchParams({tenant}));
  await page.getByLabel('API credential', {exact: true}).fill(process.env.DJOB_CONSOLE_PLATFORM_TOKEN);
  await page.getByRole('button', {name: 'Sign in', exact: true}).click();
  async function commit(reason) {
    if (reason) await page.getByLabel('Reason', {exact: true}).fill(reason);
    await page.getByRole('button', {name: 'Review request', exact: true}).click();
    await expect(page.getByRole('heading', {name: 'Review changes', exact: true})).toBeVisible();
    await page.getByRole('button', {name: 'Confirm and submit', exact: true}).click();
    await expect(page.getByRole('heading', {name: 'Change confirmed', exact: true})).toBeVisible();
    await page.getByRole('button', {name: 'Done', exact: true}).click();
  }
  await page.getByRole('link', {name: 'release-v1', exact: true}).click();
  await page.getByRole('button', {name: 'Publish release', exact: true}).click();
  await expect(page.getByRole('dialog')).toContainText('become immutable');
  await commit('Keep v1 available alongside future releases');
  await page.goto('/#/jobs?' + new URLSearchParams({tenant}));
  await page.getByRole('button', {name: 'New job', exact: true}).click();
  await page.getByLabel('ID', {exact: true}).fill('release-job');
  await page.getByLabel('Handler', {exact: true}).fill('sha256');
  await page.getByLabel('Input', {exact: true}).fill('Pinned to v1');
  await page.getByLabel('Worker release (optional)', {exact: true}).fill('release-v1');
  await commit();
  await api('mutate', {launch: {definition_id: 'release-job', definition_revision: '1', run_id: 'old-run'}});
  await page.goto('/#/deployments?' + new URLSearchParams({tenant}));
  await page.getByRole('link', {name: 'release-v1', exact: true}).click();
  await expect(page.getByRole('button', {name: 'Edit', exact: true})).toHaveCount(0);
  await expect(page.getByRole('button', {name: 'Stop', exact: true})).toHaveCount(0);
  await page.getByRole('button', {name: 'Begin retirement', exact: true}).click();
  await commit('Close new-run admission while the old job remains queued');
  await page.getByRole('link', {name: 'release-v1', exact: true}).click();
  await expect(page.getByRole('button', {name: 'Resume new runs', exact: true})).toBeVisible();
  await expect(page.getByRole('dialog').getByText('Outstanding release runs', {exact: true}).locator('..')).toContainText('1');
  await expect(page.getByRole('button', {name: 'Retire', exact: true})).toHaveCount(0);
  await page.screenshot({path: testInfo.outputPath('retiring-worker-release.png'), fullPage: false});
  await page.getByRole('link', {name: 'Inspect outstanding runs', exact: true}).click();
  await expect(page.getByRole('region', {name: 'Worker release filter'})).toContainText('release-v1');
  await expect(page.getByRole('link', {name: 'old-run', exact: true})).toBeVisible();
  await page.reload();
  await expect(page.getByRole('region', {name: 'Worker release filter'})).toContainText('release-v1');
  await page.getByRole('link', {name: 'old-run', exact: true}).click();
  await expect(page.getByRole('dialog').getByRole('heading', {name: 'old-run', exact: true})).toBeVisible();
  await expect(page).toHaveURL(/worker_release=release-v1/);
  await page.getByRole('dialog').getByRole('button', {name: 'Close dialog', exact: true}).click();
  await page.getByRole('link', {name: 'Inspect release', exact: true}).click();
  await expect(page.getByRole('button', {name: 'Resume new runs', exact: true})).toBeVisible();

  await api('mutate', {launch: {definition_id: 'release-job', definition_revision: '1', run_id: 'refused-run'}}, false);
  await api('mutate', {cancel: {run_id: 'old-run', reason: 'Withdraw queued fixture run'}});
  await page.getByRole('dialog').getByRole('button', {name: 'Refresh', exact: true}).click();
  await expect(page.getByRole('button', {name: 'Retire', exact: true})).toBeVisible();
  await page.getByRole('button', {name: 'Retire', exact: true}).click();
  await commit('All outstanding v1 jobs are resolved');
});

test('paginates outstanding release runs without mixing unpinned or completed work', async ({page}, testInfo) => {
  const tenant = 'release-pages-' + testInfo.workerIndex;
  const {controller, auth} = await seedWorker(page, tenant);
  let nonce = 0;
  const api = async (path, body) => {
    const response = await page.request.post(controller + '/api/jobs/v1/' + path, {headers: auth, data: {command: {request_id: tenant + '/page/' + (++nonce), at: new Date().toISOString()}, tenant_id: tenant, ...body}});
    expect(response.ok(), await response.text()).toBeTruthy();
  };
  await api('worker-deployments/mutate', {id: 'release-v1', worker_id: 'company-worker', action: 'put', expected_revision: '0', reason: 'Release inspection fixture', spec: {image: 'ghcr.io/company/worker@sha256:' + 'a'.repeat(64), config_secret: 'worker-v1', replicas: 1}});
  await api('worker-deployments/mutate', {id: 'release-v1', worker_id: 'company-worker', action: 'publish', expected_revision: '1', reason: 'Publish release inspection fixture'});
  await api('mutate', {put_definition: {definition: {id: 'pinned', handler: 'fixture-only', handler_version: '1', runtime: 'steps/v1', worker_release: 'release-v1'}}});
  await api('mutate', {put_definition: {definition: {id: 'ordinary', handler: 'fixture-only', handler_version: '1', runtime: 'steps/v1'}}});
  for (let n = 0; n < 34; n++) await api('mutate', {launch: {definition_id: 'pinned', definition_revision: '1', run_id: 'release-run-' + String(n).padStart(2, '0')}});
  await api('mutate', {launch: {definition_id: 'pinned', definition_revision: '1', run_id: 'completed-v1'}});
  await api('mutate', {cancel: {run_id: 'completed-v1', reason: 'No longer outstanding'}});
  await api('mutate', {launch: {definition_id: 'ordinary', definition_revision: '1', run_id: 'ordinary-work'}});
  await page.goto('/#/runs?' + new URLSearchParams({tenant, worker_release: 'release-v1'}));
  await page.getByLabel('API credential', {exact: true}).fill(process.env.DJOB_CONSOLE_PLATFORM_TOKEN);
  await page.getByRole('button', {name: 'Sign in', exact: true}).click();
  await expect(page.getByRole('link', {name: /^release-run-/})).toHaveCount(32);
  await expect(page.getByRole('link', {name: 'ordinary-work', exact: true})).toHaveCount(0);
  await expect(page.getByRole('link', {name: 'completed-v1', exact: true})).toHaveCount(0);
  await page.getByRole('button', {name: 'Load next page', exact: true}).click();
  await expect(page.getByRole('link', {name: /^release-run-/})).toHaveCount(34);
  await expect(page.getByRole('button', {name: 'Load next page', exact: true})).toHaveCount(0);
  await page.setViewportSize({width: 390, height: 844});
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({path: testInfo.outputPath('mobile-release-runs.png'), fullPage: false});
  await page.getByRole('link', {name: 'All runs', exact: true}).click();
  await expect(page.getByRole('region', {name: 'Worker release filter'})).toHaveCount(0);
  await expect(page.getByRole('link', {name: 'ordinary-work', exact: true})).toBeVisible();
});

test('inspects paginated release references and migrates a schedule independently of its current job', async ({page}, testInfo) => {
  const tenant = 'release-references-' + testInfo.workerIndex;
  const {controller, auth} = await seedWorker(page, tenant);
  let nonce = 0;
  async function api(path, body) {
    const response = await page.request.post(controller + '/api/jobs/v1/' + path, {headers: auth, data: {command: {request_id: tenant + '/' + (++nonce), at: new Date().toISOString()}, tenant_id: tenant, ...body}});
    expect(response.ok(), await response.text()).toBeTruthy();
  }
  await api('worker-enrollments/mutate', {worker_id: 'worker-v2', action: 'enroll', expected_revision: '0', subject: 'company/worker-v2', reason: 'Parallel release', credential_hash: createHash('sha256').update(tenant + '/v2').digest('hex')});
  for (const [id, worker, digest] of [['v1', 'company-worker', 'a'], ['v2', 'worker-v2', 'b']]) {
    await api('worker-deployments/mutate', {id, worker_id: worker, action: 'put', expected_revision: '0', reason: 'Parallel release fixture', spec: {image: 'ghcr.io/company/worker@sha256:' + digest.repeat(64), config_secret: 'worker-' + id, replicas: 1}});
    await api('worker-deployments/mutate', {id, worker_id: worker, action: 'publish', expected_revision: '1', reason: 'Publish parallel release'});
  }
  const job = (id, worker_release = '') => ({id, handler: 'fixture-only', handler_version: '1', runtime: 'steps/v1', worker_release});
  await api('mutate', {put_definition: {definition: job('zz-migrating', 'v1')}});
  await api('mutate', {put_definition: {definition: job('zz-v1-only', 'v1')}});
  for (let n = 0; n < 33; n++) {
    const id = 'u' + String(n).padStart(2, '0');
    await api('mutate', {put_definition: {definition: job(id)}});
    await api('mutate', {put_trigger: {definition: {id, definition_id: id, definition_revision: '1', event: {source: 'fixture'}, max_pending: 1}}});
  }
  for (const [id, paused] of [['zz-active', false], ['zz-paused', true]]) {
    await api('mutate', {put_trigger: {definition: {id, definition_id: 'zz-migrating', definition_revision: '1', event: {source: 'fixture'}, max_pending: 1, paused}}});
  }
  await api('mutate', {put_definition: {expected_revision: '1', definition: job('zz-migrating', 'v2')}});
  await page.goto('/#/deployments?' + new URLSearchParams({tenant, id: 'v1'}));
  await page.getByLabel('API credential', {exact: true}).fill(process.env.DJOB_CONSOLE_PLATFORM_TOKEN);
  await page.getByRole('button', {name: 'Sign in', exact: true}).click();
  await page.getByRole('button', {name: 'Inspect job and schedule references', exact: true}).click();
  const dialog = page.getByRole('dialog');
  const jobs = dialog.getByRole('region', {name: 'Jobs referencing release', exact: true});
  const schedules = dialog.getByRole('region', {name: 'Schedules referencing release', exact: true});
  await expect(jobs).toContainText('Partial inspection');
  await expect(jobs).toContainText('32 records examined');
  await expect(jobs).toContainText('0 matching references loaded');
  let failed = false;
  await page.route('**/api/console/v1/query/list', async route => {
    const q = route.request().postDataJSON();
    if (!failed && q.kind === 'DEFINITION' && q.page_token) { failed = true; await route.fulfill({status: 503, json: {error: 'Reference read unavailable'}}); return; }
    await route.continue();
  });
  await jobs.getByRole('button', {name: 'Load more jobs', exact: true}).click();
  await expect(jobs).toContainText('Reference read unavailable');
  await expect(jobs).toContainText('32 records examined');
  await jobs.getByRole('button', {name: 'Retry jobs', exact: true}).click();
  await expect(jobs.getByRole('link', {name: 'zz-v1-only', exact: true})).toBeVisible();
  await expect(jobs.getByRole('link', {name: 'zz-migrating', exact: true})).toHaveCount(0);
  await expect(jobs).toContainText('All pages read');
  await schedules.getByRole('button', {name: 'Load more schedules', exact: true}).click();
  await expect(schedules.getByRole('link', {name: 'zz-active', exact: true})).toBeVisible();
  await expect(schedules.getByRole('link', {name: 'zz-paused', exact: true})).toBeVisible();
  await expect(schedules).toContainText('zz-migrating@1');
  await expect(schedules).toContainText('Paused');
  await expect(schedules).toContainText('Active');
  await dialog.evaluate(node => { node.scrollTop = 0; });
  await page.screenshot({path: testInfo.outputPath('desktop-release-references.png'), fullPage: true});
  await page.setViewportSize({width: 390, height: 844});
  expect(await dialog.evaluate(node => node.scrollWidth <= node.clientWidth)).toBeTruthy();
  await dialog.evaluate(node => { node.scrollTop = 0; });
  await page.screenshot({path: testInfo.outputPath('mobile-release-references.png'), fullPage: true});
  await schedules.getByRole('link', {name: 'zz-active', exact: true}).click();
  await expect(dialog.getByRole('heading', {name: 'zz-active', exact: true})).toBeVisible();
  await dialog.getByRole('button', {name: 'Edit', exact: true}).click();
  await page.getByLabel('Job revision', {exact: true}).fill('2');
  await page.getByRole('button', {name: 'Review request', exact: true}).click();
  const q = JSON.parse(await dialog.locator('pre').first().innerText());
  expect(q.put_trigger.expected_revision).toBe('1');
  expect(q.put_trigger.definition.definition_revision).toBe('2');
  await page.getByRole('button', {name: 'Confirm and submit', exact: true}).click();
  await expect(page.getByRole('heading', {name: 'Change confirmed', exact: true})).toBeVisible();
  let release, received;
  const gate = new Promise(done => { release = done; });
  const held = new Promise(done => { received = done; });
  await page.route('**/api/console/v1/mutations/current', async route => {
    const response = await route.fetch(); received();
    await gate; await route.fulfill({response});
  }, {times: 1});
  await page.getByRole('button', {name: 'Done', exact: true}).click();
  await held;
  await page.goto('/#/deployments?' + new URLSearchParams({tenant, id: 'v2'}));
  await expect(dialog.getByRole('heading', {name: 'v2', exact: true})).toBeVisible();
  const late = page.waitForResponse(response => response.url().endsWith('/mutations/current') && response.request().method() === 'DELETE');
  release(); await late;
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  await expect(dialog.getByRole('heading', {name: 'v2', exact: true})).toBeVisible();
  await page.getByRole('button', {name: 'Inspect job and schedule references', exact: true}).click();
  await jobs.getByRole('button', {name: 'Load more jobs', exact: true}).click();
  await schedules.getByRole('button', {name: 'Load more schedules', exact: true}).click();
  await expect(jobs.getByRole('link', {name: 'zz-migrating', exact: true})).toBeVisible();
  await expect(schedules.getByRole('link', {name: 'zz-active', exact: true})).toBeVisible();
  await expect(schedules).toContainText('zz-migrating@2');
  await expect(schedules.getByRole('link', {name: 'zz-paused', exact: true})).toHaveCount(0);
  await expect(jobs.getByRole('link', {name: 'zz-v1-only', exact: true})).toHaveCount(0);
});
