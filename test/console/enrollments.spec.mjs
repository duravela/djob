import {test, expect} from '@playwright/test';
import {createHash} from 'node:crypto';
import {readFile} from 'node:fs/promises';

const controllerURL = () => 'http://127.0.0.1:' + process.env.DJOB_CONSOLE_TEST_HTTP_PORT;
const accessPath = tenant => '/#/enrollments?' + new URLSearchParams({tenant});
async function seed(page, tenant) {
  const response = await page.request.post(controllerURL() + '/api/jobs/v1/mutate', {headers: {Authorization: 'Bearer ' + process.env.DJOB_CONSOLE_PLATFORM_TOKEN}, data: {command: {request_id: 'enrollment-fixture/' + tenant, at: new Date().toISOString()}, tenant_id: tenant, put_tenant: {max_running: '4'}}});
  expect(response.ok(), await response.text()).toBeTruthy();
}
async function signIn(page, tenant, role = 'PLATFORM') {
  await page.goto(accessPath(tenant));
  await page.getByLabel('API credential', {exact: true}).fill(process.env['DJOB_CONSOLE_' + role + '_TOKEN']);
  await page.getByRole('button', {name: 'Sign in', exact: true}).click();
  await expect(page.getByRole('heading', {name: 'Worker access', exact: true})).toBeVisible();
}
async function review(page) {
  await page.getByLabel('Reason', {exact: true}).fill('Deploy company measurement capability');
  await page.getByRole('button', {name: 'Review request', exact: true}).click();
  await expect(page.getByRole('heading', {name: 'Review changes', exact: true})).toBeVisible();
  return JSON.parse(await page.getByRole('dialog').locator('pre').innerText());
}
async function confirm(page) {
  await page.getByRole('button', {name: 'Confirm and submit', exact: true}).click();
  await expect(page.getByRole('heading', {name: 'Change confirmed', exact: true})).toBeVisible();
}
async function finish(page, secret = false) {
  await page.getByRole('button', {name: 'Done', exact: true}).click();
  if (secret) {
    await expect(page.getByRole('heading', {name: 'Close enrollment review?', exact: true})).toBeVisible();
    await page.getByRole('button', {name: 'Close and clear credential', exact: true}).click();
  }
  await expect(page.getByRole('dialog')).not.toBeVisible();
}
async function configDownload(page) {
  const downloaded = page.waitForEvent('download');
  await page.getByRole('button', {name: 'Download worker configuration', exact: true}).click();
  const file = await downloaded;
  const cfg = JSON.parse(await readFile(await file.path(), 'utf8'));
  await file.delete();
  return cfg;
}
async function createEnrollment(page, tenant, worker) {
  const response = await page.request.post(controllerURL() + '/api/jobs/v1/worker-enrollments/mutate', {headers: {Authorization: 'Bearer ' + process.env.DJOB_CONSOLE_PLATFORM_TOKEN}, data: {command: {request_id: 'enrollment-fixture/' + tenant + '/' + worker, at: new Date().toISOString()}, tenant_id: tenant, worker_id: worker, subject: 'company/measurement', action: 'enroll', expected_revision: '0', reason: 'fixture', credential_hash: createHash('sha256').update(tenant + '/' + worker).digest('hex')}});
  expect(response.ok(), await response.text()).toBeTruthy();
}

test('enroll, download, drain, resume, rotate and revoke a company worker through reviewed changes', async ({page}, testInfo) => {
  const tenant = 'enrollment-browser', worker = 'measurement-company';
  await seed(page, tenant); await signIn(page, tenant);
  await page.getByRole('button', {name: 'Enroll worker', exact: true}).click();
  await page.getByLabel('Worker ID', {exact: true}).fill(worker);
  await page.getByLabel('Audit subject', {exact: true}).fill('company/measurement');
  const initial = await review(page);
  expect(initial.action).toBe('enroll'); expect(initial.expected_revision).toBe('0'); expect(initial.credential_hash).toMatch(/^[a-f0-9]{64}$/);
  expect(initial.token).toBeUndefined();
  await page.reload();
  await page.getByRole('button', {name: 'Open review', exact: true}).click();
  expect(JSON.parse(await page.getByRole('dialog').locator('pre').innerText())).toEqual(initial);
  await confirm(page);
  const cfg = await configDownload(page);
  expect(cfg.transport).toBe('grpc');
  expect(cfg.tenant_id).toBe(tenant); expect(cfg.worker_id).toBe(worker); expect(cfg.token).toMatch(/^djobw1\.[a-f0-9]{64}$/);
  expect(createHash('sha256').update(cfg.token).digest('hex')).toBe(initial.credential_hash);
  expect(await page.locator('body').innerText()).not.toContain(cfg.token);
  expect(await (await page.request.get('/api/console/v1/session')).text()).not.toContain(cfg.token);
  await finish(page, true);
  const row = page.getByRole('row').filter({has: page.getByRole('link', {name: worker, exact: true})});
  await expect(row).toContainText('enabled');
  await page.getByRole('link', {name: worker, exact: true}).click();
  await page.getByRole('button', {name: 'Drain worker', exact: true}).click();
  const draining = await review(page); expect(draining.expected_revision).toBe('1');
  await confirm(page); await finish(page);
  await expect(row).toContainText('draining');
  await page.getByRole('link', {name: worker, exact: true}).click();
  await page.getByRole('button', {name: 'Resume admission', exact: true}).click();
  await review(page); await confirm(page); await finish(page);
  await expect(row).toContainText('enabled');
  await page.getByRole('link', {name: worker, exact: true}).click();
  await page.getByRole('button', {name: 'Rotate credential', exact: true}).click();
  const rotating = await review(page); expect(rotating.expected_revision).toBe('3'); expect(rotating.credential_hash).not.toBe(initial.credential_hash);
  await confirm(page);
  const replacement = await configDownload(page); expect(replacement.token).not.toBe(cfg.token);
  expect(replacement.transport).toBe('grpc');
  await finish(page, true);
  await page.getByRole('link', {name: worker, exact: true}).click();
  await expect(page.getByRole('dialog')).toContainText('company/measurement');
  await page.screenshot({path: testInfo.outputPath('desktop-worker-access.png'), fullPage: false});
  await page.getByRole('button', {name: 'Revoke access', exact: true}).click();
  await expect(page.getByRole('dialog')).toContainText('does not establish physical quiescence');
  await review(page); await confirm(page); await finish(page);
  await expect(row).toContainText('revoked');
  await page.getByRole('link', {name: worker, exact: true}).click();
  await expect(page.getByRole('button', {name: 'Rotate credential', exact: true})).toHaveCount(0);
  await page.getByRole('button', {name: 'Close dialog', exact: true}).click();
  await page.setViewportSize({width: 390, height: 844});
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
  await page.screenshot({path: testInfo.outputPath('mobile-worker-access.png'), fullPage: true});
});

test('tenant readers inspect enrolled identities and cannot create or administer access', async ({page}) => {
  await createEnrollment(page, 'alpha', 'reader-company-worker');
  await signIn(page, 'alpha', 'READER');
  await expect(page.getByRole('button', {name: 'Enroll worker', exact: true})).toHaveCount(0);
  await page.getByRole('link', {name: 'reader-company-worker', exact: true}).click();
  await expect(page.getByRole('dialog')).toContainText('company/measurement');
  await expect(page.getByRole('button', {name: /Rotate credential|Drain worker|Revoke access/})).toHaveCount(0);
  await page.reload();
  await expect(page.getByRole('dialog').getByRole('heading', {name: 'reader-company-worker', exact: true})).toBeVisible();
});

test('enrollment pagination and direct lookup work independently of failed list reads', async ({page}) => {
  const tenant = 'enrollment-pages'; await seed(page, tenant);
  for (let i = 0; i < 35; i++) await createEnrollment(page, tenant, 'worker-' + String(i).padStart(2, '0'));
  await signIn(page, tenant);
  await expect(page.locator('#records tbody tr')).toHaveCount(32);
  await page.getByRole('button', {name: 'Load next page', exact: true}).click();
  await expect(page.locator('#records tbody tr')).toHaveCount(35);
  await page.route('**/api/console/v1/worker-enrollments/list', route => route.fulfill({status: 503, json: {error: 'Enrollment list unavailable'}}));
  await page.reload();
  await expect(page.locator('#page-error')).toContainText('Enrollment list unavailable');
  await expect(page.getByRole('heading', {name: 'No records loaded', exact: true})).toBeVisible();
  await page.getByLabel('Open by ID', {exact: true}).fill('worker-34');
  await page.getByRole('button', {name: 'Open record', exact: true}).click();
  await expect(page.getByRole('dialog').getByRole('heading', {name: 'worker-34', exact: true})).toBeVisible();
});
