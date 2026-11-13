import {test, expect} from '@playwright/test';

test('reviews replacement stages and learner catch-up on desktop and mobile', async ({page}, info) => {
  const m = {enabled:true, cluster_id:'4096', member_id:'1', leader_id:'1', configuration:'topology-a', catalog:'catalog', scope:'scope', catalog_size:8, max_id:'3', voters:[1,2,3], learners:[], commit:'9007199254740993', progress:[{member_id:'1', match:'9007199254740993', recent_active:true}]};
  let plan = null, advances = 0, statusReads = 0;
  await page.route('**/api/console/v1/cluster/*', async route => {
    const method = route.request().url().split('/').at(-1);
    if (method === 'status') statusReads++;
    if (method === 'plan') plan = {old:'2', new:'4', digest:'reviewed-digest', reason:'Confirmed volume loss', before:structuredClone(m), intent:null, complete:false};
    if (method === 'advance') {
      expect(route.request().postDataJSON().approval).toBe('reviewed-digest');
      if (++advances === 1) {
        m.learners = [4]; m.max_id = '4'; m.configuration = 'with-learner';
        m.progress.push({member_id:'4', match:'9007199254740992', recent_active:true});
        plan.intent = {action:'add_learner'};
      } else {
        expect(m.progress[1].match).toBe(m.commit);
        m.voters.push(4); m.learners = []; m.configuration = 'promoted';
        plan.intent = {action:'promote'};
      }
    }
    await route.fulfill({json:{membership:m, plan}});
  });
  await page.goto('/#/cluster?tenant=%61lpha');
  await page.getByLabel('API credential', {exact:true}).fill(process.env.DJOB_CONSOLE_PLATFORM_TOKEN);
  await page.getByRole('button', {name:'Sign in', exact:true}).click();
  await expect(page.getByRole('heading', {name:'Cluster', exact:true})).toBeVisible();
  await expect(page.getByRole('table')).toBeVisible();
  // Canonical navigation keeps the decoded workspace but changes the hash.
  const beforeNavigation = statusReads;
  await page.getByRole('link', {name:'Cluster', exact:true}).click();
  await expect.poll(() => statusReads).toBeGreaterThan(beforeNavigation);
  await expect(page).toHaveURL(/#\/cluster\?tenant=alpha$/);
  await expect(page.getByRole('table')).toBeVisible();
  await page.getByLabel('Old nonleader voter ID', {exact:true}).fill('2');
  await page.getByLabel('Fresh higher catalog ID', {exact:true}).fill('4');
  await page.getByLabel('Audited reason', {exact:true}).fill('Confirmed volume loss');
  await page.getByRole('button', {name:'Build plan for review'}).click();
  await expect(page.getByText('reviewed-digest', {exact:true})).toBeVisible();
  await page.getByRole('button', {name:'Approve digest and advance one stage'}).click();
  const learner = page.getByRole('row').filter({has:page.getByRole('cell', {name:'4', exact:true})});
  await expect(learner.getByRole('cell', {name:'Catching up', exact:true})).toBeVisible();
  // A later leader observation confirms replication, using exact uint64 values.
  m.progress[1].match = m.commit;
  await page.getByRole('button', {name:'Refresh', exact:true}).click();
  await expect(learner.getByRole('cell', {name:'Caught up at observation', exact:true})).toBeVisible();
  await page.getByRole('button', {name:'Approve digest and advance one stage'}).click();
  await expect(page.getByText('Reconcile promote', {exact:true})).toBeVisible();
  expect(plan.before.learners).toEqual([]);
  expect(plan.before.voters).toEqual([1,2,3]);
  await page.screenshot({path:info.outputPath('cluster-desktop.png'), fullPage:true});
  await page.setViewportSize({width:390, height:844});
  await page.screenshot({path:info.outputPath('cluster-mobile.png'), fullPage:true});
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
  await page.reload();
  await expect(page.getByText('Reconcile promote', {exact:true})).toBeVisible();
});
