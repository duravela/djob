import {test, expect} from '@playwright/test';

test('load-balanced consoles keep login and review through rolling replacement', async ({page, context}, testInfo) => {
  await page.goto('/');
  await page.getByLabel('API credential', {exact: true}).fill(process.env.DJOB_CONSOLE_WRITER_TOKEN);
  await page.getByRole('button', {name: 'Sign in', exact: true}).click();
  await expect(page.getByRole('navigation', {name: 'Management'})).toBeVisible();
  const cookie = (await context.cookies()).find(c => c.name.includes('djob-console'));
  await page.getByRole('button', {name: 'New job', exact: true}).click();
  await page.getByLabel('ID', {exact: true}).fill('rolling-browser-job');
  await page.getByLabel('Handler', {exact: true}).fill('sha256');
  await page.getByLabel('Input', {exact: true}).fill('Keep this review across consoles');
  await expect(page.locator('[data-draft-status]')).toHaveText('Draft saved');
  expect((await page.request.post('/__test/roll', {data: {index: 0}})).ok()).toBeTruthy();
  await page.reload();
  await expect(page.getByLabel('ID', {exact: true})).toHaveValue('rolling-browser-job');
  await expect(page.getByLabel('Input', {exact: true})).toHaveValue('Keep this review across consoles');
  await expect(page.locator('[data-draft-status]')).toHaveText('Draft restored');
  await page.screenshot({path:testInfo.outputPath('restored-editor.png'),fullPage:true});
  await page.getByRole('button', {name: 'Review request', exact: true}).click();
  await expect(page.getByRole('heading', {name: 'Review changes', exact: true})).toBeVisible();
  const envelope = await page.getByRole('dialog').locator('pre').innerText();
  for (const index of [0, 1]) {
    const replacement = page.request.post('/__test/roll', {data: {index}});
    for (let n = 0; n < 3; n++) {
      const response = await page.request.get('/api/console/v1/session');
      expect(response.status()).toBe(200);
      await page.waitForTimeout(100);
    }
    expect((await replacement).ok()).toBeTruthy();
    await page.reload();
    await expect(page.getByRole('navigation', {name: 'Management'})).toBeVisible();
    await page.getByRole('button', {name: 'Open review', exact: true}).click();
    await expect(page.getByRole('dialog').locator('pre')).toHaveText(envelope);
    expect((await context.cookies()).find(c => c.name === cookie.name)?.value).toBe(cookie.value);
  }
  await page.getByRole('button', {name: 'Confirm and submit', exact: true}).click();
  await expect(page.getByRole('heading', {name: 'Change confirmed', exact: true})).toBeVisible();
  const status = await (await page.request.get('/__test/status')).json();
  expect(status.every(replica => replica.served > 0 && !replica.draining)).toBeTruthy();
  expect(await page.evaluate(() => [localStorage.length, sessionStorage.length])).toEqual([0, 0]);
});

test('release plans and exact next-step reviews survive console replacement',async({page})=>{
 const {createHash}=await import('node:crypto');let nonce=0;
 const endpoint='https://127.0.0.1:'+process.env.DJOB_CONSOLE_TEST_HTTP_PORT;
 async function api(path,body){const response=await page.request.post(endpoint+'/api/jobs/v1/'+path,{ignoreHTTPSErrors:true,headers:{Authorization:'Bearer '+process.env.DJOB_CONSOLE_PLATFORM_TOKEN},data:{tenant_id:'alpha',command:{request_id:'shared-plan/'+(++nonce),at:new Date().toISOString()},...body}});expect(response.ok(),await response.text()).toBeTruthy();return response.json();}
 for(const id of ['shared-v1','shared-v2']){
 await api('worker-enrollments/mutate',{worker_id:id,action:'enroll',expected_revision:'0',subject:'company/test',reason:'Shared release fixture',credential_hash:createHash('sha256').update(id).digest('hex')});
 await api('worker-deployments/mutate',{id,worker_id:id,action:'put',expected_revision:'0',reason:'Shared release fixture',spec:{image:'registry.example/worker@sha256:'+(id.endsWith('v1')?'a':'b').repeat(64),config_secret:id,replicas:1}});await api('worker-deployments/mutate',{id,worker_id:id,action:'publish',expected_revision:'1',reason:'Shared release fixture'});
 }
 await api('mutate',{put_definition:{definition:{id:'shared-plan-job',runtime:'steps/v1',handler:'sha256',handler_version:'1',worker_release:'shared-v1'}}});
 await page.goto('/#/deployments?tenant=alpha');await page.getByLabel('API credential',{exact:true}).fill(process.env.DJOB_CONSOLE_PLATFORM_TOKEN);await page.getByRole('button',{name:'Sign in',exact:true}).click();await page.getByRole('button',{name:'Promote or roll back',exact:true}).click();await page.getByLabel('Source release',{exact:true}).fill('shared-v1');await page.getByLabel('Target release',{exact:true}).fill('shared-v2');await page.getByLabel('I explicitly accept proceeding without a successful target canary.',{exact:true}).check();await page.getByLabel('I explicitly accept proceeding without observed target availability and capabilities.',{exact:true}).check();await page.getByRole('button',{name:'Build reviewed plan',exact:true}).click();await expect(page.getByRole('heading',{name:'Release plan',exact:true})).toBeVisible();
 for(const index of [0,1]){expect((await page.request.post('/__test/roll',{data:{index}})).ok()).toBeTruthy();}
 await page.reload();await page.getByRole('button',{name:'Promote or roll back',exact:true}).click();await page.getByRole('button',{name:'Open retained plan',exact:true}).click();await page.getByRole('button',{name:'Review next step',exact:true}).click();await expect(page.getByRole('heading',{name:'Review changes',exact:true})).toBeVisible();const envelope=await page.getByRole('dialog').locator('pre').first().innerText();
 for(const index of [0,1]){expect((await page.request.post('/__test/roll',{data:{index}})).ok()).toBeTruthy();}
 await page.reload();await page.getByRole('button',{name:'Open review',exact:true}).click();await expect(page.getByRole('dialog').locator('pre').first()).toHaveText(envelope);await page.getByRole('button',{name:'Confirm and submit',exact:true}).click();await expect(page.getByRole('heading',{name:'Change confirmed',exact:true})).toBeVisible();
 await page.getByRole('button',{name:'Done',exact:true}).click();await page.getByRole('button',{name:'Promote or roll back',exact:true}).click();await page.getByRole('button',{name:'Open retained plan',exact:true}).click();await expect(page.getByRole('dialog')).toContainText('1 / 1');
});
