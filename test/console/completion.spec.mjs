import {test, expect} from '@playwright/test';
import {createHash} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {writeFile,readFile} from 'node:fs/promises';
import {resolve} from 'node:path';
const execute=promisify(execFile);
const controller=()=> 'http://127.0.0.1:'+process.env.DJOB_CONSOLE_TEST_HTTP_PORT;
const auth=role=>({Authorization:'Bearer '+process.env['DJOB_CONSOLE_'+role+'_TOKEN']});
async function signIn(page,role='PLATFORM',path='/#/jobs?tenant=alpha') {
 await page.goto(path);await page.getByLabel('API credential',{exact:true}).fill(process.env['DJOB_CONSOLE_'+role+'_TOKEN']);await page.getByRole('button',{name:'Sign in',exact:true}).click();await expect(page.getByRole('navigation',{name:'Management'})).toBeVisible();
}
async function mutate(page,id,body,tenant='alpha') {
 const response=await page.request.post(controller()+'/api/jobs/v1/mutate',{headers:auth('PLATFORM'),data:{tenant_id:tenant,command:{request_id:'completion/'+id,at:new Date().toISOString()},...body}});expect(response.ok(),await response.text()).toBeTruthy();return response.json();
}
async function confirm(page) {
 await page.getByRole('button',{name:'Confirm and submit',exact:true}).click();await expect(page.getByRole('heading',{name:'Change confirmed',exact:true})).toBeVisible();await page.getByRole('button',{name:'Done',exact:true}).click();await expect(page.getByRole('dialog')).not.toBeVisible();
}

test('company forms generate reviewed opaque input without rounding uint64 values',async({page},testInfo)=>{
 await signIn(page,'WRITER');await page.getByRole('button',{name:'New from company form',exact:true}).click();await page.getByRole('button',{name:'Company checksum',exact:true}).click();
 await page.getByLabel('Job ID',{exact:true}).fill('company-form-job');await page.getByLabel('Sequence',{exact:true}).fill('18446744073709551615');await page.getByLabel('Description',{exact:true}).fill('Meter reading');
 await page.screenshot({path:testInfo.outputPath('company-form.png'),fullPage:true});await page.getByRole('button',{name:'Review request',exact:true}).click();
 const request=JSON.parse(await page.getByRole('dialog').locator('pre').first().innerText());const input=Buffer.from(request.put_definition.definition.input,'base64').toString();expect(input).toBe('{"sequence":18446744073709551615,"label":"Meter reading"}');expect(request.put_definition.definition.handler).toBe('sha256');await confirm(page);
 await page.getByRole('link',{name:'company-form-job',exact:true}).click();await page.getByRole('button',{name:'Launch now',exact:true}).click();await page.getByLabel('Run ID (optional)',{exact:true}).fill('company-form-run');await page.getByRole('button',{name:'Review request',exact:true}).click();await confirm(page);
 await expect.poll(async()=>{
 const result=await page.request.post(controller()+'/api/jobs/v1/get',{headers:auth('READER'),data:{reference:{kind:'RUN',tenant_id:'alpha',id:'company-form-run'}}});return (await result.json()).run.status;
 }).toBe('succeeded');
});

test('operator deletes terminal history and inspects its permanent audit',async({page})=>{
 await mutate(page,'purge-definition',{put_definition:{definition:{id:'purge-job',runtime:'steps/v1',handler:'not-installed',handler_version:'1'}}});await mutate(page,'purge-launch',{launch:{definition_id:'purge-job',definition_revision:'1',run_id:'purge-run'}});await mutate(page,'purge-cancel',{cancel:{run_id:'purge-run',reason:'Withdraw synthetic job'}});
 await signIn(page,'PLATFORM','/#/runs?tenant=alpha&id=purge-run');await page.getByRole('button',{name:'Delete retained history',exact:true}).click();
 await page.getByLabel('Evidence and reason',{exact:true}).fill('Remove synthetic completed history');await page.getByLabel('I understand that retained run history will be permanently removed.',{exact:true}).check();await page.getByRole('button',{name:'Review request',exact:true}).click();
 const request=JSON.parse(await page.getByRole('dialog').locator('pre').first().innerText());expect(request.purge.run_id).toBe('purge-run');expect(request.purge.batch_size).toBe(128);
 await page.getByRole('button',{name:'Confirm and submit',exact:true}).click();await expect(page.getByRole('heading',{name:'Change confirmed',exact:true})).toBeVisible();await page.getByRole('link',{name:'Open result',exact:true}).click();
 await expect(page.getByRole('dialog')).toContainText('Remove synthetic completed history');await expect(page.getByRole('dialog')).toContainText('console/platform');
 const gone=await page.request.post(controller()+'/api/jobs/v1/get',{headers:auth('READER'),data:{reference:{kind:'RUN',tenant_id:'alpha',id:'purge-run'}}});expect(gone.status()).toBe(404);
});

test('weighted policy creates pinned runs and busy releases reject restart and scale-down',async({page})=>{
 const tenant='weighted-browser';let nonce=0;
 async function api(path,body,ok=true){const response=await page.request.post(controller()+'/api/jobs/v1/'+path,{headers:auth('PLATFORM'),data:{tenant_id:tenant,command:{request_id:'weighted-browser/'+(++nonce),at:new Date().toISOString()},...body}});expect(response.ok(),await response.text()).toBe(ok);return response.json();}
 await mutate(page,'weighted-tenant',{put_tenant:{max_running:'4'}},tenant);
 for(const id of ['v1','v2']){
 await api('worker-enrollments/mutate',{worker_id:id,action:'enroll',expected_revision:'0',subject:'company/'+id,reason:'Weighted fixture',credential_hash:createHash('sha256').update('weighted/'+id).digest('hex')});
 await api('worker-deployments/mutate',{id,worker_id:id,action:'put',expected_revision:'0',reason:'Weighted fixture',spec:{image:'ghcr.io/company/worker@sha256:'+(id==='v1'?'a':'b').repeat(64),config_secret:'worker-'+id,replicas:2}});
 await api('worker-deployments/mutate',{id,worker_id:id,action:'publish',expected_revision:'1',reason:'Publish weighted fixture'});
 }
 await signIn(page,'PLATFORM','/#/jobs?tenant='+tenant);await page.getByRole('button',{name:'New job',exact:true}).click();await page.getByLabel('ID',{exact:true}).fill('weighted');await page.getByLabel('Handler',{exact:true}).fill('not-installed');await page.getByLabel('Release weights (optional)',{exact:true}).fill('{"v1":5000,"v2":5000}');await page.getByRole('button',{name:'Review request',exact:true}).click();await confirm(page);
 const counts={v1:0,v2:0};
 for(let n=0;n<12;n++) {const ack=await api('mutate',{launch:{definition_id:'weighted',definition_revision:'1',run_id:'weighted-'+n}});const response=await page.request.post(controller()+'/api/jobs/v1/get',{headers:auth('PLATFORM'),data:{reference:{kind:'RUN',tenant_id:tenant,id:ack.reference.id}}});const run=(await response.json()).run;expect(['v1','v2']).toContain(run.definition.worker_release);counts[run.definition.worker_release]++;}
 expect(counts.v1).toBeGreaterThan(0);expect(counts.v2).toBeGreaterThan(0);
 for(const [action,extra] of [['restart',{}],['scale',{replicas:1}]])await api('worker-deployments/mutate',{id:'v1',worker_id:'v1',action,expected_revision:'2',reason:'Must refuse busy disruption',...extra},false);
 await api('worker-deployments/mutate',{id:'v1',worker_id:'v1',action:'scale',replicas:3,expected_revision:'2',reason:'Add capacity while busy'});
});


test('reviewed release plans apply and rollback through real public APIs',async({page},testInfo)=>{
 const tenant='promotion-cli';let nonce=0;
 async function api(path,body){const response=await page.request.post(controller()+'/api/jobs/v1/'+path,{headers:auth('PLATFORM'),data:{tenant_id:tenant,command:{request_id:'promotion-cli/'+(++nonce),at:new Date().toISOString()},...body}});expect(response.ok(),await response.text()).toBeTruthy();return response.json();}
 await mutate(page,'promotion-tenant',{put_tenant:{max_running:'4'}},tenant);
 for(const id of ['v1','v2']) {
 await api('worker-enrollments/mutate',{worker_id:id,action:'enroll',expected_revision:'0',subject:'company/'+id,reason:'Promotion fixture',credential_hash:createHash('sha256').update('promotion/'+id).digest('hex')});
 await api('worker-deployments/mutate',{id,worker_id:id,action:'put',expected_revision:'0',reason:'Promotion fixture',spec:{image:'ghcr.io/company/worker@sha256:'+(id==='v1'?'a':'b').repeat(64),config_secret:'worker-'+id,replicas:1}});
 await api('worker-deployments/mutate',{id,worker_id:id,action:'publish',expected_revision:'1',reason:'Publish promotion fixture'});
 }
 await api('mutate',{put_definition:{definition:{id:'job',runtime:'steps/v1',handler:'not-installed',handler_version:'1',worker_release:'v1'}}});
 await api('mutate',{put_trigger:{definition:{id:'events',definition_id:'job',definition_revision:'1',event:{source:'fixture'},paused:true,max_pending:1}}});
 await api('mutate',{launch:{definition_id:'job',definition_revision:'1',run_id:'before-promotion'}});
 const credential=testInfo.outputPath('operator.headers'),planFile=testInfo.outputPath('promotion.json'),rollbackFile=testInfo.outputPath('rollback.json');
 await writeFile(credential,'Authorization: Bearer '+process.env.DJOB_CONSOLE_PLATFORM_TOKEN+'\n',{mode:0o600});
 const tool=resolve('..','..','scripts','manage-release.py');
 const args=['--controllers',controller(),'--credential-file',credential,'--plan-file',planFile];
 await execute('python3',[tool,'plan',...args,'--tenant',tenant,'--source','v1','--target','v2','--allow-unqualified']);
 const reviewed=await execute('python3',[tool,'review','--plan-file',planFile]);const checksum=reviewed.stdout.match(/SHA256: ([a-f0-9]{64})/)[1];
 let blocked=false;try {await execute('python3',[tool,'apply',...args,'--approve',checksum]);}catch(error){blocked=error.stderr.includes('could not be established');}expect(blocked).toBeTruthy();
 await execute('python3',[tool,'apply',...args,'--approve',checksum,'--allow-unobserved']);
 const get=async(kind,id)=>{const response=await page.request.post(controller()+'/api/jobs/v1/get',{headers:auth('PLATFORM'),data:{reference:{kind,tenant_id:tenant,id}}});expect(response.ok(),await response.text()).toBeTruthy();return response.json();};
 expect((await get('DEFINITION','job')).definition.worker_release).toBe('v2');expect((await get('TRIGGER','events')).trigger.definition.definition_revision).toBe('2');expect((await get('RUN','before-promotion')).run.definition.worker_release).toBe('v1');
 await api('mutate',{launch:{definition_id:'job',definition_revision:'2',run_id:'after-promotion'}});
 await execute('python3',[tool,'rollback',...args,'--output',rollbackFile,'--allow-unqualified']);const rollbackReview=await execute('python3',[tool,'review','--plan-file',rollbackFile]);const rollbackChecksum=rollbackReview.stdout.match(/SHA256: ([a-f0-9]{64})/)[1];
 await execute('python3',[tool,'apply','--controllers',controller(),'--credential-file',credential,'--plan-file',rollbackFile,'--approve',rollbackChecksum,'--allow-unobserved']);
 expect((await get('DEFINITION','job')).definition.worker_release).toBe('v1');expect((await get('TRIGGER','events')).trigger.definition.definition_revision).toBe('3');expect((await get('RUN','after-promotion')).run.definition.worker_release).toBe('v2');
 const state=JSON.parse(await readFile(planFile+'.state','utf8'));expect(state.operations.every(op=>op.status==='confirmed')).toBeTruthy();
});

test('console retains a reviewed promotion and builds rollback from confirmed steps',async({page},testInfo)=>{
 const tenant='console-release-plan';let nonce=0;
 async function api(path,body){const response=await page.request.post(controller()+'/api/jobs/v1/'+path,{headers:auth('PLATFORM'),data:{tenant_id:tenant,command:{request_id:'console-release/'+(++nonce),at:new Date().toISOString()},...body}});expect(response.ok(),await response.text()).toBeTruthy();return response.json();}
 await api('mutate',{put_tenant:{max_running:'4'}});
 for(const id of ['v1','v2']){
 await api('worker-enrollments/mutate',{worker_id:id,action:'enroll',expected_revision:'0',subject:'test/company',reason:'Console release fixture',credential_hash:createHash('sha256').update('fixture-'+id).digest('hex')});
 await api('worker-deployments/mutate',{id,worker_id:id,action:'put',expected_revision:'0',reason:'Console release fixture',spec:{image:'ghcr.io/company/worker@sha256:'+(id==='v1'?'a':'b').repeat(64),config_secret:'worker-'+id,replicas:1}});
 await api('worker-deployments/mutate',{id,worker_id:id,action:'publish',expected_revision:'1',reason:'Console release fixture'});
 }
 await api('mutate',{put_definition:{definition:{id:'console-plan-job',runtime:'steps/v1',handler:'not-installed',handler_version:'1',worker_release:'v1'}}});
 await api('mutate',{put_trigger:{definition:{id:'console-plan-events',definition_id:'console-plan-job',definition_revision:'1',event:{source:'test'},paused:true,max_pending:1}}});
 await signIn(page,'PLATFORM','/#/deployments?tenant='+tenant);
 await page.getByRole('button',{name:'Promote or roll back',exact:true}).click();
 await page.getByLabel('Source release',{exact:true}).fill('v1');await page.getByLabel('Target release',{exact:true}).fill('v2');
 await page.getByLabel('I explicitly accept proceeding without a successful target canary.',{exact:true}).check();
 await page.getByLabel('I explicitly accept proceeding without observed target availability and capabilities.',{exact:true}).check();
 await page.getByRole('button',{name:'Build reviewed plan',exact:true}).click();await expect(page.getByRole('heading',{name:'Release plan',exact:true})).toBeVisible();
 await page.screenshot({path:testInfo.outputPath('release-plan-desktop.png'),fullPage:true});await page.setViewportSize({width:390,height:844});expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBeTruthy();await page.screenshot({path:testInfo.outputPath('release-plan-mobile.png'),fullPage:false});await page.setViewportSize({width:1280,height:720});
 for(let i=0;i<2;i++){
 await page.getByRole('button',{name:'Review next step',exact:true}).click();await expect(page.getByRole('heading',{name:'Review changes',exact:true})).toBeVisible();
 await page.getByRole('button',{name:'Confirm and submit',exact:true}).click();await expect(page.getByRole('heading',{name:'Change confirmed',exact:true})).toBeVisible();
 await page.getByRole('button',{name:'Open release plan',exact:true}).click();
 }
 await expect(page.getByRole('dialog')).toContainText('2 / 2');
 await page.reload();await page.getByRole('button',{name:'Promote or roll back',exact:true}).click();await page.getByRole('button',{name:'Open retained plan',exact:true}).click();await expect(page.getByRole('dialog')).toContainText('2 / 2');
 await page.getByRole('button',{name:'Prepare rollback',exact:true}).click();await page.getByLabel('I explicitly accept proceeding without a successful target canary.',{exact:true}).check();await page.getByLabel('I explicitly accept proceeding without observed target availability and capabilities.',{exact:true}).check();await page.getByRole('button',{name:'Build reviewed plan',exact:true}).click();await expect(page.getByRole('heading',{name:'Release plan',exact:true})).toBeVisible();
 for(let i=0;i<2;i++){await page.getByRole('button',{name:'Review next step',exact:true}).click();await page.getByRole('button',{name:'Confirm and submit',exact:true}).click();await expect(page.getByRole('heading',{name:'Change confirmed',exact:true})).toBeVisible();await page.getByRole('button',{name:'Open release plan',exact:true}).click();}
 const response=await page.request.post(controller()+'/api/jobs/v1/get',{headers:auth('PLATFORM'),data:{reference:{tenant_id:tenant,kind:'DEFINITION',id:'console-plan-job'}}});expect((await response.json()).definition.worker_release).toBe('v1');
});
