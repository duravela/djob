import test from 'node:test';
import assert from 'node:assert/strict';
import {buildCompanyMutation} from './forms.js';
const profile={id:'sample',tenant_id:'alpha',runtime:'steps/v1',handler:'company',handler_version:'1',worker_release:'v1',fields:[{name:'serial',label:'Serial',type:'uint64',required:true},{name:'enabled',label:'Enabled',type:'boolean'},{name:'options',label:'Options',type:'json'}]};
test('company forms preserve exact uint64 and JSON input bytes and fixed handler routing',()=>{
 const fields=new Map(Object.entries({id:'sample-job',company_serial:'18446744073709551615',company_enabled:'false',company_options:'{"n":9007199254740993}',handler:'forged',worker_release:'v2'}));
 const q=buildCompanyMutation(profile,fields,'alpha');assert.equal(q.put_definition.definition.handler,'company');assert.equal(q.put_definition.definition.worker_release,'v1');
 assert.equal(Buffer.from(q.put_definition.definition.input,'base64').toString(),'{"serial":18446744073709551615,"enabled":false,"options":{"n":9007199254740993}}');
});
test('company forms reject foreign scope, missing values, malformed input and overflowing integers',()=>{
 for(const value of ['', '-1','18446744073709551616','1.5','1e3']) assert.throws(()=>buildCompanyMutation(profile,new Map(Object.entries({id:'job',company_serial:value})),'alpha'));
 assert.throws(()=>buildCompanyMutation(profile,new Map([['id','job'],['company_serial','1']]),'beta'));
 assert.throws(()=>buildCompanyMutation(profile,new Map([['id','job'],['company_serial','1'],['company_options','{']]),'alpha'));
});
