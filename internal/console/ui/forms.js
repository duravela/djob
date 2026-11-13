import {encodeBytes} from './model.js';

export function buildCompanyMutation(profile,fields,tenant) {
 if(!profile||profile.tenant_id!==tenant||!Array.isArray(profile.fields)||profile.fields.length>32)throw new Error('The company form does not match this tenant.');
 const id=String(fields.get('id')||'').trim();if(!id||new TextEncoder().encode(id).length>256)throw new Error('Provide a job ID within 256 UTF-8 bytes.');
 const input=[];
 for(const field of profile.fields) {
  const raw=String(fields.get('company_'+field.name)??'');
  if(raw===''&&field.required)throw new Error(field.label+' is required.');
  if(raw==='')continue;
  let value;
  if(field.type==='string')value=JSON.stringify(raw);
  else if(field.type==='boolean'){if(!['true','false'].includes(raw))throw new Error(field.label+' must be true or false.');value=raw;}
  else if(field.type==='integer'||field.type==='uint64'){
   if(!/^-?(0|[1-9][0-9]*)$/.test(raw))throw new Error(field.label+' must be an exact integer.');const n=BigInt(raw),min=field.type==='uint64'?0n:-9223372036854775808n,max=field.type==='uint64'?18446744073709551615n:9223372036854775807n;if(n<min||n>max)throw new Error(field.label+' is outside its integer range.');value=raw;
  } else if(field.type==='number'){if(!/^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$/.test(raw)||!Number.isFinite(Number(raw)))throw new Error(field.label+' must be a finite JSON number.');value=raw;}
  else if(field.type==='json'){try{JSON.parse(raw);}catch{throw new Error(field.label+' must be valid JSON.');}value=raw;}
  else throw new Error('Unsupported company form field.');
  input.push(JSON.stringify(field.name)+':'+value);
 }
 const payload='{'+input.join(',')+'}';if(new TextEncoder().encode(payload).length>65536)throw new Error('Company input exceeds 64 KiB.');
 return {tenant_id:tenant,put_definition:{expected_revision:'0',definition:{id,runtime:profile.runtime,handler:profile.handler,handler_version:profile.handler_version,binding_id:profile.binding_id||'',worker_release:profile.worker_release||'',resources:profile.resources||[],input:encodeBytes(payload,'json')}}};
}
