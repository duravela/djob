import {spawn} from 'node:child_process';
import {mkdtempSync, writeFileSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createServer} from 'node:http';

const binaries = process.env.DJOB_CONSOLE_TEST_BIN_DIR;
if (!binaries) throw new Error('Run scripts/test-console.sh to build the real controller and console first.');
const data = mkdtempSync(join(tmpdir(), 'djob-console-browser-'));
const auth = join(data, 'auth.json');
const forms = join(data, 'forms.json');
writeFileSync(forms, JSON.stringify([{id:'company_checksum',label:'Company checksum',tenant_id:'alpha',runtime:'steps/v1',handler:'sha256',handler_version:'1',fields:[{name:'sequence',label:'Sequence',type:'uint64',required:true},{name:'label',label:'Description',type:'string',required:true}]}]),{mode:0o600});
const credentials = [
  {subject: 'console/platform', role: 'operator', token: process.env.DJOB_CONSOLE_PLATFORM_TOKEN},
  {subject: 'console/writer', role: 'writer', tenant_id: 'alpha', token: process.env.DJOB_CONSOLE_WRITER_TOKEN},
  {subject: 'console/reader', role: 'reader', tenant_id: 'alpha', token: process.env.DJOB_CONSOLE_READER_TOKEN},
];
writeFileSync(auth, JSON.stringify({credentials}), {mode: 0o600});
const address = name => '127.0.0.1:' + process.env['DJOB_CONSOLE_TEST_' + name + '_PORT'];
const controllerURL = 'http://' + address('HTTP');
const children = [];
const counts = new Map();
let target;
let stopping = false;
function start(binary, args) {
  const child = spawn(join(binaries, binary), args, {stdio: ['ignore', 'inherit', 'inherit']});
  children.push(child);
  child.on('error', async err => { console.error(err.message); await stop(1); });
  child.on('exit', code => { if (!stopping) { console.error(binary + ' exited: ' + code); stop(1); } });
}
async function stop(code = 0) {
  if (stopping) return;
  stopping = true;
  await Promise.all(children.map(child => new Promise(resolve => {
    if (child.exitCode !== null) return resolve();
    child.once('exit', resolve); child.kill('SIGTERM');
    setTimeout(() => { child.kill('SIGKILL'); resolve(); }, 15000).unref();
  })));
  target?.closeAllConnections();
  target?.close();
  rmSync(data, {recursive: true, force: true});
  process.exit(code);
}
process.on('SIGTERM', () => stop());
process.on('SIGINT', () => stop());
async function ready(url) {
  const deadline = Date.now() + 40000;
  while (Date.now() < deadline) {
    try { if ((await fetch(url)).ok) return; } catch {}
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  throw new Error('Fixture did not become ready: ' + url);
}
try {
  // Synthetic, resource-free HTTP effects exercise the public worker records.
  // All traffic stays on loopback; the fixture never contacts a real gateway.
  target = createServer((request, response) => {
    if (request.method === 'GET' && request.url === '/counts') {
      response.setHeader('Content-Type', 'application/json');
      response.end(JSON.stringify(Object.fromEntries(counts))); return;
    }
    if (request.method !== 'POST' || !request.url.startsWith('/effects/')) { response.writeHead(404).end(); return; }
    counts.set(request.url, (counts.get(request.url) || 0) + 1);
    request.resume();
    request.once('end', () => {
      if (request.url.endsWith('/uncertain')) { request.socket.destroy(); return; }
      response.setHeader('Content-Type', 'application/json');
      response.writeHead(202).end(JSON.stringify({accepted: true}));
    });
  });
  await new Promise((resolve, reject) => { target.once('error', reject); target.listen(Number(process.env.DJOB_CONSOLE_TEST_TARGET_PORT), '127.0.0.1', resolve); });
  start('djob', ['--data-dir', join(data, 'controller'), '--id', '1', '--peers', 'http://' + address('RAFT'), '--http', address('HTTP'), '--grpc', address('GRPC'), '--api-auth-file', auth, '--jobs', '--jobs-http']);
  await ready(controllerURL + '/healthz');
  const response = await fetch(controllerURL + '/api/jobs/v1/mutate', {method: 'POST', headers: {'Content-Type': 'application/json', Authorization: 'Bearer ' + process.env.DJOB_CONSOLE_PLATFORM_TOKEN}, body: JSON.stringify({command: {request_id: 'browser-fixture/alpha', at: new Date().toISOString()}, tenant_id: 'alpha', put_tenant: {max_running: '4'}})});
  if (!response.ok) throw new Error('Could not provision browser fixture tenant: ' + await response.text());
  start('djob-console', ['--forms-file', forms, '--controllers', controllerURL, '--listen', address('CONSOLE'), '--public-url', 'http://' + address('CONSOLE')]);
} catch (error) { console.error(error.message); await stop(1); }
