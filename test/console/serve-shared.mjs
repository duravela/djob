import {spawn, execFileSync} from 'node:child_process';
import {mkdtempSync, writeFileSync, readFileSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createServer, request as httpRequest} from 'node:http';
import {request as httpsRequest} from 'node:https';
import net from 'node:net';

const bin = process.env.DJOB_CONSOLE_TEST_BIN_DIR;
if (!bin) throw new Error('Run scripts/test-console-shared.sh');
const dir = mkdtempSync(join(tmpdir(), 'djob-shared-browser-'));
const address = name => '127.0.0.1:' + process.env['DJOB_CONSOLE_TEST_' + name + '_PORT'];
const origin = 'http://' + address('CONSOLE');
const controller = 'https://' + address('HTTP');
const cert = join(dir, 'tls.pem'), key = join(dir, 'tls-key.pem'), auth = join(dir, 'auth.json');
execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', key, '-out', cert, '-days', '1', '-subj', '/CN=localhost', '-addext', 'subjectAltName=IP:127.0.0.1,DNS:localhost'], {stdio: 'ignore'});
writeFileSync(auth, JSON.stringify({credentials: [
  {subject: 'platform', role: 'operator', token: process.env.DJOB_CONSOLE_PLATFORM_TOKEN},
  {subject: 'writer', role: 'writer', tenant_id: 'alpha', token: process.env.DJOB_CONSOLE_WRITER_TOKEN},
]}), {mode: 0o600});
const ca = readFileSync(cert);
let stopping = false, proxy, round = 0;
const children = new Set(), expected = new Set(), replicas = [];
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
function start(binary, args) {
  const child = spawn(join(bin, binary), args, {stdio: ['ignore', 'inherit', 'inherit']});
  children.add(child);
  child.on('error', err => {console.error(err); stop(1);});
  child.on('exit', code => {children.delete(child); if (!stopping && !expected.has(child)) {console.error(binary + ' exited: ' + code); stop(1);}});
  return child;
}
async function stop(code = 0) {
  if (stopping) return;
  stopping = true;
  await Promise.all([...children].map(child => terminate(child)));
  proxy?.closeAllConnections(); proxy?.close();
  rmSync(dir, {recursive: true, force: true});
  process.exit(code);
}
function terminate(child) {
  expected.add(child);
  return new Promise(resolve => {
    if (child.exitCode !== null) return resolve();
    child.once('exit', resolve); child.kill('SIGTERM');
    setTimeout(() => {child.kill('SIGKILL'); resolve();}, 10000).unref();
  });
}
process.on('SIGTERM', () => stop()); process.on('SIGINT', () => stop());
function call(url, body, token, headers = {}) {
  const data = body === undefined ? null : JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const req = (url.startsWith('https:') ? httpsRequest : httpRequest)(url, {ca, method: data ? 'POST' : 'GET', headers: {...headers, ...(data ? {'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data)} : {}), ...(token ? {Authorization: 'Bearer ' + token} : {})}}, res => {
      const chunks = []; res.on('data', c => chunks.push(c)); res.on('end', () => resolve({status: res.statusCode, text: Buffer.concat(chunks).toString()}));
    });
    req.on('error', reject); req.setTimeout(3000, () => req.destroy(new Error('fixture request timeout'))); req.end(data);
  });
}
async function ready(url, headers) {
  for (let i = 0; i < 200; i++) {try {if ((await call(url, undefined, undefined, headers)).status === 200) return;} catch {} await sleep(100);}
  throw new Error('Not ready: ' + url);
}
async function port() {
  const server = net.createServer(); await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const value = server.address().port; await new Promise(resolve => server.close(resolve)); return value;
}
function startConsole(replica) {
  replica.child = start('djob-console', ['--controllers', controller, '--controller-tls-ca', cert, '--listen', '127.0.0.1:' + replica.port, '--public-url', origin, '--shared-sessions', '--drain-delay=100ms', '--shutdown-timeout=5s']);
}
try {
  start('djob', ['--data-dir', join(dir, 'controller'), '--id=1', '--peers=http://' + address('RAFT'), '--http', address('HTTP'), '--grpc', address('GRPC'), '--api-auth-file', auth, '--api-tls-cert', cert, '--api-tls-key', key, '--console-sessions', '--jobs']);
  await ready(controller + '/readyz');
  const activated = await call(controller + '/api/jobs/v1/console-sessions/activate', {Policy: {MaxRecords: 128, MaxCiphertextBytes: 67108864, Limits: {MaxCiphertextBytes: 4194304, MaxLifetime: 86400000000000}}}, process.env.DJOB_CONSOLE_PLATFORM_TOKEN);
  if (activated.status !== 200) throw new Error(activated.text);
  const tenant = await call(controller + '/api/jobs/v1/mutate', {command: {request_id: 'shared-browser/tenant', at: new Date().toISOString()}, tenant_id: 'alpha', put_tenant: {max_running: '4'}}, process.env.DJOB_CONSOLE_PLATFORM_TOKEN);
  if (tenant.status !== 200) throw new Error(tenant.text);
  for (let i = 0; i < 2; i++) {const replica = {port: await port(), draining: false, served: 0}; replicas.push(replica); startConsole(replica); await ready('http://127.0.0.1:' + replica.port + '/readyz', {Host: new URL(origin).host});}
  proxy = createServer(async (req, res) => {
    if (req.url === '/__test/status') {res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify(replicas.map(r => ({served: r.served, draining: r.draining})))); return;}
    if (req.method === 'POST' && req.url === '/__test/roll') {
      try {
        let text = ''; for await (const c of req) text += c;
        const replica = replicas[JSON.parse(text).index];
        if (!replica || replica.draining) {res.writeHead(409).end(); return;}
        replica.draining = true;
        await terminate(replica.child);
        startConsole(replica); await ready('http://127.0.0.1:' + replica.port + '/readyz', {Host: new URL(origin).host});
        replica.draining = false; res.setHeader('Content-Type', 'application/json'); res.end('{}');
      } catch (err) {res.writeHead(500).end(String(err));} return;
    }
    for (let n = 0; n < replicas.length; n++) {
      const replica = replicas[(round++) % replicas.length];
      if (replica.draining) continue;
      try {if ((await call('http://127.0.0.1:' + replica.port + '/readyz', undefined, undefined, {Host: new URL(origin).host})).status !== 200) continue;} catch {continue;}
      replica.served++;
      const upstream = httpRequest({hostname: '127.0.0.1', port: replica.port, path: req.url, method: req.method, headers: {...req.headers, host: new URL(origin).host}}, response => {res.writeHead(response.statusCode, response.headers); response.pipe(res);});
      // Never replay a forwarded POST after an ambiguous upstream failure.
      upstream.on('error', () => {if (!res.headersSent) res.writeHead(503, {'Content-Type': 'application/json'}); res.end('{"error":"upstream interrupted"}');});
      req.pipe(upstream); return;
    }
    res.writeHead(503, {'Content-Type': 'application/json'}).end('{"error":"no ready console"}');
  });
  await new Promise(resolve => proxy.listen(Number(process.env.DJOB_CONSOLE_TEST_CONSOLE_PORT), '127.0.0.1', resolve));
} catch (error) {console.error(error); await stop(1);}
