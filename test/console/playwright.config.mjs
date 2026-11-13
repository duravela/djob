import {defineConfig} from '@playwright/test';
import {randomBytes} from 'node:crypto';
import net from 'node:net';

for (const role of ['PLATFORM', 'WRITER', 'READER']) {
  process.env['DJOB_CONSOLE_' + role + '_TOKEN'] ||= randomBytes(32).toString('base64url');
}
for (const name of ['HTTP', 'GRPC', 'RAFT', 'CONSOLE', 'TARGET']) {
  const key = 'DJOB_CONSOLE_TEST_' + name + '_PORT';
  if (!process.env[key]) {
    const server = net.createServer();
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    process.env[key] = String(server.address().port);
    await new Promise(resolve => server.close(resolve));
  }
}
const baseURL = 'http://127.0.0.1:' + process.env.DJOB_CONSOLE_TEST_CONSOLE_PORT;
export default defineConfig({
  testDir: '.', testMatch: '*.spec.mjs', testIgnore: 'shared.spec.mjs', workers: 1, timeout: 60000,
  outputDir: '../../tmp/console-browser-results',
  use: {baseURL, browserName: 'chromium', screenshot: 'only-on-failure', trace: 'retain-on-failure'},
  webServer: {command: 'node serve.mjs', url: baseURL + '/healthz', timeout: 60000, reuseExistingServer: false, gracefulShutdown: {signal: 'SIGTERM', timeout: 10000}},
});
