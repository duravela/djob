import base from './playwright.config.mjs';
export default {...base, outputDir: '../../tmp/console-shared-browser-results', testMatch: 'shared.spec.mjs', testIgnore: [], webServer: {...base.webServer, command: 'node serve-shared.mjs'}};
