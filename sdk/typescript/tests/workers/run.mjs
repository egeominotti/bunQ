/** Workers e2e runner — spawns two real bunqueue servers (plain + auth),
 * boots the worker inside workerd via wrangler's unstable_dev, hits every
 * route and asserts the JSON assertions computed inside the Workers runtime.
 *
 * Usage: node run.mjs   (requires `bun` for the server and npm-installed deps)
 */

import { spawn } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { connect } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { unstable_dev } from 'wrangler';

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..', '..', '..');
const AUTH_TOKEN = 'workers-secret';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function portOpen(port) {
  return new Promise((res) => {
    const s = connect({ host: '127.0.0.1', port }, () => {
      s.destroy();
      res(true);
    });
    s.on('error', () => res(false));
    s.setTimeout(500, () => {
      s.destroy();
      res(false);
    });
  });
}

async function startServer(extraEnv = {}) {
  const port = 22_000 + Math.floor(Math.random() * 8_000);
  const proc = spawn('bun', ['src/main.ts'], {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      TCP_PORT: String(port),
      HTTP_PORT: String(port + 1),
      BUNQUEUE_DATA_PATH: join(mkdtempSync(join(tmpdir(), 'bunqueue-wk-')), 'bunq.db'),
      ...extraEnv,
    },
    stdio: 'ignore',
  });
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    if (await portOpen(port)) return { port, proc };
    await sleep(100);
  }
  proc.kill();
  throw new Error('bunqueue server did not start');
}

const CHECKS = {
  '/add-query': (r) =>
    r.name === 'checkout' &&
    r.priority === 9 &&
    r.customMatch === true &&
    ['waiting', 'prioritized'].includes(r.state) &&
    r.missingIsNull === true &&
    Array.isArray(r.tags) &&
    r.tags[0] === 'cf',
  '/bulk': (r) => r.created === 200 && r.count === 200 && r.uniqueIds === 200,
  '/controls': (r) =>
    r.paused === true &&
    r.resumed === false &&
    r.stateDelayed === 'delayed' &&
    ['waiting', 'prioritized'].includes(r.statePromoted) &&
    r.updatedV === 2 &&
    r.updatedPriority === 77 &&
    r.removed === true,
  '/big': (r) => r.intact === true,
  '/unicode': (r) => r.emoji === true && r.jp === true && r.deep === true,
  '/pipeline': (r) => r.created === 100 && r.count === 100 && r.uniqueIds === 100,
  '/consume': (r) => r.pulled === 10 && r.completed === 10 && r.sampleOk === true,
  '/dlq': (r) =>
    r.state === 'failed' &&
    r.dlqSize === 1 &&
    r.retried === 1 &&
    ['waiting', 'prioritized'].includes(r.stateAfter),
  '/flows': (r) => r.chainLength === 3 && r.children === 2 && r.parentWaitsChildren === true,
  '/cron': (r) => r.listed === true && r.spawned >= 1 && r.removed === true,
  '/auth': (r) => r.authedAdd === true && r.unauthedRejected === true,
};

const main = await startServer();
const authSrv = await startServer({ AUTH_TOKENS: AUTH_TOKEN });
let worker;
let failed = 0;

try {
  worker = await unstable_dev('worker-app.ts', {
    config: 'wrangler.toml',
    vars: {
      BQ_HOST: '127.0.0.1',
      BQ_PORT: String(main.port),
      BQ_AUTH_PORT: String(authSrv.port),
      BQ_AUTH_TOKEN: AUTH_TOKEN,
    },
    experimental: { disableExperimentalWarning: true },
  });

  for (const [route, check] of Object.entries(CHECKS)) {
    const started = Date.now();
    try {
      const resp = await worker.fetch(`http://w${route}`);
      const body = await resp.json();
      if (!body.ok) throw new Error(body.error ?? `HTTP ${resp.status}`);
      if (!check(body)) throw new Error(`assertion failed: ${JSON.stringify(body)}`);
      console.log(`PASS workers ${route} (${Date.now() - started}ms)`);
    } catch (err) {
      failed += 1;
      console.error(`FAIL workers ${route}: ${err.message ?? err}`);
    }
  }
} finally {
  if (worker) await worker.stop();
  main.proc.kill();
  authSrv.proc.kill();
}

const total = Object.keys(CHECKS).length;
console.log(`\n${total - failed}/${total} passed (inside workerd)`);
process.exit(failed > 0 ? 1 : 0);
