/** Workers e2e runner — spawns two real bunqueue servers (plain + auth),
 * boots the worker inside workerd via wrangler's unstable_dev, hits every
 * route and asserts the JSON assertions computed inside the Workers runtime.
 *
 * Usage: node run.mjs   (requires `bun` for the server and npm-installed deps)
 */

import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { connect, createServer } from 'node:net';
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
  const reservation = createServer();
  reservation.listen(0, '127.0.0.1');
  await once(reservation, 'listening');
  const port = reservation.address().port;
  await new Promise((resolveClose) => reservation.close(resolveClose));
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-wk-'));
  const proc = spawn('bun', ['src/main.ts'], {
    cwd: REPO_ROOT,
    env: {
      PATH: process.env.PATH,
      TCP_PORT: String(port),
      HTTP_PORT: '0',
      BUNQUEUE_EMBEDDED: '0',
      BUNQUEUE_DATA_PATH: join(directory, 'bunq.db'),
      ...extraEnv,
    },
    stdio: 'ignore',
  });
  const exited = once(proc, 'exit');
  const stop = async () => {
    if (proc.exitCode === null && proc.signalCode === null) {
      proc.kill('SIGTERM');
      const timer = setTimeout(() => proc.kill('SIGKILL'), 5_000);
      try {
        await exited;
      } finally {
        clearTimeout(timer);
      }
    }
    rmSync(directory, { recursive: true, force: true });
  };
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    if (await portOpen(port)) return { port, stop };
    if (proc.exitCode !== null) break;
    await sleep(100);
  }
  await stop();
  throw new Error('bunqueue server did not start');
}

const CHECKS = {
  '/canonical-queue': (r) =>
    r.namespaces.length === 1 &&
    r.namespaces[0] === 'registered' &&
    r.count === 1 &&
    r.isolated === true &&
    r.priority === 4 &&
    r.attempts === 1 &&
    r.deduplicated === true &&
    r.groupConcurrency === 2 &&
    r.rate.max === 3 &&
    r.rate.duration === 2_000 &&
    r.paused === true &&
    r.concurrency === 3 &&
    r.logs[0] === '[info] portable log' &&
    r.logCount === 1 &&
    r.schedulerShape === true &&
    r.dlqShape === true &&
    r.failed === 1 &&
    r.metricCount === 1 &&
    r.removed === true,
  '/canonical-flow': (r) =>
    r.children === 1 &&
    r.state === 'waiting-children' &&
    r.dependencies === 1 &&
    r.portableIds === true &&
    r.countAfter === 0,
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
  '/flows': (r) =>
    r.chainLength === 3 &&
    r.children === 2 &&
    r.parentWaitsChildren === true &&
    r.portableIds === true,
  '/cron': (r) => r.listed === true && r.spawned >= 1 && r.removed === true,
  '/auth': (r) => r.authedAdd === true && r.unauthedRejected === true,
  '/api-moves': (r) =>
    r.failedState === 'failed' && r.completedState === 'completed' && r.dlqAfter === 0,
  '/api-job-methods': (r) =>
    r.delayed === 'delayed' && r.promotedOk === true && r.dataV === 2 && r.priority === 42,
  '/api-children': (r) => r.surfaced >= 1 && r.childDetached === true,
  '/api-admin-extras': (r) => r.schedulers === 1 && r.disabled === true,
  '/simple-mode': (r) =>
    r.settled === true &&
    r.middlewareRan === true &&
    r.receipt === 99 &&
    r.retried === 3 &&
    r.recovered === true &&
    r.circuit === 'closed',
};

let main;
let authSrv;
let worker;
let failed = 0;

try {
  main = await startServer();
  authSrv = await startServer({ AUTH_TOKENS: AUTH_TOKEN });
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
  if (main) await main.stop();
  if (authSrv) await authSrv.stop();
}

const total = Object.keys(CHECKS).length;
console.log(`\n${total - failed}/${total} passed (inside workerd)`);
process.exit(failed > 0 ? 1 : 0);
