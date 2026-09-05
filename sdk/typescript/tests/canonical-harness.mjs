import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { connect, createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../../..');
const sleep = (ms) => new Promise((resolveSleep) => setTimeout(resolveSleep, ms));

async function freePort() {
  const reservation = createServer();
  reservation.listen(0, '127.0.0.1');
  await once(reservation, 'listening');
  const port = reservation.address().port;
  await new Promise((resolveClose, reject) =>
    reservation.close((error) => (error ? reject(error) : resolveClose()))
  );
  return port;
}

async function isListening(port) {
  return new Promise((resolveReady) => {
    const socket = connect({ host: '127.0.0.1', port });
    const finish = (ready) => {
      socket.destroy();
      resolveReady(ready);
    };
    socket.once('connect', () => finish(true));
    socket.once('error', () => finish(false));
    socket.setTimeout(250, () => finish(false));
  });
}

/** Every canonical runtime campaign owns a fresh broker and temporary SQLite. */
export async function withBroker(run) {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-canonical-'));
  let broker;
  let exited;
  let logs = '';
  try {
    const port = await freePort();
    broker = spawn('bun', ['src/main.ts'], {
      cwd: root,
      env: {
        PATH: process.env.PATH,
        TCP_PORT: String(port),
        HTTP_PORT: '0',
        BUNQUEUE_EMBEDDED: '0',
        BUNQUEUE_DATA_PATH: join(directory, 'broker.db'),
        LOG_LEVEL: 'error',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    exited = once(broker, 'exit');
    // Retain both complete streams for startup/runtime failure diagnostics.
    broker.stdout.on('data', (chunk) => {
      logs += chunk;
    });
    broker.stderr.on('data', (chunk) => {
      logs += chunk;
    });
    const deadline = Date.now() + 30_000;
    while (!(await isListening(port))) {
      if (broker.exitCode !== null || Date.now() >= deadline)
        throw new Error(`Broker startup failed: ${logs}`);
      await sleep(25);
    }
    await run({ host: '127.0.0.1', port, poolSize: 1 });
  } catch (error) {
    if (logs) console.error(logs);
    throw error;
  } finally {
    if (broker && broker.exitCode === null && broker.signalCode === null) {
      broker.kill('SIGTERM');
      const timer = setTimeout(() => broker.kill('SIGKILL'), 5_000);
      try {
        await exited;
      } finally {
        clearTimeout(timer);
      }
    }
    rmSync(directory, { recursive: true, force: true });
  }
}

export async function waitFor(check, timeoutMs = 15_000) {
  const deadline = Date.now() + timeoutMs;
  while (!(await check())) {
    if (Date.now() >= deadline) throw new Error('Canonical runtime condition timed out');
    await sleep(20);
  }
}
