/**
 * CONTRACT — what it takes for an embedded process to terminate.
 *
 * Run: bun test test/repro-embedded-close-hangs.test.ts
 *
 * `Queue.close()`, `Worker.close()` and `Engine.close()` release what they own, but
 * not the process. The QueueManager behind embedded mode is a process-wide singleton
 * whose maintenance intervals (cleanup, timeouts, dependency checks, stall checks,
 * DLQ maintenance, lock expiration — src/application/backgroundTasks.ts) keep the
 * event loop alive, and no client-side close path stops them: one Queue closing must
 * not disarm the timers another Queue in the same process still depends on.
 *
 * The supported shutdown is therefore `close()` THEN `shutdownManager()`, which is
 * what the guides document (guide/quickstart, integrations, hono, elysia,
 * simple-mode) and what actually flushes: `SqliteStorage.close()` drains the write
 * buffer and checkpoints the WAL before closing the database.
 *
 * These tests pin that contract in both directions, because a script that ends
 * without terminating is the shape of a CLI task, a cron container or a CI step, and
 * the workaround people reach for on their own — a bare `process.exit()` — is exactly
 * what truncates unflushed writes.
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const CLIENT_SRC = join(import.meta.dir, '..', 'src', 'client');

let dir = '';

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'bq-close-'));
});

afterEach(() => {
  if (dir && existsSync(dir)) rmSync(dir, { recursive: true, force: true });
});

/** Spawn a child and report whether it terminated on its own within the budget. */
async function exitsOnItsOwn(script: string, budgetMs: number): Promise<boolean> {
  const path = join(dir, `child-${Math.random().toString(36).slice(2)}.ts`);
  writeFileSync(path, script);
  const proc = Bun.spawn(['bun', path, dir], { stdout: 'pipe', stderr: 'pipe' });

  const exited = await Promise.race([
    proc.exited.then(() => true),
    Bun.sleep(budgetMs).then(() => false),
  ]);
  if (!exited) proc.kill('SIGKILL');
  await proc.exited;
  return exited;
}

describe('embedded shutdown contract', () => {
  test('close() + shutdownManager() lets a Queue/Worker script terminate', async () => {
    const script = `
import { Queue, Worker, shutdownManager } from ${JSON.stringify(CLIENT_SRC)};
const dir = Bun.argv[2];
const dataPath = dir + '/q.db';

const queue = new Queue('tasks', { embedded: true, dataPath });
const worker = new Worker('tasks', async () => ({ ok: true }), { embedded: true, dataPath });

await queue.add('job', { n: 1 });
await Bun.sleep(500);

await worker.close(true);
queue.close();
shutdownManager();   // stops the process-wide maintenance timers AND flushes
// No process.exit(): reaching the end of the module must be enough.
`;
    expect(await exitsOnItsOwn(script, 20_000)).toBe(true);
  }, 60_000);

  test('close() + shutdownManager() lets a workflow Engine script terminate', async () => {
    const script = `
import { shutdownManager } from ${JSON.stringify(CLIENT_SRC)};
import { Workflow, Engine } from ${JSON.stringify(join(CLIENT_SRC, 'workflow'))};
const dir = Bun.argv[2];

const flow = new Workflow('flow').step('only', () => ({ done: true }));
const engine = new Engine({ embedded: true, dataPath: dir + '/wf.db' });
engine.register(flow);
const run = await engine.start('flow', {});

const deadline = Date.now() + 5000;
while (engine.getExecution(run.id)?.state !== 'completed' && Date.now() < deadline) {
  await Bun.sleep(25);
}
await engine.close(true);
shutdownManager();
`;
    expect(await exitsOnItsOwn(script, 20_000)).toBe(true);
  }, 60_000);

  test('close() alone leaves the process alive — the documented reason shutdownManager exists', async () => {
    const script = `
import { Queue } from ${JSON.stringify(CLIENT_SRC)};
const dir = Bun.argv[2];
const queue = new Queue('tasks', { embedded: true, dataPath: dir + '/q2.db' });
await queue.add('job', { n: 1 });
await Bun.sleep(300);
queue.close();   // deliberately WITHOUT shutdownManager()
`;
    // Pinned so the day this becomes true, whoever changed it notices that the
    // guides' shutdown snippets can be simplified — rather than it drifting silently.
    expect(await exitsOnItsOwn(script, 8_000)).toBe(false);
  }, 60_000);
});
