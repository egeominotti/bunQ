/**
 * STABILITY BUG: Worker.close(false) hangs forever when group-concurrency-limited
 * jobs are buffered.
 *
 * Source: src/client/worker/worker.ts
 *
 * Root cause:
 *   _doClose(force=false) runs a graceful drain loop (lines 523-528):
 *
 *     while (this.activeJobs > 0 || bufferSize() > 0) {
 *       await Bun.sleep(50);   // <-- NO timeout, NO escape hatch
 *     }
 *
 *   It sets `_closing = true` first. But every code path that would advance
 *   the buffer early-returns once `_closing` is set:
 *     - poll()       line 568:  `if (!this.running || this._closing) return;`
 *     - tryProcess() line 593:  `if (!this.running || this._closing) return;`
 *     - tryProcess() line 602:  re-check after await -> return on _closing
 *     - startJob().finally line 787: `if (this.running && !this._closing) this.poll();`
 *       -> when the one running job completes during close, poll() is NOT called.
 *
 *   With a GroupConcurrencyLimiter {groupKey, max:1}, only ONE job of the group
 *   may run at a time. A batch pull buffers many same-group jobs (bufferSize > 0)
 *   while only 1 is active. The dedicated "all buffered jobs are group-limited"
 *   retry path (lines 634-642) only fires from poll()/tryProcess(), which are
 *   unreachable during close. So once the single active job finishes:
 *       activeJobs == 0  but  bufferSize() > 0  forever
 *   -> the drain loop spins on `Bun.sleep(50)` and close(false) NEVER resolves.
 *
 * Correct (post-fix) behavior asserted here:
 *   close(false) must resolve in a bounded time even when group-limited jobs
 *   are buffered (graceful close should drain or release the buffer, not hang).
 *
 * This test FAILS NOW (RED): close(false) loses the Promise.race against the
 * timeout, proving the hang is real. The CONTROL test (no group limiter) shows
 * close(false) resolves promptly, isolating the group-limiter path as the cause.
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { Queue, Worker, shutdownManager } from '../src/client';

interface GroupJob {
  group: string;
  n: number;
}

const TIMEOUT_MS = 5000;
const tmpDirs: string[] = [];

function makeTmpDataPath(): string {
  const dir = mkdtempSync(join(tmpdir(), 'bq-close-grouplimit-'));
  tmpDirs.push(dir);
  return join(dir, 'queue.db');
}

beforeEach(() => {
  shutdownManager();
});

afterEach(() => {
  shutdownManager();
  for (const dir of tmpDirs.splice(0)) {
    try {
      rmSync(dir, { recursive: true, force: true });
    } catch {
      // best-effort temp cleanup
    }
  }
});

/** Resolves to 'resolved' if p settles before timeoutMs, else 'timeout'. */
async function raceTimeout(
  p: Promise<unknown>,
  timeoutMs: number
): Promise<'resolved' | 'timeout'> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeoutP = new Promise<'timeout'>((resolve) => {
    timer = setTimeout(() => resolve('timeout'), timeoutMs);
  });
  const result = await Promise.race([p.then(() => 'resolved' as const), timeoutP]);
  if (timer) clearTimeout(timer);
  return result;
}

describe('STABILITY: Worker.close(false) with buffered group-limited jobs', () => {
  test('close(false) must resolve, not hang, when same-group jobs are buffered', async () => {
    const dataPath = makeTmpDataPath();
    const queue = new Queue<GroupJob>('grp-close-hang', { embedded: true, dataPath });
    queue.obliterate();

    // Push 20 jobs all in the same group BEFORE the worker starts pulling, so a
    // single batch pull buffers many same-group jobs while only 1 may run (max:1).
    for (let i = 0; i < 20; i++) {
      await queue.add('task', { group: 'group', n: i });
    }

    let started = 0;
    const worker = new Worker<GroupJob, string>(
      'grp-close-hang',
      async (job) => {
        started++;
        // Sleep long enough that jobs remain buffered (not yet processed) when
        // we trigger close(). With max:1 only one of these runs at a time.
        await Bun.sleep(200);
        return `done-${job.data.n}`;
      },
      {
        embedded: true,
        dataPath,
        concurrency: 10,
        batchSize: 20,
        limiter: { groupKey: 'group', max: 1, duration: 0 },
      }
    );

    try {
      // Wait until the worker has pulled a batch (1 active, the rest buffered).
      const deadline = Date.now() + 3000;
      while (started < 1 && Date.now() < deadline) {
        await Bun.sleep(20);
      }
      expect(started).toBeGreaterThanOrEqual(1);

      // Give the batch pull a moment to populate the pending buffer with the
      // remaining same-group jobs (all blocked by the group limiter).
      await Bun.sleep(50);

      // Graceful close. Correct behavior: this resolves within TIMEOUT_MS.
      // Buggy behavior: the drain loop spins forever because buffered
      // group-limited jobs are never advanced during _closing.
      const outcome = await raceTimeout(worker.close(false), TIMEOUT_MS);

      expect(outcome).toBe('resolved');
    } finally {
      // Force-close to release resources. NOTE: close() dedupes via
      // _closingPromise, so a force-close after a hung graceful close returns
      // the SAME hung promise — we must not await it unbounded here or the
      // test would hang on cleanup instead of failing the assertion above.
      void worker.close(true).catch(() => {});
      queue.close();
    }
  }, 12000);

  test('CONTROL: close(false) resolves promptly WITHOUT a group limiter', async () => {
    const dataPath = makeTmpDataPath();
    const queue = new Queue<GroupJob>('grp-close-ctrl', { embedded: true, dataPath });
    queue.obliterate();

    for (let i = 0; i < 20; i++) {
      await queue.add('task', { group: 'group', n: i });
    }

    let started = 0;
    const worker = new Worker<GroupJob, string>(
      'grp-close-ctrl',
      async (job) => {
        started++;
        await Bun.sleep(200);
        return `done-${job.data.n}`;
      },
      {
        embedded: true,
        dataPath,
        concurrency: 10,
        batchSize: 20,
        // No limiter -> no group concurrency gate; buffer drains normally.
      }
    );

    try {
      const deadline = Date.now() + 3000;
      while (started < 1 && Date.now() < deadline) {
        await Bun.sleep(20);
      }
      expect(started).toBeGreaterThanOrEqual(1);
      await Bun.sleep(50);

      const outcome = await raceTimeout(worker.close(false), TIMEOUT_MS);
      expect(outcome).toBe('resolved');
    } finally {
      void worker.close(true).catch(() => {});
      queue.close();
    }
  }, 12000);
});
