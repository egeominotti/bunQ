/**
 * ISSUE #104: `finishedOn` is always `undefined` on jobs returned by
 * `queue.getJobs()` / `getJobsAsync()` / `getCompleted()` / `getFailed()` in
 * embedded mode, even for completed/failed jobs. The SAME job fetched via
 * `queue.getJob(id)` correctly returns `finishedOn` (and `processedOn`).
 *
 * Root cause:
 *   - `getJob(id)` → `toPublicJob` → `buildJobProperties` maps
 *       finishedOn: job.completedAt ?? undefined
 *       processedOn: job.startedAt ?? undefined
 *   - `getJobs()` / `getJobsAsync()` build the public job via `createSimpleJob`,
 *     which HARDCODES `processedOn: undefined` / `finishedOn: undefined`.
 *     The reflection meta (`metaFromJob`) never carries completedAt/startedAt,
 *     and the result is never patched (unlike `progress`/`priority`/`attempts`).
 *
 * CORRECT (post-fix) behavior: list endpoints populate `finishedOn` and
 * `processedOn` from the internal job's `completedAt`/`startedAt`, matching
 * `getJob(id)` exactly.
 *
 * Run with: BUNQUEUE_EMBEDDED=1 bun test test/repro-issue104-getjobs-finishedon.test.ts
 *
 * NOTE: these tests run in EMBEDDED mode (preload sets BUNQUEUE_EMBEDDED=1), so the
 * `*Async` variants short-circuit to the sync `getJobs` path. The TCP transport
 * branch (`getJobsAsync` TCP + `getJob(id)` TCP parity) is covered by
 * scripts/tcp/test-query-operations.ts → "Testing finishedOn/processedOn over TCP".
 */

import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Issue #104: getJobs() must populate finishedOn/processedOn', () => {
  let queue: Queue<{ hello: string }>;
  let worker: Worker<{ hello: string }, string>;
  const QUEUE = 'issue104-finishedon';

  /** Poll the job state until it reaches `target` (or timeout). */
  async function waitForState(id: string, target: string, timeoutMs = 3000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if ((await queue.getJobState(id)) === target) return;
      await Bun.sleep(20);
    }
    throw new Error(`job ${id} never reached state "${target}"`);
  }

  beforeEach(async () => {
    queue = new Queue<{ hello: string }>(QUEUE);
    queue.obliterate();
    await Bun.sleep(50);
  });

  afterEach(async () => {
    await worker?.close();
    shutdownManager();
  });

  it('getJobs() (sync) populates finishedOn + processedOn for a completed job', async () => {
    const job = await queue.add('test-job', { hello: 'world' });
    worker = new Worker<{ hello: string }, string>(QUEUE, async () => 'done', { concurrency: 1 });
    await waitForState(job.id, 'completed');

    const jobs = queue.getJobs({ end: 100 });
    const found = jobs.find((j) => j.id === job.id);
    expect(found).toBeDefined();

    // BUG: both are undefined pre-fix.
    expect(typeof found!.finishedOn).toBe('number');
    expect(found!.finishedOn).toBeGreaterThan(0);
    expect(typeof found!.processedOn).toBe('number');
    expect(found!.processedOn).toBeGreaterThan(0);
  });

  it('getJobs() finishedOn/processedOn match getJob(id) exactly', async () => {
    const job = await queue.add('test-job', { hello: 'world' });
    worker = new Worker<{ hello: string }, string>(QUEUE, async () => 'done', { concurrency: 1 });
    await waitForState(job.id, 'completed');

    const found = queue.getJobs({ end: 100 }).find((j) => j.id === job.id);
    const single = await queue.getJob(job.id);

    // The working path (getJob) is the reference.
    expect(single?.finishedOn).toBeDefined();
    expect(single?.processedOn).toBeDefined();

    // List path must agree with single-fetch path.
    expect(found!.finishedOn).toBe(single!.finishedOn);
    expect(found!.processedOn).toBe(single!.processedOn);
  });

  it('getCompletedAsync() populates finishedOn', async () => {
    const job = await queue.add('test-job', { hello: 'world' });
    worker = new Worker<{ hello: string }, string>(QUEUE, async () => 'done', { concurrency: 1 });
    await waitForState(job.id, 'completed');

    const completed = await queue.getCompletedAsync(0, 50);
    const found = completed.find((j) => j.id === job.id);
    expect(found).toBeDefined();
    expect(typeof found!.finishedOn).toBe('number');
    expect(found!.finishedOn).toBeGreaterThan(0);
  });

  it('getJobsAsync() (embedded) populates finishedOn', async () => {
    const job = await queue.add('test-job', { hello: 'world' });
    worker = new Worker<{ hello: string }, string>(QUEUE, async () => 'done', { concurrency: 1 });
    await waitForState(job.id, 'completed');

    const jobs = await queue.getJobsAsync({ state: 'completed', start: 0, end: 50 });
    const found = jobs.find((j) => j.id === job.id);
    expect(found).toBeDefined();
    expect(typeof found!.finishedOn).toBe('number');
  });

  it('getFailed() agrees with getJob(id) on processedOn/finishedOn', async () => {
    // A failed job WAS processed (startedAt set) → processedOn must be populated,
    // matching getJob(id). It has no completedAt (only the success path sets it),
    // so finishedOn stays undefined in BOTH paths — the point of #104 is parity.
    const job = await queue.add('test-job', { hello: 'world' }, { attempts: 1 });
    worker = new Worker<{ hello: string }, string>(
      QUEUE,
      async () => {
        throw new Error('boom');
      },
      { concurrency: 1 }
    );
    await waitForState(job.id, 'failed');

    const found = queue.getFailed(0, 50).find((j) => j.id === job.id);
    const single = await queue.getJob(job.id);
    expect(found).toBeDefined();

    // processedOn populated and consistent with the single-fetch path.
    expect(typeof single?.processedOn).toBe('number');
    expect(found!.processedOn).toBe(single!.processedOn);
    // finishedOn consistent (both undefined — no completedAt on the fail path).
    expect(found!.finishedOn).toBe(single!.finishedOn);
  });

  it('a still-waiting job leaves finishedOn undefined (no false positive)', async () => {
    // No worker → job stays waiting; finishedOn must remain undefined.
    const job = await queue.add('test-job', { hello: 'world' });
    await Bun.sleep(100);

    const found = queue.getJobs({ state: 'waiting', end: 100 }).find((j) => j.id === job.id);
    expect(found).toBeDefined();
    expect(found!.finishedOn).toBeUndefined();
    expect(found!.processedOn).toBeUndefined();
  });
});
