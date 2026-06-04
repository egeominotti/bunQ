/**
 * Repro: #92 — getJobCounts() / per-state lists disagree with the real state.
 *
 * Bug 1 — failed list is empty on the storage (standalone-server) path.
 *   A job that exhausts its attempts is moved to the `dlq` table and its `jobs`
 *   row is removed (verified: after a failure `SELECT * FROM jobs` is empty and
 *   `dlq` has 1 row). getJobCounts().failed counts the DLQ (=1), getJobState()
 *   returns 'failed', getJob(id) finds it — but getJobs({state:'failed'}) on the
 *   storage path runs `SELECT * FROM jobs WHERE state='failed'` and so returns [].
 *   The in-memory (no-storage) path already reads shard.getDlq() for 'failed',
 *   which is why embedded-in-memory does not reproduce. The standalone server and
 *   embedded-with-dataPath both go through the storage path -> empty failed list.
 *   A dedicated storage-backed QueueManager exercises exactly that path.
 *
 * Bug 2 — pause() double-counts and the paused list is empty.
 *   getJobCounts() returns `waiting: counts.waiting` AND `paused: counts.waiting`,
 *   so one waiting job is counted under BOTH waiting and paused. getJobs({state:
 *   'paused'}) has no branch and returns []. BullMQ semantics (the JobCounts
 *   contract this API mirrors): a paused queue reports its waiting jobs as
 *   `paused`, with waiting -> 0, and they are enumerable via the paused list.
 *
 * Both assertions are RED before the fix and GREEN after.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { Queue } from '../src/client/queue/queue';
import { getSharedManager } from '../src/client/manager';
import { unlink } from 'node:fs/promises';

const DB = './test-issue92.db';
async function cleanup() {
  for (const f of [DB, `${DB}-wal`, `${DB}-shm`]) {
    if (await Bun.file(f).exists()) await unlink(f);
  }
}
const tick = (ms = 50) => new Promise((r) => setTimeout(r, ms));

describe('#92 Bug 1: failed list must include the DLQ on the storage path', () => {
  let qm: QueueManager;
  beforeEach(async () => {
    await cleanup();
    qm = new QueueManager({ dataPath: DB });
  });
  afterEach(async () => {
    qm.shutdown();
    await cleanup();
  });

  test('an exhausted job is counted AND enumerable via getJobs({state:failed})', async () => {
    const Q = 'failq';
    await qm.push(Q, { data: { x: 1 }, maxAttempts: 1, removeOnFail: false });
    const pulled = await qm.pull(Q, 0);
    expect(pulled).not.toBeNull();
    await qm.fail(pulled!.id, 'boom');
    await tick();

    // These already work today.
    expect(await qm.getJobState(pulled!.id)).toBe('failed');
    expect(qm.getQueueJobCounts(Q).failed).toBe(1);
    expect(qm.getJob(pulled!.id)).not.toBeNull();

    // BUG: the failed LIST comes back empty on the storage path.
    const failed = qm.getJobs(Q, { state: 'failed', start: 0, end: 50 });
    expect(failed.length).toBe(1);
    expect(String(failed[0].id)).toBe(String(pulled!.id));
  });
});

describe('#92 Bug 2: pause must not double-count and the paused list must be populated', () => {
  let qm: QueueManager;
  beforeEach(async () => {
    await cleanup();
    qm = new QueueManager({ dataPath: DB });
    getSharedManager().drain('pause-counts'); // isolate the client-side count test
  });
  afterEach(async () => {
    qm.shutdown();
    await cleanup();
  });

  test('client getJobCounts(): a waiting job is counted once — under paused, not both', async () => {
    const q = new Queue('pause-counts', { embedded: true });
    await q.add('job', { x: 1 });

    const before = q.getJobCounts();
    expect(before.waiting).toBe(1);
    expect(before.paused).toBe(0);

    q.pause();
    const after = q.getJobCounts();
    // BullMQ semantics: waiting jobs move into `paused`, not counted twice.
    expect(after.paused).toBe(1);
    expect(after.waiting).toBe(0);

    q.resume();
  });

  test('client getJobCounts(): prioritized jobs are absorbed into paused too (not left in prioritized)', async () => {
    getSharedManager().drain('pause-prio-counts');
    const q = new Queue('pause-prio-counts', { embedded: true });
    await q.add('w', { x: 1 }); // priority 0 -> waiting
    await q.add('p', { x: 2 }, { priority: 10 }); // priority > 0 -> prioritized

    const before = q.getJobCounts();
    expect(before.waiting).toBe(1);
    expect(before.prioritized).toBe(1);
    expect(before.paused).toBe(0);

    q.pause();
    const after = q.getJobCounts();
    // Both ready buckets collapse into `paused`; neither is double-counted.
    expect(after.waiting).toBe(0);
    expect(after.prioritized).toBe(0);
    expect(after.paused).toBe(2);

    q.resume();
  });

  test('QueueManager.getJobs(): paused queue lists its jobs under paused, not waiting', async () => {
    const Q = 'pause-list';
    await qm.push(Q, { data: { x: 1 } });
    qm.pause(Q);

    const paused = qm.getJobs(Q, { state: 'paused', start: 0, end: 50 });
    expect(paused.length).toBe(1);

    const waiting = qm.getJobs(Q, { state: 'waiting', start: 0, end: 50 });
    expect(waiting.length).toBe(0);
  });

  test('prioritized job in a paused queue: lists agree with counts (no #92 regression)', async () => {
    const Q = 'pause-prio-list';
    await qm.push(Q, { data: { x: 1 }, priority: 10 }); // prioritized
    qm.pause(Q);

    // The job surfaces ONLY under `paused` — not prioritized, not waiting.
    expect(qm.getJobs(Q, { state: 'paused', start: 0, end: 50 }).length).toBe(1);
    expect(qm.getJobs(Q, { state: 'prioritized', start: 0, end: 50 }).length).toBe(0);
    expect(qm.getJobs(Q, { state: 'waiting', start: 0, end: 50 }).length).toBe(0);
  });

  test('failed jobs paginate on the storage path without duplicates or gaps', async () => {
    const Q = 'page-fail';
    // 2 failed first (queue empty, so pull targets exactly these), then 3 waiting.
    for (let i = 0; i < 2; i++) {
      await qm.push(Q, { data: { f: i }, maxAttempts: 1, removeOnFail: false });
      const p = await qm.pull(Q, 0);
      await qm.fail(p!.id, 'boom');
    }
    for (let i = 0; i < 3; i++) await qm.push(Q, { data: { w: i } });
    await tick();

    // 5 total (3 jobs-table + 2 DLQ). Walk it in pages of 2 — every row exactly once.
    const seen = new Set<string>();
    let total = 0;
    for (let start = 0; start < 6; start += 2) {
      const page = qm.getJobs(Q, { start, end: start + 2 });
      total += page.length;
      for (const j of page) seen.add(String(j.id));
    }
    expect(seen.size).toBe(5); // nothing missing
    expect(total).toBe(5); // nothing duplicated

    // Mixed jobs-table + DLQ state filter paginates cleanly too (no dups/gaps).
    const mixedSeen = new Set<string>();
    let mixedTotal = 0;
    for (let start = 0; start < 6; start += 2) {
      const page = qm.getJobs(Q, { state: ['waiting', 'failed'], start, end: start + 2 });
      mixedTotal += page.length;
      for (const j of page) mixedSeen.add(String(j.id));
    }
    expect(mixedSeen.size).toBe(5);
    expect(mixedTotal).toBe(5);

    // Descending order survives the merge.
    const desc = qm.getJobs(Q, { state: ['waiting', 'failed'], start: 0, end: 50, asc: false });
    expect(desc.length).toBe(5);
    for (let i = 1; i < desc.length; i++) {
      expect(desc[i].createdAt).toBeLessThanOrEqual(desc[i - 1].createdAt);
    }
  });
});

// The reporter's embedded-without-dataPath case uses the no-storage path (where
// 'failed' already read the DLQ correctly). Guard that the paused fix applies
// there too, and that the failed list keeps working.
describe('#92 in-memory path (no storage) parity', () => {
  let qm: QueueManager;
  beforeEach(() => {
    qm = new QueueManager({}); // no dataPath -> storage === null -> in-memory path
  });
  afterEach(() => {
    qm.shutdown();
  });

  test('failed list works AND paused queue lists jobs under paused, not waiting', async () => {
    const QF = 'mem-fail';
    await qm.push(QF, { data: { x: 1 }, maxAttempts: 1, removeOnFail: false });
    const pulled = await qm.pull(QF, 0);
    await qm.fail(pulled!.id, 'boom');
    await tick();
    expect(qm.getJobs(QF, { state: 'failed', start: 0, end: 50 }).length).toBe(1);

    const QP = 'mem-pause';
    await qm.push(QP, { data: { x: 1 } });
    qm.pause(QP);
    expect(qm.getJobs(QP, { state: 'paused', start: 0, end: 50 }).length).toBe(1);
    expect(qm.getJobs(QP, { state: 'waiting', start: 0, end: 50 }).length).toBe(0);
  });
});
