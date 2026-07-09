/**
 * Sibling fixes found while auditing #111 for the same "client silently drops a
 * field the wire/domain supports" class:
 *
 *  - retryJobs({ state:'failed', count }) must cap the number of DLQ entries
 *    retried (was ignored → the whole DLQ was retried).
 *  - Queue.moveJobToFailed() must persist the stacktrace (#74) and honour
 *    UnrecoverableError (both were dropped on the Queue reflection path).
 */
import { describe, test, expect } from 'bun:test';
import { Queue } from '../src/client';
import { UnrecoverableError } from '../src/client/errors';
import { getSharedManager } from '../src/client/manager';

/** Push, activate and DLQ `n` jobs on `queue` (attempts:1 → first fail = DLQ). */
async function populateDlq(queue: Queue, name: string, n: number): Promise<void> {
  const mgr = getSharedManager();
  for (let i = 0; i < n; i++) {
    await queue.add('j', { i }, { attempts: 1 });
  }
  for (let i = 0; i < n; i++) {
    const job = await mgr.pull(name, 0);
    if (job) await queue.moveJobToFailed(String(job.id), new Error(`boom-${i}`));
  }
}

describe('#111 audit siblings', () => {
  test('retryJobs({ state:failed, count }) retries only `count` DLQ entries', async () => {
    const name = 'sib-retry-count';
    const q = new Queue(name, { embedded: true });
    await populateDlq(q, name, 5);

    // Poll for all 5 in the DLQ.
    let dlq = q.getDlq();
    for (let i = 0; i < 30 && dlq.length < 5; i++) {
      await Bun.sleep(20);
      dlq = q.getDlq();
    }
    expect(dlq.length).toBe(5);

    await q.retryJobs({ state: 'failed', count: 2 });

    // 2 retried → 3 remain in the DLQ (was: whole DLQ drained).
    const remaining = q.getDlq().length;
    expect(remaining).toBe(3);

    q.close();
  });

  test('retryJobs with a negative/NaN count retries NOTHING (no full drain)', async () => {
    const mgr = getSharedManager();
    for (const bad of [-1, Number.NaN]) {
      const name = `sib-retry-bad-${String(bad)}`;
      const q = new Queue(name, { embedded: true });
      await populateDlq(q, name, 4);
      let dlq = q.getDlq();
      for (let i = 0; i < 30 && dlq.length < 4; i++) {
        await Bun.sleep(20);
        dlq = q.getDlq();
      }
      expect(dlq.length).toBe(4);

      // Clamped to 0 → retries nothing; must NOT fall through to clear-all.
      await q.retryJobs({ state: 'failed', count: bad });
      expect(q.getDlq().length).toBe(4);
      void mgr;
      q.close();
    }
  });

  test('Queue.moveJobToFailed persists the stacktrace', async () => {
    const name = 'sib-fail-stack';
    const q = new Queue(name, { embedded: true });
    const mgr = getSharedManager();

    await q.add('j', { v: 1 }, { attempts: 1 });
    const job = await mgr.pull(name, 0);
    expect(job).toBeTruthy();
    await q.moveJobToFailed(String(job!.id), new Error('stack-marker-xyz'));

    let dlq = q.getDlq();
    for (let i = 0; i < 30 && dlq.length === 0; i++) {
      await Bun.sleep(20);
      dlq = q.getDlq();
    }
    expect(dlq.length).toBe(1);
    const stack = dlq[0].job.stacktrace;
    expect(Array.isArray(stack)).toBe(true);
    expect(stack!.length).toBeGreaterThan(0);
    expect(stack!.join('\n')).toContain('stack-marker-xyz');

    q.close();
  });

  test('Queue.moveJobToFailed honours UnrecoverableError (no retry, straight to DLQ)', async () => {
    const name = 'sib-fail-unrecoverable';
    const q = new Queue(name, { embedded: true });
    const mgr = getSharedManager();

    // attempts:3 — a normal failure would retry; UnrecoverableError must not.
    await q.add('j', { v: 1 }, { attempts: 3 });
    const job = await mgr.pull(name, 0);
    await q.moveJobToFailed(String(job!.id), new UnrecoverableError('do-not-retry'));

    let dlq = q.getDlq();
    for (let i = 0; i < 30 && dlq.length === 0; i++) {
      await Bun.sleep(20);
      dlq = q.getDlq();
    }
    // In the DLQ despite attempts:3 → it was not retried.
    expect(dlq.length).toBe(1);
    expect(await q.getJobState(String(job!.id))).toBe('failed');

    q.close();
  });
});
