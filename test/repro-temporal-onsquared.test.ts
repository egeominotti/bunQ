/**
 * Repro: temporal index insert is O(n²) for a batch of jobs sharing one
 * createdAt timestamp (the addBulk case — `now` is captured once, so every
 * job in the batch gets the same createdAt).
 *
 * Root cause: SkipList.insert's duplicate-check loop walks ALL nodes whose
 * comparator result is 0. The temporal comparator was `createdAt - createdAt`,
 * so a same-createdAt batch makes every prior node compare-equal and the
 * dedup scan becomes O(n) per insert => O(n²) for the batch.
 *
 * Fix: total-order comparator (createdAt, then jobId tie-break) so compare===0
 * means a true duplicate only => dedup scan is O(1) and insert is O(log n).
 */
import { describe, expect, test } from 'bun:test';
import { TemporalManager } from '../src/domain/queue/temporalManager';
import type { JobId } from '../src/domain/types/job';

const jid = (n: number): JobId => `job-${n.toString().padStart(8, '0')}` as JobId;

describe('temporal index O(n²) repro', () => {
  test('inserting a large same-createdAt batch stays fast (not O(n²))', () => {
    const tm = new TemporalManager();
    const N = 30000;
    const createdAt = 1_700_000_000_000; // identical for the whole batch

    const t0 = Bun.nanoseconds();
    for (let i = 0; i < N; i++) tm.addToIndex(createdAt, jid(i), 'q');
    const ms = (Bun.nanoseconds() - t0) / 1e6;

    expect(tm.indexSize).toBe(N);
    // O(n²) at N=30k is ~9e8 comparisons => multiple seconds. O(n log n) is tens
    // of ms. A 1500ms bound leaves a ~50x margin so it is not flaky.
    expect(ms).toBeLessThan(1500);
  });

  test('dedup still works: same jobId is inserted once', () => {
    const tm = new TemporalManager();
    const createdAt = 1_700_000_000_000;
    tm.addToIndex(createdAt, jid(1), 'q');
    tm.addToIndex(createdAt, jid(1), 'q'); // duplicate jobId, same createdAt
    tm.addToIndex(createdAt, jid(2), 'q');
    expect(tm.indexSize).toBe(2);
  });

  test('getOldJobs still returns entries ordered by createdAt', () => {
    const tm = new TemporalManager();
    const base = Date.now() - 1_000_000;
    // insert out of order; same and different timestamps
    tm.addToIndex(base + 300, jid(3), 'q');
    tm.addToIndex(base + 100, jid(1), 'q');
    tm.addToIndex(base + 100, jid(2), 'q'); // tie on createdAt
    tm.addToIndex(base + 200, jid(4), 'q');

    const old = tm.getOldJobs('q', 1000, 100);
    expect(old.length).toBe(4);
    // non-decreasing by createdAt
    for (let i = 1; i < old.length; i++) {
      expect(old[i].createdAt).toBeGreaterThanOrEqual(old[i - 1].createdAt);
    }
  });

  test('removeFromIndex still removes a same-createdAt entry', () => {
    const tm = new TemporalManager();
    const createdAt = 1_700_000_000_000;
    for (let i = 0; i < 100; i++) tm.addToIndex(createdAt, jid(i), 'q');
    tm.removeFromIndex(jid(50));
    expect(tm.indexSize).toBe(99);
    const remaining = tm.getOldJobs('q', -1e15, 1000).map((e) => e.jobId);
    expect(remaining).not.toContain(jid(50));
    expect(remaining.length).toBe(99);
  });

  test('queue lookup and removal do not scan unrelated queues', () => {
    const tm = new TemporalManager();
    const base = Date.now() - 1_000_000;
    const unrelatedCount = 500_000;

    for (let i = 0; i < unrelatedCount; i++) {
      tm.addToIndex(base + i, jid(i), 'unrelated');
    }
    const targetId = 'target-job' as JobId;
    tm.addToIndex(base + unrelatedCount, targetId, 'target');

    let startedAt = Bun.nanoseconds();
    const old = tm.getOldJobs('target', 0, 1);
    const lookupMs = (Bun.nanoseconds() - startedAt) / 1e6;

    startedAt = Bun.nanoseconds();
    tm.removeFromIndex(targetId);
    const removalMs = (Bun.nanoseconds() - startedAt) / 1e6;

    expect(old).toEqual([{ jobId: targetId, createdAt: base + unrelatedCount }]);
    expect(tm.indexSize).toBe(unrelatedCount);
    expect(lookupMs + removalMs).toBeLessThan(4);
  });
});
