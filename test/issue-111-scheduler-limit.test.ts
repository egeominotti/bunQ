/**
 * Test: Issue #111 — upsertJobScheduler ignores RepeatOpts.limit
 *
 * `queue.upsertJobScheduler()` accepts a `limit` in RepeatOpts but the client
 * scheduler never mapped it to the underlying cron's `maxLimit`, so it was
 * always persisted as NULL and the run cap never took effect. It must be
 * forwarded on both the embedded and TCP paths, and surfaced back through
 * SchedulerInfo (`limit`).
 */
import { describe, test, expect } from 'bun:test';
import { Queue } from '../src/client';
import { getSharedManager } from '../src/client/manager';

describe('Issue #111: upsertJobScheduler forwards limit -> maxLimit', () => {
  test('embedded: limit is persisted on the underlying cron (maxLimit)', async () => {
    const queue = new Queue('issue-111-embedded', { embedded: true });

    await queue.upsertJobScheduler(
      'sched-limit',
      { every: 60_000, limit: 5 },
      { name: 'test-job', data: { hello: 'world' } }
    );

    // Assert the run cap actually reached the cron engine, not NULL.
    const cron = getSharedManager().getCron('sched-limit');
    expect(cron).toBeDefined();
    expect(cron?.maxLimit).toBe(5);

    queue.close();
  });

  test('embedded: SchedulerInfo surfaces limit via getJobSchedulers/getJobScheduler', async () => {
    const queue = new Queue('issue-111-expose', { embedded: true });

    await queue.upsertJobScheduler(
      'sched-expose',
      { every: 30_000, limit: 3 },
      { name: 'job', data: {} }
    );

    const all = await queue.getJobSchedulers();
    const found = all.find((s) => s.id === 'sched-expose');
    expect(found).toBeDefined();
    expect(found?.limit).toBe(3);

    const one = await queue.getJobScheduler('sched-expose');
    expect(one?.limit).toBe(3);

    queue.close();
  });

  test('embedded: omitting limit leaves maxLimit null (no regression)', async () => {
    const queue = new Queue('issue-111-nolimit', { embedded: true });

    await queue.upsertJobScheduler('sched-nolimit', { every: 60_000 }, { data: {} });

    const cron = getSharedManager().getCron('sched-nolimit');
    expect(cron?.maxLimit).toBeNull();

    const one = await queue.getJobScheduler('sched-nolimit');
    expect(one?.limit).toBeUndefined();

    queue.close();
  });
});
