/**
 * Bug #60: Deduplication does not work for JobScheduler
 *
 * When using upsertJobScheduler with deduplication options, duplicate jobs
 * pile up because the dedup config is never passed to the cron system.
 * The cron scheduler fires pushJob() without uniqueKey or dedup options,
 * so every tick creates a new job regardless of deduplication settings.
 *
 * @see https://github.com/egeominotti/bunqueue/issues/60
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue } from '../src/client';
import { getSharedManager } from '../src/client/manager';

describe('Bug #60: upsertJobScheduler should respect deduplication', () => {
  let queue: Queue;

  beforeEach(() => {
    queue = new Queue('test-dedup-scheduler', { embedded: true });
  });

  afterEach(async () => {
    // Remove all crons from shared manager to prevent test pollution
    const manager = getSharedManager();
    for (const cron of manager.listCrons()) {
      manager.removeCron(cron.name);
    }
    await queue.obliterate();
    await queue.close();
  });

  test('scheduler with deduplication should not create duplicate jobs', async () => {
    const manager = getSharedManager();

    // Set up a scheduler that fires every 100ms with deduplication
    await queue.upsertJobScheduler(
      'dedup-scheduler',
      { every: 100 },
      {
        name: 'dedup-task',
        data: { action: 'process' },
        opts: {
          deduplication: {
            id: 'dedup-scheduler-job',
            ttl: 60_000,
          },
        },
      }
    );

    // Wait for multiple cron ticks to fire (at least 3-4 ticks at 100ms each)
    await Bun.sleep(500);

    // Check how many jobs were created in the queue
    const counts = manager.getQueueJobCounts('test-dedup-scheduler');
    const totalJobs = counts.waiting + counts.active + counts.delayed;

    // With deduplication, there should be at most 1 job (not 4-5 duplicates)
    // Without the fix, this will be ~4-5 jobs (one per tick)
    expect(totalJobs).toBeLessThanOrEqual(1);
  });

  test('scheduler dedup options should be stored in cron job', async () => {
    const manager = getSharedManager();

    await queue.upsertJobScheduler(
      'dedup-cron-check',
      { every: 60_000 },
      {
        name: 'check-task',
        data: { x: 1 },
        opts: {
          deduplication: {
            id: 'my-dedup-key',
            ttl: 3600_000,
            replace: true,
          },
        },
      }
    );

    const crons = manager.listCrons();
    const cron = crons.find((c) => c.name === 'dedup-cron-check');
    expect(cron).toBeDefined();

    // The cron job should carry deduplication info so pushJob can use it
    const cronAny = cron as Record<string, unknown>;
    expect(cronAny.uniqueKey).toBe('my-dedup-key');
  });

  test('scheduler with dedup replace=true should update existing job', async () => {
    const manager = getSharedManager();

    await queue.upsertJobScheduler(
      'replace-scheduler',
      { every: 100 },
      {
        name: 'replace-task',
        data: { version: 1 },
        opts: {
          deduplication: {
            id: 'replace-dedup-key',
            ttl: 60_000,
            replace: true,
          },
        },
      }
    );

    // Wait for multiple ticks
    await Bun.sleep(400);

    const counts = manager.getQueueJobCounts('test-dedup-scheduler');
    const totalJobs = counts.waiting + counts.active + counts.delayed;

    // With replace=true deduplication, should still be at most 1 job
    expect(totalJobs).toBeLessThanOrEqual(1);
  });
});
