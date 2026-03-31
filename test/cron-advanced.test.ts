/**
 * Cron Advanced Tests (Embedded Mode)
 *
 * Tests advanced scheduler behavior using the Queue#upsertJobScheduler API:
 * - Repeat every N ms
 * - Upsert updates existing scheduler
 * - Remove scheduler stops production
 * - Multiple schedulers on same queue
 * - getJobSchedulers lists all
 * - Scheduler with job template
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Cron Advanced - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('repeat every N ms produces jobs at interval', async () => {
    const queue = new Queue('cron-adv-repeat', { embedded: true });
    queue.obliterate();

    const completedJobs: string[] = [];

    await queue.upsertJobScheduler('repeat-1s', { every: 1000 }, {
      name: 'tick',
      data: { source: 'repeat' },
    });

    const worker = new Worker(
      'cron-adv-repeat',
      async (job) => {
        completedJobs.push(job.id);
        return { ok: true };
      },
      { embedded: true }
    );

    // Wait 3.5 seconds for at least 3 fires
    for (let i = 0; i < 35; i++) await Bun.sleep(100);

    await queue.removeJobScheduler('repeat-1s');
    await worker.close();

    expect(completedJobs.length).toBeGreaterThanOrEqual(3);

    queue.obliterate();
    queue.close();
  }, 20000);

  test('upsert updates existing scheduler to new interval', async () => {
    const queue = new Queue('cron-adv-upsert', { embedded: true });
    queue.obliterate();

    const completedJobs: string[] = [];

    // Create scheduler with slow interval (5s)
    await queue.upsertJobScheduler('upsert-sched', { every: 5000 }, {
      name: 'slow',
      data: { version: 1 },
    });

    // Upsert same ID with fast interval (1s)
    await queue.upsertJobScheduler('upsert-sched', { every: 1000 }, {
      name: 'fast',
      data: { version: 2 },
    });

    const worker = new Worker(
      'cron-adv-upsert',
      async (job) => {
        completedJobs.push(job.id);
        return { ok: true };
      },
      { embedded: true }
    );

    // Wait 2.5 seconds - should have at least 2 fires at 1s interval
    for (let i = 0; i < 25; i++) await Bun.sleep(100);

    await queue.removeJobScheduler('upsert-sched');
    await worker.close();

    expect(completedJobs.length).toBeGreaterThanOrEqual(2);

    queue.obliterate();
    queue.close();
  }, 20000);

  test('remove scheduler stops job production', async () => {
    const queue = new Queue('cron-adv-remove', { embedded: true });
    queue.obliterate();

    const completedJobs: string[] = [];

    await queue.upsertJobScheduler('remove-sched', { every: 500 }, {
      name: 'removable',
      data: { source: 'remove-test' },
    });

    const worker = new Worker(
      'cron-adv-remove',
      async (job) => {
        completedJobs.push(job.id);
        return { ok: true };
      },
      { embedded: true }
    );

    // Wait for at least 2 jobs to be produced
    for (let i = 0; i < 30; i++) {
      await Bun.sleep(100);
      if (completedJobs.length >= 2) break;
    }

    expect(completedJobs.length).toBeGreaterThanOrEqual(2);
    const countBeforeRemove = completedJobs.length;

    // Remove the scheduler
    await queue.removeJobScheduler('remove-sched');

    // Wait 2 more seconds - no new jobs should be produced
    for (let i = 0; i < 20; i++) await Bun.sleep(100);

    // Allow at most 1 extra job (could have been in-flight at removal time)
    expect(completedJobs.length).toBeLessThanOrEqual(countBeforeRemove + 1);

    await worker.close();
    queue.obliterate();
    queue.close();
  }, 20000);

  test('multiple schedulers on same queue - fast produces more than slow', async () => {
    const queue = new Queue('cron-adv-multi', { embedded: true });
    queue.obliterate();

    const fastJobs: string[] = [];
    const slowJobs: string[] = [];

    await queue.upsertJobScheduler('fast', { every: 500 }, {
      name: 'fast-job',
      data: { scheduler: 'fast' },
    });

    await queue.upsertJobScheduler('slow', { every: 2000 }, {
      name: 'slow-job',
      data: { scheduler: 'slow' },
    });

    const worker = new Worker(
      'cron-adv-multi',
      async (job) => {
        const data = job.data as { scheduler: string };
        if (data.scheduler === 'fast') {
          fastJobs.push(job.id);
        } else if (data.scheduler === 'slow') {
          slowJobs.push(job.id);
        }
        return { ok: true };
      },
      { embedded: true }
    );

    // Wait 4 seconds
    for (let i = 0; i < 40; i++) await Bun.sleep(100);

    await queue.removeJobScheduler('fast');
    await queue.removeJobScheduler('slow');
    await worker.close();

    // Fast (500ms) should have produced more than slow (2000ms)
    expect(fastJobs.length).toBeGreaterThan(slowJobs.length);

    queue.obliterate();
    queue.close();
  }, 20000);

  test('getJobSchedulers lists all schedulers', async () => {
    const queue = new Queue('cron-adv-list', { embedded: true });
    queue.obliterate();

    await queue.upsertJobScheduler('sched-a', { every: 60000 }, {
      name: 'job-a',
      data: { id: 'a' },
    });
    await queue.upsertJobScheduler('sched-b', { every: 60000 }, {
      name: 'job-b',
      data: { id: 'b' },
    });
    await queue.upsertJobScheduler('sched-c', { every: 60000 }, {
      name: 'job-c',
      data: { id: 'c' },
    });

    const schedulers = await queue.getJobSchedulers();
    const ids = schedulers.map((s) => s.id).sort();

    expect(schedulers.length).toBe(3);
    expect(ids).toEqual(['sched-a', 'sched-b', 'sched-c']);

    const count = await queue.getJobSchedulersCount();
    expect(count).toBe(3);

    await queue.removeJobScheduler('sched-a');
    await queue.removeJobScheduler('sched-b');
    await queue.removeJobScheduler('sched-c');

    queue.obliterate();
    queue.close();
  }, 20000);

  test('scheduler with job template preserves name and data', async () => {
    const queue = new Queue('cron-adv-template', { embedded: true });
    queue.obliterate();

    await queue.upsertJobScheduler('template-sched', { every: 500 }, {
      name: 'report',
      data: { type: 'daily' },
    });

    let receivedName = '';
    let receivedData: unknown = null;

    const worker = new Worker(
      'cron-adv-template',
      async (job) => {
        receivedName = job.name;
        receivedData = job.data;
        return { ok: true };
      },
      { embedded: true }
    );

    // Wait for at least 1 job to be produced and processed
    for (let i = 0; i < 30; i++) {
      await Bun.sleep(100);
      if (receivedName) break;
    }

    await queue.removeJobScheduler('template-sched');
    await worker.close();

    expect(receivedName).toBe('report');
    const data = receivedData as Record<string, unknown>;
    expect(data.type).toBe('daily');

    queue.obliterate();
    queue.close();
  }, 20000);
});
