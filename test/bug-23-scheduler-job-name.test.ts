/**
 * Bug #23: job.name is always 'default' for jobs created via upsertJobScheduler
 *
 * When a cron job fires, the pushed job does not include the `name` from the
 * jobTemplate in its data. The worker then falls back to 'default' because
 * it extracts `name` from `job.data.name`.
 *
 * @see https://github.com/egeominotti/bunqueue/discussions/23
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker } from '../src/client';
import { getSharedManager } from '../src/client/manager';

describe('Bug #23: upsertJobScheduler job.name should use jobTemplate.name', () => {
  let queue: Queue;

  beforeEach(() => {
    queue = new Queue('test-job-name', { embedded: true });
  });

  afterEach(async () => {
    await queue.close();
  });

  test('cron job data should include name from jobTemplate', async () => {
    const manager = getSharedManager();

    await queue.upsertJobScheduler(
      'my-scheduler',
      { every: 60000 },
      {
        name: 'send-email',
        data: { to: 'user@test.com' },
      }
    );

    // Check that the cron job data includes the name
    const crons = manager.listCrons();
    const cron = crons.find((c) => c.name === 'my-scheduler');

    expect(cron).toBeDefined();
    const cronData = cron!.data as Record<string, unknown>;
    expect(cronData.name).toBe('send-email');
  });

  test('worker should receive correct job.name from scheduled job', async () => {
    await queue.upsertJobScheduler(
      'email-scheduler',
      { every: 100 },
      {
        name: 'send-newsletter',
        data: { type: 'weekly' },
      }
    );

    // Wait for the cron to fire
    await Bun.sleep(200);

    const receivedName = await new Promise<string>((resolve) => {
      const worker = new Worker(
        'test-job-name',
        async (job) => {
          resolve(job.name);
          return { done: true };
        },
        { embedded: true }
      );
    });

    expect(receivedName).toBe('send-newsletter');
  });

  test('cron job data should preserve existing data fields alongside name', async () => {
    const manager = getSharedManager();

    await queue.upsertJobScheduler(
      'data-scheduler',
      { every: 60000 },
      {
        name: 'process-data',
        data: { customerId: 'abc123', priority: 'high' },
      }
    );

    const crons = manager.listCrons();
    const cron = crons.find((c) => c.name === 'data-scheduler');

    expect(cron).toBeDefined();
    const cronData = cron!.data as Record<string, unknown>;
    expect(cronData.name).toBe('process-data');
    expect(cronData.customerId).toBe('abc123');
    expect(cronData.priority).toBe('high');
  });

  test('job.name should default to "default" when jobTemplate has no name', async () => {
    const manager = getSharedManager();

    await queue.upsertJobScheduler(
      'no-name-scheduler',
      { every: 60000 },
      {
        data: { foo: 'bar' },
      }
    );

    const crons = manager.listCrons();
    const cron = crons.find((c) => c.name === 'no-name-scheduler');

    expect(cron).toBeDefined();
    const cronData = cron!.data as Record<string, unknown>;
    // When no name is provided, it should either be undefined or 'default'
    expect(cronData.name === undefined || cronData.name === 'default').toBe(true);
  });
});
