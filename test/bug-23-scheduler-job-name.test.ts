/**
 * Bug #23: job.name is always 'default' for jobs created via upsertJobScheduler
 *
 * Scheduler identity, spawned-job name, and user data are stored separately.
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

  test('cron job should store jobTemplate.name outside user data', async () => {
    const manager = getSharedManager();

    await queue.upsertJobScheduler(
      'my-scheduler',
      { every: 60000 },
      {
        name: 'send-email',
        data: { to: 'user@test.com' },
      }
    );

    const crons = manager.listCrons();
    const cron = crons.find((c) => c.name === 'my-scheduler');

    expect(cron).toBeDefined();
    expect(cron!.jobName).toBe('send-email');
    expect(cron!.data).toEqual({ to: 'user@test.com' });
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

    let worker: Worker | undefined;
    const receivedName = await new Promise<string>((resolve) => {
      worker = new Worker(
        'test-job-name',
        async (job) => {
          resolve(job.name);
          return { done: true };
        },
        { embedded: true }
      );
    });
    // Close the worker — otherwise it keeps polling forever (leaked across the
    // whole suite) and a stray poll during another test's disk-IO injection
    // surfaces as an "Unhandled error between tests" (emit('error') with no
    // listener). See CI failure on 2.8.14.
    await worker?.close();

    expect(receivedName).toBe('send-newsletter');
  });

  test('cron job data should preserve every existing user field', async () => {
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
    expect(cron!.jobName).toBe('process-data');
    expect(cron!.data).toEqual({ customerId: 'abc123', priority: 'high' });
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
    expect(cron!.jobName).toBe('default');
    expect(cron!.data).toEqual({ foo: 'bar' });
  });
});
