/**
 * Issue #53 - Worker 'log' event never fires
 *
 * Bug: When job.log() is called inside the processor, the Worker never emits
 * the 'log' event. SandboxedWorker works correctly, but regular Worker does not.
 *
 * The user reports: "The log is never sent to the worker.
 * I am never receiving the log. Sandboxed worker log does work tho!"
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Issue #53 - Worker log event', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('worker.on("log") should fire when job.log() is called in processor', async () => {
    const queueName = `issue53-log-event-${Date.now()}`;
    const logMessages: string[] = [];
    let completed = false;

    const queue = new Queue(queueName);

    const worker = new Worker(
      queueName,
      async (job) => {
        await job.log('step-1: starting');
        await job.log('step-2: processing');
        await job.log('step-3: done');
        return { ok: true };
      },
    );

    worker.on('log', (_job, message) => {
      logMessages.push(message);
    });

    worker.on('completed', () => {
      completed = true;
    });

    await queue.add('task', { value: 1 });

    // Wait for job to complete
    for (let i = 0; i < 50; i++) {
      await Bun.sleep(100);
      if (completed) break;
    }

    expect(completed).toBe(true);
    expect(logMessages).toContain('step-1: starting');
    expect(logMessages).toContain('step-2: processing');
    expect(logMessages).toContain('step-3: done');
    expect(logMessages).toHaveLength(3);

    await worker.close();
  });

  test('log event should include the job object', async () => {
    const queueName = `issue53-log-job-ref-${Date.now()}`;
    let logJob: any = null;
    let logMessage = '';
    let completed = false;

    const queue = new Queue(queueName);

    const worker = new Worker(
      queueName,
      async (job) => {
        await job.log('hello from job');
        return {};
      },
    );

    worker.on('log', (job, message) => {
      logJob = job;
      logMessage = message;
    });

    worker.on('completed', () => {
      completed = true;
    });

    await queue.add('test-task', { foo: 'bar' });

    for (let i = 0; i < 50; i++) {
      await Bun.sleep(100);
      if (completed) break;
    }

    expect(completed).toBe(true);
    expect(logMessage).toBe('hello from job');
    expect(logJob).not.toBeNull();
    expect(logJob.id).toBeDefined();
    expect(logJob.data).toBeDefined();

    await worker.close();
  });

  test('logs should also be persisted (retrievable via manager)', async () => {
    const queueName = `issue53-log-persist-${Date.now()}`;
    let completedJobId = '';

    const queue = new Queue(queueName);

    const worker = new Worker(
      queueName,
      async (job) => {
        await job.log('persisted-log-1');
        await job.log('persisted-log-2');
        return {};
      },
    );

    worker.on('completed', (job) => {
      completedJobId = job.id;
    });

    await queue.add('task', {});

    for (let i = 0; i < 50; i++) {
      await Bun.sleep(100);
      if (completedJobId) break;
    }

    expect(completedJobId).toBeTruthy();

    // Verify logs were stored
    const { getSharedManager } = await import('../src/client/manager');
    const { jobId } = await import('../src/domain/types/job');
    const manager = getSharedManager();
    const logs = manager.getLogs(jobId(completedJobId));
    const messages = logs.map((l: any) => l.message ?? l);
    expect(messages).toContain('persisted-log-1');
    expect(messages).toContain('persisted-log-2');

    await worker.close();
  });
});
