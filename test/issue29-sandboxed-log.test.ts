/**
 * Issue #29 - SandboxedWorker: processor file does not let me use `log` method
 *
 * Bug: The `log` method IS available in the processor via IPC, and the worker
 * handles it by calling ops.addLog(), but the SandboxedWorker never emits
 * a 'log' event. So .on('log', ...) listeners never fire.
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { SandboxedWorker } from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { unlink } from 'fs/promises';

describe('Issue #29 - SandboxedWorker log method', () => {
  let manager: QueueManager;
  let logProcessorPath: string;

  beforeAll(async () => {
    manager = new QueueManager();

    // Create a processor that uses the log method
    logProcessorPath = `${Bun.env.TMPDIR ?? '/tmp'}/test-log-processor-${Date.now()}.ts`;
    await Bun.write(
      logProcessorPath,
      `
      export default async (job: {
        id: string;
        data: any;
        queue: string;
        attempts: number;
        progress: (value: number) => void;
        log: (message: string) => void;
      }) => {
        job.log('step-1: starting');
        job.log('step-2: processing');
        job.log('step-3: done');
        return { processed: true };
      };
    `
    );
  });

  afterAll(async () => {
    await manager.shutdown();
    try {
      await unlink(logProcessorPath);
    } catch {
      // Ignore cleanup errors
    }
  });

  test('should emit log event when processor calls job.log()', async () => {
    const queueName = `issue29-log-event-${Date.now()}`;
    const logMessages: string[] = [];

    const worker = new SandboxedWorker(queueName, {
      processor: logProcessorPath,
      concurrency: 1,
      timeout: 5000,
      manager,
    });

    worker.on('log', (_job: any, message: string) => {
      logMessages.push(message);
    });

    let completed = false;
    worker.on('completed', () => {
      completed = true;
    });

    await worker.start();

    await manager.push(queueName, { data: { value: 1 } });

    // Wait for job to complete
    for (let i = 0; i < 30; i++) {
      await Bun.sleep(100);
      if (completed) break;
    }

    expect(completed).toBe(true);
    expect(logMessages).toContain('step-1: starting');
    expect(logMessages).toContain('step-2: processing');
    expect(logMessages).toContain('step-3: done');

    await worker.stop();
  });

  test('should store logs via addLog so they are retrievable', async () => {
    const queueName = `issue29-log-store-${Date.now()}`;

    const worker = new SandboxedWorker(queueName, {
      processor: logProcessorPath,
      concurrency: 1,
      timeout: 5000,
      manager,
    });

    let completedJobId: string | null = null;
    worker.on('completed', (job: any) => {
      completedJobId = String(job.id);
    });

    await worker.start();

    const job = await manager.push(queueName, { data: { value: 2 } });

    // Wait for job to complete
    for (let i = 0; i < 30; i++) {
      await Bun.sleep(100);
      if (completedJobId) break;
    }

    expect(completedJobId).toBe(String(job.id));

    // Logs should have been stored via addLog
    const logs = manager.getLogs(job.id);
    expect(logs.length).toBeGreaterThanOrEqual(3);
    expect(logs.map((l: any) => l.message ?? l)).toContain('step-1: starting');

    await worker.stop();
  });
});
