/**
 * Flow Children Tests (Embedded Mode)
 *
 * Tests for job.getChildrenValues() inside worker handlers.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, FlowProducer, shutdownManager } from '../src/client';

describe('Flow Children - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('job.getChildrenValues() returns child results inside worker handler', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-job-children', { embedded: true });
    queue.obliterate();

    let jobChildrenValues: Record<string, unknown> = {};
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'flow-job-children',
      async (job) => {
        const data = job.data as { role: string; value?: number };

        if (data.role === 'child') {
          await Bun.sleep(50);
          return { childResult: (data.value ?? 0) * 10 };
        }

        if (data.role === 'parent') {
          jobChildrenValues = await job.getChildrenValues();
          resolve!();
          return { summary: 'done' };
        }

        return {};
      },
      { embedded: true, concurrency: 5 }
    );

    await flow.add({
      name: 'parent',
      queueName: 'flow-job-children',
      data: { role: 'parent' },
      children: [
        { name: 'child-a', queueName: 'flow-job-children', data: { role: 'child', value: 3 } },
        { name: 'child-b', queueName: 'flow-job-children', data: { role: 'child', value: 7 } },
      ],
    });

    await done;
    await Bun.sleep(200);

    const values = Object.values(jobChildrenValues)
      .map((v) => (v as { childResult: number }).childResult)
      .sort((a, b) => a - b);
    expect(values).toEqual([30, 70]);

    await worker.close();
    flow.close();
    queue.close();
  }, 15000);
});
