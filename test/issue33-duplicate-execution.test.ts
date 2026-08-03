/**
 * Issue #33 - stalled processing generations
 *
 * A worker with heartbeats disabled can receive the same job again after the
 * broker confirms a stall. Delivery is at-least-once, so processor invocation
 * may repeat; the safety contract is that a stale invocation cannot complete
 * or overwrite the newer processing generation.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { checkStalledJobs } from '../src/application/stallDetection';
import type { BackgroundContext } from '../src/application/types';
import { FlowProducer, Queue, Worker, shutdownManager } from '../src/client';
import { getSharedManager } from '../src/client/manager';

function backgroundContext(): BackgroundContext {
  return (
    getSharedManager() as unknown as {
      contextFactory: { getBackgroundContext(): BackgroundContext };
    }
  ).contextFactory.getBackgroundContext();
}

async function forceConfirmedStall(): Promise<void> {
  await Bun.sleep(5);
  const context = backgroundContext();
  checkStalledJobs(context);
  checkStalledJobs(context);
}

async function waitUntil(
  predicate: () => boolean | Promise<boolean>,
  label: string
): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (!(await predicate())) {
    if (Date.now() >= deadline) throw new Error(`Timed out waiting for ${label}`);
    await Bun.sleep(10);
  }
}

afterEach(() => {
  shutdownManager();
});

describe('Issue #33 - stalled processing generations', () => {
  test('a stale invocation cannot overwrite the current generation', async () => {
    const queue = new Queue('issue33-generation', { embedded: true });
    await queue.obliterateAsync();
    await queue.setStallConfigAsync({
      enabled: true,
      stallInterval: 0,
      gracePeriod: 0,
      maxStalls: 5,
    });

    let releaseFirst = (): void => undefined;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const tokens: Array<string | undefined> = [];
    let deliveries = 0;
    const worker = new Worker(
      queue.name,
      async (job) => {
        const generation = ++deliveries;
        tokens.push(job.token);
        if (generation === 1) await firstGate;
        return { generation };
      },
      { embedded: true, concurrency: 2, heartbeatInterval: 0, lockDuration: 60_000 }
    );

    try {
      const job = await queue.add(
        'process-large-file',
        { file: 'large-file.zip' },
        { attempts: 10, backoff: 0 }
      );
      await waitUntil(() => deliveries === 1, 'the first delivery');

      await forceConfirmedStall();
      await waitUntil(() => deliveries === 2, 'the replacement delivery');
      expect(tokens[0]).toBeString();
      expect(tokens[1]).toBeString();
      expect(tokens[1]).not.toBe(tokens[0]);

      await waitUntil(async () => (await queue.getJobState(job.id)) === 'completed', 'completion');
      expect(getSharedManager().getResult(job.id)).toEqual({ generation: 2 });

      releaseFirst();
      await Bun.sleep(50);
      expect(await queue.getJobState(job.id)).toBe('completed');
      expect(getSharedManager().getResult(job.id)).toEqual({ generation: 2 });
      expect(deliveries).toBe(2);
    } finally {
      releaseFirst();
      await worker.close(true);
      await queue.close();
    }
  }, 15_000);

  test('a stale flow step does not block or duplicate the next step', async () => {
    const queue = new Queue('issue33-flow-generation', { embedded: true });
    const flow = new FlowProducer({ embedded: true });
    await queue.obliterateAsync();
    await queue.setStallConfigAsync({
      enabled: true,
      stallInterval: 0,
      gracePeriod: 0,
      maxStalls: 5,
    });

    const executions = new Map<number, number>();
    let releaseFirst = (): void => undefined;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    let worker: Worker<{ step: number }, { step: number; generation: number }> | null = null;

    try {
      worker = new Worker(
        queue.name,
        async (job) => {
          const step = job.data.step;
          const generation = (executions.get(step) ?? 0) + 1;
          executions.set(step, generation);
          if (step === 1 && generation === 1) await firstGate;
          return { step, generation };
        },
        { embedded: true, concurrency: 2, heartbeatInterval: 0, lockDuration: 60_000 }
      );

      await flow.addChain([
        { name: 'step-0', queueName: queue.name, data: { step: 0 } },
        { name: 'step-1', queueName: queue.name, data: { step: 1 } },
        { name: 'step-2', queueName: queue.name, data: { step: 2 } },
      ]);

      await waitUntil(() => executions.get(1) === 1, 'the first stalled flow delivery');
      await forceConfirmedStall();
      await waitUntil(() => executions.get(1) === 2, 'the replacement flow delivery');
      await waitUntil(() => executions.get(2) === 1, 'the final flow step');

      expect(executions.get(0)).toBe(1);
      expect(executions.get(1)).toBe(2);
      expect(executions.get(2)).toBe(1);

      releaseFirst();
      await Bun.sleep(50);
      expect(executions.get(2)).toBe(1);
    } finally {
      releaseFirst();
      await worker?.close(true);
      await flow.close();
      await queue.close();
    }
  }, 15_000);
});
