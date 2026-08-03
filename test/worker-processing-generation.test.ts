import { afterEach, describe, expect, test } from 'bun:test';
import { checkStalledJobs } from '../src/application/stallDetection';
import type { QueueManager } from '../src/application/queueManager';
import type { BackgroundContext } from '../src/application/types';
import { jobId } from '../src/domain/types/job';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitForState,
  waitUntil,
} from './docs-guide-support';

let harness: CoreE2eHarness | null = null;

afterEach(async () => {
  await closeHarness(harness);
  harness = null;
});

function backgroundContext(manager: QueueManager): BackgroundContext {
  return (
    manager as unknown as {
      contextFactory: { getBackgroundContext(): BackgroundContext };
    }
  ).contextFactory.getBackgroundContext();
}

async function forceStall(manager: QueueManager): Promise<void> {
  await Bun.sleep(5);
  const context = backgroundContext(manager);
  checkStalledJobs(context);
  checkStalledJobs(context);
}

for (const mode of MODES) {
  describe(`worker processing generations [${mode}]`, () => {
    test('a stale outcome cannot alter the current lease, counters, heartbeat, or cancellation', async () => {
      harness = await startHarness('processing-generation', mode);
      const queue = harness.queue(`generation-${mode}`);
      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 0,
        maxStalls: 10,
        gracePeriod: 0,
      });

      let finishFirst = (): void => undefined;
      const firstGate = new Promise<void>((resolve) => {
        finishFirst = resolve;
      });
      let finishSecond = (): void => undefined;
      const secondGate = new Promise<void>((resolve) => {
        finishSecond = resolve;
      });
      const tokens: Array<string | undefined> = [];
      const errors: Error[] = [];
      let deliveries = 0;

      const worker = harness.worker(
        queue.name,
        async (job) => {
          const generation = ++deliveries;
          tokens.push(job.token);
          if (generation === 1) {
            await firstGate;
            throw new Error('stale generation failed');
          }
          await secondGate;
          return { generation };
        },
        {
          concurrency: 2,
          batchSize: 1,
          heartbeatInterval: 500,
          lockDuration: 60_000,
          useLocks: true,
        }
      );
      worker.on('error', (error) => errors.push(error));

      const job = await queue.add('generation', {}, { attempts: 10, backoff: 0, durable: true });
      await waitForState(queue, job.id, 'active', 10_000);
      await waitUntil(() => deliveries === 1, 'the first processing generation', 5_000);

      await forceStall(harness.brokerManager());
      await waitUntil(() => deliveries === 2, 'the second processing generation', 10_000);

      expect(tokens[0]).toBeString();
      expect(tokens[1]).toBeString();
      expect(tokens[1]).not.toBe(tokens[0]);
      expect(worker.cancelJob(job.id, 'generation cancellation')).toBe(true);

      finishFirst();
      await waitUntil(
        async () => {
          const info = (await queue.getWorkers()).find((entry) =>
            entry.queues?.includes(queue.name)
          );
          return info?.activeJobs === 1;
        },
        'worker counters to retain only the current generation',
        5_000
      );

      const currentToken = tokens[1];
      await waitUntil(
        () => {
          const lock = harness?.brokerManager().getLockInfo(jobId(job.id));
          return lock?.token === currentToken && lock.renewalCount > 0;
        },
        'the current generation heartbeat',
        5_000
      );
      expect(await queue.getJobState(job.id)).toBe('active');
      expect(harness.brokerManager().getResult(jobId(job.id))).toBeUndefined();
      expect(worker.isJobCancelled(job.id)).toBe(true);
      expect(errors).toEqual([]);

      finishSecond();
      await waitForState(queue, job.id, 'completed', 10_000);
      expect(harness.brokerManager().getResult(jobId(job.id))).toEqual({ generation: 2 });
      expect(worker.isJobCancelled(job.id)).toBe(false);
    }, 30_000);

    test('manual processing accepts a newer delivery while the stale generation is still running', async () => {
      harness = await startHarness('manual-processing-generation', mode);
      const queue = harness.queue(`manual-generation-${mode}`);
      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 0,
        maxStalls: 10,
        gracePeriod: 0,
      });

      let finishFirst = (): void => undefined;
      const firstGate = new Promise<void>((resolve) => {
        finishFirst = resolve;
      });
      let finishSecond = (): void => undefined;
      const secondGate = new Promise<void>((resolve) => {
        finishSecond = resolve;
      });
      let deliveries = 0;
      const worker = harness.worker(
        queue.name,
        async () => {
          const generation = ++deliveries;
          if (generation === 1) await firstGate;
          else await secondGate;
          return { generation };
        },
        { autorun: false, concurrency: 2, useLocks: true, lockDuration: 60_000 }
      );

      const job = await queue.add('manual-generation', {}, { attempts: 10, durable: true });
      const first = await worker.getNextJob();
      expect(first?.id).toBe(job.id);
      if (!first) throw new Error('Expected the first manual delivery');
      const firstToken = first.token;
      expect(firstToken).toBeString();
      expect(harness.brokerManager().getLockInfo(jobId(job.id))?.token).toBe(firstToken);
      const firstProcessing = worker.processJobManually(first);
      await waitUntil(() => deliveries === 1, 'the first manual generation', 5_000);

      await forceStall(harness.brokerManager());
      let second = await worker.getNextJob();
      await waitUntil(
        async () => {
          second ??= await worker.getNextJob();
          return second !== undefined;
        },
        'the second manual delivery',
        5_000
      );
      if (!second) throw new Error('Expected the second manual delivery');
      const secondToken = second.token;
      expect(secondToken).toBeString();
      expect(secondToken).not.toBe(firstToken);
      expect(harness.brokerManager().getLockInfo(jobId(job.id))?.token).toBe(secondToken);
      const secondProcessing = worker.processJobManually(second);
      await waitUntil(() => deliveries === 2, 'the second manual generation', 5_000);

      finishFirst();
      await firstProcessing;
      expect(await queue.getJobState(job.id)).toBe('active');
      expect(harness.brokerManager().getLockInfo(jobId(job.id))?.token).toBe(secondToken);

      finishSecond();
      await secondProcessing;
      await waitForState(queue, job.id, 'completed', 10_000);
      expect(harness.brokerManager().getResult(jobId(job.id))).toEqual({ generation: 2 });
    }, 30_000);
  });
}
