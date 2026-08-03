import { afterEach, describe, expect, test } from 'bun:test';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitForState,
} from './docs-guide-support';

let harness: CoreE2eHarness | null = null;

afterEach(async () => {
  await closeHarness(harness);
  harness = null;
});

for (const mode of MODES) {
  describe(`Worker manual lease propagation [${mode}]`, () => {
    test('getNextJob exposes clean job fields and processJobManually reuses its lease', async () => {
      harness = await startHarness('worker-manual-lease-complete', mode);
      const queue = harness.queue<{ value: number }>('manual-complete');
      let processorToken: string | undefined;
      const worker = harness.worker<{ value: number }, number>(
        queue.name,
        async (job) => {
          processorToken = job.token;
          return job.data.value * 2;
        },
        { autorun: false }
      );
      await worker.waitUntilReady();

      const added = await queue.add('calculate', { value: 21 }, { durable: true });
      const pulled = await worker.getNextJob();

      expect(pulled?.id).toBe(added.id);
      expect(pulled?.name).toBe('calculate');
      expect(pulled?.data).toEqual({ value: 21 });
      expect(pulled?.token).toBeString();
      if (!pulled) throw new Error('Expected a manually pulled job');

      await worker.processJobManually(pulled);
      await waitForState(queue, added.id, 'completed', 10_000);
      expect(processorToken).toBe(pulled.token);
      expect((await queue.getJob(added.id))?.returnvalue).toBe(42);
    });

    test('processJobManually reuses the tracked lease on failure', async () => {
      harness = await startHarness('worker-manual-lease-fail', mode);
      const queue = harness.queue('manual-fail');
      let processorToken: string | undefined;
      const worker = harness.worker(
        queue.name,
        async (job) => {
          processorToken = job.token;
          throw new Error('manual expected failure');
        },
        { autorun: false }
      );
      await worker.waitUntilReady();

      const added = await queue.add('failing-task', {}, { attempts: 1, durable: true });
      const pulled = await worker.getNextJob();
      expect(pulled?.token).toBeString();
      if (!pulled) throw new Error('Expected a manually pulled job');

      await worker.processJobManually(pulled);
      await waitForState(queue, added.id, 'failed', 10_000);
      expect(processorToken).toBe(pulled.token);
      expect((await queue.getJob(added.id))?.failedReason).toBe('manual expected failure');
    });

    test('an explicit wrong token is rejected without consuming the tracked delivery', async () => {
      harness = await startHarness('worker-manual-lease-mismatch', mode);
      const queue = harness.queue('manual-token-mismatch');
      let runs = 0;
      const worker = harness.worker(
        queue.name,
        async () => {
          runs++;
          return 'completed with tracked token';
        },
        { autorun: false }
      );
      await worker.waitUntilReady();

      const added = await queue.add('token-check', {}, { durable: true });
      const pulled = await worker.getNextJob();
      expect(pulled?.token).toBeString();
      if (!pulled) throw new Error('Expected a manually pulled job');

      await expect(worker.processJobManually(pulled, 'wrong-token')).rejects.toThrow(
        'Invalid or expired lock token'
      );
      expect(runs).toBe(0);
      expect(await queue.getJobState(added.id)).toBe('active');

      await worker.processJobManually(pulled);
      await waitForState(queue, added.id, 'completed', 10_000);
      expect(runs).toBe(1);
    });
  });
}
