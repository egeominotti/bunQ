/**
 * Regressions for client lifecycle ownership.
 *
 * Closing one Queue more than once must not release another Queue's shared
 * TCP pool. A Worker that has started closing must not be resumed by a stale
 * callback.
 */
import { describe, expect, test } from 'bun:test';
import { CoreE2eHarness } from './core-e2e/support/harness';

describe('client lifecycle ownership', () => {
  test('closing a Queue twice releases its shared TCP pool reference once', async () => {
    const harness = await CoreE2eHarness.start('tcp', 'queue-double-close');
    try {
      const owner = harness.queue('owner', { connection: { poolSize: 4 } });
      const peer = harness.queue('peer', { connection: { poolSize: 4 } });
      const ownerPool = (owner as unknown as { tcpPool: { isClosed(): boolean } }).tcpPool;
      const peerPool = (peer as unknown as { tcpPool: unknown }).tcpPool;

      expect(peerPool).toBe(ownerPool);
      await peer.add('before-close', { ok: true });
      owner.close();
      expect(ownerPool.isClosed()).toBe(false);
      owner.close();
      expect(ownerPool.isClosed()).toBe(false);

      const job = await peer.add('after-close', { ok: true });
      expect(await peer.getJobState(job.id)).toBe('waiting');
      peer.close();
      expect(ownerPool.isClosed()).toBe(true);
    } finally {
      await harness.close();
    }
  });

  test('disconnect followed by close releases a shared TCP pool reference once', async () => {
    const harness = await CoreE2eHarness.start('tcp', 'queue-disconnect-close');
    try {
      const owner = harness.queue('owner', { connection: { poolSize: 4 } });
      const peer = harness.queue('peer', { connection: { poolSize: 4 } });

      await peer.add('before-close', { ok: true });
      await owner.disconnect();
      owner.close();

      const job = await peer.add('after-close', { ok: true });
      expect(await peer.getJobState(job.id)).toBe('waiting');
    } finally {
      await harness.close();
    }
  });

  test('a stale resume cannot process jobs after Worker close has started', async () => {
    const harness = await CoreE2eHarness.start('tcp', 'worker-close-resume');
    try {
      const queue = harness.queue<{ index: number }>('jobs');
      let processed = 0;
      const worker = harness.worker(
        queue.name,
        () => {
          processed++;
          return 'done';
        },
        {
          batchSize: 5,
          concurrency: 5,
          pollTimeout: 0,
        }
      );
      worker.on('error', () => undefined);

      await worker.waitUntilReady();
      worker.pause();
      await Bun.sleep(30);
      await Promise.all(
        Array.from({ length: 5 }, (_, index) => queue.add('job', { index }, { durable: true }))
      );

      const closing = worker.close();
      const ownedClosingPromise = worker.closing;
      worker.resume();
      worker.run();
      expect(worker.closing).toBe(ownedClosingPromise);
      await closing;
      await Bun.sleep(250);

      expect(worker.isClosed()).toBe(true);
      expect(processed).toBe(0);
      expect(await queue.getJobCountsAsync()).toMatchObject({
        active: 0,
        completed: 0,
        waiting: 5,
      });
    } finally {
      await harness.close();
    }
  });
});
