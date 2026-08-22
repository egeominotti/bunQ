import { afterEach, describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import type { DlqFilter } from '../../src/client';
import {
  closeTcpHarness,
  completeJob,
  failJob,
  makeTcpQueue,
  startTcpHarness,
  type TcpHarness,
} from './tcp-harness';

let harness: TcpHarness | null = null;

function setup(): TcpHarness {
  harness = startTcpHarness();
  return harness;
}

function name(prefix: string): string {
  return `${prefix}-${Bun.randomUUIDv7()}`;
}

afterEach(async () => {
  await closeTcpHarness(harness);
  harness = null;
});

describe('TCP DLQ async facade returns authoritative results', () => {
  test('getDlqConfigAsync reads server configuration without relying on client cache', async () => {
    const h = setup();
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 12 }),
        fc.integer({ min: 1, max: 50_000 }),
        async (maxAutoRetries, maxEntries) => {
          const queueName = name('dlq-config');
          h.manager.setDlqConfig(queueName, { autoRetry: true, maxAutoRetries, maxEntries });
          const queue = makeTcpQueue(h, queueName);
          const config = await queue.getDlqConfigAsync();
          expect(config.autoRetry).toBe(true);
          expect(config.maxAutoRetries).toBe(maxAutoRetries);
          expect(config.maxEntries).toBe(maxEntries);
          await queue.close();
        }
      ),
      { numRuns: 8 }
    );
  });

  test('getDlqAsync returns entries and their failure metadata', async () => {
    const h = setup();
    const queueName = name('get-dlq');
    const queue = makeTcpQueue(h, queueName);
    const id = await failJob(h, queue, queueName, { failed: true });
    const entries = await queue.getDlqAsync();
    expect(entries.map((entry) => entry.job.id)).toContain(id);
    expect(entries.find((entry) => entry.job.id === id)?.reason).toBe('max_attempts_exceeded');
  });

  test('getDlqStatsAsync returns non-zero server statistics', async () => {
    const h = setup();
    const queueName = name('dlq-stats');
    const queue = makeTcpQueue(h, queueName);
    await failJob(h, queue, queueName, { failed: true });
    const stats = await queue.getDlqStatsAsync();
    expect(stats.total).toBe(1);
    expect(stats.byReason.max_attempts_exceeded).toBe(1);
  });

  test('retryDlqAsync returns the number actually retried', async () => {
    const h = setup();
    const queueName = name('retry-dlq');
    const queue = makeTcpQueue(h, queueName);
    const id = await failJob(h, queue, queueName, { failed: true });
    const retried = await queue.retryDlqAsync(id);
    expect(retried).toBe(1);
    expect(await queue.getJobState(id)).toBe('waiting');
  });

  test('retryDlqByFilterAsync retries only entries selected by the filter', async () => {
    const h = setup();
    const queueName = name('retry-dlq-filter');
    const queue = makeTcpQueue(h, queueName);
    const id = await failJob(h, queue, queueName, { failed: true });
    const filter = { reason: 'max_attempts_exceeded' } as DlqFilter;
    expect(await queue.retryDlqByFilterAsync(filter)).toBe(1);
    expect(await queue.getJobState(id)).toBe('waiting');
  });

  test('purgeDlqAsync returns the number removed and empties the server DLQ', async () => {
    const h = setup();
    const queueName = name('purge-dlq');
    const queue = makeTcpQueue(h, queueName);
    await failJob(h, queue, queueName, { failed: true });
    const purged = await queue.purgeDlqAsync();
    expect(purged).toBe(1);
    expect(h.manager.getDlqCount(queueName)).toBe(0);
  });

  test('removeDlqJobAsync removes only the requested DLQ entry', async () => {
    const h = setup();
    const queueName = name('remove-dlq-job');
    const queue = makeTcpQueue(h, queueName);
    const removedId = await failJob(h, queue, queueName, { removed: true });
    const retainedId = await failJob(h, queue, queueName, { retained: true });

    expect(await queue.removeDlqJob(removedId)).toBe(true);
    expect(await queue.removeDlqJobAsync(removedId)).toBe(false);
    const aliasId = await failJob(h, queue, queueName, { alias: true });
    expect(await queue.removeDlqJobAsync(aliasId)).toBe(true);
    expect((await queue.getDlqAsync()).map((entry) => entry.job.id)).toEqual([retainedId]);
  });

  test('removeDlqJob rejects broker failures instead of reporting a missing entry', async () => {
    const h = setup();
    const queue = makeTcpQueue(h, name('remove-dlq-job-error'));
    const originalRemove = h.manager.removeDlqJob;
    h.manager.removeDlqJob = () => {
      throw new Error('forced removal failure');
    };

    try {
      await expect(queue.removeDlqJob('failure-id')).rejects.toThrow('forced removal failure');
    } finally {
      h.manager.removeDlqJob = originalRemove;
    }
  });

  test('retryCompletedAsync returns the number moved back to waiting', async () => {
    const h = setup();
    const queueName = name('retry-completed');
    const queue = makeTcpQueue(h, queueName);
    const id = await completeJob(h, queue, queueName, { completed: true });
    const retried = await queue.retryCompletedAsync(id);
    expect(retried).toBe(1);
    expect(await queue.getJobState(id)).toBe('waiting');
  });

  test('getStallConfigAsync reads authoritative server configuration', async () => {
    const h = setup();
    const queueName = name('stall-config');
    const queue = makeTcpQueue(h, queueName);
    h.manager.setStallConfig(queueName, {
      enabled: false,
      stallInterval: 4321,
      maxStalls: 7,
      gracePeriod: 987,
    });
    expect(await queue.getStallConfigAsync()).toEqual({
      enabled: false,
      stallInterval: 4321,
      maxStalls: 7,
      gracePeriod: 987,
    });
  });
});
