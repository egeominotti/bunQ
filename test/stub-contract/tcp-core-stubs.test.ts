import { afterEach, describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import { closeTcpHarness, makeTcpQueue, startTcpHarness, type TcpHarness } from './tcp-harness';

let harness: TcpHarness | null = null;
const runs = 8;

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

describe('TCP Queue limit getters read broker state', () => {
  test('getGlobalConcurrency round-trips generated limits', async () => {
    const h = setup();
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 32 }), async (limit) => {
        const queue = makeTcpQueue(h, name('tcp-concurrency'));
        await queue.setGlobalConcurrencyAsync(limit);
        expect(await queue.getGlobalConcurrency()).toBe(limit);
        await queue.close();
      }),
      { numRuns: runs }
    );
  });

  test('getGlobalRateLimit round-trips generated limits and durations', async () => {
    const h = setup();
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 100 }),
        fc.integer({ min: 50, max: 30_000 }),
        async (max, duration) => {
          const queue = makeTcpQueue(h, name('tcp-rate'));
          await queue.setGlobalRateLimitAsync(max, duration);
          expect(await queue.getGlobalRateLimit()).toEqual({ max, duration });
          await queue.close();
        }
      ),
      { numRuns: runs }
    );
  });

  test('getRateLimitTtl reports the broker-side temporary TTL', async () => {
    const h = setup();
    const queue = makeTcpQueue(h, name('tcp-rate-ttl'));
    await queue.rateLimit(2_000);
    const remaining = await queue.getRateLimitTtl();
    expect(remaining).toBeGreaterThan(0);
    expect(remaining).toBeLessThanOrEqual(2_000);
  });

  test('isMaxed observes active global-concurrency slots', async () => {
    const h = setup();
    const queueName = name('tcp-maxed');
    const queue = makeTcpQueue(h, queueName);
    await queue.setGlobalConcurrencyAsync(1);
    await queue.add('task', { value: 1 });
    expect(await h.manager.pull(queueName)).not.toBeNull();
    expect(await queue.isMaxed()).toBe(true);
  });
});

describe('TCP Queue deduplication methods use unique-key ownership', () => {
  test('getDeduplicationJobId resolves a generated deduplication ID', async () => {
    const h = setup();
    await fc.assert(
      fc.asyncProperty(fc.uuid(), async (deduplicationId) => {
        const queue = makeTcpQueue(h, name('tcp-dedup-get'));
        const job = await queue.add('task', {}, { deduplication: { id: deduplicationId } });
        expect(await queue.getDeduplicationJobId(deduplicationId)).toBe(job.id);
        await queue.close();
      }),
      { numRuns: runs }
    );
  });

  test('Queue.removeDeduplicationKey releases a TCP unique key', async () => {
    const h = setup();
    const queue = makeTcpQueue(h, name('tcp-dedup-remove'));
    const deduplicationId = Bun.randomUUIDv7();
    const first = await queue.add('task', { value: 1 }, { deduplication: { id: deduplicationId } });
    expect(await queue.removeDeduplicationKey(deduplicationId)).toBe(1);
    const second = await queue.add(
      'task',
      { value: 2 },
      { deduplication: { id: deduplicationId } }
    );
    expect(second.id).not.toBe(first.id);
  });

  test('Job.removeDeduplicationKey releases only its own TCP key', async () => {
    const h = setup();
    const queue = makeTcpQueue(h, name('tcp-job-dedup-remove'));
    const deduplicationId = Bun.randomUUIDv7();
    const job = await queue.add('task', {}, { deduplication: { id: deduplicationId } });
    expect(await job.removeDeduplicationKey()).toBe(true);
    expect(await queue.getDeduplicationJobId(deduplicationId)).toBeNull();
  });
});
