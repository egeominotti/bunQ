import { CoreE2eHarness, type CoreE2eMode } from '../support/harness';
import { CoverageTracker, ensure, ensureEqual, eventually } from '../support/tracker';

export async function runQueueLimitsWorkerContract(mode: CoreE2eMode): Promise<CoverageTracker> {
  const harness = await CoreE2eHarness.start(mode, 'queue-limits-worker');
  const tracker = new CoverageTracker(mode, 'queue-limits-worker-contract');
  let release = () => undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });

  try {
    const queue = harness.queue<{ value: number }>('limits');

    tracker.call('Queue', 'setGlobalConcurrency', () => queue.setGlobalConcurrency(2));
    await eventually(
      () => queue.getGlobalConcurrency(),
      (value) => value === 2,
      'setGlobalConcurrency was not applied'
    );
    const concurrency = await tracker.invoke('Queue', 'getGlobalConcurrency', () =>
      queue.getGlobalConcurrency()
    );
    ensure(concurrency === 2, `getGlobalConcurrency returned ${concurrency}`);
    tracker.call('Queue', 'removeGlobalConcurrency', () => queue.removeGlobalConcurrency());
    await eventually(
      () => queue.getGlobalConcurrency(),
      (value) => value === null,
      'removeGlobalConcurrency was not applied'
    );
    await tracker.invoke('Queue', 'setGlobalConcurrencyAsync', () =>
      queue.setGlobalConcurrencyAsync(1)
    );

    const worker = harness.worker<{ value: number }>(queue.name, async () => {
      await gate;
    });
    await worker.waitUntilReady();
    const active = await queue.add('active', { value: 1 }, { durable: true });
    await eventually(
      () => queue.getJobState(active.id),
      (state) => state === 'active',
      'concurrency fixture did not become active'
    );
    const maxed = await tracker.invoke('Queue', 'isMaxed', () => queue.isMaxed());
    ensure(maxed, 'isMaxed did not observe exhausted concurrency');
    await tracker.invoke('Queue', 'removeGlobalConcurrencyAsync', () =>
      queue.removeGlobalConcurrencyAsync()
    );
    ensure((await queue.getGlobalConcurrency()) === null, 'async concurrency removal failed');

    tracker.call('Queue', 'setGlobalRateLimit', () => queue.setGlobalRateLimit(3, 60_000));
    await eventually(
      () => queue.getGlobalRateLimit(),
      (value) => (value as { max?: number } | null)?.max === 3,
      'setGlobalRateLimit was not applied'
    );
    const rate = await tracker.invoke('Queue', 'getGlobalRateLimit', () =>
      queue.getGlobalRateLimit()
    );
    ensureEqual(rate, { max: 3, duration: 60_000 }, 'getGlobalRateLimit mismatch');
    tracker.call('Queue', 'removeGlobalRateLimit', () => queue.removeGlobalRateLimit());
    await eventually(
      () => queue.getGlobalRateLimit(),
      (value) => value === null,
      'removeGlobalRateLimit was not applied'
    );
    await tracker.invoke('Queue', 'setGlobalRateLimitAsync', () =>
      queue.setGlobalRateLimitAsync(2, 30_000)
    );
    ensureEqual(
      await queue.getGlobalRateLimit(),
      { max: 2, duration: 30_000 },
      'setGlobalRateLimitAsync mismatch'
    );
    await tracker.invoke('Queue', 'removeGlobalRateLimitAsync', () =>
      queue.removeGlobalRateLimitAsync()
    );

    await tracker.invoke('Queue', 'rateLimit', () => queue.rateLimit(2_000));
    const ttl = await tracker.invoke('Queue', 'getRateLimitTtl', () => queue.getRateLimitTtl());
    ensure(ttl > 0 && ttl <= 2_000, `getRateLimitTtl returned ${ttl}`);
    await queue.removeGlobalRateLimitAsync();

    const deduplicationId = crypto.randomUUID();
    const owner = await queue.add(
      'dedup',
      { value: 2 },
      { deduplication: { id: deduplicationId }, durable: true }
    );
    const ownerId = await tracker.invoke('Queue', 'getDeduplicationJobId', () =>
      queue.getDeduplicationJobId(deduplicationId)
    );
    ensure(ownerId === owner.id, 'getDeduplicationJobId returned the wrong owner');
    const removed = await tracker.invoke('Queue', 'removeDeduplicationKey', () =>
      queue.removeDeduplicationKey(deduplicationId)
    );
    ensure(removed === 1, `removeDeduplicationKey returned ${removed}`);

    tracker.call('Queue', 'setStallConfig', () =>
      queue.setStallConfig({ enabled: true, maxStalls: 7, gracePeriod: 250 })
    );
    await eventually(
      () => queue.getStallConfigAsync(),
      (value) => (value as { maxStalls?: number }).maxStalls === 7,
      'setStallConfig was not applied'
    );
    if (mode === 'embedded') {
      const localStall = tracker.call('Queue', 'getStallConfig', () => queue.getStallConfig());
      ensure(localStall.maxStalls === 7, 'getStallConfig missed cached/live value');
    }
    await tracker.invoke('Queue', 'setStallConfigAsync', () =>
      queue.setStallConfigAsync({ maxStalls: 5, stallInterval: 2_000 })
    );
    const stall = await tracker.invoke('Queue', 'getStallConfigAsync', () =>
      queue.getStallConfigAsync()
    );
    ensure(stall.maxStalls === 5 && stall.stallInterval === 2_000, 'stall config mismatch');

    const workers = await tracker.invoke('Queue', 'getWorkers', () => queue.getWorkers());
    ensure(
      workers.some((entry) => entry.queues?.includes(queue.name)),
      'getWorkers missed worker'
    );
    const workerCount = await tracker.invoke('Queue', 'getWorkersCount', () =>
      queue.getWorkersCount()
    );
    ensure(workerCount >= 1, `getWorkersCount returned ${workerCount}`);

    release();
    await eventually(
      () => queue.getJobState(active.id),
      (state) => state === 'completed',
      'metrics fixture did not complete'
    );
    const metrics = await tracker.invoke('Queue', 'getMetrics', () =>
      queue.getMetrics('completed', 0, -1)
    );
    ensure(metrics.meta.count >= 1, `getMetrics returned ${metrics.meta.count}`);
    const trimmed = await tracker.invoke('Queue', 'trimEvents', () => queue.trimEvents(1));
    ensure(trimmed > 0, `trimEvents did not remove journal entries: ${trimmed}`);
    const retrimmed = await queue.trimEvents(1);
    ensure(retrimmed === 0, `trimEvents was not idempotent: ${retrimmed}`);
  } finally {
    release();
    await harness.close();
  }

  return tracker;
}
