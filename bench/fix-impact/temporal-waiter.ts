import { elapsedMs, padded, rounded, type Harness } from './harness';

export function benchmarkTemporalIndex(harness: Harness): void {
  const { TemporalManager } = harness.runtime;
  const { temporalUnrelatedJobs, temporalRemovalSamples, temporalQuerySamples } = harness.profile;
  const manager = new TemporalManager();
  const base = Date.now() - temporalUnrelatedJobs - 10_000;
  for (let i = 0; i < temporalUnrelatedJobs; i++) {
    manager.addToIndex(base + i, padded('unrelated-', i), 'unrelated');
  }
  for (let i = 0; i < temporalRemovalSamples; i++) {
    manager.addToIndex(base + temporalUnrelatedJobs + i, padded('target-', i), 'target');
  }

  const lookupSamples: number[] = [];
  let firstLookup: Array<{ jobId: string; createdAt: number }> = [];
  for (let i = 0; i < temporalQuerySamples; i++) {
    const startedAt = Bun.nanoseconds();
    firstLookup = manager.getOldJobs('target', 0, 1);
    lookupSamples.push(elapsedMs(startedAt));
    harness.sink ^= firstLookup.length;
  }
  harness.addResult({
    id: 'temporal-queue-lookup',
    category: 'temporal-index',
    operation: 'getOldJobs for sparse queue',
    workload: {
      unrelatedJobs: temporalUnrelatedJobs,
      targetJobs: temporalRemovalSamples,
      limit: 1,
    },
    samples: lookupSamples,
    correctness: {
      firstId: firstLookup[0]?.jobId,
      expectedFirstId: padded('target-', 0),
      exact: firstLookup[0]?.jobId === padded('target-', 0),
    },
    comparable: true,
  });

  const removalSamples: number[] = [];
  for (let i = 0; i < temporalRemovalSamples; i++) {
    const startedAt = Bun.nanoseconds();
    manager.removeFromIndex(padded('target-', i));
    removalSamples.push(elapsedMs(startedAt));
  }
  harness.addResult({
    id: 'temporal-remove-by-id',
    category: 'temporal-index',
    operation: 'removeFromIndex by job id',
    workload: { unrelatedJobs: temporalUnrelatedJobs, removals: temporalRemovalSamples },
    samples: removalSamples,
    correctness: {
      finalSize: manager.indexSize,
      expectedSize: temporalUnrelatedJobs,
      exact: manager.indexSize === temporalUnrelatedJobs,
    },
    comparable: true,
  });
}

export async function benchmarkWaiters(harness: Harness): Promise<void> {
  const { WaiterManager } = harness.runtime;
  const { waiterCount, waiterSamples } = harness.profile;
  const samples: number[] = [];
  const finalLengths: number[] = [];
  for (let sample = 0; sample < waiterSamples; sample++) {
    const manager = new WaiterManager();
    const waiters = Array.from({ length: waiterCount }, () => manager.waitForJob(60_000));
    const startedAt = Bun.nanoseconds();
    manager.notifyBatch(waiterCount);
    await Promise.all(waiters);
    samples.push(elapsedMs(startedAt));
    finalLengths.push(manager.length);
  }

  const debtManager = new WaiterManager();
  debtManager.notifyBatch(waiterCount);
  await debtManager.waitForJob(100);
  const secondStartedAt = Bun.nanoseconds();
  await debtManager.waitForJob(25);
  const secondWaitMs = elapsedMs(secondStartedAt);
  harness.addResult({
    id: 'waiter-notify-batch',
    category: 'waiters',
    operation: 'notifyBatch active waiters',
    workload: { waiters: waiterCount },
    samples,
    correctness: {
      finalLengths,
      allDrained: finalLengths.every((length) => length === 0),
      secondWaitAfterSurplusMs: rounded(secondWaitMs),
      surplusCoalesced: secondWaitMs >= 15,
    },
    comparable: true,
  });
}

export function benchmarkDelayedHeap(harness: Harness): void {
  const { TemporalManager } = harness.runtime;
  const { delayedChurnJobs, delayedSamples } = harness.profile;
  const samples: number[] = [];
  const retainedHeapEntries: number[] = [];
  const liveEntries: number[] = [];
  for (let sample = 0; sample < delayedSamples; sample++) {
    const manager = new TemporalManager();
    const runAt = Date.now() + 3_600_000;
    const startedAt = Bun.nanoseconds();
    for (let i = 0; i < delayedChurnJobs; i++) {
      const id = padded('delayed-', i);
      manager.addDelayed(id, runAt + i);
      manager.removeDelayed(id);
    }
    samples.push(elapsedMs(startedAt));
    const sizes = manager.getSizes();
    retainedHeapEntries.push(sizes.delayedHeap);
    liveEntries.push(sizes.delayedJobIds);
  }
  harness.addResult({
    id: 'delayed-heap-churn',
    category: 'memory',
    operation: 'add/remove delayed job churn',
    workload: { addRemovePairs: delayedChurnJobs },
    samples,
    correctness: {
      liveEntries,
      retainedHeapEntries,
      noRetainedStaleEntries:
        liveEntries.every((count) => count === 0) &&
        retainedHeapEntries.every((count) => count === 0),
    },
    comparable: true,
    note: 'Retained heap entries are deterministic; RSS is intentionally not used.',
  });
}
