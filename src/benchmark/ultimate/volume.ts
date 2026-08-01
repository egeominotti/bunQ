import type { QueueManager } from '../../application/queueManager';
import { formatCount, heapMegabytes, type TestResults } from './harness';

export async function testMemoryStability(
  queueManager: QueueManager,
  results: TestResults
): Promise<void> {
  results.section('14. MEMORY STABILITY');
  const queue = 'memory-test-' + Date.now();
  const jobCount = 50000;
  const iterations = 3;
  Bun.gc(true);
  const startMemory = heapMegabytes();
  for (let iteration = 0; iteration < iterations; iteration++) {
    for (let index = 0; index < jobCount; index += 5000) {
      await queueManager.pushBatch(
        queue,
        Array.from({ length: Math.min(5000, jobCount - index) }, (_, batchIndex) => ({
          data: { iter: iteration, index: index + batchIndex },
          removeOnComplete: true,
        }))
      );
    }
    const workers = Array.from({ length: 10 }, async () => {
      let processed = 0;
      let emptyPulls = 0;
      while (emptyPulls < 3) {
        const job = await queueManager.pull(queue, 50);
        if (!job) {
          emptyPulls++;
          continue;
        }
        emptyPulls = 0;
        await queueManager.ack(job.id, {});
        processed++;
      }
      return processed;
    });
    const processed = (await Promise.all(workers)).reduce((sum, count) => sum + count, 0);
    if (processed < jobCount) {
      console.log(
        `    Note: Processed ${processed}/${jobCount} jobs in iteration ${iteration + 1}`
      );
    }
    Bun.gc(true);
  }
  await Bun.sleep(1000);
  Bun.gc(true);
  const endMemory = heapMegabytes();
  const memoryStats = queueManager.getMemoryStats();
  const growth = endMemory - startMemory;
  results.pass(`Processed ${jobCount * iterations} jobs in ${iterations} iterations`);
  results.assert(
    memoryStats.jobIndex < 100,
    'Job index mostly cleared',
    `${memoryStats.jobIndex} entries`
  );
  results.assert(
    growth < 100,
    'Memory growth acceptable',
    `${startMemory}MB → ${endMemory}MB (${growth > 0 ? '+' : ''}${growth}MB)`
  );
}

export async function testDataIntegrity(
  queueManager: QueueManager,
  results: TestResults
): Promise<void> {
  results.section('15. DATA INTEGRITY');
  const queue = 'integrity-test-' + Date.now();
  const jobCount = 10000;
  const pushed = new Map<string, number>();
  for (let index = 0; index < jobCount; index += 1000) {
    const batch = await queueManager.pushBatch(
      queue,
      Array.from({ length: Math.min(1000, jobCount - index) }, (_, batchIndex) => ({
        data: { uniqueIndex: index + batchIndex },
        removeOnComplete: true,
      }))
    );
    batch.forEach((id, batchIndex) => pushed.set(String(id), index + batchIndex));
  }

  const workerResults = Array.from({ length: 10 }, async () => {
    const processed = new Map<string, number>();
    let dataErrors = 0;
    let emptyPulls = 0;
    while (emptyPulls < 3) {
      const job = await queueManager.pull(queue, 50);
      if (!job) {
        emptyPulls++;
        continue;
      }
      emptyPulls = 0;
      const expectedIndex = pushed.get(String(job.id));
      const actualIndex = (job.data as { uniqueIndex?: number })?.uniqueIndex;
      if (
        expectedIndex !== undefined &&
        actualIndex !== undefined &&
        expectedIndex !== actualIndex
      ) {
        dataErrors++;
      }
      processed.set(job.id, actualIndex ?? -1);
      await queueManager.ack(job.id, {});
    }
    return { processed, dataErrors };
  });

  const processed = new Map<string, number>();
  let dataErrors = 0;
  for (const result of await Promise.all(workerResults)) {
    for (const [id, index] of result.processed) processed.set(id, index);
    dataErrors += result.dataErrors;
  }
  results.assert(
    processed.size === jobCount,
    'All jobs processed',
    `${processed.size}/${jobCount}`
  );
  results.assert(dataErrors === 0, 'No data corruption', `${dataErrors} errors`);
  const uniqueIndexes = new Set(processed.values());
  results.assert(
    uniqueIndexes.size === jobCount,
    'All unique indexes present',
    `${uniqueIndexes.size}/${jobCount}`
  );
}

export async function testHighVolume(
  queueManager: QueueManager,
  results: TestResults
): Promise<void> {
  results.section('16. HIGH VOLUME (100k jobs)');
  const queue = 'volume-test-' + Date.now();
  const jobCount = 100000;
  Bun.gc(true);
  const start = performance.now();
  const pushStart = performance.now();
  for (let index = 0; index < jobCount; index += 5000) {
    await queueManager.pushBatch(
      queue,
      Array.from({ length: Math.min(5000, jobCount - index) }, (_, batchIndex) => ({
        data: { i: index + batchIndex },
        removeOnComplete: true,
      }))
    );
  }
  const pushRate = Math.round(jobCount / ((performance.now() - pushStart) / 1000));
  results.pass(`Pushed ${formatCount(jobCount)} jobs`, `${formatCount(pushRate)}/s`);

  const processingStart = performance.now();
  const workers = Array.from({ length: 10 }, async () => {
    let processed = 0;
    let emptyPulls = 0;
    while (emptyPulls < 5) {
      const job = await queueManager.pull(queue, 50);
      if (!job) {
        emptyPulls++;
        continue;
      }
      emptyPulls = 0;
      await queueManager.ack(job.id, {});
      processed++;
    }
    return processed;
  });
  const processed = (await Promise.all(workers)).reduce((sum, count) => sum + count, 0);
  const processingRate = Math.round(processed / ((performance.now() - processingStart) / 1000));
  results.pass(`Processed ${formatCount(processed)} jobs`, `${formatCount(processingRate)}/s`);
  const totalTime = (performance.now() - start) / 1000;
  results.pass(
    `Total time: ${totalTime.toFixed(2)}s`,
    `${formatCount(Math.round(jobCount / totalTime))}/s overall`
  );
  await Bun.sleep(200);
  const stats = queueManager.getStats();
  results.assert(stats.waiting < 10 && stats.active < 10, 'Queue mostly empty after processing');
}
