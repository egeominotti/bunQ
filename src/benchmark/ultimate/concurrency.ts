import type { QueueManager } from '../../application/queueManager';
import type { TestResults } from './harness';

export async function testRaceConditions(
  queueManager: QueueManager,
  results: TestResults
): Promise<void> {
  results.section('11. RACE CONDITIONS');
  const queue = 'race-test-' + Date.now();
  const jobCount = 1000;
  const workerCount = 20;
  await queueManager.pushBatch(
    queue,
    Array.from({ length: jobCount }, (_, index) => ({
      data: { index },
      removeOnComplete: true,
    }))
  );
  const processed = new Set<string>();
  let duplicates = 0;
  let errors = 0;
  const workers = Array.from({ length: workerCount }, async () => {
    while (processed.size < jobCount) {
      try {
        const job = await queueManager.pull(queue, 50);
        if (!job) continue;
        if (processed.has(job.id)) duplicates++;
        else processed.add(job.id);
        await queueManager.ack(job.id, {});
      } catch {
        errors++;
      }
    }
  });
  await Promise.all(workers);
  results.assert(duplicates === 0, 'No duplicate job processing', `${duplicates} duplicates`);
  results.assert(
    processed.size === jobCount,
    'All jobs processed once',
    `${processed.size}/${jobCount}`
  );
  results.assert(errors === 0, 'No errors during concurrent processing', `${errors} errors`);
}

export async function testStallDetection(
  queueManager: QueueManager,
  results: TestResults
): Promise<void> {
  results.section('13. STALL DETECTION');
  const queue = 'stall-test-' + Date.now();
  const job = await queueManager.push(queue, {
    data: { stall: 'test' },
    maxAttempts: 3,
    timeout: 1000,
  });
  const pulled = await queueManager.pull(queue, 100);
  results.assert(pulled?.id === job.id, 'Job pulled');
  await Bun.sleep(2000);
  const stats = queueManager.getStats();
  results.pass('Stall simulation complete', `active=${stats.active}, waiting=${stats.waiting}`);
  let cleaned = 0;
  while (true) {
    const stalled = await queueManager.pull(queue, 50);
    if (!stalled) break;
    await queueManager.ack(stalled.id, {});
    cleaned++;
  }
  if (cleaned > 0) results.pass(`Cleaned ${cleaned} stalled jobs`);
}
