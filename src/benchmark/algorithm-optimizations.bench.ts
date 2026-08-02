/**
 * Benchmark for the P0 + P1 algorithmic optimizations.
 *
 * Covers:
 * 1. getStats() - O(32) instead of O(n)
 * 2. Cron tick - O(k log n) instead of O(n)
 * 3. Dependency resolution - O(m) instead of O(n×k)
 * 4. Rate limiter - O(1) instead of O(w)
 * 5. cleanQueue() - O(log n + k) instead of O(n)
 * 6. Batch pull - O(1) lock instead of O(b)
 */

import { QueueManager } from '../application/queueManager';
import { ProtocolRateLimiter } from '../infrastructure/server/rateLimiter';

// Timing utility.
function measure(name: string, fn: () => void, iterations: number = 1): number {
  const start = performance.now();
  for (let i = 0; i < iterations; i++) {
    fn();
  }
  const elapsed = performance.now() - start;
  const perOp = elapsed / iterations;
  console.log(
    `  ${name}: ${perOp.toFixed(3)}ms per op (${iterations} iterations, ${elapsed.toFixed(1)}ms total)`
  );
  return perOp;
}

async function measureAsync(
  name: string,
  fn: () => Promise<void>,
  iterations: number = 1
): Promise<number> {
  const start = performance.now();
  for (let i = 0; i < iterations; i++) {
    await fn();
  }
  const elapsed = performance.now() - start;
  const perOp = elapsed / iterations;
  console.log(
    `  ${name}: ${perOp.toFixed(3)}ms per op (${iterations} iterations, ${elapsed.toFixed(1)}ms total)`
  );
  return perOp;
}

async function benchmarkGetStats() {
  console.log('\n📊 Benchmark: getStats()');
  console.log('='.repeat(50));

  const qm = new QueueManager();

  // Populate with jobs.
  const jobCounts = [1000, 10000, 50000];

  for (const count of jobCounts) {
    // Reset
    qm.shutdown();
    const qm2 = new QueueManager();

    // Push job
    console.log(`\n  Pushing ${count} jobs...`);
    const pushStart = performance.now();
    for (let i = 0; i < count; i++) {
      await qm2.push('bench-queue', { data: { i } });
    }
    console.log(`  Push completed in ${(performance.now() - pushStart).toFixed(0)}ms`);

    // Benchmark getStats
    measure(
      `getStats() with ${count} jobs`,
      () => {
        qm2.getStats();
      },
      1000
    );

    qm2.shutdown();
  }
}

function benchmarkCronTick() {
  console.log('\n⏰ Benchmark: Cron Scheduler');
  console.log('='.repeat(50));

  const qm = new QueueManager();

  const cronCounts = [100, 1000, 5000];

  for (const count of cronCounts) {
    // Add cron jobs.
    console.log(`\n  Adding ${count} cron jobs...`);
    for (let i = 0; i < count; i++) {
      qm.addCron({
        name: `cron-${i}`,
        queue: 'cron-queue',
        data: { i },
        repeatEvery: 3600000 + i * 1000, // One hour plus an offset to spread executions.
      });
    }

    // Benchmark cron getStats, including the nextRun lookup.
    measure(
      `Cron getStats with ${count} crons`,
      () => {
        qm.getStats();
      },
      1000
    );

    // Clean up.
    for (let i = 0; i < count; i++) {
      qm.removeCron(`cron-${i}`);
    }
  }

  qm.shutdown();
}

function benchmarkRateLimiter() {
  console.log('\n🚦 Benchmark: Rate Limiter');
  console.log('='.repeat(50));

  const windowSizes = [100, 500, 1000];

  for (const maxReq of windowSizes) {
    const limiter = new ProtocolRateLimiter({
      windowMs: 60000,
      maxRequests: maxReq,
    });

    // Fill the window.
    console.log(`\n  Filling window with ${maxReq} requests...`);
    for (let i = 0; i < maxReq - 1; i++) {
      limiter.isAllowed('client-1');
    }

    // Benchmark isAllowed with an almost-full window.
    measure(
      `isAllowed() with ${maxReq - 1} requests in window`,
      () => {
        limiter.isAllowed('client-1');
        limiter.getRemaining('client-1');
      },
      10000
    );

    limiter.stop();
  }
}

async function benchmarkCleanQueue() {
  console.log('\n🧹 Benchmark: cleanQueue()');
  console.log('='.repeat(50));

  const jobCounts = [1000, 10000, 50000];

  for (const count of jobCounts) {
    const qm = new QueueManager();

    // Use regular jobs because a negative delay cannot simulate an old createdAt value.
    console.log(`\n  Pushing ${count} jobs...`);
    for (let i = 0; i < count; i++) {
      await qm.push('clean-queue', { data: { i } });
    }

    // Benchmark cleanQueue with a large graceMs: it scans without removing anything.
    measure(
      `cleanQueue() scan with ${count} jobs`,
      () => {
        qm.clean('clean-queue', 1, undefined, 100); // graceMs=1 ms scans for jobs older than 1 ms.
      },
      100
    );

    qm.shutdown();
  }
}

async function benchmarkBatchPull() {
  console.log('\n📦 Benchmark: Batch Pull');
  console.log('='.repeat(50));

  const batchSizes = [10, 50, 100];

  for (const batchSize of batchSizes) {
    const qm = new QueueManager();

    // Push enough jobs for the batch.
    const totalJobs = batchSize * 10;
    console.log(`\n  Pushing ${totalJobs} jobs for batch size ${batchSize}...`);
    for (let i = 0; i < totalJobs; i++) {
      await qm.push('batch-queue', { data: { i } });
    }

    // Benchmark batch pull
    let pulled = 0;
    await measureAsync(
      `Batch pull ${batchSize} jobs`,
      async () => {
        const jobs: unknown[] = [];
        for (let i = 0; i < batchSize; i++) {
          const job = await qm.pull('batch-queue', 0);
          if (job) {
            jobs.push(job);
            await qm.ack(job.id);
          }
        }
        pulled += jobs.length;
      },
      5
    );

    console.log(`  Total pulled: ${pulled}`);
    qm.shutdown();
  }
}

async function benchmarkDependencyResolution() {
  console.log('\n🔗 Benchmark: Dependency Resolution');
  console.log('='.repeat(50));

  const qm = new QueueManager();

  // Create the parent job.
  console.log('\n  Creating parent job...');
  const parent = await qm.push('dep-queue', { data: { type: 'parent' } });

  // Create dependent jobs.
  const childCounts = [10, 50, 100];

  for (const count of childCounts) {
    console.log(`\n  Creating ${count} child jobs depending on parent...`);
    for (let i = 0; i < count; i++) {
      await qm.push('dep-queue', {
        data: { type: 'child', i },
        dependsOn: [parent.id],
      });
    }

    // Pull and complete the queued parent to trigger dependency resolution.
    const pulledParent = await qm.pull('dep-queue', 0);
    if (pulledParent) {
      const start = performance.now();
      await qm.ack(pulledParent.id);
      // Wait for dependency resolution to finish.
      await Bun.sleep(100);
      const elapsed = performance.now() - start;
      console.log(`  Dependency resolution for ${count} children: ${elapsed.toFixed(2)}ms`);
    }

    // Clean up the child jobs.
    for (let i = 0; i < count; i++) {
      const child = await qm.pull('dep-queue', 0);
      if (child) await qm.ack(child.id);
    }
  }

  qm.shutdown();
}

async function runAllBenchmarks() {
  console.log('🚀 bunQ Algorithm Optimizations Benchmark');
  console.log('==========================================\n');
  console.log('Testing P0 + P1 optimizations...\n');

  await benchmarkGetStats();
  benchmarkCronTick();
  benchmarkRateLimiter();
  await benchmarkCleanQueue();
  await benchmarkBatchPull();
  await benchmarkDependencyResolution();

  console.log('\n✅ Benchmark completed!');
}

runAllBenchmarks().catch(console.error);
