#!/usr/bin/env bun
/**
 * Benchmark: job:list (getJobs) performance
 * Tests with 1k, 5k, 10k, 20k jobs across multiple queues
 */

import { Queue } from '../src/client';

const SIZES = [1000, 5000, 10000, 20000];
const QUEUES_COUNT = 4;
const PAGE_SIZE = 50;

async function bench() {
  console.log('=== job:list (getJobs) Benchmark ===\n');

  for (const size of SIZES) {
    const queues: Queue[] = [];
    const perQueue = Math.floor(size / QUEUES_COUNT);

    // Create queues in embedded mode
    for (let q = 0; q < QUEUES_COUNT; q++) {
      const queue = new Queue(`bench-q${q}`, { embedded: true });
      queues.push(queue);

      // Push jobs — mix of immediate and delayed
      const batch: { name: string; data: unknown; opts?: unknown }[] = [];
      for (let j = 0; j < perQueue; j++) {
        const isDelayed = j % 5 === 0; // 20% delayed
        batch.push({
          name: `job-${j}`,
          data: { idx: j, queue: `bench-q${q}` },
          opts: isDelayed ? { delay: 60000 } : { priority: j % 10 },
        });
      }
      await queue.addBulk(batch);
    }

    const targetQueue = queues[0];

    // Warm up
    targetQueue.getJobs({ state: 'waiting', start: 0, end: PAGE_SIZE - 1 });

    // Benchmark: getJobs with different state filters
    const tests = [
      { label: 'waiting only', opts: { state: 'waiting' as const, start: 0, end: PAGE_SIZE - 1 } },
      { label: 'delayed only', opts: { state: 'delayed' as const, start: 0, end: PAGE_SIZE - 1 } },
      { label: 'all states', opts: { state: ['waiting', 'delayed', 'active', 'completed', 'failed'], start: 0, end: PAGE_SIZE - 1 } },
      { label: 'no filter', opts: { start: 0, end: PAGE_SIZE - 1 } },
      { label: 'page 10', opts: { state: 'waiting' as const, start: 450, end: 499 } },
    ];

    console.log(`--- ${size} total jobs (${perQueue}/queue × ${QUEUES_COUNT} queues) ---`);

    for (const t of tests) {
      const RUNS = 500;
      const start = performance.now();
      for (let r = 0; r < RUNS; r++) {
        targetQueue.getJobs(t.opts);
      }
      const elapsed = performance.now() - start;
      const avgUs = ((elapsed / RUNS) * 1000).toFixed(1);
      console.log(`  ${t.label.padEnd(20)} ${avgUs} µs/call  (${RUNS} runs)`);
    }

    // Cleanup
    for (const q of queues) {
      q.obliterate();
      await q.close();
    }
    console.log('');
  }
}

bench().catch(console.error);
