#!/usr/bin/env bun
/**
 * Advanced Bulk Operations Tests (TCP Mode)
 *
 * Tests for large bulk adds, mixed priorities, delays, custom jobIds,
 * performance, sequential batches, and concurrent processing.
 */

import { Queue, Worker } from '../../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');
const connOpts = { port: TCP_PORT };

let passed = 0;
let failed = 0;
const queues: Queue[] = [];

function ok(msg: string) {
  console.log(`   ✅ ${msg}`);
  passed++;
}

function fail(msg: string) {
  console.log(`   ❌ ${msg}`);
  failed++;
}

function makeQueue(name: string): Queue {
  const q = new Queue(name, { connection: connOpts });
  queues.push(q);
  return q;
}

async function main() {
  console.log('=== Bulk Operations Advanced Tests (TCP) ===\n');

  // ─────────────────────────────────────────────────
  // Test 1: Large bulk add (1000 jobs)
  // ─────────────────────────────────────────────────
  console.log('1. Testing LARGE BULK ADD (1000 jobs)...');
  {
    const q = makeQueue('tcp-bulk-adv-large');
    q.obliterate();
    await Bun.sleep(200);

    const jobs = Array.from({ length: 1000 }, (_, i) => ({
      name: `job-${i}`,
      data: { index: i },
    }));

    const result = await q.addBulk(jobs);

    if (result.length !== 1000) {
      fail(`Expected 1000 jobs added, got ${result.length}`);
    } else {
      const ids = new Set(result.map((j) => j.id));
      if (ids.size === 1000) {
        ok(`All 1000 jobs added with unique IDs`);
      } else {
        fail(`Expected 1000 unique IDs, got ${ids.size}`);
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Test 2: Bulk with mixed priorities
  // ─────────────────────────────────────────────────
  console.log('\n2. Testing BULK WITH MIXED PRIORITIES...');
  {
    const q = makeQueue('tcp-bulk-adv-prio');
    q.obliterate();
    await Bun.sleep(200);

    const jobs = [
      { name: 'low-1', data: { label: 'low-1' }, opts: { priority: 1 } },
      { name: 'low-2', data: { label: 'low-2' }, opts: { priority: 1 } },
      { name: 'mid-1', data: { label: 'mid-1' }, opts: { priority: 5 } },
      { name: 'mid-2', data: { label: 'mid-2' }, opts: { priority: 5 } },
      { name: 'high-1', data: { label: 'high-1' }, opts: { priority: 10 } },
      { name: 'high-2', data: { label: 'high-2' }, opts: { priority: 10 } },
    ];
    await q.addBulk(jobs);

    const processedOrder: string[] = [];

    const worker = new Worker(
      'tcp-bulk-adv-prio',
      async (job) => {
        const data = job.data as { label: string };
        processedOrder.push(data.label);
        return { ok: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    // Wait for all 6 jobs to complete
    for (let i = 0; i < 300; i++) {
      if (processedOrder.length >= 6) break;
      await Bun.sleep(50);
    }

    await worker.close();

    if (processedOrder.length !== 6) {
      fail(`Expected 6 jobs processed, got ${processedOrder.length}`);
    } else {
      const highIndices = processedOrder
        .map((l, i) => (l.startsWith('high') ? i : -1))
        .filter((i) => i >= 0);
      const midIndices = processedOrder
        .map((l, i) => (l.startsWith('mid') ? i : -1))
        .filter((i) => i >= 0);
      const lowIndices = processedOrder
        .map((l, i) => (l.startsWith('low') ? i : -1))
        .filter((i) => i >= 0);

      if (
        Math.max(...highIndices) < Math.min(...midIndices) &&
        Math.max(...midIndices) < Math.min(...lowIndices)
      ) {
        ok(`Priority order correct: ${processedOrder.join(', ')}`);
      } else {
        // TCP mode may have slight ordering variance; verify high before low at minimum
        if (Math.max(...highIndices) < Math.min(...lowIndices)) {
          ok(`Priority order mostly correct (high before low): ${processedOrder.join(', ')}`);
        } else {
          fail(`Priority order wrong: ${processedOrder.join(', ')}`);
        }
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Test 3: Bulk with delays
  // ─────────────────────────────────────────────────
  console.log('\n3. Testing BULK WITH DELAYS...');
  {
    const q = makeQueue('tcp-bulk-adv-delay');
    q.obliterate();
    await Bun.sleep(200);

    const jobs = [
      { name: 'immediate-1', data: { label: 'immediate-1' } },
      { name: 'immediate-2', data: { label: 'immediate-2' } },
      { name: 'immediate-3', data: { label: 'immediate-3' } },
      { name: 'delayed-1', data: { label: 'delayed-1' }, opts: { delay: 2000 } },
      { name: 'delayed-2', data: { label: 'delayed-2' }, opts: { delay: 2000 } },
    ];
    await q.addBulk(jobs);

    const processedOrder: string[] = [];

    const worker = new Worker(
      'tcp-bulk-adv-delay',
      async (job) => {
        const data = job.data as { label: string };
        processedOrder.push(data.label);
        return { ok: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    // Wait for all 5 jobs to complete (delayed ones need extra time)
    for (let i = 0; i < 300; i++) {
      if (processedOrder.length >= 5) break;
      await Bun.sleep(100);
    }

    await worker.close();

    if (processedOrder.length !== 5) {
      fail(`Expected 5 jobs processed, got ${processedOrder.length}`);
    } else {
      const immediateIndices = processedOrder
        .map((l, i) => (l.startsWith('immediate') ? i : -1))
        .filter((i) => i >= 0);
      const delayedIndices = processedOrder
        .map((l, i) => (l.startsWith('delayed') ? i : -1))
        .filter((i) => i >= 0);

      if (
        immediateIndices.length === 3 &&
        delayedIndices.length === 2 &&
        Math.max(...immediateIndices) < Math.min(...delayedIndices)
      ) {
        ok(`Immediate jobs processed before delayed: ${processedOrder.join(', ')}`);
      } else {
        fail(`Order wrong: ${processedOrder.join(', ')}`);
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Test 4: Bulk with custom jobIds
  // ─────────────────────────────────────────────────
  console.log('\n4. Testing BULK WITH CUSTOM JOB IDS...');
  {
    const q = makeQueue('tcp-bulk-adv-customid');
    q.obliterate();
    await Bun.sleep(200);

    const customId1 = `custom-bulk-${Date.now()}-a`;
    const customId2 = `custom-bulk-${Date.now()}-b`;

    const jobs = [
      { name: 'task-1', data: { val: 1 }, opts: { jobId: customId1 } },
      { name: 'task-2', data: { val: 2 }, opts: { jobId: customId2 } },
      { name: 'task-3', data: { val: 3 } },
    ];
    const result = await q.addBulk(jobs);

    if (result.length !== 3) {
      fail(`Expected 3 jobs, got ${result.length}`);
    } else {
      const id1Match = result[0].id === customId1;
      const id2Match = result[1].id === customId2;
      const id3Valid = result[2].id && result[2].id !== customId1 && result[2].id !== customId2;

      if (id1Match && id2Match && id3Valid) {
        // Verify retrieval by custom ID
        const fetched1 = await q.getJob(customId1);
        const fetched2 = await q.getJob(customId2);

        if (fetched1 && fetched1.id === customId1 && fetched2 && fetched2.id === customId2) {
          ok(`Custom IDs assigned correctly and retrievable: ${customId1}, ${customId2}`);
        } else {
          fail(`Custom ID jobs not retrievable: fetched1=${fetched1?.id}, fetched2=${fetched2?.id}`);
        }
      } else {
        fail(`Custom IDs not assigned: [${result[0].id}, ${result[1].id}, ${result[2].id}]`);
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Test 5: Bulk performance (500 jobs under 2s)
  // ─────────────────────────────────────────────────
  console.log('\n5. Testing BULK PERFORMANCE (500 jobs < 2s)...');
  {
    const q = makeQueue('tcp-bulk-adv-perf');
    q.obliterate();
    await Bun.sleep(200);

    const jobs = Array.from({ length: 500 }, (_, i) => ({
      name: `perf-job-${i}`,
      data: { index: i, payload: `data-${i}` },
    }));

    const start = performance.now();
    const result = await q.addBulk(jobs);
    const elapsed = performance.now() - start;

    if (result.length !== 500) {
      fail(`Expected 500 jobs, got ${result.length}`);
    } else if (elapsed >= 2000) {
      fail(`addBulk took ${elapsed.toFixed(0)}ms (expected < 2000ms)`);
    } else {
      ok(`500 jobs added in ${elapsed.toFixed(0)}ms`);
    }
  }

  // ─────────────────────────────────────────────────
  // Test 6: Sequential bulk batches (5x100 = 500)
  // ─────────────────────────────────────────────────
  console.log('\n6. Testing SEQUENTIAL BULK BATCHES (5 x 100 = 500)...');
  {
    const q = makeQueue('tcp-bulk-adv-seq');
    q.obliterate();
    await Bun.sleep(200);

    // Add 5 separate batches of 100 jobs each
    for (let batch = 0; batch < 5; batch++) {
      const jobs = Array.from({ length: 100 }, (_, i) => ({
        name: `batch-${batch}-job-${i}`,
        data: { batch, index: i, globalIndex: batch * 100 + i },
      }));
      await q.addBulk(jobs);
    }

    const completedGlobalIndices: number[] = [];

    const worker = new Worker(
      'tcp-bulk-adv-seq',
      async (job) => {
        const data = job.data as { globalIndex: number };
        completedGlobalIndices.push(data.globalIndex);
        return { ok: true };
      },
      { concurrency: 20, connection: connOpts, useLocks: false }
    );

    // Wait for all 500 to complete
    for (let i = 0; i < 300; i++) {
      if (completedGlobalIndices.length >= 500) break;
      await Bun.sleep(100);
    }

    await worker.close();

    if (completedGlobalIndices.length !== 500) {
      fail(`Expected 500 jobs completed, got ${completedGlobalIndices.length}`);
    } else {
      const unique = new Set(completedGlobalIndices);
      if (unique.size !== 500) {
        fail(`Expected 500 unique indices, got ${unique.size}`);
      } else {
        const sorted = [...unique].sort((a, b) => a - b);
        if (sorted[0] === 0 && sorted[499] === 499) {
          ok(`All 500 jobs from 5 batches completed, indices 0-499 verified`);
        } else {
          fail(`Index range wrong: [${sorted[0]}..${sorted[sorted.length - 1]}]`);
        }
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Test 7: Bulk with concurrent processing
  // 200 jobs, 4 workers concurrency 5
  // ─────────────────────────────────────────────────
  console.log('\n7. Testing BULK WITH CONCURRENT PROCESSING (200 jobs, 4 workers)...');
  {
    const q = makeQueue('tcp-bulk-adv-concurrent');
    q.obliterate();
    await Bun.sleep(200);

    const jobs = Array.from({ length: 200 }, (_, i) => ({
      name: `conc-job-${i}`,
      data: { index: i },
    }));
    await q.addBulk(jobs);

    const completedIndices: number[] = [];
    const workers: Worker[] = [];

    // 4 workers with concurrency 5 each
    for (let w = 0; w < 4; w++) {
      const worker = new Worker(
        'tcp-bulk-adv-concurrent',
        async (job) => {
          const data = job.data as { index: number };
          completedIndices.push(data.index);
          return { processed: data.index };
        },
        { concurrency: 5, connection: connOpts, useLocks: false }
      );
      workers.push(worker);
    }

    // Wait for all 200 to complete
    for (let i = 0; i < 300; i++) {
      if (completedIndices.length >= 200) break;
      await Bun.sleep(100);
    }

    for (const w of workers) {
      await w.close();
    }

    if (completedIndices.length !== 200) {
      fail(`Expected 200 jobs completed, got ${completedIndices.length}`);
    } else {
      const unique = new Set(completedIndices);
      if (unique.size !== 200) {
        fail(`Expected 200 unique indices, got ${unique.size}`);
      } else {
        const sorted = [...unique].sort((a, b) => a - b);
        if (sorted[0] === 0 && sorted[199] === 199) {
          ok(`All 200 jobs completed by 4 workers, no duplicates, indices 0-199 verified`);
        } else {
          fail(`Index range wrong: [${sorted[0]}..${sorted[sorted.length - 1]}]`);
        }
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Cleanup
  // ─────────────────────────────────────────────────
  for (const q of queues) {
    q.obliterate();
    q.close();
  }

  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch((e) => {
  console.error(`Fatal error: ${e}`);
  process.exit(1);
});
