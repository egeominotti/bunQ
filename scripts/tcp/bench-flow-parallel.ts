#!/usr/bin/env bun
/**
 * Benchmark: Flow parallel vs sequential in TCP mode
 *
 * Measures the real-world impact of Promise.all for sibling creation
 * by comparing FlowProducer operations against a sequential baseline.
 */

import { Queue, FlowProducer } from '../../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');
const connOpts = { port: TCP_PORT };

async function main() {
  console.log('=== Flow Parallel Benchmark (TCP Mode) ===\n');
  console.log(`Connecting to localhost:${TCP_PORT}...\n`);

  const flow = new FlowProducer({ connection: connOpts });
  const queue = new Queue('bench-flow-tcp', { connection: connOpts });

  // Warm up connection
  await queue.add('warmup', { x: 1 });

  // ── Baseline: single push latency ──
  const singleRuns = 20;
  const singleStart = Bun.nanoseconds();
  for (let i = 0; i < singleRuns; i++) {
    await queue.add('single', { i });
  }
  const singleAvgMs = (Bun.nanoseconds() - singleStart) / 1_000_000 / singleRuns;
  console.log(`Single push avg: ${singleAvgMs.toFixed(2)}ms\n`);

  // ── Test 1: Sequential baseline (await in loop — old behavior) ──
  const childCounts = [5, 10, 20];

  for (const n of childCounts) {
    console.log(`--- ${n} children ---`);

    // Sequential: push N jobs one by one (simulates old await-in-loop)
    const seqStart = Bun.nanoseconds();
    for (let i = 0; i < n; i++) {
      await queue.add(`seq-child-${i}`, { index: i });
    }
    const seqMs = (Bun.nanoseconds() - seqStart) / 1_000_000;

    // Parallel: push N jobs with Promise.all (simulates new behavior)
    const parStart = Bun.nanoseconds();
    await Promise.all(
      Array.from({ length: n }, (_, i) => queue.add(`par-child-${i}`, { index: i }))
    );
    const parMs = (Bun.nanoseconds() - parStart) / 1_000_000;

    // FlowProducer.add() with N children (uses Promise.all internally now)
    const flowStart = Bun.nanoseconds();
    await flow.add({
      name: 'parent',
      queueName: 'bench-flow-tcp',
      data: { role: 'parent' },
      children: Array.from({ length: n }, (_, i) => ({
        name: `child-${i}`,
        queueName: 'bench-flow-tcp',
        data: { index: i },
      })),
    });
    const flowMs = (Bun.nanoseconds() - flowStart) / 1_000_000;

    // FlowProducer.addBulkThen() with N parallel jobs
    const bulkStart = Bun.nanoseconds();
    await flow.addBulkThen(
      Array.from({ length: n }, (_, i) => ({
        name: `par-${i}`,
        queueName: 'bench-flow-tcp',
        data: { index: i },
      })),
      { name: 'final', queueName: 'bench-flow-tcp', data: {} }
    );
    const bulkMs = (Bun.nanoseconds() - bulkStart) / 1_000_000;

    const speedup = seqMs / parMs;

    console.log(`  Sequential (await loop): ${seqMs.toFixed(2)}ms`);
    console.log(`  Parallel (Promise.all):  ${parMs.toFixed(2)}ms`);
    console.log(`  FlowProducer.add():      ${flowMs.toFixed(2)}ms`);
    console.log(`  FlowProducer.bulkThen(): ${bulkMs.toFixed(2)}ms`);
    console.log(`  Speedup seq→parallel:    ${speedup.toFixed(1)}x`);
    console.log();
  }

  // ── Test 2: addBulk() ──
  console.log('--- addBulk: 10 independent flows ---');
  const bulkSeqStart = Bun.nanoseconds();
  for (let i = 0; i < 10; i++) {
    await flow.add({ name: `flow-${i}`, queueName: 'bench-flow-tcp', data: { i } });
  }
  const bulkSeqMs = (Bun.nanoseconds() - bulkSeqStart) / 1_000_000;

  const bulkParStart = Bun.nanoseconds();
  await flow.addBulk(
    Array.from({ length: 10 }, (_, i) => ({
      name: `flow-${i}`,
      queueName: 'bench-flow-tcp',
      data: { i },
    }))
  );
  const bulkParMs = (Bun.nanoseconds() - bulkParStart) / 1_000_000;

  console.log(`  Sequential (manual loop): ${bulkSeqMs.toFixed(2)}ms`);
  console.log(`  addBulk() (Promise.all):  ${bulkParMs.toFixed(2)}ms`);
  console.log(`  Speedup:                  ${(bulkSeqMs / bulkParMs).toFixed(1)}x`);

  // Cleanup
  flow.close();
  await queue.close();

  console.log('\n=== Done ===');
}

main().catch(console.error);
