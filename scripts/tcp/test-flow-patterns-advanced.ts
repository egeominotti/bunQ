#!/usr/bin/env bun
/**
 * Advanced Flow Patterns Tests (TCP Mode)
 *
 * Tests advanced flow patterns: deep chains, multi-queue flows,
 * fan-out/fan-in, concurrent independent chains, delayed steps,
 * and large fan-out scenarios. TCP mode cannot use getParentResult
 * (embedded-only), so tests verify execution order and completion.
 */

import { Queue, Worker, FlowProducer } from '../../src/client';

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
  console.log('=== Advanced Flow Patterns Tests (TCP) ===\n');

  const flow = new FlowProducer({ connection: connOpts, useLocks: false });

  // ─────────────────────────────────────────────────
  // Test 1: Deep chain (10 steps execute in correct order)
  // ─────────────────────────────────────────────────
  console.log('1. Testing DEEP CHAIN (10 steps)...');
  {
    const q = makeQueue('tcp-flowpat-deep-chain');
    q.obliterate();
    await Bun.sleep(200);

    const STEP_COUNT = 10;
    const executionOrder: number[] = [];

    const worker = new Worker('tcp-flowpat-deep-chain', async (job) => {
      const data = job.data as { step: number };
      executionOrder.push(data.step);
      await Bun.sleep(20);
      return { step: data.step, completed: true };
    }, { concurrency: 1, connection: connOpts, useLocks: false });

    const steps = Array.from({ length: STEP_COUNT }, (_, i) => ({
      name: `step-${i}`,
      queueName: 'tcp-flowpat-deep-chain',
      data: { step: i },
    }));

    await flow.addChain(steps);

    for (let i = 0; i < 200; i++) {
      if (executionOrder.length >= STEP_COUNT) break;
      await Bun.sleep(100);
    }

    if (executionOrder.length === STEP_COUNT) {
      let inOrder = true;
      for (let i = 0; i < STEP_COUNT; i++) {
        if (executionOrder[i] !== i) { inOrder = false; break; }
      }
      if (inOrder) {
        ok(`Deep chain: all ${STEP_COUNT} steps executed in order (${executionOrder.join(' -> ')})`);
      } else {
        fail(`Steps out of order: [${executionOrder.join(', ')}]`);
      }
    } else {
      fail(`Expected ${STEP_COUNT} steps, got ${executionOrder.length}: [${executionOrder.join(', ')}]`);
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 2: Multi-queue flow (chain across 3 different queues)
  // ─────────────────────────────────────────────────
  console.log('\n2. Testing MULTI-QUEUE FLOW (3 queues)...');
  {
    const qA = makeQueue('tcp-flowpat-mq-a');
    const qB = makeQueue('tcp-flowpat-mq-b');
    const qC = makeQueue('tcp-flowpat-mq-c');
    qA.obliterate();
    qB.obliterate();
    qC.obliterate();
    await Bun.sleep(200);

    const executionOrder: Array<{ queue: string; step: string }> = [];

    const workerA = new Worker('tcp-flowpat-mq-a', async (job) => {
      const data = job.data as { step: string };
      executionOrder.push({ queue: 'a', step: data.step });
      return { from: 'queue-a', step: data.step };
    }, { concurrency: 1, connection: connOpts, useLocks: false });

    const workerB = new Worker('tcp-flowpat-mq-b', async (job) => {
      const data = job.data as { step: string };
      executionOrder.push({ queue: 'b', step: data.step });
      return { from: 'queue-b', step: data.step };
    }, { concurrency: 1, connection: connOpts, useLocks: false });

    const workerC = new Worker('tcp-flowpat-mq-c', async (job) => {
      const data = job.data as { step: string };
      executionOrder.push({ queue: 'c', step: data.step });
      return { from: 'queue-c', step: data.step };
    }, { concurrency: 1, connection: connOpts, useLocks: false });

    await flow.addChain([
      { name: 'ingest', queueName: 'tcp-flowpat-mq-a', data: { step: 'ingest' } },
      { name: 'transform', queueName: 'tcp-flowpat-mq-b', data: { step: 'transform' } },
      { name: 'validate', queueName: 'tcp-flowpat-mq-c', data: { step: 'validate' } },
      { name: 'enrich', queueName: 'tcp-flowpat-mq-a', data: { step: 'enrich' } },
      { name: 'store', queueName: 'tcp-flowpat-mq-b', data: { step: 'store' } },
      { name: 'finalize', queueName: 'tcp-flowpat-mq-c', data: { step: 'finalize' } },
    ]);

    for (let i = 0; i < 200; i++) {
      if (executionOrder.length >= 6) break;
      await Bun.sleep(100);
    }

    const expected = [
      { queue: 'a', step: 'ingest' },
      { queue: 'b', step: 'transform' },
      { queue: 'c', step: 'validate' },
      { queue: 'a', step: 'enrich' },
      { queue: 'b', step: 'store' },
      { queue: 'c', step: 'finalize' },
    ];

    if (executionOrder.length === 6) {
      let match = true;
      for (let i = 0; i < 6; i++) {
        if (executionOrder[i].queue !== expected[i].queue || executionOrder[i].step !== expected[i].step) {
          match = false;
          break;
        }
      }
      if (match) {
        ok(`Multi-queue flow: ${executionOrder.map(e => `${e.queue}:${e.step}`).join(' -> ')}`);
      } else {
        fail(`Order mismatch: [${executionOrder.map(e => `${e.queue}:${e.step}`).join(', ')}]`);
      }
    } else {
      fail(`Expected 6 steps, got ${executionOrder.length}: [${executionOrder.map(e => `${e.queue}:${e.step}`).join(', ')}]`);
    }

    await workerA.close();
    await workerB.close();
    await workerC.close();
  }

  // ─────────────────────────────────────────────────
  // Test 3: Fan-out fan-in (5 parallel + 1 final)
  // ─────────────────────────────────────────────────
  console.log('\n3. Testing FAN-OUT FAN-IN (5 parallel + 1 final)...');
  {
    const q = makeQueue('tcp-flowpat-fanout5');
    q.obliterate();
    await Bun.sleep(200);

    const PARALLEL_COUNT = 5;
    const executionOrder: string[] = [];
    let finalExecuted = false;

    const worker = new Worker('tcp-flowpat-fanout5', async (job) => {
      const data = job.data as { task: string; value?: number };
      executionOrder.push(data.task);

      if (data.task === 'aggregate') {
        finalExecuted = true;
        return { aggregated: true };
      }

      await Bun.sleep(20);
      return { result: (data.value ?? 0) * 2 };
    }, { concurrency: 10, connection: connOpts, useLocks: false });

    const parallelSteps = Array.from({ length: PARALLEL_COUNT }, (_, i) => ({
      name: `worker-${i}`,
      queueName: 'tcp-flowpat-fanout5',
      data: { task: `worker-${i}`, value: i + 1 },
    }));

    await flow.addBulkThen(parallelSteps, {
      name: 'aggregate',
      queueName: 'tcp-flowpat-fanout5',
      data: { task: 'aggregate' },
    });

    for (let i = 0; i < 200; i++) {
      if (finalExecuted) break;
      await Bun.sleep(100);
    }

    const parallelTasks = executionOrder.filter(t => t.startsWith('worker-'));
    const aggregateIdx = executionOrder.indexOf('aggregate');

    if (finalExecuted && parallelTasks.length === PARALLEL_COUNT && aggregateIdx === executionOrder.length - 1) {
      ok(`Fan-out fan-in: ${parallelTasks.length} parallel tasks, aggregate last (idx ${aggregateIdx}/${executionOrder.length - 1})`);
    } else {
      fail(`finalExecuted=${finalExecuted}, parallel=${parallelTasks.length}, aggregateIdx=${aggregateIdx}, order=[${executionOrder.join(', ')}]`);
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 4: Multiple independent chains (10 concurrent 3-step chains)
  // ─────────────────────────────────────────────────
  console.log('\n4. Testing MULTIPLE INDEPENDENT CHAINS (10 x 3 steps)...');
  {
    const q = makeQueue('tcp-flowpat-multi-chains');
    q.obliterate();
    await Bun.sleep(200);

    const CHAIN_COUNT = 10;
    const STEPS_PER_CHAIN = 3;
    const chainResults = new Map<number, number[]>();
    let completedChains = 0;

    const worker = new Worker('tcp-flowpat-multi-chains', async (job) => {
      const data = job.data as { chainId: number; step: number };

      if (!chainResults.has(data.chainId)) {
        chainResults.set(data.chainId, []);
      }
      chainResults.get(data.chainId)!.push(data.step);

      await Bun.sleep(10);

      if (data.step === STEPS_PER_CHAIN - 1) {
        completedChains++;
      }

      return { chainId: data.chainId, step: data.step };
    }, { concurrency: 20, connection: connOpts, useLocks: false });

    // Launch 10 independent 3-step chains concurrently
    await Promise.all(
      Array.from({ length: CHAIN_COUNT }, (_, chainId) =>
        flow.addChain(
          Array.from({ length: STEPS_PER_CHAIN }, (_, step) => ({
            name: `chain-${chainId}-step-${step}`,
            queueName: 'tcp-flowpat-multi-chains',
            data: { chainId, step },
          }))
        )
      )
    );

    for (let i = 0; i < 200; i++) {
      if (completedChains >= CHAIN_COUNT) break;
      await Bun.sleep(100);
    }

    if (completedChains === CHAIN_COUNT) {
      let allInOrder = true;
      for (let chainId = 0; chainId < CHAIN_COUNT; chainId++) {
        const steps = chainResults.get(chainId);
        if (!steps || steps.length !== STEPS_PER_CHAIN) {
          allInOrder = false;
          break;
        }
        for (let s = 0; s < STEPS_PER_CHAIN; s++) {
          if (steps[s] !== s) { allInOrder = false; break; }
        }
        if (!allInOrder) break;
      }
      if (allInOrder) {
        ok(`All ${CHAIN_COUNT} chains completed with ${STEPS_PER_CHAIN} steps each in correct order`);
      } else {
        const details = Array.from(chainResults.entries()).map(([id, s]) => `chain${id}=[${s.join(',')}]`).join(', ');
        fail(`Chains completed but order wrong: ${details}`);
      }
    } else {
      fail(`Only ${completedChains}/${CHAIN_COUNT} chains completed`);
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 5: Chain with delayed step (delay:300ms respected)
  // ─────────────────────────────────────────────────
  console.log('\n5. Testing CHAIN WITH DELAYED STEP...');
  {
    const q = makeQueue('tcp-flowpat-delayed');
    q.obliterate();
    await Bun.sleep(200);

    const executionTimestamps: Array<{ step: number; time: number }> = [];

    const worker = new Worker('tcp-flowpat-delayed', async (job) => {
      const data = job.data as { step: number };
      executionTimestamps.push({ step: data.step, time: Date.now() });
      return { step: data.step, ok: true };
    }, { concurrency: 1, connection: connOpts, useLocks: false });

    const startTime = Date.now();

    await flow.addChain([
      { name: 'step-0', queueName: 'tcp-flowpat-delayed', data: { step: 0 } },
      { name: 'step-1', queueName: 'tcp-flowpat-delayed', data: { step: 1 }, opts: { delay: 300 } },
      { name: 'step-2', queueName: 'tcp-flowpat-delayed', data: { step: 2 } },
    ]);

    for (let i = 0; i < 200; i++) {
      if (executionTimestamps.length >= 3) break;
      await Bun.sleep(100);
    }

    if (executionTimestamps.length === 3) {
      const orderCorrect =
        executionTimestamps[0].step === 0 &&
        executionTimestamps[1].step === 1 &&
        executionTimestamps[2].step === 2;

      const step1Time = executionTimestamps[1].time;
      const delayRespected = (step1Time - startTime) >= 250;
      const step2AfterStep1 = executionTimestamps[2].time >= executionTimestamps[1].time;

      if (orderCorrect && delayRespected && step2AfterStep1) {
        ok(`Chain with delay: order correct, step-1 delayed by ${step1Time - startTime}ms (>=250ms), step-2 after step-1`);
      } else {
        fail(`orderCorrect=${orderCorrect}, delayRespected=${delayRespected} (${step1Time - startTime}ms), step2AfterStep1=${step2AfterStep1}`);
      }
    } else {
      fail(`Expected 3 timestamps, got ${executionTimestamps.length}`);
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 6: Large fan-out (20 parallel + 1 final)
  // ─────────────────────────────────────────────────
  console.log('\n6. Testing LARGE FAN-OUT (20 parallel + 1 final)...');
  {
    const q = makeQueue('tcp-flowpat-fanout20');
    q.obliterate();
    await Bun.sleep(200);

    const PARALLEL_COUNT = 20;
    const completedParallel = new Set<number>();
    let finalExecuted = false;

    const worker = new Worker('tcp-flowpat-fanout20', async (job) => {
      const data = job.data as { task: string; index?: number };

      if (data.task === 'final') {
        finalExecuted = true;
        return { final: true, parallelCompleted: completedParallel.size };
      }

      completedParallel.add(data.index!);
      await Bun.sleep(10);
      return { index: data.index, processed: true };
    }, { concurrency: 20, connection: connOpts, useLocks: false });

    const parallelSteps = Array.from({ length: PARALLEL_COUNT }, (_, i) => ({
      name: `parallel-${i}`,
      queueName: 'tcp-flowpat-fanout20',
      data: { task: `parallel-${i}`, index: i },
    }));

    await flow.addBulkThen(parallelSteps, {
      name: 'final-merge',
      queueName: 'tcp-flowpat-fanout20',
      data: { task: 'final' },
    });

    for (let i = 0; i < 200; i++) {
      if (finalExecuted) break;
      await Bun.sleep(100);
    }

    if (finalExecuted && completedParallel.size === PARALLEL_COUNT) {
      ok(`Large fan-out: all ${PARALLEL_COUNT} parallel tasks completed before final`);
    } else {
      fail(`finalExecuted=${finalExecuted}, parallelCompleted=${completedParallel.size}/${PARALLEL_COUNT}`);
    }

    await worker.close();
  }

  // ─── Cleanup ───
  flow.close();
  for (const q of queues) { q.obliterate(); q.close(); }

  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((e) => {
  console.error(`Fatal error: ${e}`);
  process.exit(1);
});
