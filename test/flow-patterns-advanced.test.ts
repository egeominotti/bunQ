/**
 * Advanced Flow Patterns Tests (Embedded Mode)
 *
 * Tests advanced flow patterns: deep chains, multi-queue flows,
 * fan-out/fan-in, error handling in chains, concurrent independent chains,
 * delayed steps, and large fan-out scenarios.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, FlowProducer, shutdownManager } from '../src/client';

describe('Advanced Flow Patterns - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('deep chain: 10 steps execute in correct order', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-deep-chain', { embedded: true });
    queue.obliterate();

    const STEP_COUNT = 10;
    const executionOrder: number[] = [];
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'flow-deep-chain',
      async (job) => {
        const data = job.data as { step: number };
        executionOrder.push(data.step);

        if (data.step === STEP_COUNT - 1) resolve!();
        return { step: data.step, completed: true };
      },
      { embedded: true, concurrency: 1 }
    );

    const steps = Array.from({ length: STEP_COUNT }, (_, i) => ({
      name: `step-${i}`,
      queueName: 'flow-deep-chain',
      data: { step: i },
    }));

    await flow.addChain(steps);

    await done;
    await Bun.sleep(200);

    expect(executionOrder).toHaveLength(STEP_COUNT);
    // Verify strict sequential order: 0, 1, 2, ..., 9
    for (let i = 0; i < STEP_COUNT; i++) {
      expect(executionOrder[i]).toBe(i);
    }

    await worker.close();
    flow.close();
    queue.close();
  }, 30000);

  test('multi-queue flow: chain across 3 different queues', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queueA = new Queue('flow-mq-a', { embedded: true });
    const queueB = new Queue('flow-mq-b', { embedded: true });
    const queueC = new Queue('flow-mq-c', { embedded: true });
    queueA.obliterate();
    queueB.obliterate();
    queueC.obliterate();

    const executionOrder: Array<{ queue: string; step: string }> = [];
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const workerA = new Worker(
      'flow-mq-a',
      async (job) => {
        const data = job.data as { step: string };
        executionOrder.push({ queue: 'a', step: data.step });
        return { from: 'queue-a', step: data.step };
      },
      { embedded: true, concurrency: 1 }
    );

    const workerB = new Worker(
      'flow-mq-b',
      async (job) => {
        const data = job.data as { step: string };
        executionOrder.push({ queue: 'b', step: data.step });
        return { from: 'queue-b', step: data.step };
      },
      { embedded: true, concurrency: 1 }
    );

    const workerC = new Worker(
      'flow-mq-c',
      async (job) => {
        const data = job.data as { step: string };
        executionOrder.push({ queue: 'c', step: data.step });
        if (data.step === 'finalize') resolve!();
        return { from: 'queue-c', step: data.step };
      },
      { embedded: true, concurrency: 1 }
    );

    await flow.addChain([
      { name: 'ingest', queueName: 'flow-mq-a', data: { step: 'ingest' } },
      { name: 'transform', queueName: 'flow-mq-b', data: { step: 'transform' } },
      { name: 'validate', queueName: 'flow-mq-c', data: { step: 'validate' } },
      { name: 'enrich', queueName: 'flow-mq-a', data: { step: 'enrich' } },
      { name: 'store', queueName: 'flow-mq-b', data: { step: 'store' } },
      { name: 'finalize', queueName: 'flow-mq-c', data: { step: 'finalize' } },
    ]);

    await done;
    await Bun.sleep(200);

    expect(executionOrder).toHaveLength(6);
    expect(executionOrder[0]).toEqual({ queue: 'a', step: 'ingest' });
    expect(executionOrder[1]).toEqual({ queue: 'b', step: 'transform' });
    expect(executionOrder[2]).toEqual({ queue: 'c', step: 'validate' });
    expect(executionOrder[3]).toEqual({ queue: 'a', step: 'enrich' });
    expect(executionOrder[4]).toEqual({ queue: 'b', step: 'store' });
    expect(executionOrder[5]).toEqual({ queue: 'c', step: 'finalize' });

    await workerA.close();
    await workerB.close();
    await workerC.close();
    flow.close();
    queueA.close();
    queueB.close();
    queueC.close();
  }, 30000);

  test('fan-out fan-in: 5 parallel tasks then 1 final task', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-fanout5', { embedded: true });
    queue.obliterate();

    const PARALLEL_COUNT = 5;
    const executionOrder: string[] = [];
    let finalExecuted = false;
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'flow-fanout5',
      async (job) => {
        const data = job.data as { task: string; value?: number };
        executionOrder.push(data.task);

        if (data.task === 'aggregate') {
          finalExecuted = true;
          resolve!();
          return { aggregated: true };
        }

        await Bun.sleep(20);
        return { result: (data.value ?? 0) * 2 };
      },
      { embedded: true, concurrency: 10 }
    );

    const parallelSteps = Array.from({ length: PARALLEL_COUNT }, (_, i) => ({
      name: `worker-${i}`,
      queueName: 'flow-fanout5',
      data: { task: `worker-${i}`, value: i + 1 },
    }));

    await flow.addBulkThen(parallelSteps, {
      name: 'aggregate',
      queueName: 'flow-fanout5',
      data: { task: 'aggregate' },
    });

    await done;
    await Bun.sleep(200);

    expect(finalExecuted).toBe(true);
    // The aggregate task must be last
    expect(executionOrder.indexOf('aggregate')).toBe(executionOrder.length - 1);
    // All 5 parallel tasks must have executed
    const parallelTasks = executionOrder.filter((t) => t.startsWith('worker-'));
    expect(parallelTasks).toHaveLength(PARALLEL_COUNT);

    await worker.close();
    flow.close();
    queue.close();
  }, 30000);

  test('error in chain step: step 2 fails, step 3 never runs', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-chain-error', { embedded: true });
    queue.obliterate();

    const executedSteps: number[] = [];
    const failedSteps: number[] = [];
    let step1Resolve: () => void;
    const step1Done = new Promise<void>((r) => (step1Resolve = r));

    const worker = new Worker(
      'flow-chain-error',
      async (job) => {
        const data = job.data as { step: number };
        executedSteps.push(data.step);

        if (data.step === 0) {
          return { step: 0, ok: true };
        }

        if (data.step === 1) {
          throw new Error('Intentional failure at step 1');
        }

        // Step 2 should never execute
        return { step: data.step, ok: true };
      },
      { embedded: true, concurrency: 1 }
    );

    worker.on('failed', (job) => {
      const data = job.data as { step: number };
      failedSteps.push(data.step);
      step1Resolve!();
    });

    await flow.addChain([
      { name: 'step-0', queueName: 'flow-chain-error', data: { step: 0 } },
      { name: 'step-1', queueName: 'flow-chain-error', data: { step: 1 }, opts: { attempts: 1 } },
      { name: 'step-2', queueName: 'flow-chain-error', data: { step: 2 } },
    ]);

    await step1Done;
    // Wait extra time to confirm step 2 does NOT execute
    await Bun.sleep(1000);

    // Step 0 completed successfully, step 1 was attempted (and failed)
    expect(executedSteps).toContain(0);
    expect(executedSteps).toContain(1);
    // Step 2 should never have run because step 1 (its dependency) failed
    expect(executedSteps).not.toContain(2);
    // Step 1 failed
    expect(failedSteps).toContain(1);

    await worker.close();
    flow.close();
    queue.close();
  }, 30000);

  test('multiple independent chains: 10 concurrent 3-step chains', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-multi-chains', { embedded: true });
    queue.obliterate();

    const CHAIN_COUNT = 10;
    const STEPS_PER_CHAIN = 3;
    const chainResults = new Map<number, number[]>();
    let completedChains = 0;
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'flow-multi-chains',
      async (job) => {
        const data = job.data as { chainId: number; step: number };

        if (!chainResults.has(data.chainId)) {
          chainResults.set(data.chainId, []);
        }
        chainResults.get(data.chainId)!.push(data.step);

        await Bun.sleep(10);

        if (data.step === STEPS_PER_CHAIN - 1) {
          completedChains++;
          if (completedChains === CHAIN_COUNT) resolve!();
        }

        return { chainId: data.chainId, step: data.step };
      },
      { embedded: true, concurrency: 20 }
    );

    // Launch 10 independent 3-step chains concurrently
    await Promise.all(
      Array.from({ length: CHAIN_COUNT }, (_, chainId) =>
        flow.addChain(
          Array.from({ length: STEPS_PER_CHAIN }, (_, step) => ({
            name: `chain-${chainId}-step-${step}`,
            queueName: 'flow-multi-chains',
            data: { chainId, step },
          }))
        )
      )
    );

    // Wait for all chains to complete
    for (let i = 0; i < 200; i++) {
      if (completedChains >= CHAIN_COUNT) break;
      await Bun.sleep(100);
    }

    expect(completedChains).toBe(CHAIN_COUNT);

    // Verify each chain executed all 3 steps in correct order
    for (let chainId = 0; chainId < CHAIN_COUNT; chainId++) {
      const steps = chainResults.get(chainId);
      expect(steps).toBeDefined();
      expect(steps).toHaveLength(STEPS_PER_CHAIN);
      // Within each chain, steps must be in order: 0, 1, 2
      expect(steps).toEqual([0, 1, 2]);
    }

    await worker.close();
    flow.close();
    queue.close();
  }, 30000);

  test('chain with delayed step: delay is respected and chain completes in order', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-delayed-step', { embedded: true });
    queue.obliterate();

    const executionTimestamps: Array<{ step: number; time: number }> = [];
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'flow-delayed-step',
      async (job) => {
        const data = job.data as { step: number };
        executionTimestamps.push({ step: data.step, time: Date.now() });

        if (data.step === 2) resolve!();
        return { step: data.step, ok: true };
      },
      { embedded: true, concurrency: 1 }
    );

    const startTime = Date.now();

    await flow.addChain([
      { name: 'step-0', queueName: 'flow-delayed-step', data: { step: 0 } },
      { name: 'step-1', queueName: 'flow-delayed-step', data: { step: 1 }, opts: { delay: 300 } },
      { name: 'step-2', queueName: 'flow-delayed-step', data: { step: 2 } },
    ]);

    await done;
    await Bun.sleep(200);

    expect(executionTimestamps).toHaveLength(3);
    // Verify execution order
    expect(executionTimestamps[0].step).toBe(0);
    expect(executionTimestamps[1].step).toBe(1);
    expect(executionTimestamps[2].step).toBe(2);

    // Step 1 should have been delayed by at least 250ms from chain start
    // (allowing some tolerance for processing time)
    const step1Time = executionTimestamps[1].time;
    expect(step1Time - startTime).toBeGreaterThanOrEqual(250);

    // Step 2 must execute after step 1
    expect(executionTimestamps[2].time).toBeGreaterThanOrEqual(executionTimestamps[1].time);

    await worker.close();
    flow.close();
    queue.close();
  }, 30000);

  test('large fan-out: 20 parallel tasks then 1 final task', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-fanout20', { embedded: true });
    queue.obliterate();

    const PARALLEL_COUNT = 20;
    const completedParallel = new Set<number>();
    let finalExecuted = false;
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'flow-fanout20',
      async (job) => {
        const data = job.data as { task: string; index?: number };

        if (data.task === 'final') {
          finalExecuted = true;
          resolve!();
          return { final: true, parallelCompleted: completedParallel.size };
        }

        // Mark parallel task as completed
        completedParallel.add(data.index!);
        await Bun.sleep(10);
        return { index: data.index, processed: true };
      },
      { embedded: true, concurrency: 20 }
    );

    const parallelSteps = Array.from({ length: PARALLEL_COUNT }, (_, i) => ({
      name: `parallel-${i}`,
      queueName: 'flow-fanout20',
      data: { task: `parallel-${i}`, index: i },
    }));

    await flow.addBulkThen(parallelSteps, {
      name: 'final-merge',
      queueName: 'flow-fanout20',
      data: { task: 'final' },
    });

    await done;
    await Bun.sleep(200);

    expect(finalExecuted).toBe(true);
    // All 20 parallel tasks must have completed before the final task
    expect(completedParallel.size).toBe(PARALLEL_COUNT);

    await worker.close();
    flow.close();
    queue.close();
  }, 30000);
});
