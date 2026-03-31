/**
 * Issue #31 - Not being able to fetch parent data in chained jobs
 *
 * Reproduces the bug where child jobs in a chain cannot fetch
 * the parent job's result. Tests both addChain() and add() flows.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, FlowProducer, shutdownManager } from '../src/client';

describe('Issue #31 - Parent result in chained jobs', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('addChain: child can fetch parent result via getParentResult()', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('chain-parent-result', { embedded: true });
    queue.obliterate();

    const parentResults: Array<{ step: number; parentResult: unknown }> = [];
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'chain-parent-result',
      async (job) => {
        const data = job.data as { step: number; __flowParentId?: string };
        if (data.__flowParentId) {
          const parentResult = flow.getParentResult(data.__flowParentId);
          parentResults.push({ step: data.step, parentResult });
        }
        if (data.step === 2) resolve!();
        return { value: data.step * 10 };
      },
      { embedded: true, concurrency: 1 }
    );

    await flow.addChain([
      { name: 'step-0', queueName: 'chain-parent-result', data: { step: 0 } },
      { name: 'step-1', queueName: 'chain-parent-result', data: { step: 1 } },
      { name: 'step-2', queueName: 'chain-parent-result', data: { step: 2 } },
    ]);

    await done;
    // Give time for final ack
    await Bun.sleep(50);

    expect(parentResults).toHaveLength(2);
    // Step 1 should have step 0's result
    expect(parentResults[0].step).toBe(1);
    expect(parentResults[0].parentResult).toEqual({ value: 0 });
    // Step 2 should have step 1's result
    expect(parentResults[1].step).toBe(2);
    expect(parentResults[1].parentResult).toEqual({ value: 10 });

    await worker.close();
    flow.close();
    queue.close();
  });

  test('addChain: many concurrent chains - parent results always available', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('chain-stress', { embedded: true });
    queue.obliterate();

    const CHAIN_COUNT = 20;
    const STEPS_PER_CHAIN = 5;
    const failures: string[] = [];
    let completedChains = 0;
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'chain-stress',
      async (job) => {
        const data = job.data as {
          step: number;
          chainId: number;
          __flowParentId?: string;
        };

        if (data.__flowParentId) {
          const parentResult = flow.getParentResult<{ value: number }>(
            data.__flowParentId
          );
          if (parentResult === undefined) {
            failures.push(
              `Chain ${data.chainId} step ${data.step}: parent result undefined (parentId: ${data.__flowParentId})`
            );
          } else if (parentResult.value !== (data.step - 1) * 100 + data.chainId) {
            failures.push(
              `Chain ${data.chainId} step ${data.step}: wrong parent result ${JSON.stringify(parentResult)}`
            );
          }
        }

        if (data.step === STEPS_PER_CHAIN - 1) {
          completedChains++;
          if (completedChains === CHAIN_COUNT) resolve!();
        }

        return { value: data.step * 100 + data.chainId };
      },
      { embedded: true, concurrency: 3 }
    );

    // Create many chains concurrently
    const chainPromises = Array.from({ length: CHAIN_COUNT }, (_, chainId) =>
      flow.addChain(
        Array.from({ length: STEPS_PER_CHAIN }, (_, step) => ({
          name: `step-${step}`,
          queueName: 'chain-stress',
          data: { step, chainId },
        }))
      )
    );
    await Promise.all(chainPromises);

    await done;
    await Bun.sleep(100);

    if (failures.length > 0) {
      console.log('Failures:', failures);
    }
    expect(failures).toHaveLength(0);

    await worker.close();
    flow.close();
    queue.close();
  });

  test('add() with children: parent can fetch children values via getChildrenValues()', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-children-vals', { embedded: true });
    queue.obliterate();

    let parentChildrenValues: Record<string, unknown> | null = null;
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const childrenProcessed = { count: 0 };

    const worker = new Worker(
      'flow-children-vals',
      async (job) => {
        const data = job.data as { role: string; value: number };

        if (data.role === 'parent') {
          // Parent should wait for children, then get their values
          // Retry fetching children values with timeout
          let values: Record<string, unknown> = {};
          for (let attempt = 0; attempt < 50; attempt++) {
            values = await queue.getChildrenValues(job.id);
            if (Object.keys(values).length === 2) break;
            await Bun.sleep(50);
          }
          parentChildrenValues = values;
          resolve!();
          return { summary: 'done' };
        }

        // Child job
        childrenProcessed.count++;
        return { childValue: data.value * 10 };
      },
      { embedded: true, concurrency: 3 }
    );

    await flow.add({
      name: 'parent',
      queueName: 'flow-children-vals',
      data: { role: 'parent', value: 0 },
      children: [
        { name: 'child-a', queueName: 'flow-children-vals', data: { role: 'child', value: 1 } },
        { name: 'child-b', queueName: 'flow-children-vals', data: { role: 'child', value: 2 } },
      ],
    });

    await done;
    await Bun.sleep(100);

    expect(childrenProcessed.count).toBe(2);
    expect(parentChildrenValues).not.toBeNull();
    // Should have 2 children results
    const keys = Object.keys(parentChildrenValues!);
    expect(keys.length).toBe(2);
    // Values should be the children's return values
    const vals = Object.values(parentChildrenValues!);
    const childValues = vals.map((v: any) => v.childValue).sort();
    expect(childValues).toEqual([10, 20]);

    await worker.close();
    flow.close();
    queue.close();
  });

  test('add() with children: parent processes before children complete (race)', async () => {
    /**
     * This test exposes the race condition:
     * In FlowProducer.add(), the parent job has NO dependsOn on children.
     * Parent can be pulled and processed BEFORE children complete.
     * getChildrenValues() then returns empty/partial results.
     */
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-race', { embedded: true });
    queue.obliterate();

    const results: Array<{
      parentId: string;
      childrenValues: Record<string, unknown>;
      childrenCount: number;
    }> = [];
    const FLOW_COUNT = 10;
    let completedParents = 0;
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'flow-race',
      async (job) => {
        const data = job.data as { role: string; value: number; flowIdx?: number };

        if (data.role === 'child') {
          // Simulate some work
          await Bun.sleep(Math.random() * 10);
          return { childResult: data.value };
        }

        // Parent - immediately try getChildrenValues
        const childrenValues = await queue.getChildrenValues(job.id);
        results.push({
          parentId: job.id,
          childrenValues,
          childrenCount: Object.keys(childrenValues).length,
        });

        completedParents++;
        if (completedParents === FLOW_COUNT) resolve!();
        return { parentDone: true };
      },
      { embedded: true, concurrency: 5 }
    );

    // Create multiple flows concurrently
    for (let i = 0; i < FLOW_COUNT; i++) {
      await flow.add({
        name: 'parent',
        queueName: 'flow-race',
        data: { role: 'parent', value: 0, flowIdx: i },
        children: [
          { name: 'child-1', queueName: 'flow-race', data: { role: 'child', value: 1 } },
          { name: 'child-2', queueName: 'flow-race', data: { role: 'child', value: 2 } },
          { name: 'child-3', queueName: 'flow-race', data: { role: 'child', value: 3 } },
        ],
      });
    }

    await done;
    await Bun.sleep(200);

    // Check how many parents got all 3 children values
    const fullResults = results.filter((r) => r.childrenCount === 3);
    const partialResults = results.filter((r) => r.childrenCount < 3);

    if (partialResults.length > 0) {
      console.log(
        `RACE CONDITION: ${partialResults.length}/${FLOW_COUNT} parents got partial/empty children values`
      );
      for (const r of partialResults) {
        console.log(`  Parent ${r.parentId}: got ${r.childrenCount}/3 children values`);
      }
    }

    // BUG: In the current implementation, parent jobs are NOT held
    // until children complete (addWaitingParent is never called).
    // So parents may process before children, getting empty results.
    // This assertion documents the expected behavior:
    expect(fullResults.length).toBe(FLOW_COUNT);

    await worker.close();
    flow.close();
    queue.close();
  });

  test('addChain: result available immediately after parent completes', async () => {
    /**
     * Tests that when a parent job completes in a chain,
     * its result is immediately available to the next job.
     * This tests the synchronous LRU + SQLite storage path.
     */
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('chain-immediate', { embedded: true });
    queue.obliterate();

    let resultAvailable = false;
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'chain-immediate',
      async (job) => {
        const data = job.data as { step: number; __flowParentId?: string };

        if (data.step === 1 && data.__flowParentId) {
          // Check result is available IMMEDIATELY (no retries)
          const result = flow.getParentResult(data.__flowParentId);
          resultAvailable = result !== undefined;
          resolve!();
        }

        return { step: data.step, computed: data.step * 42 };
      },
      { embedded: true, concurrency: 1 }
    );

    await flow.addChain([
      { name: 'first', queueName: 'chain-immediate', data: { step: 0 } },
      { name: 'second', queueName: 'chain-immediate', data: { step: 1 } },
    ]);

    await done;
    await Bun.sleep(50);

    expect(resultAvailable).toBe(true);

    await worker.close();
    flow.close();
    queue.close();
  });
});
