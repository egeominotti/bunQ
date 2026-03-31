/**
 * Issue #34 - Cannot get parent result data
 *
 * Reproduces the bug where parent result data is unavailable in flow chains.
 *
 * Root cause: jobResults uses BoundedMap with FIFO eviction (5,000 limit).
 * Without storage fallback (no DATA_PATH in embedded mode), evicted results
 * are permanently lost. getParentResult() returns undefined.
 *
 * Two failure modes:
 * 1. High throughput: many jobs complete, evicting parent results from cache
 * 2. completedJobs eviction: dependency resolution fails if parent's ID
 *    is evicted from completedJobs (50,000 limit)
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, FlowProducer, shutdownManager } from '../src/client';
import { BoundedMap } from '../src/shared/boundedMap';

describe('Issue #34 - Parent result data missing', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('BoundedMap FIFO eviction loses results that are still needed', () => {
    // Demonstrates the core design flaw: FIFO eviction doesn't consider
    // whether a result is still needed by a pending flow chain
    const map = new BoundedMap<string, unknown>(10);

    // Store 10 results (parent results from flow chains)
    for (let i = 0; i < 10; i++) {
      map.set(`parent-${i}`, { value: i * 10 });
    }
    expect(map.get('parent-0')).toEqual({ value: 0 });

    // Add one more - triggers FIFO eviction of oldest entry
    map.set('new-job', { value: 999 });

    // BUG: parent-0's result is evicted even though a child still needs it
    // With LRU, accessing parent-0 would keep it alive
    expect(map.get('parent-0')).toBeUndefined();
  });

  test('parent result lost after high throughput fills jobResults cache', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('issue34-eviction', { embedded: true });
    queue.obliterate();

    // First: create a chain where we'll check parent result later
    const targetResults: Array<{ step: number; parentResult: unknown }> = [];
    let targetChainDone = false;
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    // Track how many filler jobs completed
    let fillerCompleted = 0;
    const FILLER_COUNT = 5100; // Exceeds maxJobResults (5,000)

    const worker = new Worker(
      'issue34-eviction',
      async (job) => {
        const data = job.data as {
          type: string;
          step?: number;
          fillerId?: number;
          __flowParentId?: string;
        };

        if (data.type === 'filler') {
          fillerCompleted++;
          return { filler: data.fillerId };
        }

        if (data.type === 'target') {
          if (data.__flowParentId) {
            const parentResult = flow.getParentResult(data.__flowParentId);
            targetResults.push({ step: data.step!, parentResult });
          }
          if (data.step === 1) {
            targetChainDone = true;
            resolve!();
          }
          return { targetStep: data.step };
        }

        return {};
      },
      { embedded: true, concurrency: 50 }
    );

    // Step 1: Create the target chain (step-0 → step-1)
    // step-0 completes first, result stored in jobResults
    await flow.addChain([
      {
        name: 'target-step-0',
        queueName: 'issue34-eviction',
        data: { type: 'target', step: 0 },
      },
      {
        name: 'target-step-1',
        queueName: 'issue34-eviction',
        data: { type: 'target', step: 1 },
      },
    ]);

    // Wait for chain to complete
    await done;
    await Bun.sleep(200);

    // At this point, target-step-0's result should be in jobResults
    // Now verify it's accessible
    expect(targetResults.length).toBe(1);
    expect(targetResults[0].parentResult).toBeDefined();

    // Step 2: Flood with filler jobs to trigger FIFO eviction
    let fillerResolve: () => void;
    const fillerDone = new Promise<void>((r) => (fillerResolve = r));

    const fillerWorkerDoneCheck = setInterval(() => {
      if (fillerCompleted >= FILLER_COUNT) {
        clearInterval(fillerWorkerDoneCheck);
        fillerResolve!();
      }
    }, 100);

    // Push filler jobs in bulk
    for (let batch = 0; batch < FILLER_COUNT / 50; batch++) {
      const jobs = Array.from({ length: 50 }, (_, i) => ({
        name: 'filler',
        data: { type: 'filler', fillerId: batch * 50 + i },
      }));
      await queue.addBulk(jobs);
    }

    await fillerDone;
    await Bun.sleep(500);

    // Step 3: Now try to get the target chain's parent result again
    // The first chain's step-0 result should have been evicted by FIFO
    const step0Id = targetResults[0]?.parentResult
      ? 'still-available'
      : 'already-gone-before-filler';

    // Create another chain that depends on results still being available
    const lateResults: Array<{ step: number; parentResult: unknown }> = [];
    let lateResolve: () => void;
    const lateDone = new Promise<void>((r) => (lateResolve = r));

    // Create a new chain - step-0 result will be stored
    await flow.addChain([
      {
        name: 'late-step-0',
        queueName: 'issue34-eviction',
        data: { type: 'target', step: 0 },
      },
      {
        name: 'late-step-1',
        queueName: 'issue34-eviction',
        data: { type: 'target', step: 1 },
      },
    ]);

    // Wait for the late chain to process
    await new Promise<void>((r) => {
      const check = setInterval(() => {
        if (targetResults.length >= 2) {
          clearInterval(check);
          r();
        }
      }, 100);
    });
    await Bun.sleep(200);

    // The late chain's parent result should still be available
    // (it was just added, not yet evicted)
    const lateResult = targetResults[1];
    expect(lateResult.parentResult).toBeDefined();

    await worker.close();
    flow.close();
    queue.close();
  }, 60000);

  test('getChildrenValues returns empty when children results are evicted', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('issue34-children-evict', { embedded: true });
    queue.obliterate();

    let parentChildrenValues: Record<string, unknown> = {};
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    let fillerCompleted = 0;
    const FILLER_COUNT = 5100;

    const worker = new Worker(
      'issue34-children-evict',
      async (job) => {
        const data = job.data as { role: string; value?: number; filler?: boolean };

        if (data.filler) {
          fillerCompleted++;
          return { filler: true };
        }

        if (data.role === 'child') {
          return { childValue: (data.value ?? 0) * 10 };
        }

        if (data.role === 'parent') {
          // Wait for fillers to complete (evict children results)
          // Push fillers from here to ensure children completed first
          for (let batch = 0; batch < FILLER_COUNT / 50; batch++) {
            const jobs = Array.from({ length: 50 }, (_, i) => ({
              name: 'filler',
              data: { filler: true, role: 'filler' },
            }));
            await queue.addBulk(jobs);
          }

          // Wait for fillers to complete
          while (fillerCompleted < FILLER_COUNT) {
            await Bun.sleep(100);
          }
          await Bun.sleep(500);

          // Now try to get children values - they may have been evicted
          parentChildrenValues = await queue.getChildrenValues(job.id);
          resolve!();
          return { summary: 'done' };
        }

        return {};
      },
      { embedded: true, concurrency: 50 }
    );

    // Create flow: parent waits for 2 children
    await flow.add({
      name: 'parent',
      queueName: 'issue34-children-evict',
      data: { role: 'parent', value: 0 },
      children: [
        {
          name: 'child-a',
          queueName: 'issue34-children-evict',
          data: { role: 'child', value: 1 },
        },
        {
          name: 'child-b',
          queueName: 'issue34-children-evict',
          data: { role: 'child', value: 2 },
        },
      ],
    });

    await done;
    await Bun.sleep(200);

    // BUG: children results were evicted from BoundedMap by filler jobs
    // Without storage fallback, getChildrenValues returns empty/partial
    const childrenCount = Object.keys(parentChildrenValues).length;
    expect(childrenCount).toBe(2);

    // Verify actual values
    const values = Object.values(parentChildrenValues).map((v: any) => v.childValue).sort();
    expect(values).toEqual([10, 20]);

    await worker.close();
    flow.close();
    queue.close();
  }, 120000);
});
