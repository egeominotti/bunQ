/**
 * AUDIT BUG H8 — flow failure maps memory leak
 * (src/application/queueManager.ts:86-89)
 *
 * The maps `failedChildrenValues` and `ignoredChildrenFailures` are populated
 * when child jobs fail with continueParentOnFailure / ignoreDependencyOnFailure
 * (queueManager.ts ~1352-1354 and ~1417-1419), but they are NEVER cleaned up
 * when the parent job completes normally. The AckContext / onJobCompleted path
 * does not touch them. They are only deleted in obliterate() (lines 780-781)
 * and are even OMITTED from shutdown()'s clear() block (1683-1702).
 *
 * Result: unbounded memory growth proportional to the number of parent-job
 * completions that involved a failed child with these flags.
 *
 * These tests assert the CORRECT behavior:
 *   1. After a parent flow fully completes, the entry for that parent must NOT
 *      be retained in failedChildrenValues / ignoredChildrenFailures.
 *   2. shutdown() must clear both maps.
 *
 * With the current buggy code, (1) and (2) FAIL → RED.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, FlowProducer, shutdownManager } from '../src/client';
import { getSharedManager } from '../src/client/manager';
import { jobId } from '../src/domain/types/job';

// Wait up to `ms` for a predicate to become true.
async function waitFor(predicate: () => Promise<boolean> | boolean, ms = 6000): Promise<void> {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error(`waitFor timed out after ${ms}ms`);
}

// Direct access to the private maps under test.
function mapSizes(): { failed: number; ignored: number } {
  const qm = getSharedManager() as unknown as {
    failedChildrenValues: Map<unknown, unknown>;
    ignoredChildrenFailures: Map<unknown, unknown>;
  };
  return {
    failed: qm.failedChildrenValues.size,
    ignored: qm.ignoredChildrenFailures.size,
  };
}

describe('AUDIT H8 - flow failure maps leak on parent completion', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('continueParentOnFailure: failedChildrenValues entry is removed after parent completes', async () => {
    const queueName = 'h8-continue-parent';
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue(queueName, { embedded: true });
    queue.obliterate();

    // Baseline: nothing tracked yet.
    expect(mapSizes().failed).toBe(0);

    const worker = new Worker(
      queueName,
      async (job) => {
        const data = job.data as { role: string };
        if (data.role === 'child') {
          throw new Error('child boom');
        }
        return { parentDone: true };
      },
      { embedded: true, concurrency: 5 }
    );

    const result = await flow.add({
      name: 'parent',
      queueName,
      data: { role: 'parent' },
      children: [
        {
          name: 'child',
          queueName,
          data: { role: 'child' },
          opts: { continueParentOnFailure: true, attempts: 1 },
        },
      ],
    });

    const parentJobId = result.job.id;
    const childJobId = result.children?.[0]?.job?.id;
    expect(parentJobId).toBeDefined();
    expect(childJobId).toBeDefined();

    // Wait for the parent to actually complete.
    await waitFor(async () => (await queue.getJobState(parentJobId!)) === 'completed');

    // Sanity: the failure WAS recorded for this parent at some point.
    const manager = getSharedManager();
    const recorded = await manager.getFailedChildrenValues(jobId(parentJobId!));
    // (This is just to show the map is the right one — it holds the failure.)
    const recordedKeys = Object.keys(recorded);
    // It either still holds the entry (buggy) — that's exactly the leak.

    const sizes = mapSizes();

    // CORRECT BEHAVIOR: once the parent has fully completed, its tracking entry
    // must be released. A finished parent should not pin failure metadata forever.
    expect(sizes.failed).toBe(0);
    // And the per-parent record must be gone too.
    expect(recordedKeys.length).toBe(0);

    await worker.close();
  });

  test('ignoreDependencyOnFailure: ignoredChildrenFailures entry is removed after parent completes', async () => {
    const queueName = 'h8-ignore-dep';
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue(queueName, { embedded: true });
    queue.obliterate();

    expect(mapSizes().ignored).toBe(0);

    let parentJobId: string | undefined;

    const worker = new Worker(
      queueName,
      async (job) => {
        const data = job.data as { role: string };
        if (data.role === 'child') {
          throw new Error('ignored boom');
        }
        return { parentDone: true };
      },
      { embedded: true, concurrency: 5 }
    );

    const result = await flow.add({
      name: 'parent',
      queueName,
      data: { role: 'parent' },
      children: [
        {
          name: 'child',
          queueName,
          data: { role: 'child' },
          opts: { ignoreDependencyOnFailure: true, attempts: 1 },
        },
      ],
    });

    parentJobId = result.job.id;
    expect(parentJobId).toBeDefined();

    await waitFor(async () => (await queue.getJobState(parentJobId!)) === 'completed');

    const sizes = mapSizes();

    // CORRECT BEHAVIOR: completed parent must not pin ignored-failure metadata.
    expect(sizes.ignored).toBe(0);

    await worker.close();
  });

  test('shutdown() clears failedChildrenValues and ignoredChildrenFailures', async () => {
    const queueName = 'h8-shutdown-clear';
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue(queueName, { embedded: true });
    queue.obliterate();

    // Parent A → continueParentOnFailure (populates failedChildrenValues)
    const resA = await flow.add({
      name: 'parentA',
      queueName,
      data: { role: 'parent' },
      children: [
        {
          name: 'childA',
          queueName,
          data: { role: 'child' },
          opts: { continueParentOnFailure: true, attempts: 1 },
        },
      ],
    });

    // Parent B → ignoreDependencyOnFailure (populates ignoredChildrenFailures)
    const resB = await flow.add({
      name: 'parentB',
      queueName,
      data: { role: 'parent' },
      children: [
        {
          name: 'childB',
          queueName,
          data: { role: 'child' },
          opts: { ignoreDependencyOnFailure: true, attempts: 1 },
        },
      ],
    });

    // Drive only the children to terminal failure. No worker consumes the
    // promoted parents, so both records deterministically remain live until
    // shutdown rather than racing parent completion.
    const manager = getSharedManager();
    const expectedChildren = new Set([resA.children![0].job.id, resB.children![0].job.id]);
    for (let index = 0; index < 2; index++) {
      const child = await manager.pull(queueName);
      expect(child).not.toBeNull();
      expect(expectedChildren.delete(String(child!.id))).toBe(true);
      await manager.fail(child!.id, 'shutdown-child boom', undefined, true);
    }
    expect(expectedChildren.size).toBe(0);

    const before = mapSizes();
    expect(before.failed).toBeGreaterThan(0);
    expect(before.ignored).toBeGreaterThan(0);

    // Capture the manager BEFORE shutdown (shutdownManager nulls the singleton).
    const qm = manager as unknown as {
      failedChildrenValues: Map<unknown, unknown>;
      ignoredChildrenFailures: Map<unknown, unknown>;
      shutdown: () => void;
    };

    // CORRECT BEHAVIOR: shutdown() must clear these maps like every other
    // in-memory collection it clears (jobIndex, completedJobs, jobLocks, ...).
    qm.shutdown();

    expect(qm.failedChildrenValues.size).toBe(0);
    expect(qm.ignoredChildrenFailures.size).toBe(0);
  });
});
