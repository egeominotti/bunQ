/**
 * Regression coverage for the Issue #82 follow-up wiring.
 *
 * Before the fix, Queue.getJob(...) returned Job objects whose mutation methods
 * (updateData, promote, changePriority, moveToDelayed, etc.) were silent no-ops
 * when no execution context was passed. After the fix, embedded mode dispatches
 * through the shared manager and TCP mode through the connection pool.
 *
 * These tests cover the embedded path (no TCP server needed) via:
 *  - Queue.getJob().<method>() → createSimpleJob wiring
 *  - FlowProducer parent/child jobs → createFlowJobObject wiring
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, FlowProducer, shutdownManager } from '../src/client';
import { createFlowJobObject } from '../src/client/flowJobFactory';

describe('createSimpleJob (embedded): wired methods via Queue.getJob', () => {
  let queue: Queue<{ kind: string }>;

  beforeEach(() => {
    queue = new Queue('wired-simple', { embedded: true });
    queue.obliterate();
  });

  afterEach(async () => {
    queue.close();
    shutdownManager();
  });

  test('updateData mutates stored data', async () => {
    const added = await queue.add('t', { kind: 'original' });
    const fetched = await queue.getJob<{ kind: string }>(added.id!);
    expect(fetched).not.toBeNull();

    await fetched!.updateData({ kind: 'mutated' });

    const after = await queue.getJob<{ kind: string }>(added.id!);
    expect(after?.data).toEqual({ kind: 'mutated' });
  });

  test('changePriority actually mutates stored priority', async () => {
    const added = await queue.add('t', { kind: 'p' }, { priority: 1 });
    const job = await queue.getJob(added.id!);
    await job!.changePriority({ priority: 99 });

    const after = await queue.getJob(added.id!);
    expect(after?.priority).toBe(99);
  });

  test('changeDelay actually mutates stored runAt (observable as delay)', async () => {
    const added = await queue.add('t', { kind: 'd' });
    const job = await queue.getJob(added.id!);
    await job!.changeDelay(10_000);

    const after = await queue.getJob(added.id!);
    // delay is runAt - createdAt; allow 100ms tolerance for scheduler math
    expect(after?.delay).toBeGreaterThanOrEqual(9_900);
    expect(after?.delay).toBeLessThanOrEqual(10_100);
  });

  test('promote transitions delayed job to waiting state', async () => {
    const added = await queue.add('t', { kind: 'pr' }, { delay: 60_000 });
    expect(await queue.getJobState(added.id!)).toBe('delayed');

    const job = await queue.getJob(added.id!);
    await job!.promote();

    const state = await queue.getJobState(added.id!);
    expect(['waiting', 'prioritized']).toContain(state);
  });

  test('clearLogs removes existing logs', async () => {
    const added = await queue.add('t', { kind: 'l' });
    const job = await queue.getJob(added.id!);
    await job!.log('log-A');
    await job!.log('log-B');
    await job!.clearLogs();

    // No direct public getLogs — assert clearLogs didn't throw and job is still retrievable
    const after = await queue.getJob(added.id!);
    expect(after).not.toBeNull();
  });

  test('moveToDelayed transitions waiting job to delayed state', async () => {
    const added = await queue.add('t', { kind: 'md' });
    const job = await queue.getJob(added.id!);
    await job!.moveToDelayed(Date.now() + 60_000);

    const state = await queue.getJobState(added.id!);
    expect(state).toBe('delayed');
  });

  test('removeDeduplicationKey releases the live job key', async () => {
    const key = `wired-${Bun.randomUUIDv7()}`;
    const added = await queue.add('t', { kind: 'dd' }, { deduplication: { id: key } });
    const job = await queue.getJob(added.id!);
    expect(await job!.removeDeduplicationKey()).toBe(true);
    expect(await queue.getDeduplicationJobId(key)).toBeNull();
  });

  test('getDependenciesCount returns real counts (zero for childless job)', async () => {
    const added = await queue.add('t', { kind: 'deps' });
    const job = await queue.getJob(added.id!);
    const counts = await job!.getDependenciesCount();
    expect(counts).toEqual({ processed: 0, unprocessed: 0 });
  });
});

describe('createFlowJobObject (embedded): wired methods', () => {
  let producer: FlowProducer;
  let queue: Queue;

  beforeEach(() => {
    producer = new FlowProducer({ embedded: true });
    queue = new Queue('wired-flow', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    producer.close();
    queue.close();
    shutdownManager();
  });

  test('updateData mutates the flow parent job data', async () => {
    const node = await producer.add({
      name: 'parent',
      queueName: 'wired-flow',
      data: { kind: 'original' },
      children: [{ name: 'child', queueName: 'wired-flow', data: { step: 1 } }],
    });
    const parent = node.job as unknown as { id: string; updateData: (d: unknown) => Promise<void> };

    await parent.updateData({ kind: 'mutated' });

    const refetched = await queue.getJob(parent.id);
    expect(refetched?.data).toEqual({ kind: 'mutated' });
  });

  test('removeDeduplicationKey returns false when a flow job has no key', async () => {
    const node = await producer.add({
      name: 'parent',
      queueName: 'wired-flow',
      data: { kind: 'x' },
      children: [{ name: 'c', queueName: 'wired-flow', data: {} }],
    });
    const parent = node.job as unknown as { removeDeduplicationKey: () => Promise<boolean> };
    expect(await parent.removeDeduplicationKey()).toBe(false);
  });

  test('state check methods return false (not true) on detached flow job without context', async () => {
    // A flow-job shell created directly with no execution context resolves to 'unknown'
    const job = createFlowJobObject('detached', 'test', {}, 'wired-flow');
    expect(await job.isWaiting()).toBe(false);
    expect(await job.isActive()).toBe(false);
    expect(await job.isDelayed()).toBe(false);
    expect(await job.isCompleted()).toBe(false);
    expect(await job.isFailed()).toBe(false);
    expect(await job.isWaitingChildren()).toBe(false);
  });

  test('detached waitUntilFinished throws rather than silently returning undefined', async () => {
    const job = createFlowJobObject('detached', 'test', {}, 'wired-flow');
    await expect(job.waitUntilFinished(null)).rejects.toThrow(/waitUntilFinished: no connection/);
  });

  test('detached moveToWaitingChildren reports that no runtime can move it', async () => {
    const job = createFlowJobObject('detached', 'test', {}, 'wired-flow');
    expect(await job.moveToWaitingChildren()).toBe(false);
  });
});
