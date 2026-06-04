/**
 * Issue #56: getJobCounts doesn't return delayed or paused counts
 *
 * The JobCounts type should include delayed and paused fields,
 * matching BullMQ's getJobCounts() return type.
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { Queue } from '../src/client/queue/queue';
import { getSharedManager } from '../src/client/manager';

describe('Issue #56: getJobCounts should include delayed and paused', () => {
  const QUEUE_NAME = 'test-issue56';

  beforeEach(() => {
    const manager = getSharedManager();
    manager.drain(QUEUE_NAME);
  });

  test('getJobCounts returns delayed count', async () => {
    const queue = new Queue(QUEUE_NAME, { embedded: true });

    await queue.add('immediate', { msg: 'now' });
    await queue.add('delayed1', { msg: 'later' }, { delay: 60000 });
    await queue.add('delayed2', { msg: 'later2' }, { delay: 120000 });

    const counts = queue.getJobCounts();

    expect(counts.waiting).toBe(1);
    // Issue #56: delayed is missing from JobCounts
    expect((counts as any).delayed).toBe(2);
  });

  test('getJobCounts returns paused count (0 when not paused)', async () => {
    const queue = new Queue(QUEUE_NAME, { embedded: true });

    await queue.add('job1', { msg: 'test' });

    const counts = queue.getJobCounts();

    // paused should be 0 when queue is not paused
    expect((counts as any).paused).toBe(0);
  });

  test('getJobCounts returns paused count matching waiting when paused', async () => {
    const queue = new Queue(QUEUE_NAME, { embedded: true });

    await queue.add('job1', { msg: 'test1' });
    await queue.add('job2', { msg: 'test2' });

    queue.pause();
    const counts = queue.getJobCounts();

    // BullMQ semantics (#92): when paused, ready jobs are reported under `paused`
    // and `waiting` drops to 0 (a job is never counted in both at once).
    expect((counts as any).paused).toBe(2);
    expect(counts.waiting).toBe(0);

    queue.resume();
  });

  test('getJobCountsAsync returns delayed count', async () => {
    const queue = new Queue(QUEUE_NAME, { embedded: true });

    await queue.add('immediate', { msg: 'now' });
    await queue.add('delayed1', { msg: 'later' }, { delay: 60000 });

    const counts = await queue.getJobCountsAsync();

    expect(counts.waiting).toBe(1);
    expect((counts as any).delayed).toBe(1);
  });

  test('getJobCountsAsync returns paused count', async () => {
    const queue = new Queue(QUEUE_NAME, { embedded: true });

    await queue.add('job1', { msg: 'test' });

    const counts = await queue.getJobCountsAsync();

    expect((counts as any).paused).toBe(0);
  });
});
