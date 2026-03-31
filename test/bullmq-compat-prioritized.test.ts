/**
 * BullMQ v5 Compatibility: "prioritized" state
 * In BullMQ v5, jobs with priority > 0 that are waiting have state "prioritized", not "waiting".
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, shutdownManager } from '../src/client';

describe('BullMQ v5 "prioritized" state compatibility', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('prioritized-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  test('job with priority=0 should have state "waiting"', async () => {
    const job = await queue.add('low', { value: 1 }, { priority: 0 });
    const state = await queue.getJobState(job.id);
    expect(state).toBe('waiting');
  });

  test('job with priority>0 should have state "prioritized"', async () => {
    const job = await queue.add('high', { value: 2 }, { priority: 5 });
    const state = await queue.getJobState(job.id);
    expect(state).toBe('prioritized');
  });

  test('getJobCounts returns separate waiting and prioritized counts', async () => {
    // Add 2 normal jobs (priority=0)
    await queue.add('normal1', { value: 1 });
    await queue.add('normal2', { value: 2 });

    // Add 3 prioritized jobs (priority>0)
    await queue.add('high1', { value: 3 }, { priority: 1 });
    await queue.add('high2', { value: 4 }, { priority: 5 });
    await queue.add('high3', { value: 5 }, { priority: 10 });

    const counts = queue.getJobCounts();
    expect(counts.waiting).toBe(2);
    expect(counts.prioritized).toBe(3);
  });

  test('job with no explicit priority (default=0) should have state "waiting"', async () => {
    const job = await queue.add('default', { value: 10 });
    const state = await queue.getJobState(job.id);
    expect(state).toBe('waiting');
  });

  test('delayed job should still have state "delayed" regardless of priority', async () => {
    const job = await queue.add('delayed-high', { value: 20 }, { priority: 5, delay: 60000 });
    const state = await queue.getJobState(job.id);
    expect(state).toBe('delayed');
  });
});
