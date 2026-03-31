/**
 * getCountsPerPriority Test
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { Queue } from '../src/client/queue/queue';
import { getSharedManager } from '../src/client/manager';

describe('getCountsPerPriority', () => {
  beforeEach(async () => {
    // Clean up
    const manager = getSharedManager();
    manager.drain('test-priority-counts');
  });

  test('returns empty object for empty queue', () => {
    const queue = new Queue('test-priority-counts', { embedded: true });
    const counts = queue.getCountsPerPriority();
    expect(counts).toEqual({});
  });

  test('counts jobs by priority', async () => {
    const queue = new Queue<{ msg: string }>('test-priority-counts', { embedded: true });

    // Add jobs with different priorities
    await queue.add('low', { msg: 'low priority' }, { priority: 1 });
    await queue.add('low', { msg: 'low priority 2' }, { priority: 1 });
    await queue.add('medium', { msg: 'medium priority' }, { priority: 5 });
    await queue.add('medium', { msg: 'medium priority 2' }, { priority: 5 });
    await queue.add('medium', { msg: 'medium priority 3' }, { priority: 5 });
    await queue.add('high', { msg: 'high priority' }, { priority: 10 });

    const counts = queue.getCountsPerPriority();

    expect(counts[1]).toBe(2);  // 2 low priority jobs
    expect(counts[5]).toBe(3);  // 3 medium priority jobs
    expect(counts[10]).toBe(1); // 1 high priority job
  });

  test('default priority is 0', async () => {
    const queue = new Queue<{ msg: string }>('test-priority-counts', { embedded: true });

    // Add jobs without specifying priority (default is 0)
    await queue.add('default1', { msg: 'default' });
    await queue.add('default2', { msg: 'default' });

    const counts = queue.getCountsPerPriority();

    expect(counts[0]).toBe(2); // 2 jobs with default priority 0
  });

  test('handles negative priorities', async () => {
    const queue = new Queue<{ msg: string }>('test-priority-counts', { embedded: true });

    await queue.add('negative', { msg: 'negative' }, { priority: -5 });
    await queue.add('positive', { msg: 'positive' }, { priority: 5 });
    await queue.add('zero', { msg: 'zero' }, { priority: 0 });

    const counts = queue.getCountsPerPriority();

    expect(counts[-5]).toBe(1);
    expect(counts[0]).toBe(1);
    expect(counts[5]).toBe(1);
  });

  test('works via async method', async () => {
    const queue = new Queue<{ msg: string }>('test-priority-counts', { embedded: true });

    await queue.add('job1', { msg: 'test' }, { priority: 3 });
    await queue.add('job2', { msg: 'test' }, { priority: 3 });

    const counts = await queue.getCountsPerPriorityAsync();

    expect(counts[3]).toBe(2);
  });
});
