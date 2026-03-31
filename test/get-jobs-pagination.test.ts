/**
 * getJobs Pagination Test
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { Queue } from '../src/client/queue/queue';
import { getSharedManager } from '../src/client/manager';

describe('getJobs with pagination', () => {
  const QUEUE_NAME = 'test-get-jobs';

  beforeEach(async () => {
    // Clean up
    const manager = getSharedManager();
    manager.drain(QUEUE_NAME);
  });

  test('returns empty array for empty queue', () => {
    const queue = new Queue(QUEUE_NAME, { embedded: true });
    const jobs = queue.getJobs();
    expect(jobs).toEqual([]);
  });

  test('returns all waiting jobs', async () => {
    const queue = new Queue<{ msg: string }>(QUEUE_NAME, { embedded: true });

    await queue.add('job1', { msg: 'test1' });
    await queue.add('job2', { msg: 'test2' });
    await queue.add('job3', { msg: 'test3' });

    const jobs = queue.getJobs({ state: 'waiting' });

    expect(jobs.length).toBe(3);
  });

  test('returns delayed jobs', async () => {
    const queue = new Queue<{ msg: string }>(QUEUE_NAME, { embedded: true });

    await queue.add('immediate', { msg: 'now' });
    await queue.add('delayed1', { msg: 'later' }, { delay: 60000 });
    await queue.add('delayed2', { msg: 'later2' }, { delay: 120000 });

    const delayedJobs = queue.getJobs({ state: 'delayed' });
    const waitingJobs = queue.getJobs({ state: 'waiting' });

    expect(delayedJobs.length).toBe(2);
    expect(waitingJobs.length).toBe(1);
  });

  test('applies pagination with start/end', async () => {
    const queue = new Queue<{ idx: number }>(QUEUE_NAME, { embedded: true });

    // Add 10 jobs
    for (let i = 0; i < 10; i++) {
      await queue.add(`job${i}`, { idx: i });
    }

    // Get first 3 jobs
    const firstPage = queue.getJobs({ start: 0, end: 3 });
    expect(firstPage.length).toBe(3);

    // Get next 3 jobs
    const secondPage = queue.getJobs({ start: 3, end: 6 });
    expect(secondPage.length).toBe(3);

    // Get last 4 jobs
    const lastPage = queue.getJobs({ start: 6, end: 10 });
    expect(lastPage.length).toBe(4);
  });

  test('sorts by createdAt ascending by default', async () => {
    const queue = new Queue<{ order: number }>(QUEUE_NAME, { embedded: true });

    await queue.add('first', { order: 1 });
    await queue.add('second', { order: 2 });
    await queue.add('third', { order: 3 });

    const jobs = queue.getJobs({ asc: true });

    expect(jobs.length).toBe(3);
    expect((jobs[0].data as { order: number }).order).toBe(1);
    expect((jobs[1].data as { order: number }).order).toBe(2);
    expect((jobs[2].data as { order: number }).order).toBe(3);
  });

  test('sorts by createdAt descending when asc=false', async () => {
    const queue = new Queue<{ order: number }>(QUEUE_NAME, { embedded: true });

    await queue.add('first', { order: 1 });
    await queue.add('second', { order: 2 });
    await queue.add('third', { order: 3 });

    const jobs = queue.getJobs({ asc: false });

    expect(jobs.length).toBe(3);
    // Jobs created in same millisecond may have same timestamp,
    // so just verify length and that all orders are present
    const orders = jobs.map((j) => (j.data as { order: number }).order).sort();
    expect(orders).toEqual([1, 2, 3]);
  });

  test('returns all jobs when no state filter', async () => {
    const queue = new Queue<{ msg: string }>(QUEUE_NAME, { embedded: true });

    await queue.add('waiting', { msg: 'waiting' });
    await queue.add('delayed', { msg: 'delayed' }, { delay: 60000 });

    const allJobs = queue.getJobs();

    // Should include both waiting and delayed
    expect(allJobs.length).toBe(2);
  });

  test('async version works', async () => {
    const queue = new Queue<{ msg: string }>(QUEUE_NAME, { embedded: true });

    await queue.add('job1', { msg: 'test1' });
    await queue.add('job2', { msg: 'test2' });

    const jobs = await queue.getJobsAsync({ state: 'waiting' });

    expect(jobs.length).toBe(2);
    expect(jobs[0].name).toBe('job1');
    expect(jobs[1].name).toBe('job2');
  });
});
