/**
 * Issue #55: Support multiple statuses in getJobs
 *
 * getJobs should accept a single state string OR an array of states,
 * matching BullMQ's getJobs(types?: JobType | JobType[]) interface.
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { Queue } from '../src/client/queue/queue';
import { getSharedManager } from '../src/client/manager';

describe('Issue #55: getJobs should support multiple statuses', () => {
  const QUEUE_NAME = 'test-issue55';

  beforeEach(() => {
    const manager = getSharedManager();
    manager.drain(QUEUE_NAME);
  });

  test('getJobs accepts array of states: waiting + delayed', async () => {
    const queue = new Queue<{ msg: string }>(QUEUE_NAME, { embedded: true });

    await queue.add('immediate', { msg: 'now' });
    await queue.add('delayed', { msg: 'later' }, { delay: 60000 });

    // Single states work
    const waiting = queue.getJobs({ state: 'waiting' });
    const delayed = queue.getJobs({ state: 'delayed' });
    expect(waiting.length).toBe(1);
    expect(delayed.length).toBe(1);

    // Array of states should return both
    const both = queue.getJobs({ state: ['waiting', 'delayed'] as any });
    expect(both.length).toBe(2);
  });

  test('getJobs with single state string still works (backwards compatible)', async () => {
    const queue = new Queue<{ msg: string }>(QUEUE_NAME, { embedded: true });

    await queue.add('job1', { msg: 'test1' });
    await queue.add('job2', { msg: 'test2' });

    const jobs = queue.getJobs({ state: 'waiting' });
    expect(jobs.length).toBe(2);
  });

  test('getJobsAsync accepts array of states', async () => {
    const queue = new Queue<{ msg: string }>(QUEUE_NAME, { embedded: true });

    await queue.add('immediate', { msg: 'now' });
    await queue.add('delayed', { msg: 'later' }, { delay: 60000 });

    const jobs = await queue.getJobsAsync({ state: ['waiting', 'delayed'] as any });
    expect(jobs.length).toBe(2);
  });

  test('getJobs with empty array returns all jobs (like no state)', async () => {
    const queue = new Queue<{ msg: string }>(QUEUE_NAME, { embedded: true });

    await queue.add('immediate', { msg: 'now' });
    await queue.add('delayed', { msg: 'later' }, { delay: 60000 });

    const noFilter = queue.getJobs();
    const emptyArray = queue.getJobs({ state: [] as any });
    expect(emptyArray.length).toBe(noFilter.length);
  });

  test('server-side getJobs supports array of states', async () => {
    const manager = getSharedManager();

    await manager.push(QUEUE_NAME, { data: { msg: 'now' } });
    await manager.push(QUEUE_NAME, { data: { msg: 'later' }, delay: 60000 });

    // Single state
    const waiting = manager.getJobs(QUEUE_NAME, { state: 'waiting' });
    const delayed = manager.getJobs(QUEUE_NAME, { state: 'delayed' });
    expect(waiting.length).toBe(1);
    expect(delayed.length).toBe(1);

    // Array of states
    const both = manager.getJobs(QUEUE_NAME, { state: ['waiting', 'delayed'] as any });
    expect(both.length).toBe(2);
  });
});
