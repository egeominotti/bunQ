/**
 * Issue #45: job.getState() returns 'unknown' inside 'active' event
 * Reproduction test - verifies that getState() returns 'active' during the active event
 */

import { describe, it, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Issue #45: job.getState() inside active event', () => {
  afterEach(() => {
    shutdownManager();
  });

  it('should return "active" from job.getState() inside active event', async () => {
    const queue = new Queue<{ value: number }>('issue-45-test');
    const stateResults: string[] = [];

    const worker = new Worker<{ value: number }>(
      'issue-45-test',
      async (job) => {
        await Bun.sleep(50);
        return { done: true };
      },
      { embedded: true }
    );

    worker.on('active', async (job) => {
      const state = await job.getState();
      stateResults.push(state);
    });

    await queue.add('task', { value: 1 });

    await Bun.sleep(300);

    expect(stateResults.length).toBe(1);
    expect(stateResults[0]).toBe('active');

    await worker.close();
    queue.close();
  });
});
