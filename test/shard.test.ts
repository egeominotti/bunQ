/**
 * Shard Tests
 */

import { describe, test, expect } from 'bun:test';
import { Shard } from '../src/domain/queue/shard';
import { createJob, jobId } from '../src/domain/types/job';

describe('Shard', () => {
  function makeJob(id: number, queue = 'test') {
    return createJob(jobId(`test-job-${id}`), queue, { data: { id } }, Date.now());
  }

  test('should create and get queue', () => {
    const shard = new Shard();
    const queue = shard.getQueue('emails');

    expect(queue).toBeDefined();
    expect(queue.isEmpty).toBe(true);
  });

  test('should track queue state', () => {
    const shard = new Shard();
    const state = shard.getState('emails');

    expect(state.paused).toBe(false);
    expect(state.rateLimit).toBeNull();
  });

  test('should pause and resume queue', () => {
    const shard = new Shard();

    shard.pause('emails');
    expect(shard.isPaused('emails')).toBe(true);

    shard.resume('emails');
    expect(shard.isPaused('emails')).toBe(false);
  });

  test('should manage unique keys', () => {
    const shard = new Shard();

    expect(shard.isUniqueAvailable('emails', 'key1')).toBe(true);

    shard.registerUniqueKey('emails', 'key1', jobId('test-job-1'));
    expect(shard.isUniqueAvailable('emails', 'key1')).toBe(false);

    shard.releaseUniqueKey('emails', 'key1');
    expect(shard.isUniqueAvailable('emails', 'key1')).toBe(true);
  });

  test('should manage FIFO groups', () => {
    const shard = new Shard();

    expect(shard.isGroupActive('emails', 'group1')).toBe(false);

    shard.activateGroup('emails', 'group1');
    expect(shard.isGroupActive('emails', 'group1')).toBe(true);

    shard.releaseGroup('emails', 'group1');
    expect(shard.isGroupActive('emails', 'group1')).toBe(false);
  });

  test('should manage DLQ', () => {
    const shard = new Shard();
    const job = makeJob(1, 'emails');

    shard.addToDlq(job);
    expect(shard.getDlqCount('emails')).toBe(1);

    const dlqJobs = shard.getDlq('emails');
    expect(dlqJobs.length).toBe(1);
    expect(dlqJobs[0].id).toBe(job.id);

    // removeFromDlq now returns DlqEntry, not Job
    const removed = shard.removeFromDlq('emails', job.id);
    expect(removed?.job.id).toBe(job.id);
    expect(shard.getDlqCount('emails')).toBe(0);
  });

  test('should clear DLQ', () => {
    const shard = new Shard();
    shard.addToDlq(makeJob(1, 'emails'));
    shard.addToDlq(makeJob(2, 'emails'));

    const count = shard.clearDlq('emails');
    expect(count).toBe(2);
    expect(shard.getDlqCount('emails')).toBe(0);
  });

  test('should drain queue', () => {
    const shard = new Shard();
    const queue = shard.getQueue('emails');
    queue.push(makeJob(1, 'emails'));
    queue.push(makeJob(2, 'emails'));

    const result = shard.drain('emails');
    expect(result.count).toBe(2);
    expect(result.jobIds.length).toBe(2);
    expect(queue.isEmpty).toBe(true);
  });

  test('should obliterate queue completely', () => {
    const shard = new Shard();
    shard.getQueue('emails').push(makeJob(1, 'emails'));
    shard.addToDlq(makeJob(2, 'emails'));
    shard.registerUniqueKey('emails', 'key1', jobId('test-job-3'));
    shard.pause('emails');

    shard.obliterate('emails');

    expect(shard.queues.has('emails')).toBe(false);
    expect(shard.dlq.has('emails')).toBe(false);
    expect(shard.isPaused('emails')).toBe(false);
  });

  test('should get queue names', () => {
    const shard = new Shard();
    shard.getQueue('emails');
    shard.getQueue('notifications');
    shard.addToDlq(makeJob(1, 'reports'));

    const names = shard.getQueueNames();
    expect(names).toContain('emails');
    expect(names).toContain('notifications');
    expect(names).toContain('reports');
  });

  test('should release job resources', () => {
    const shard = new Shard();
    shard.registerUniqueKey('emails', 'key1', jobId('test-job-4'));
    shard.activateGroup('emails', 'group1');

    shard.releaseJobResources('emails', 'key1', 'group1');

    expect(shard.isUniqueAvailable('emails', 'key1')).toBe(true);
    expect(shard.isGroupActive('emails', 'group1')).toBe(false);
  });

  describe('running stats', () => {
    test('should track queued jobs count', () => {
      const shard = new Shard();
      const job1 = makeJob(1, 'emails');
      const job2 = makeJob(2, 'emails');

      shard.incrementQueued(job1.id, false);
      shard.incrementQueued(job2.id, false);

      expect(shard.getStats().queuedJobs).toBe(2);

      shard.decrementQueued(job1.id);
      expect(shard.getStats().queuedJobs).toBe(1);
    });

    test('should track delayed jobs separately', () => {
      const shard = new Shard();
      const job1 = makeJob(1, 'emails');
      const job2 = makeJob(2, 'emails');

      shard.incrementQueued(job1.id, false); // Not delayed
      shard.incrementQueued(job2.id, true); // Delayed

      const stats = shard.getStats();
      expect(stats.queuedJobs).toBe(2);
      expect(stats.delayedJobs).toBe(1);
    });

    test('should track DLQ count in stats', () => {
      const shard = new Shard();
      const job = makeJob(1, 'emails');

      shard.addToDlq(job);

      expect(shard.getStats().dlqJobs).toBe(1);
    });

    test('should handle decrement for already removed job', () => {
      const shard = new Shard();
      const job = makeJob(1, 'emails');

      // Decrement without incrementing first
      shard.decrementQueued(job.id);

      // Should not go negative
      expect(shard.getStats().queuedJobs).toBe(0);
    });
  });

  describe('dependency index', () => {
    test('should register dependencies', () => {
      const shard = new Shard();
      const parentId = jobId('parent-1');
      const childId = jobId('child-1');

      shard.registerDependencies(childId, [parentId]);

      const waiting = shard.getJobsWaitingFor(parentId);
      expect(waiting).toBeDefined();
      expect(waiting!.has(childId)).toBe(true);
    });

    test('should unregister dependencies', () => {
      const shard = new Shard();
      const parentId = jobId('parent-1');
      const childId = jobId('child-1');

      shard.registerDependencies(childId, [parentId]);
      shard.unregisterDependencies(childId, [parentId]);

      const waiting = shard.getJobsWaitingFor(parentId);
      expect(waiting?.has(childId)).toBeFalsy();
    });

    test('should track multiple children per parent', () => {
      const shard = new Shard();
      const parentId = jobId('parent-1');
      const child1 = jobId('child-1');
      const child2 = jobId('child-2');
      const child3 = jobId('child-3');

      shard.registerDependencies(child1, [parentId]);
      shard.registerDependencies(child2, [parentId]);
      shard.registerDependencies(child3, [parentId]);

      const waiting = shard.getJobsWaitingFor(parentId);
      expect(waiting?.size).toBe(3);
    });

    test('should return undefined for unknown parent', () => {
      const shard = new Shard();
      const waiting = shard.getJobsWaitingFor(jobId('unknown'));
      expect(waiting).toBeUndefined();
    });
  });

  describe('temporal index', () => {
    test('should track jobs by creation time', () => {
      const shard = new Shard();
      const now = Date.now();

      const job1 = makeJob(1, 'emails');
      const job2 = makeJob(2, 'emails');

      // Increment with timestamps
      shard.incrementQueued(job1.id, false, now - 10000, 'emails');
      shard.incrementQueued(job2.id, false, now - 5000, 'emails');

      // Get jobs older than 3 seconds
      const oldJobs = shard.getOldJobs('emails', 3000, 10);

      expect(oldJobs.length).toBe(2);
    });

    test('should limit returned jobs', () => {
      const shard = new Shard();
      const now = Date.now();

      for (let i = 0; i < 10; i++) {
        const job = makeJob(i, 'emails');
        shard.incrementQueued(job.id, false, now - 10000, 'emails');
      }

      const oldJobs = shard.getOldJobs('emails', 1000, 3);
      expect(oldJobs.length).toBe(3);
    });

    test('should filter by queue', () => {
      const shard = new Shard();
      const now = Date.now();

      shard.incrementQueued(jobId('job-1'), false, now - 10000, 'emails');
      shard.incrementQueued(jobId('job-2'), false, now - 10000, 'notifications');

      const emailJobs = shard.getOldJobs('emails', 1000, 10);
      expect(emailJobs.length).toBe(1);
    });
  });
});
