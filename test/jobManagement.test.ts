/**
 * Job Management Tests
 * Cancel, progress, update, priority, promote, discard
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('Job Management', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  describe('cancel', () => {
    test('should cancel waiting job', async () => {
      const job = await qm.push('test-queue', { data: { msg: 'cancel me' } });

      const cancelled = await qm.cancel(job.id);
      expect(cancelled).toBe(true);

      // Job should no longer exist
      const found = await qm.getJob(job.id);
      expect(found).toBeNull();
    });

    test('should return false for non-existent job', async () => {
      const { jobId } = await import('../src/domain/types/job');
      const cancelled = await qm.cancel(jobId('non-existent'));
      expect(cancelled).toBe(false);
    });

    test('should release unique key on cancel', async () => {
      const uniqueJob = await qm.push('test-queue', {
        data: { msg: 'first' },
        uniqueKey: 'unique1',
      });

      await qm.cancel(uniqueJob.id);

      // Should be able to push with same unique key now
      const newJob = await qm.push('test-queue', {
        data: { msg: 'second' },
        uniqueKey: 'unique1',
      });
      expect(newJob).toBeDefined();
    });
  });

  describe('updateProgress', () => {
    test('should update job progress', async () => {
      await qm.push('test-queue', { data: { msg: 'test' } });
      const pulled = await qm.pull('test-queue');

      const updated = await qm.updateProgress(pulled!.id, 50);
      expect(updated).toBe(true);

      const progress = qm.getProgress(pulled!.id);
      expect(progress?.progress).toBe(50);
    });

    test('should update progress with message', async () => {
      await qm.push('test-queue', { data: { msg: 'test' } });
      const pulled = await qm.pull('test-queue');

      await qm.updateProgress(pulled!.id, 75, 'Processing step 3');

      const progress = qm.getProgress(pulled!.id);
      expect(progress?.progress).toBe(75);
      expect(progress?.message).toBe('Processing step 3');
    });

    test('should clamp progress to 0-100', async () => {
      await qm.push('test-queue', { data: {} });
      const pulled = await qm.pull('test-queue');

      await qm.updateProgress(pulled!.id, 150);
      expect(qm.getProgress(pulled!.id)?.progress).toBe(100);

      await qm.updateProgress(pulled!.id, -10);
      expect(qm.getProgress(pulled!.id)?.progress).toBe(0);
    });

    test('should return false for non-active job', async () => {
      const job = await qm.push('test-queue', { data: {} });
      // Job is waiting, not active
      const updated = await qm.updateProgress(job.id, 50);
      expect(updated).toBe(false);
    });
  });

  describe('updateJobData', () => {
    test('should update waiting job data', async () => {
      const job = await qm.push('test-queue', { data: { original: true } });

      const updated = await qm.updateJobData(job.id, { modified: true });
      expect(updated).toBe(true);

      const found = await qm.getJob(job.id);
      expect(found?.data).toEqual({ modified: true });
    });

    test('should update active job data', async () => {
      await qm.push('test-queue', { data: { original: true } });
      const pulled = await qm.pull('test-queue');

      const updated = await qm.updateJobData(pulled!.id, { modified: true });
      expect(updated).toBe(true);
    });

    test('should return false for non-existent job', async () => {
      const { jobId } = await import('../src/domain/types/job');
      const updated = await qm.updateJobData(jobId('non-existent'), { data: 'new' });
      expect(updated).toBe(false);
    });
  });

  describe('changePriority', () => {
    test('should change job priority', async () => {
      const job = await qm.push('test-queue', { data: {}, priority: 5 });

      const changed = await qm.changePriority(job.id, 10);
      expect(changed).toBe(true);

      const found = await qm.getJob(job.id);
      expect(found?.priority).toBe(10);
    });

    test('should affect pull order', async () => {
      const low = await qm.push('test-queue', { data: { type: 'low' }, priority: 1 });
      await qm.push('test-queue', { data: { type: 'high' }, priority: 10 });

      // Change low priority to highest
      await qm.changePriority(low.id, 100);

      const first = await qm.pull('test-queue');
      expect((first?.data as { type: string }).type).toBe('low');
    });

    test('should return false for active job', async () => {
      await qm.push('test-queue', { data: {} });
      const pulled = await qm.pull('test-queue');

      const changed = await qm.changePriority(pulled!.id, 10);
      expect(changed).toBe(false);
    });
  });

  describe('promote', () => {
    test('should promote delayed job to immediate', async () => {
      const job = await qm.push('test-queue', {
        data: { msg: 'delayed' },
        delay: 60000, // 1 minute delay
      });

      // Should not be pullable yet
      let pulled = await qm.pull('test-queue', 0);
      expect(pulled).toBeNull();

      // Promote it
      const promoted = await qm.promote(job.id);
      expect(promoted).toBe(true);

      // Should be pullable now
      pulled = await qm.pull('test-queue');
      expect(pulled).not.toBeNull();
    });

    test('should return false for non-delayed job', async () => {
      const job = await qm.push('test-queue', { data: {} });

      const promoted = await qm.promote(job.id);
      expect(promoted).toBe(false);
    });
  });

  describe('moveToDelayed', () => {
    test('should move active job back to delayed', async () => {
      await qm.push('test-queue', { data: { msg: 'test' } });
      const pulled = await qm.pull('test-queue');

      const moved = await qm.moveToDelayed(pulled!.id, 5000);
      expect(moved).toBe(true);

      // Job should not be pullable
      const next = await qm.pull('test-queue', 0);
      expect(next).toBeNull();

      // Stats should show delayed
      const stats = qm.getStats();
      expect(stats.active).toBe(0);
      expect(stats.delayed).toBe(1);
    });

    test('should return false for non-active job', async () => {
      const job = await qm.push('test-queue', { data: {} });

      const moved = await qm.moveToDelayed(job.id, 5000);
      expect(moved).toBe(false);
    });
  });

  describe('discard', () => {
    test('should move waiting job to DLQ', async () => {
      const job = await qm.push('test-queue', { data: { msg: 'discard me' } });

      const discarded = await qm.discard(job.id);
      expect(discarded).toBe(true);

      // Should be in DLQ
      const dlq = qm.getDlq('test-queue');
      expect(dlq.length).toBe(1);
      expect(dlq[0].data).toEqual({ msg: 'discard me' });
    });

    test('should move active job to DLQ', async () => {
      await qm.push('test-queue', { data: { msg: 'discard me' } });
      const pulled = await qm.pull('test-queue');

      const discarded = await qm.discard(pulled!.id);
      expect(discarded).toBe(true);

      const dlq = qm.getDlq('test-queue');
      expect(dlq.length).toBe(1);
    });

    test('should return false for non-existent job', async () => {
      const { jobId } = await import('../src/domain/types/job');
      const discarded = await qm.discard(jobId('non-existent'));
      expect(discarded).toBe(false);
    });
  });

  describe('getJobByCustomId', () => {
    test('should find job by custom ID', async () => {
      await qm.push('test-queue', {
        data: { msg: 'test' },
        customId: 'my-custom-id',
      });

      const job = qm.getJobByCustomId('my-custom-id');
      expect(job).not.toBeNull();
      expect(job?.data).toEqual({ msg: 'test' });
    });

    test('should return null for non-existent custom ID', () => {
      const job = qm.getJobByCustomId('non-existent');
      expect(job).toBeNull();
    });
  });
});
