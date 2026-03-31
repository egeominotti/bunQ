/**
 * Job Dependencies Tests
 * Tests for job dependencies, parent-child relationships, and dependency chains
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

// Helper to wait for dependency resolution (runs on interval)
const waitForDepCheck = () => Bun.sleep(60);

describe('Job Dependencies', () => {
  let qm: QueueManager;

  beforeEach(() => {
    // Use short dependency check interval for faster tests
    qm = new QueueManager({ dependencyCheckMs: 50 });
  });

  afterEach(() => {
    qm.shutdown();
  });

  describe('dependsOn', () => {
    test('should not pull job with unmet dependencies', async () => {
      // Push job A
      const jobA = await qm.push('test', { data: { id: 'A' } });

      // Push job B that depends on A
      const jobB = await qm.push('test', {
        data: { id: 'B' },
        dependsOn: [jobA.id],
      });

      // Job B should not be pullable yet
      const pulled = await qm.pull('test', 0);
      expect(pulled?.id).toBe(jobA.id);

      // Pull job A again should return null (A is now active)
      const pulledAgain = await qm.pull('test', 0);
      expect(pulledAgain).toBeNull();

      // Acknowledge job A
      await qm.ack(jobA.id, { result: 'done' });

      // Wait for dependency resolution interval
      await waitForDepCheck();

      // Now job B should be pullable
      const pulledB = await qm.pull('test', 0);
      expect(pulledB?.id).toBe(jobB.id);
    });

    test('should handle multiple dependencies', async () => {
      const jobA = await qm.push('test', { data: { id: 'A' } });
      const jobB = await qm.push('test', { data: { id: 'B' } });

      // Job C depends on both A and B
      const jobC = await qm.push('test', {
        data: { id: 'C' },
        dependsOn: [jobA.id, jobB.id],
      });

      // Pull and ack job A
      const pulledA = await qm.pull('test', 0);
      expect(pulledA?.id).toBe(jobA.id);
      await qm.ack(jobA.id, { result: 'A done' });

      // Job C should still not be pullable (waiting on B)
      const pulledB = await qm.pull('test', 0);
      expect(pulledB?.id).toBe(jobB.id);

      // Pull C should fail (B not done)
      const tryC = await qm.pull('test', 0);
      expect(tryC).toBeNull();

      // Ack B
      await qm.ack(jobB.id, { result: 'B done' });

      // Wait for dependency resolution
      await waitForDepCheck();

      // Now C should be pullable
      const pulledC = await qm.pull('test', 0);
      expect(pulledC?.id).toBe(jobC.id);
    });

    test('should handle dependency chain', async () => {
      // A -> B -> C (chain)
      const jobA = await qm.push('test', { data: { id: 'A' } });
      const jobB = await qm.push('test', {
        data: { id: 'B' },
        dependsOn: [jobA.id],
      });
      const jobC = await qm.push('test', {
        data: { id: 'C' },
        dependsOn: [jobB.id],
      });

      // Should only be able to pull A initially
      const first = await qm.pull('test', 0);
      expect(first?.id).toBe(jobA.id);

      // Ack A, should unlock B
      await qm.ack(jobA.id, {});
      await waitForDepCheck();

      const second = await qm.pull('test', 0);
      expect(second?.id).toBe(jobB.id);

      // Ack B, should unlock C
      await qm.ack(jobB.id, {});
      await waitForDepCheck();

      const third = await qm.pull('test', 0);
      expect(third?.id).toBe(jobC.id);
    });

    test('should handle already completed dependency', async () => {
      // Push and complete job A
      const jobA = await qm.push('test', { data: { id: 'A' } });
      const pulled = await qm.pull('test', 0);
      await qm.ack(pulled!.id, {});

      // Push job B depending on already completed A
      const jobB = await qm.push('test', {
        data: { id: 'B' },
        dependsOn: [jobA.id],
      });

      // B should be immediately pullable (dependency already satisfied)
      const pulledB = await qm.pull('test', 0);
      expect(pulledB?.id).toBe(jobB.id);
    });

    test('should handle non-existent dependency gracefully', async () => {
      // Push job depending on non-existent job ID
      // Non-existent dependencies should be treated as not satisfied
      // since we can't verify they were ever completed
      const job = await qm.push('test', {
        data: { id: 'orphan' },
        dependsOn: ['non-existent-id' as any],
      });

      // Job should wait in waitingDeps since dependency isn't in completedJobs
      const pulled = await qm.pull('test', 0);
      expect(pulled).toBeNull();

      // Verify the job was created by checking we can push another with same data
      // (job is in waitingDeps, waiting for non-existent dependency to complete)
      expect(job.id).toBeDefined();
      expect(job.dependsOn.length).toBe(1);
    });
  });

  describe('parent-child relationships', () => {
    test('should track parent job', async () => {
      const parent = await qm.push('test', { data: { id: 'parent' } });

      const child = await qm.push('test', {
        data: { id: 'child' },
        parentId: parent.id,
      });

      const fetchedChild = await qm.getJob(child.id);
      expect(fetchedChild?.parentId).toBe(parent.id);
    });

    test('should allow pulling parent and child independently', async () => {
      const parent = await qm.push('test', { data: { id: 'parent' } });
      const child = await qm.push('test', {
        data: { id: 'child' },
        parentId: parent.id,
      });

      // Both should be pullable
      const first = await qm.pull('test', 0);
      const second = await qm.pull('test', 0);

      expect([first?.id, second?.id]).toContain(parent.id);
      expect([first?.id, second?.id]).toContain(child.id);
    });
  });
});

describe('Job Groups (FIFO)', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager({ dependencyCheckMs: 50 });
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should enforce FIFO within same group', async () => {
    // Push jobs in same group
    const job1 = await qm.push('test', {
      data: { order: 1 },
      groupId: 'group1',
    });
    const job2 = await qm.push('test', {
      data: { order: 2 },
      groupId: 'group1',
    });

    // First pull should get job1
    const pulled1 = await qm.pull('test', 0);
    expect(pulled1?.id).toBe(job1.id);

    // Second pull should return null (group1 has active job)
    const pulled2 = await qm.pull('test', 0);
    expect(pulled2).toBeNull();

    // Ack job1
    await qm.ack(job1.id, {});

    // Now job2 should be pullable
    const pulled3 = await qm.pull('test', 0);
    expect(pulled3?.id).toBe(job2.id);
  });

  test('should allow parallel processing of different groups', async () => {
    const jobA1 = await qm.push('test', { data: { group: 'A', order: 1 }, groupId: 'groupA' });
    const jobB1 = await qm.push('test', { data: { group: 'B', order: 1 }, groupId: 'groupB' });
    await qm.push('test', { data: { group: 'A', order: 2 }, groupId: 'groupA' });

    // Should be able to pull from both groups
    const pulled1 = await qm.pull('test', 0);
    const pulled2 = await qm.pull('test', 0);

    expect([pulled1?.id, pulled2?.id]).toContain(jobA1.id);
    expect([pulled1?.id, pulled2?.id]).toContain(jobB1.id);
  });

  test('should release group on job failure', async () => {
    const job1 = await qm.push('test', {
      data: { order: 1 },
      groupId: 'group1',
      maxAttempts: 1,
    });
    const job2 = await qm.push('test', {
      data: { order: 2 },
      groupId: 'group1',
    });

    // Pull and fail job1
    await qm.pull('test', 0);
    await qm.fail(job1.id, 'error');

    // Job2 should now be pullable
    const pulled = await qm.pull('test', 0);
    expect(pulled?.id).toBe(job2.id);
  });
});
