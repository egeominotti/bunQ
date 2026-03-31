/**
 * DependencyTracker Unit Tests
 * Comprehensive tests for dependency tracking and parent-child relationship management
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { DependencyTracker } from '../src/domain/queue/dependencyTracker';
import { createJob, jobId } from '../src/domain/types/job';
import type { JobId } from '../src/domain/types/job';
import type { Job } from '../src/domain/types/job';

/** Helper to create a JobId from a short string */
const jid = (n: number): JobId => jobId(`job-${n}`);

/** Helper to create a minimal Job object for testing */
function makeJob(id: number, opts?: { dependsOn?: JobId[]; parentId?: JobId }): Job {
  return createJob(jid(id), 'test-queue', {
    data: { id },
    dependsOn: opts?.dependsOn,
    parentId: opts?.parentId,
  });
}

describe('DependencyTracker', () => {
  let tracker: DependencyTracker;

  beforeEach(() => {
    tracker = new DependencyTracker();
  });

  describe('initial state', () => {
    test('should start with empty counts', () => {
      const counts = tracker.getCounts();
      expect(counts.waitingDeps).toBe(0);
      expect(counts.dependencyIndex).toBe(0);
      expect(counts.waitingChildren).toBe(0);
    });

    test('should have empty internal maps', () => {
      expect(tracker.waitingDeps.size).toBe(0);
      expect(tracker.dependencyIndex.size).toBe(0);
      expect(tracker.waitingChildren.size).toBe(0);
    });
  });

  describe('registerDependencies', () => {
    test('should register a single dependency', () => {
      tracker.registerDependencies(jid(2), [jid(1)]);

      const waiters = tracker.getJobsWaitingFor(jid(1));
      expect(waiters).toBeDefined();
      expect(waiters!.size).toBe(1);
      expect(waiters!.has(jid(2))).toBe(true);
    });

    test('should register multiple dependencies for one job', () => {
      tracker.registerDependencies(jid(3), [jid(1), jid(2)]);

      const waitersFor1 = tracker.getJobsWaitingFor(jid(1));
      expect(waitersFor1).toBeDefined();
      expect(waitersFor1!.has(jid(3))).toBe(true);

      const waitersFor2 = tracker.getJobsWaitingFor(jid(2));
      expect(waitersFor2).toBeDefined();
      expect(waitersFor2!.has(jid(3))).toBe(true);
    });

    test('should allow multiple jobs to wait for the same dependency', () => {
      tracker.registerDependencies(jid(2), [jid(1)]);
      tracker.registerDependencies(jid(3), [jid(1)]);
      tracker.registerDependencies(jid(4), [jid(1)]);

      const waiters = tracker.getJobsWaitingFor(jid(1));
      expect(waiters).toBeDefined();
      expect(waiters!.size).toBe(3);
      expect(waiters!.has(jid(2))).toBe(true);
      expect(waiters!.has(jid(3))).toBe(true);
      expect(waiters!.has(jid(4))).toBe(true);
    });

    test('should handle registering an empty dependency array', () => {
      tracker.registerDependencies(jid(1), []);
      expect(tracker.dependencyIndex.size).toBe(0);
    });

    test('should be idempotent when registering the same dependency twice', () => {
      tracker.registerDependencies(jid(2), [jid(1)]);
      tracker.registerDependencies(jid(2), [jid(1)]);

      const waiters = tracker.getJobsWaitingFor(jid(1));
      expect(waiters!.size).toBe(1);
    });

    test('should update getCounts dependencyIndex count correctly', () => {
      tracker.registerDependencies(jid(3), [jid(1), jid(2)]);

      const counts = tracker.getCounts();
      // Two distinct dependency IDs are in the index
      expect(counts.dependencyIndex).toBe(2);
    });
  });

  describe('unregisterDependencies', () => {
    test('should remove a job from dependency waiters', () => {
      tracker.registerDependencies(jid(2), [jid(1)]);
      tracker.unregisterDependencies(jid(2), [jid(1)]);

      const waiters = tracker.getJobsWaitingFor(jid(1));
      expect(waiters).toBeUndefined();
    });

    test('should clean up dependency entry when last waiter is removed', () => {
      tracker.registerDependencies(jid(2), [jid(1)]);
      tracker.unregisterDependencies(jid(2), [jid(1)]);

      // The entry for jid(1) should be completely removed
      expect(tracker.dependencyIndex.has(jid(1))).toBe(false);
      expect(tracker.getCounts().dependencyIndex).toBe(0);
    });

    test('should not clean up dependency entry when other waiters remain', () => {
      tracker.registerDependencies(jid(2), [jid(1)]);
      tracker.registerDependencies(jid(3), [jid(1)]);

      tracker.unregisterDependencies(jid(2), [jid(1)]);

      const waiters = tracker.getJobsWaitingFor(jid(1));
      expect(waiters).toBeDefined();
      expect(waiters!.size).toBe(1);
      expect(waiters!.has(jid(3))).toBe(true);
      expect(waiters!.has(jid(2))).toBe(false);
    });

    test('should handle unregistering from a non-existent dependency', () => {
      // Should not throw
      tracker.unregisterDependencies(jid(2), [jid(99)]);
      expect(tracker.getCounts().dependencyIndex).toBe(0);
    });

    test('should handle unregistering an empty dependency array', () => {
      tracker.registerDependencies(jid(2), [jid(1)]);
      tracker.unregisterDependencies(jid(2), []);

      // Original registration should remain
      const waiters = tracker.getJobsWaitingFor(jid(1));
      expect(waiters).toBeDefined();
      expect(waiters!.size).toBe(1);
    });

    test('should unregister multiple dependencies at once', () => {
      tracker.registerDependencies(jid(3), [jid(1), jid(2)]);
      tracker.unregisterDependencies(jid(3), [jid(1), jid(2)]);

      expect(tracker.getJobsWaitingFor(jid(1))).toBeUndefined();
      expect(tracker.getJobsWaitingFor(jid(2))).toBeUndefined();
      expect(tracker.getCounts().dependencyIndex).toBe(0);
    });
  });

  describe('getJobsWaitingFor', () => {
    test('should return undefined for non-existent dependency', () => {
      expect(tracker.getJobsWaitingFor(jid(999))).toBeUndefined();
    });

    test('should return the Set of waiting job IDs', () => {
      tracker.registerDependencies(jid(2), [jid(1)]);
      tracker.registerDependencies(jid(3), [jid(1)]);

      const waiters = tracker.getJobsWaitingFor(jid(1));
      expect(waiters).toBeInstanceOf(Set);
      expect(waiters!.size).toBe(2);
    });

    test('should return the live Set (mutations are visible)', () => {
      tracker.registerDependencies(jid(2), [jid(1)]);
      const waiters = tracker.getJobsWaitingFor(jid(1));

      tracker.registerDependencies(jid(3), [jid(1)]);
      // The returned set should reflect the new registration
      expect(waiters!.size).toBe(2);
    });
  });

  describe('addWaitingJob / removeWaitingJob / getWaitingJob / isWaiting', () => {
    test('should add a job to waitingDeps', () => {
      const job = makeJob(1);
      tracker.addWaitingJob(job);

      expect(tracker.isWaiting(jid(1))).toBe(true);
      expect(tracker.getWaitingJob(jid(1))).toBe(job);
      expect(tracker.getCounts().waitingDeps).toBe(1);
    });

    test('should auto-register dependencies when adding a waiting job', () => {
      const job = makeJob(3, { dependsOn: [jid(1), jid(2)] });
      tracker.addWaitingJob(job);

      expect(tracker.getJobsWaitingFor(jid(1))!.has(jid(3))).toBe(true);
      expect(tracker.getJobsWaitingFor(jid(2))!.has(jid(3))).toBe(true);
      expect(tracker.getCounts().dependencyIndex).toBe(2);
    });

    test('should not register dependencies when job has empty dependsOn', () => {
      const job = makeJob(1); // no dependsOn
      tracker.addWaitingJob(job);

      expect(tracker.getCounts().dependencyIndex).toBe(0);
    });

    test('should remove a waiting job and unregister its dependencies', () => {
      const job = makeJob(3, { dependsOn: [jid(1), jid(2)] });
      tracker.addWaitingJob(job);

      const removed = tracker.removeWaitingJob(jid(3));
      expect(removed).toBe(job);
      expect(tracker.isWaiting(jid(3))).toBe(false);
      expect(tracker.getWaitingJob(jid(3))).toBeUndefined();
      expect(tracker.getJobsWaitingFor(jid(1))).toBeUndefined();
      expect(tracker.getJobsWaitingFor(jid(2))).toBeUndefined();
      expect(tracker.getCounts().waitingDeps).toBe(0);
      expect(tracker.getCounts().dependencyIndex).toBe(0);
    });

    test('should return undefined when removing a non-existent waiting job', () => {
      const removed = tracker.removeWaitingJob(jid(999));
      expect(removed).toBeUndefined();
    });

    test('isWaiting should return false for non-existent job', () => {
      expect(tracker.isWaiting(jid(999))).toBe(false);
    });

    test('getWaitingJob should return undefined for non-existent job', () => {
      expect(tracker.getWaitingJob(jid(999))).toBeUndefined();
    });
  });

  describe('addWaitingParent / removeWaitingParent / getWaitingParent / isParentWaiting', () => {
    test('should add a parent to waitingChildren', () => {
      const parent = makeJob(10);
      tracker.addWaitingParent(parent);

      expect(tracker.isParentWaiting(jid(10))).toBe(true);
      expect(tracker.getWaitingParent(jid(10))).toBe(parent);
      expect(tracker.getCounts().waitingChildren).toBe(1);
    });

    test('should remove a parent from waitingChildren', () => {
      const parent = makeJob(10);
      tracker.addWaitingParent(parent);

      const removed = tracker.removeWaitingParent(jid(10));
      expect(removed).toBe(parent);
      expect(tracker.isParentWaiting(jid(10))).toBe(false);
      expect(tracker.getWaitingParent(jid(10))).toBeUndefined();
      expect(tracker.getCounts().waitingChildren).toBe(0);
    });

    test('should return undefined when removing a non-existent parent', () => {
      const removed = tracker.removeWaitingParent(jid(999));
      expect(removed).toBeUndefined();
    });

    test('isParentWaiting should return false for non-existent parent', () => {
      expect(tracker.isParentWaiting(jid(999))).toBe(false);
    });

    test('getWaitingParent should return undefined for non-existent parent', () => {
      expect(tracker.getWaitingParent(jid(999))).toBeUndefined();
    });

    test('should track multiple parents independently', () => {
      const parent1 = makeJob(10);
      const parent2 = makeJob(20);
      tracker.addWaitingParent(parent1);
      tracker.addWaitingParent(parent2);

      expect(tracker.getCounts().waitingChildren).toBe(2);
      expect(tracker.isParentWaiting(jid(10))).toBe(true);
      expect(tracker.isParentWaiting(jid(20))).toBe(true);

      tracker.removeWaitingParent(jid(10));
      expect(tracker.isParentWaiting(jid(10))).toBe(false);
      expect(tracker.isParentWaiting(jid(20))).toBe(true);
      expect(tracker.getCounts().waitingChildren).toBe(1);
    });
  });

  describe('getCounts', () => {
    test('should return accurate counts after complex operations', () => {
      // Add 3 waiting jobs with dependencies
      const jobA = makeJob(1, { dependsOn: [jid(10), jid(11)] });
      const jobB = makeJob(2, { dependsOn: [jid(10)] });
      const jobC = makeJob(3, { dependsOn: [jid(12)] });
      tracker.addWaitingJob(jobA);
      tracker.addWaitingJob(jobB);
      tracker.addWaitingJob(jobC);

      // Add 2 waiting parents
      const parent1 = makeJob(100);
      const parent2 = makeJob(200);
      tracker.addWaitingParent(parent1);
      tracker.addWaitingParent(parent2);

      let counts = tracker.getCounts();
      expect(counts.waitingDeps).toBe(3);
      // dep IDs in index: jid(10), jid(11), jid(12) => 3 entries
      expect(counts.dependencyIndex).toBe(3);
      expect(counts.waitingChildren).toBe(2);

      // Remove one waiting job
      tracker.removeWaitingJob(jid(2));

      counts = tracker.getCounts();
      expect(counts.waitingDeps).toBe(2);
      // jid(10) still has jid(1) waiting, jid(11) has jid(1), jid(12) has jid(3) => still 3
      expect(counts.dependencyIndex).toBe(3);

      // Remove another waiting job that was the only waiter on jid(12)
      tracker.removeWaitingJob(jid(3));

      counts = tracker.getCounts();
      expect(counts.waitingDeps).toBe(1);
      // jid(12) should be cleaned up; jid(10) and jid(11) remain => 2
      expect(counts.dependencyIndex).toBe(2);

      // Remove a parent
      tracker.removeWaitingParent(jid(100));
      counts = tracker.getCounts();
      expect(counts.waitingChildren).toBe(1);
    });
  });

  describe('complex scenarios', () => {
    test('diamond dependency: C depends on A and B, both depend on D', () => {
      // D has no dependencies, A and B depend on D, C depends on A and B
      const jobD = makeJob(4);
      const jobA = makeJob(1, { dependsOn: [jid(4)] });
      const jobB = makeJob(2, { dependsOn: [jid(4)] });
      const jobC = makeJob(3, { dependsOn: [jid(1), jid(2)] });

      tracker.addWaitingJob(jobA);
      tracker.addWaitingJob(jobB);
      tracker.addWaitingJob(jobC);

      // D should have A and B as waiters
      const waitersForD = tracker.getJobsWaitingFor(jid(4));
      expect(waitersForD!.size).toBe(2);
      expect(waitersForD!.has(jid(1))).toBe(true);
      expect(waitersForD!.has(jid(2))).toBe(true);

      // A and B should each have C as waiter
      expect(tracker.getJobsWaitingFor(jid(1))!.has(jid(3))).toBe(true);
      expect(tracker.getJobsWaitingFor(jid(2))!.has(jid(3))).toBe(true);

      // Simulate D completing: remove A and B from waiting
      tracker.removeWaitingJob(jid(1));
      tracker.removeWaitingJob(jid(2));

      // D should have no more waiters
      expect(tracker.getJobsWaitingFor(jid(4))).toBeUndefined();

      // C is still waiting for completed A and B
      expect(tracker.isWaiting(jid(3))).toBe(true);

      // Removing C cleans up the index entries for jid(1) and jid(2)
      tracker.removeWaitingJob(jid(3));
      expect(tracker.getJobsWaitingFor(jid(1))).toBeUndefined();
      expect(tracker.getJobsWaitingFor(jid(2))).toBeUndefined();
      expect(tracker.getCounts().waitingDeps).toBe(0);
      expect(tracker.getCounts().dependencyIndex).toBe(0);
    });

    test('parent waits for children to complete then proceeds', () => {
      const parent = makeJob(100);
      const child1 = makeJob(1, { parentId: jid(100) });
      const child2 = makeJob(2, { parentId: jid(100) });

      tracker.addWaitingParent(parent);

      expect(tracker.isParentWaiting(jid(100))).toBe(true);

      // Simulate children completing - parent tracks children count externally
      // Just verify we can look up and remove the parent
      const retrieved = tracker.getWaitingParent(jid(100));
      expect(retrieved).toBe(parent);

      tracker.removeWaitingParent(jid(100));
      expect(tracker.isParentWaiting(jid(100))).toBe(false);
    });

    test('register and unregister dependencies independently of waiting jobs', () => {
      // registerDependencies can be called directly (outside of addWaitingJob)
      tracker.registerDependencies(jid(5), [jid(1), jid(2), jid(3)]);

      expect(tracker.getJobsWaitingFor(jid(1))!.has(jid(5))).toBe(true);
      expect(tracker.getJobsWaitingFor(jid(2))!.has(jid(5))).toBe(true);
      expect(tracker.getJobsWaitingFor(jid(3))!.has(jid(5))).toBe(true);
      expect(tracker.getCounts().dependencyIndex).toBe(3);

      // Partial unregister
      tracker.unregisterDependencies(jid(5), [jid(1)]);
      expect(tracker.getJobsWaitingFor(jid(1))).toBeUndefined();
      expect(tracker.getJobsWaitingFor(jid(2))!.has(jid(5))).toBe(true);
      expect(tracker.getCounts().dependencyIndex).toBe(2);

      // Full unregister of remaining
      tracker.unregisterDependencies(jid(5), [jid(2), jid(3)]);
      expect(tracker.getCounts().dependencyIndex).toBe(0);
    });

    test('checking all dependencies satisfied via getJobsWaitingFor', () => {
      // Job 5 depends on jobs 1, 2, 3
      const job = makeJob(5, { dependsOn: [jid(1), jid(2), jid(3)] });
      tracker.addWaitingJob(job);

      // Simulate dependency 1 completing: check who was waiting for it
      const waitersFor1 = tracker.getJobsWaitingFor(jid(1));
      expect(waitersFor1).toBeDefined();
      expect(waitersFor1!.has(jid(5))).toBe(true);

      // After all dependencies complete, we can verify the job is still in waiting
      // (the actual resolution logic lives in the shard/manager, not in the tracker)
      expect(tracker.isWaiting(jid(5))).toBe(true);

      // Remove the job once resolved externally
      tracker.removeWaitingJob(jid(5));
      expect(tracker.isWaiting(jid(5))).toBe(false);
      expect(tracker.getCounts().waitingDeps).toBe(0);
      expect(tracker.getCounts().dependencyIndex).toBe(0);
    });

    test('overwriting a waiting job replaces the stored job', () => {
      const job1 = makeJob(1, { dependsOn: [jid(10)] });
      tracker.addWaitingJob(job1);

      // Add again with different data (same id)
      const job1v2 = makeJob(1, { dependsOn: [jid(10), jid(20)] });
      tracker.addWaitingJob(job1v2);

      // The stored job should be the latest one
      expect(tracker.getWaitingJob(jid(1))).toBe(job1v2);
      // But dependency index may have duplicate entries from both registrations
      // jid(10) has jid(1), jid(20) has jid(1)
      expect(tracker.getJobsWaitingFor(jid(10))!.has(jid(1))).toBe(true);
      expect(tracker.getJobsWaitingFor(jid(20))!.has(jid(1))).toBe(true);
    });

    test('overwriting a waiting parent replaces the stored parent', () => {
      const parent1 = makeJob(100);
      tracker.addWaitingParent(parent1);

      const parent1v2 = makeJob(100);
      tracker.addWaitingParent(parent1v2);

      expect(tracker.getWaitingParent(jid(100))).toBe(parent1v2);
      expect(tracker.getCounts().waitingChildren).toBe(1);
    });

    test('mixed operations: dependencies, waiting jobs, and parents together', () => {
      // Set up a realistic scenario
      const parent = makeJob(100);
      const child1 = makeJob(1, { dependsOn: [jid(50)], parentId: jid(100) });
      const child2 = makeJob(2, { dependsOn: [jid(50), jid(51)], parentId: jid(100) });

      tracker.addWaitingParent(parent);
      tracker.addWaitingJob(child1);
      tracker.addWaitingJob(child2);

      let counts = tracker.getCounts();
      expect(counts.waitingDeps).toBe(2);
      expect(counts.waitingChildren).toBe(1);
      // dep index: jid(50) -> {1,2}, jid(51) -> {2}
      expect(counts.dependencyIndex).toBe(2);

      // Who waits for dep 50?
      const waitersFor50 = tracker.getJobsWaitingFor(jid(50));
      expect(waitersFor50!.size).toBe(2);

      // Who waits for dep 51?
      const waitersFor51 = tracker.getJobsWaitingFor(jid(51));
      expect(waitersFor51!.size).toBe(1);
      expect(waitersFor51!.has(jid(2))).toBe(true);

      // Child1 dependency resolved, remove from waiting
      tracker.removeWaitingJob(jid(1));
      counts = tracker.getCounts();
      expect(counts.waitingDeps).toBe(1);
      // jid(50) still has child2, jid(51) still has child2
      expect(counts.dependencyIndex).toBe(2);

      // Child2 dependency resolved
      tracker.removeWaitingJob(jid(2));
      counts = tracker.getCounts();
      expect(counts.waitingDeps).toBe(0);
      expect(counts.dependencyIndex).toBe(0);

      // Parent still waiting
      expect(tracker.isParentWaiting(jid(100))).toBe(true);

      // Parent resolved
      tracker.removeWaitingParent(jid(100));
      counts = tracker.getCounts();
      expect(counts.waitingChildren).toBe(0);
    });

    test('large number of dependencies on a single job', () => {
      const deps: JobId[] = [];
      for (let i = 1; i <= 50; i++) {
        deps.push(jid(i));
      }
      const job = makeJob(100, { dependsOn: deps });
      tracker.addWaitingJob(job);

      expect(tracker.getCounts().dependencyIndex).toBe(50);
      for (let i = 1; i <= 50; i++) {
        const waiters = tracker.getJobsWaitingFor(jid(i));
        expect(waiters).toBeDefined();
        expect(waiters!.has(jid(100))).toBe(true);
      }

      tracker.removeWaitingJob(jid(100));
      expect(tracker.getCounts().dependencyIndex).toBe(0);
    });

    test('many jobs waiting for the same single dependency', () => {
      for (let i = 1; i <= 30; i++) {
        tracker.registerDependencies(jid(i), [jid(999)]);
      }

      const waiters = tracker.getJobsWaitingFor(jid(999));
      expect(waiters!.size).toBe(30);
      expect(tracker.getCounts().dependencyIndex).toBe(1);

      // Remove them one by one
      for (let i = 1; i <= 29; i++) {
        tracker.unregisterDependencies(jid(i), [jid(999)]);
      }
      expect(tracker.getJobsWaitingFor(jid(999))!.size).toBe(1);

      // Remove the last one - should clean up the entry
      tracker.unregisterDependencies(jid(30), [jid(999)]);
      expect(tracker.getJobsWaitingFor(jid(999))).toBeUndefined();
      expect(tracker.getCounts().dependencyIndex).toBe(0);
    });
  });
});
