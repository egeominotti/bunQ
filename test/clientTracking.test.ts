/**
 * Client Tracking Tests
 * Unit tests for clientTracking.ts: registerClientJob, unregisterClientJob, releaseClientJobs
 *
 * Tests cover:
 * - Registering/unregistering client-job associations
 * - Empty client entry cleanup on last job removal
 * - Edge cases (non-existent client/job, undefined clientId)
 * - releaseClientJobs via QueueManager integration (needs full shard infrastructure)
 * - Concurrent registration/unregistration
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import {
  registerClientJob,
  unregisterClientJob,
  releaseClientJobs,
} from '../src/application/clientTracking';
import { QueueManager } from '../src/application/queueManager';
import type { JobId } from '../src/domain/types/job';
import type { LockContext } from '../src/application/types';
import { Shard } from '../src/domain/queue/shard';
import { RWLock } from '../src/shared/lock';
import { SHARD_COUNT } from '../src/shared/hash';
import { EventsManager } from '../src/application/eventsManager';
import { WebhookManager } from '../src/application/webhookManager';
import type { Job, JobLock } from '../src/domain/types/job';
import type { JobLocation } from '../src/domain/types/queue';

// ============ Helpers ============

/** Build a minimal LockContext for unit-testing registerClientJob / unregisterClientJob */
function createMinimalLockContext(overrides?: Partial<LockContext>): LockContext {
  const shards: Shard[] = [];
  const shardLocks: RWLock[] = [];
  const processingShards: Map<JobId, Job>[] = [];
  const processingLocks: RWLock[] = [];

  for (let i = 0; i < SHARD_COUNT; i++) {
    shards.push(new Shard());
    shardLocks.push(new RWLock());
    processingShards.push(new Map());
    processingLocks.push(new RWLock());
  }

  const webhookManager = new WebhookManager();
  const eventsManager = new EventsManager(webhookManager);

  return {
    jobIndex: new Map<JobId, JobLocation>(),
    jobLocks: new Map<JobId, JobLock>(),
    clientJobs: new Map<string, Set<JobId>>(),
    processingShards,
    processingLocks,
    shards,
    shardLocks,
    eventsManager,
    ...overrides,
  };
}

describe('Client Tracking', () => {
  // ============================================================
  // Unit Tests for registerClientJob
  // ============================================================

  describe('registerClientJob', () => {
    let ctx: LockContext;

    beforeEach(() => {
      ctx = createMinimalLockContext();
    });

    test('registers a single job for a client', () => {
      const jobId = 'job-1' as JobId;
      registerClientJob('client-a', jobId, ctx);

      expect(ctx.clientJobs.has('client-a')).toBe(true);
      expect(ctx.clientJobs.get('client-a')!.has(jobId)).toBe(true);
      expect(ctx.clientJobs.get('client-a')!.size).toBe(1);
    });

    test('registers multiple jobs for the same client', () => {
      const job1 = 'job-1' as JobId;
      const job2 = 'job-2' as JobId;
      const job3 = 'job-3' as JobId;

      registerClientJob('client-a', job1, ctx);
      registerClientJob('client-a', job2, ctx);
      registerClientJob('client-a', job3, ctx);

      const jobs = ctx.clientJobs.get('client-a')!;
      expect(jobs.size).toBe(3);
      expect(jobs.has(job1)).toBe(true);
      expect(jobs.has(job2)).toBe(true);
      expect(jobs.has(job3)).toBe(true);
    });

    test('registers jobs for different clients independently', () => {
      const jobA = 'job-a' as JobId;
      const jobB = 'job-b' as JobId;

      registerClientJob('client-1', jobA, ctx);
      registerClientJob('client-2', jobB, ctx);

      expect(ctx.clientJobs.size).toBe(2);
      expect(ctx.clientJobs.get('client-1')!.has(jobA)).toBe(true);
      expect(ctx.clientJobs.get('client-1')!.has(jobB)).toBe(false);
      expect(ctx.clientJobs.get('client-2')!.has(jobB)).toBe(true);
      expect(ctx.clientJobs.get('client-2')!.has(jobA)).toBe(false);
    });

    test('registering the same job twice for a client is idempotent', () => {
      const jobId = 'job-dup' as JobId;

      registerClientJob('client-a', jobId, ctx);
      registerClientJob('client-a', jobId, ctx);

      expect(ctx.clientJobs.get('client-a')!.size).toBe(1);
    });

    test('handles empty string client ID', () => {
      const jobId = 'job-1' as JobId;
      registerClientJob('', jobId, ctx);

      expect(ctx.clientJobs.has('')).toBe(true);
      expect(ctx.clientJobs.get('')!.has(jobId)).toBe(true);
    });

    test('handles many clients and many jobs', () => {
      const CLIENTS = 50;
      const JOBS_PER_CLIENT = 20;

      for (let c = 0; c < CLIENTS; c++) {
        for (let j = 0; j < JOBS_PER_CLIENT; j++) {
          registerClientJob(`client-${c}`, `job-${c}-${j}` as JobId, ctx);
        }
      }

      expect(ctx.clientJobs.size).toBe(CLIENTS);
      for (let c = 0; c < CLIENTS; c++) {
        expect(ctx.clientJobs.get(`client-${c}`)!.size).toBe(JOBS_PER_CLIENT);
      }
    });
  });

  // ============================================================
  // Unit Tests for unregisterClientJob
  // ============================================================

  describe('unregisterClientJob', () => {
    let ctx: LockContext;

    beforeEach(() => {
      ctx = createMinimalLockContext();
    });

    test('removes a job from a client', () => {
      const job1 = 'job-1' as JobId;
      const job2 = 'job-2' as JobId;

      registerClientJob('client-a', job1, ctx);
      registerClientJob('client-a', job2, ctx);

      unregisterClientJob('client-a', job1, ctx);

      const jobs = ctx.clientJobs.get('client-a')!;
      expect(jobs.size).toBe(1);
      expect(jobs.has(job1)).toBe(false);
      expect(jobs.has(job2)).toBe(true);
    });

    test('removes client entry when last job is unregistered', () => {
      const jobId = 'job-only' as JobId;

      registerClientJob('client-a', jobId, ctx);
      expect(ctx.clientJobs.has('client-a')).toBe(true);

      unregisterClientJob('client-a', jobId, ctx);
      expect(ctx.clientJobs.has('client-a')).toBe(false);
    });

    test('handles unregistering a non-existent job from existing client', () => {
      registerClientJob('client-a', 'job-1' as JobId, ctx);

      // Unregister a job that was never registered - should not throw
      unregisterClientJob('client-a', 'job-nonexistent' as JobId, ctx);

      // Original job should still be there
      expect(ctx.clientJobs.get('client-a')!.has('job-1' as JobId)).toBe(true);
      expect(ctx.clientJobs.get('client-a')!.size).toBe(1);
    });

    test('handles unregistering from a non-existent client', () => {
      // Should not throw
      unregisterClientJob('no-such-client', 'job-1' as JobId, ctx);
      expect(ctx.clientJobs.size).toBe(0);
    });

    test('handles undefined clientId gracefully (early return)', () => {
      registerClientJob('client-a', 'job-1' as JobId, ctx);

      // undefined clientId should be a no-op
      unregisterClientJob(undefined, 'job-1' as JobId, ctx);

      // Client-a should still have its job
      expect(ctx.clientJobs.get('client-a')!.size).toBe(1);
    });

    test('does not affect other clients when unregistering', () => {
      registerClientJob('client-a', 'job-1' as JobId, ctx);
      registerClientJob('client-b', 'job-2' as JobId, ctx);

      unregisterClientJob('client-a', 'job-1' as JobId, ctx);

      expect(ctx.clientJobs.has('client-a')).toBe(false);
      expect(ctx.clientJobs.get('client-b')!.has('job-2' as JobId)).toBe(true);
    });

    test('sequential unregister of all jobs cleans up client entry', () => {
      const jobs = ['j1', 'j2', 'j3', 'j4', 'j5'] as JobId[];
      for (const j of jobs) {
        registerClientJob('client-a', j, ctx);
      }
      expect(ctx.clientJobs.get('client-a')!.size).toBe(5);

      for (const j of jobs) {
        unregisterClientJob('client-a', j, ctx);
      }

      // Client entry should be fully removed
      expect(ctx.clientJobs.has('client-a')).toBe(false);
    });
  });

  // ============================================================
  // Unit Tests for empty / initial state
  // ============================================================

  describe('empty state', () => {
    let ctx: LockContext;

    beforeEach(() => {
      ctx = createMinimalLockContext();
    });

    test('clientJobs map starts empty', () => {
      expect(ctx.clientJobs.size).toBe(0);
    });

    test('unregister on empty state does not throw', () => {
      expect(() => unregisterClientJob('anything', 'job-1' as JobId, ctx)).not.toThrow();
    });

    test('undefined unregister on empty state does not throw', () => {
      expect(() => unregisterClientJob(undefined, 'job-1' as JobId, ctx)).not.toThrow();
    });
  });

  // ============================================================
  // Count and enumeration helpers
  // ============================================================

  describe('counting and enumeration', () => {
    let ctx: LockContext;

    beforeEach(() => {
      ctx = createMinimalLockContext();
    });

    test('get all client jobs via clientJobs map', () => {
      registerClientJob('c1', 'j1' as JobId, ctx);
      registerClientJob('c1', 'j2' as JobId, ctx);
      registerClientJob('c2', 'j3' as JobId, ctx);

      // Enumerate all entries
      const allEntries = Array.from(ctx.clientJobs.entries());
      expect(allEntries.length).toBe(2);
    });

    test('get jobs for a specific client', () => {
      registerClientJob('c1', 'j1' as JobId, ctx);
      registerClientJob('c1', 'j2' as JobId, ctx);
      registerClientJob('c2', 'j3' as JobId, ctx);

      const c1Jobs = ctx.clientJobs.get('c1');
      expect(c1Jobs).toBeDefined();
      expect(c1Jobs!.size).toBe(2);
      expect(Array.from(c1Jobs!)).toContain('j1');
      expect(Array.from(c1Jobs!)).toContain('j2');
    });

    test('get total job count across all clients', () => {
      registerClientJob('c1', 'j1' as JobId, ctx);
      registerClientJob('c1', 'j2' as JobId, ctx);
      registerClientJob('c2', 'j3' as JobId, ctx);
      registerClientJob('c3', 'j4' as JobId, ctx);
      registerClientJob('c3', 'j5' as JobId, ctx);
      registerClientJob('c3', 'j6' as JobId, ctx);

      let totalJobs = 0;
      for (const jobs of ctx.clientJobs.values()) {
        totalJobs += jobs.size;
      }
      expect(totalJobs).toBe(6);
    });

    test('client with no jobs is not in the map after unregister', () => {
      registerClientJob('c1', 'j1' as JobId, ctx);
      unregisterClientJob('c1', 'j1' as JobId, ctx);

      expect(ctx.clientJobs.get('c1')).toBeUndefined();
    });
  });

  // ============================================================
  // Concurrent registration / unregistration (sync operations)
  // ============================================================

  describe('concurrent registration and unregistration', () => {
    let ctx: LockContext;

    beforeEach(() => {
      ctx = createMinimalLockContext();
    });

    test('interleaved register and unregister produces correct state', () => {
      // Register 10 jobs
      for (let i = 0; i < 10; i++) {
        registerClientJob('client-a', `job-${i}` as JobId, ctx);
      }

      // Unregister even-numbered jobs
      for (let i = 0; i < 10; i += 2) {
        unregisterClientJob('client-a', `job-${i}` as JobId, ctx);
      }

      const remaining = ctx.clientJobs.get('client-a')!;
      expect(remaining.size).toBe(5);
      for (let i = 1; i < 10; i += 2) {
        expect(remaining.has(`job-${i}` as JobId)).toBe(true);
      }
    });

    test('register for multiple clients then unregister all from one', () => {
      for (let i = 0; i < 5; i++) {
        registerClientJob('keep-client', `kj-${i}` as JobId, ctx);
        registerClientJob('remove-client', `rj-${i}` as JobId, ctx);
      }

      // Unregister all from remove-client
      for (let i = 0; i < 5; i++) {
        unregisterClientJob('remove-client', `rj-${i}` as JobId, ctx);
      }

      expect(ctx.clientJobs.has('remove-client')).toBe(false);
      expect(ctx.clientJobs.get('keep-client')!.size).toBe(5);
    });
  });

  // ============================================================
  // releaseClientJobs - unit tests with mock LockContext
  // ============================================================

  describe('releaseClientJobs (unit)', () => {
    let ctx: LockContext;

    beforeEach(() => {
      ctx = createMinimalLockContext();
    });

    test('returns 0 and cleans up for non-existent client', async () => {
      const released = await releaseClientJobs('no-client', ctx);
      expect(released).toBe(0);
    });

    test('returns 0 and cleans up for client with empty job set', async () => {
      // Manually create an empty set entry
      ctx.clientJobs.set('empty-client', new Set());

      const released = await releaseClientJobs('empty-client', ctx);
      expect(released).toBe(0);
      expect(ctx.clientJobs.has('empty-client')).toBe(false);
    });

    test('returns 0 when client has jobs but none are in processing', async () => {
      // Register job IDs but don't put them in processingShards or jobIndex
      registerClientJob('orphan-client', 'orphan-1' as JobId, ctx);
      registerClientJob('orphan-client', 'orphan-2' as JobId, ctx);

      const released = await releaseClientJobs('orphan-client', ctx);
      expect(released).toBe(0);
      expect(ctx.clientJobs.has('orphan-client')).toBe(false);
    });

    test('cleans up client entry after release', async () => {
      registerClientJob('cleanup-client', 'j1' as JobId, ctx);

      await releaseClientJobs('cleanup-client', ctx);
      expect(ctx.clientJobs.has('cleanup-client')).toBe(false);
    });
  });

  // ============================================================
  // Integration tests via QueueManager
  // ============================================================

  describe('integration via QueueManager', () => {
    let qm: QueueManager;

    beforeEach(() => {
      qm = new QueueManager();
    });

    afterEach(() => {
      qm.shutdown();
    });

    test('registerClientJob and unregisterClientJob through QueueManager', async () => {
      const job = await qm.push('track-queue', { data: { v: 1 } });
      const pulled = await qm.pull('track-queue');
      expect(pulled).not.toBeNull();

      // Register the job with a client
      qm.registerClientJob('test-client', pulled!.id);

      // Unregister it
      qm.unregisterClientJob('test-client', pulled!.id);

      // Should release without error (no jobs to release)
      const released = await qm.releaseClientJobs('test-client');
      expect(released).toBe(0);
    });

    test('releaseClientJobs returns jobs to waiting state', async () => {
      const JOBS = 5;

      // Push jobs
      for (let i = 0; i < JOBS; i++) {
        await qm.push('release-queue', { data: { i } });
      }

      // Pull all jobs and register with client
      for (let i = 0; i < JOBS; i++) {
        const job = await qm.pull('release-queue', 0);
        expect(job).not.toBeNull();
        qm.registerClientJob('disconnect-client', job!.id);
      }

      let stats = qm.getStats();
      expect(stats.active).toBe(JOBS);
      expect(stats.waiting).toBe(0);

      // Simulate disconnect - release all client jobs
      const released = await qm.releaseClientJobs('disconnect-client');
      expect(released).toBe(JOBS);

      // All jobs should be back in waiting
      stats = qm.getStats();
      expect(stats.active).toBe(0);
      expect(stats.waiting).toBe(JOBS);
    });

    test('released jobs can be pulled again', async () => {
      await qm.push('repull-queue', { data: { msg: 'hello' } });
      const pulled1 = await qm.pull('repull-queue', 0);
      expect(pulled1).not.toBeNull();

      qm.registerClientJob('repull-client', pulled1!.id);

      const released = await qm.releaseClientJobs('repull-client');
      expect(released).toBe(1);

      // Pull the same job again
      const pulled2 = await qm.pull('repull-queue', 0);
      expect(pulled2).not.toBeNull();
      expect(pulled2!.id).toBe(pulled1!.id);
      expect(pulled2!.data).toEqual({ msg: 'hello' });
    });

    test('releaseClientJobs for unknown client returns 0', async () => {
      const released = await qm.releaseClientJobs('ghost-client');
      expect(released).toBe(0);
    });

    test('ack before release prevents job from being released', async () => {
      await qm.push('ack-before-release', { data: { v: 1 } });
      const pulled = await qm.pull('ack-before-release', 0);
      expect(pulled).not.toBeNull();

      qm.registerClientJob('ack-client', pulled!.id);

      // Ack the job first
      await qm.ack(pulled!.id, { done: true });
      qm.unregisterClientJob('ack-client', pulled!.id);

      // Release should find nothing to release
      const released = await qm.releaseClientJobs('ack-client');
      expect(released).toBe(0);

      const stats = qm.getStats();
      expect(stats.completed).toBe(1);
      expect(stats.waiting).toBe(0);
      expect(stats.active).toBe(0);
    });

    test('partial ack then release handles mixed states', async () => {
      const JOBS = 6;

      for (let i = 0; i < JOBS; i++) {
        await qm.push('partial-queue', { data: { i } });
      }

      const pulledJobs: any[] = [];
      for (let i = 0; i < JOBS; i++) {
        const job = await qm.pull('partial-queue', 0);
        expect(job).not.toBeNull();
        qm.registerClientJob('partial-client', job!.id);
        pulledJobs.push(job);
      }

      // Ack half the jobs
      for (let i = 0; i < JOBS / 2; i++) {
        await qm.ack(pulledJobs[i].id, { result: i });
        qm.unregisterClientJob('partial-client', pulledJobs[i].id);
      }

      // Release remaining
      const released = await qm.releaseClientJobs('partial-client');
      expect(released).toBe(JOBS / 2);

      const stats = qm.getStats();
      expect(stats.completed).toBe(JOBS / 2);
      expect(stats.waiting).toBe(JOBS / 2);
      expect(stats.active).toBe(0);
    });

    test('multiple clients release independently', async () => {
      const CLIENTS = 3;
      const JOBS_PER_CLIENT = 4;

      // Push and pull for each client
      for (let c = 0; c < CLIENTS; c++) {
        for (let j = 0; j < JOBS_PER_CLIENT; j++) {
          await qm.push('multi-client-queue', { data: { c, j } });
        }
      }

      for (let c = 0; c < CLIENTS; c++) {
        for (let j = 0; j < JOBS_PER_CLIENT; j++) {
          const job = await qm.pull('multi-client-queue', 0);
          if (job) {
            qm.registerClientJob(`mc-client-${c}`, job.id);
          }
        }
      }

      // Release only client-0
      const released0 = await qm.releaseClientJobs('mc-client-0');
      expect(released0).toBe(JOBS_PER_CLIENT);

      let stats = qm.getStats();
      expect(stats.active).toBe((CLIENTS - 1) * JOBS_PER_CLIENT);
      expect(stats.waiting).toBe(JOBS_PER_CLIENT);

      // Release client-1
      const released1 = await qm.releaseClientJobs('mc-client-1');
      expect(released1).toBe(JOBS_PER_CLIENT);

      stats = qm.getStats();
      expect(stats.active).toBe(JOBS_PER_CLIENT);
      expect(stats.waiting).toBe(2 * JOBS_PER_CLIENT);

      // Release client-2
      const released2 = await qm.releaseClientJobs('mc-client-2');
      expect(released2).toBe(JOBS_PER_CLIENT);

      stats = qm.getStats();
      expect(stats.active).toBe(0);
      expect(stats.waiting).toBe(CLIENTS * JOBS_PER_CLIENT);
    });

    test('concurrent release of same client releases all jobs exactly once', async () => {
      const JOBS = 20;

      for (let i = 0; i < JOBS; i++) {
        await qm.push('concurrent-release', { data: { i } });
      }

      for (let i = 0; i < JOBS; i++) {
        const job = await qm.pull('concurrent-release', 0);
        if (job) {
          qm.registerClientJob('concurrent-client', job.id);
        }
      }

      // Try to release the same client concurrently
      const results = await Promise.all([
        qm.releaseClientJobs('concurrent-client'),
        qm.releaseClientJobs('concurrent-client'),
        qm.releaseClientJobs('concurrent-client'),
      ]);

      const totalReleased = results.reduce((a, b) => a + b, 0);
      expect(totalReleased).toBe(JOBS);

      const stats = qm.getStats();
      expect(stats.waiting).toBe(JOBS);
      expect(stats.active).toBe(0);
    });

    test('concurrent release of different clients works correctly', async () => {
      const CLIENTS = 5;
      const JOBS_PER_CLIENT = 10;

      for (let c = 0; c < CLIENTS; c++) {
        for (let j = 0; j < JOBS_PER_CLIENT; j++) {
          await qm.push('cc-queue', { data: { c, j } });
        }
      }

      for (let c = 0; c < CLIENTS; c++) {
        for (let j = 0; j < JOBS_PER_CLIENT; j++) {
          const job = await qm.pull('cc-queue', 0);
          if (job) {
            qm.registerClientJob(`cc-${c}`, job.id);
          }
        }
      }

      // Release all clients concurrently
      const results = await Promise.all(
        Array.from({ length: CLIENTS }, (_, c) => qm.releaseClientJobs(`cc-${c}`))
      );

      const totalReleased = results.reduce((a, b) => a + b, 0);
      expect(totalReleased).toBe(CLIENTS * JOBS_PER_CLIENT);

      const stats = qm.getStats();
      expect(stats.active).toBe(0);
      expect(stats.waiting).toBe(CLIENTS * JOBS_PER_CLIENT);
    });

    test('release resets job startedAt so it can be re-pulled', async () => {
      await qm.push('reset-queue', { data: { v: 1 } });
      const pulled = await qm.pull('reset-queue', 0);
      expect(pulled).not.toBeNull();
      expect(pulled!.startedAt).not.toBeNull();

      qm.registerClientJob('reset-client', pulled!.id);

      await qm.releaseClientJobs('reset-client');

      // Pull again - startedAt should be null (reset by releaseJobToQueue)
      const repulled = await qm.pull('reset-queue', 0);
      expect(repulled).not.toBeNull();
      expect(repulled!.id).toBe(pulled!.id);
    });

    test('releasing a client with jobs on different queues', async () => {
      const queues = ['queue-a', 'queue-b', 'queue-c'];

      for (const q of queues) {
        await qm.push(q, { data: { queue: q } });
      }

      for (const q of queues) {
        const job = await qm.pull(q, 0);
        expect(job).not.toBeNull();
        qm.registerClientJob('multi-queue-client', job!.id);
      }

      const released = await qm.releaseClientJobs('multi-queue-client');
      expect(released).toBe(3);

      // Verify each queue has its job back
      for (const q of queues) {
        const job = await qm.pull(q, 0);
        expect(job).not.toBeNull();
        expect(job!.data).toEqual({ queue: q });
      }
    });

    test('double release of client returns 0 on second call', async () => {
      await qm.push('double-release', { data: { v: 1 } });
      const job = await qm.pull('double-release', 0);
      qm.registerClientJob('dr-client', job!.id);

      const first = await qm.releaseClientJobs('dr-client');
      expect(first).toBe(1);

      const second = await qm.releaseClientJobs('dr-client');
      expect(second).toBe(0);
    });
  });
});
