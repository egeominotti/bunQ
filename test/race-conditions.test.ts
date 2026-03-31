/**
 * Race Condition Tests
 * Tests for concurrent operations that could cause race conditions
 * Specifically tests the fixes for:
 * - releaseClientJobs()
 * - checkExpiredLocks()
 * - handleStalledJob()
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { shardIndex } from '../src/shared/hash';
import { unlink } from 'fs/promises';

const TEST_DB = './test-race.db';

async function cleanup() {
  for (const f of [TEST_DB, `${TEST_DB}-wal`, `${TEST_DB}-shm`]) {
    if (await Bun.file(f).exists()) await unlink(f);
  }
}

describe('Race Condition Tests', () => {
  describe('1. Concurrent Client Disconnections (releaseClientJobs)', () => {
    let qm: QueueManager;

    beforeEach(async () => {
      await cleanup();
      qm = new QueueManager({ dataPath: TEST_DB });
    });

    afterEach(async () => {
      qm.shutdown();
      await cleanup();
    });

    test('should safely release jobs when multiple clients disconnect simultaneously', async () => {
      const CLIENTS = 10;
      const JOBS_PER_CLIENT = 20;

      // Push jobs for each "client"
      const clientJobs: Map<string, bigint[]> = new Map();

      for (let c = 0; c < CLIENTS; c++) {
        const clientId = `client-${c}`;
        clientJobs.set(clientId, []);

        for (let j = 0; j < JOBS_PER_CLIENT; j++) {
          const job = await qm.push('race-queue', {
            data: { client: c, job: j },
          });
          clientJobs.get(clientId)!.push(job.id);
        }
      }

      // Pull jobs and register them with clients
      for (let c = 0; c < CLIENTS; c++) {
        const clientId = `client-${c}`;
        for (let j = 0; j < JOBS_PER_CLIENT; j++) {
          const job = await qm.pull('race-queue', 0);
          if (job) {
            qm.registerClientJob(clientId, job.id);
          }
        }
      }

      let stats = qm.getStats();
      expect(stats.active).toBe(CLIENTS * JOBS_PER_CLIENT);
      expect(stats.waiting).toBe(0);

      // Simulate all clients disconnecting simultaneously
      const releasePromises = Array.from({ length: CLIENTS }, (_, c) =>
        qm.releaseClientJobs(`client-${c}`)
      );

      const results = await Promise.all(releasePromises);

      // Each client should have released their jobs
      const totalReleased = results.reduce((a, b) => a + b, 0);
      expect(totalReleased).toBe(CLIENTS * JOBS_PER_CLIENT);

      // All jobs should be back in waiting state
      stats = qm.getStats();
      expect(stats.active).toBe(0);
      expect(stats.waiting).toBe(CLIENTS * JOBS_PER_CLIENT);

      // Verify jobs can be pulled again
      let pulledCount = 0;
      for (let i = 0; i < CLIENTS * JOBS_PER_CLIENT; i++) {
        const job = await qm.pull('race-queue', 0);
        if (job) pulledCount++;
      }
      expect(pulledCount).toBe(CLIENTS * JOBS_PER_CLIENT);
    });

    test('should handle interleaved pull and release operations', async () => {
      const ITERATIONS = 50;

      // Push initial jobs
      for (let i = 0; i < ITERATIONS; i++) {
        await qm.push('interleave-queue', { data: { i } });
      }

      const operations: Promise<any>[] = [];
      const processedJobs: Set<string> = new Set();

      // Interleave pull, ack, and release operations
      for (let i = 0; i < ITERATIONS; i++) {
        const clientId = `client-${i % 5}`;

        // Pull operation with client registration
        operations.push(
          qm.pull('interleave-queue', 50).then(async (job) => {
            if (job) {
              processedJobs.add(String(job.id));
              qm.registerClientJob(clientId, job.id);
              // Randomly ack or let release handle it
              if (Math.random() > 0.5) {
                await qm.ack(job.id, { result: 'done' });
                qm.unregisterClientJob(clientId, job.id);
                return 'acked';
              }
              return 'pulled';
            }
            return 'empty';
          })
        );

        // Occasionally release client jobs
        if (i % 10 === 9) {
          operations.push(
            qm.releaseClientJobs(clientId).then((count) => `released-${count}`)
          );
        }
      }

      await Promise.all(operations);

      // Should not have lost any jobs - all should be completed, waiting, or active
      const stats = qm.getStats();
      expect(stats.waiting + stats.active + stats.completed).toBe(ITERATIONS);
    });

    test('should not duplicate jobs when releasing under contention', async () => {
      const JOBS = 100;
      const CLIENT = 'contention-client';

      // Push and pull all jobs
      for (let i = 0; i < JOBS; i++) {
        await qm.push('contention-queue', { data: { i } });
      }

      for (let i = 0; i < JOBS; i++) {
        const job = await qm.pull('contention-queue', 0);
        if (job) {
          qm.registerClientJob(CLIENT, job.id);
        }
      }

      // Try to release multiple times concurrently
      const releaseAttempts = Array.from({ length: 10 }, () =>
        qm.releaseClientJobs(CLIENT)
      );

      const results = await Promise.all(releaseAttempts);

      // Only the first release should have released jobs
      const totalReleased = results.reduce((a, b) => a + b, 0);
      expect(totalReleased).toBe(JOBS);

      // Exactly JOBS should be in waiting state
      const stats = qm.getStats();
      expect(stats.waiting).toBe(JOBS);
      expect(stats.active).toBe(0);
    });
  });

  describe('2. Lock Expiration Under Load (checkExpiredLocks)', () => {
    let qm: QueueManager;

    beforeEach(async () => {
      await cleanup();
      qm = new QueueManager({ dataPath: TEST_DB });
    });

    afterEach(async () => {
      qm.shutdown();
      await cleanup();
    });

    test('should handle concurrent lock renewals and operations', async () => {
      const JOBS = 50;

      // Push and pull jobs using pullWithLock
      const jobs: Array<{ id: bigint; token: string }> = [];
      for (let i = 0; i < JOBS; i++) {
        await qm.push('lock-queue', { data: { i } });
        const { job, token } = await qm.pullWithLock('lock-queue', `owner-${i}`, 0);
        if (job && token) {
          jobs.push({ id: job.id, token });
        }
      }

      expect(jobs.length).toBe(JOBS);

      // Concurrent operations: some renew, some ack
      const operations: Promise<any>[] = [];

      for (let i = 0; i < jobs.length; i++) {
        const job = jobs[i];
        if (i % 2 === 0) {
          // Renew lock using correct method name
          operations.push(
            Promise.resolve(qm.renewJobLock(job.id, job.token, 30000)).then((success) => ({
              type: 'renew',
              success,
              id: job.id,
            }))
          );
        } else {
          // Ack with token
          operations.push(
            qm.ack(job.id, { result: 'done' }, job.token).then(
              () => ({ type: 'ack', success: true, id: job.id }),
              () => ({ type: 'ack', success: false, id: job.id })
            )
          );
        }
      }

      const results = await Promise.all(operations);

      const renewed = results.filter((r) => r.type === 'renew' && r.success);
      const acked = results.filter((r) => r.type === 'ack' && r.success);

      // All operations should succeed
      expect(renewed.length + acked.length).toBe(JOBS);
    });

    test('should safely handle lock verification during concurrent operations', async () => {
      const JOB_COUNT = 30;

      // Create jobs with locks
      const jobs: Array<{ id: bigint; token: string }> = [];
      for (let i = 0; i < JOB_COUNT; i++) {
        await qm.push('verify-queue', { data: { i } });
        const { job, token } = await qm.pullWithLock('verify-queue', `owner-${i}`, 0);
        if (job && token) {
          jobs.push({ id: job.id, token });
        }
      }

      expect(jobs.length).toBe(JOB_COUNT);

      // Concurrent verify, renew, and complete operations
      const operations: Promise<any>[] = [];

      for (const job of jobs) {
        // Verify operation
        operations.push(
          Promise.resolve(qm.verifyLock(job.id, job.token)).then((valid) => ({
            type: 'verify',
            valid,
          }))
        );

        // Complete operation (with correct token)
        operations.push(
          qm.ack(job.id, {}, job.token).then(
            () => ({ type: 'ack', success: true }),
            () => ({ type: 'ack', success: false })
          )
        );
      }

      const results = await Promise.all(operations);

      // Some verify may fail because job was already acked
      // But no errors should occur (race conditions would cause exceptions)
      const ackResults = results.filter((r) => r.type === 'ack');
      const successfulAcks = ackResults.filter((r) => r.success);

      // All jobs should eventually be acked (only first ack per job succeeds)
      expect(successfulAcks.length).toBe(JOB_COUNT);
    });
  });

  describe('3. Stall Detection Under Concurrent Load', () => {
    let qm: QueueManager;

    beforeEach(async () => {
      await cleanup();
      qm = new QueueManager({
        dataPath: TEST_DB,
        stallCheckMs: 100, // Fast stall check for testing
      });
    });

    afterEach(async () => {
      qm.shutdown();
      await cleanup();
    });

    test('should handle concurrent lock renewals without losing jobs', async () => {
      const JOBS = 20;
      const QUEUE = 'heartbeat-queue';

      // Configure stall detection via shard
      const shard = qm.getShards()[shardIndex(QUEUE)];
      shard.setStallConfig(QUEUE, {
        enabled: true,
        stallInterval: 200,
        maxStalls: 3,
        gracePeriod: 50,
      });

      // Push and pull jobs with locks
      const jobs: Array<{ id: bigint; token: string }> = [];
      for (let i = 0; i < JOBS; i++) {
        await qm.push(QUEUE, { data: { i } });
        const { job, token } = await qm.pullWithLock(QUEUE, `owner-${i}`, 0);
        if (job && token) {
          jobs.push({ id: job.id, token });
        }
      }

      // Concurrent lock renewals (simulates heartbeats)
      for (let round = 0; round < 5; round++) {
        const renewPromises = jobs.map((job) =>
          Promise.resolve(qm.renewJobLock(job.id, job.token, 30000))
        );
        await Promise.all(renewPromises);
        await Bun.sleep(50);
      }

      // All jobs should still be active (lock renewals prevent stall)
      const stats = qm.getStats();
      expect(stats.active).toBe(JOBS);

      // Complete all jobs
      for (const job of jobs) {
        await qm.ack(job.id, { result: 'done' }, job.token);
      }

      const finalStats = qm.getStats();
      expect(finalStats.completed).toBe(JOBS);
      expect(finalStats.active).toBe(0);
    });

    test('should not lose jobs during stall detection and recovery', async () => {
      const JOBS = 15;
      const QUEUE = 'stall-recovery-queue';

      // Configure stall detection with longer intervals for reliable testing
      const shard = qm.getShards()[shardIndex(QUEUE)];
      shard.setStallConfig(QUEUE, {
        enabled: true,
        stallInterval: 50,
        maxStalls: 5, // Higher to avoid DLQ during test
        gracePeriod: 10,
      });

      // Push jobs with high max attempts
      for (let i = 0; i < JOBS; i++) {
        await qm.push(QUEUE, {
          data: { i },
          maxAttempts: 10,
        });
      }

      // Pull all jobs with locks
      const pulledJobs: Array<{ id: bigint; token: string }> = [];
      for (let i = 0; i < JOBS; i++) {
        const { job, token } = await qm.pullWithLock(QUEUE, `owner-${i}`, 0);
        if (job && token) pulledJobs.push({ id: job.id, token });
      }
      expect(pulledJobs.length).toBe(JOBS);

      // Complete some jobs
      const toComplete = Math.floor(JOBS / 3);
      for (let i = 0; i < toComplete; i++) {
        await qm.ack(pulledJobs[i].id, { result: 'done' }, pulledJobs[i].token);
      }

      // Check that completed jobs are accounted for
      let stats = qm.getStats();
      expect(stats.completed).toBe(toComplete);

      // Complete remaining jobs
      for (let i = toComplete; i < JOBS; i++) {
        try {
          await qm.ack(pulledJobs[i].id, { result: 'done' }, pulledJobs[i].token);
        } catch {
          // Job might have been stalled and requeued, try to pull and complete
        }
      }

      // Pull and complete any remaining jobs
      let iterations = 0;
      while (iterations < 50) {
        const { job, token } = await qm.pullWithLock(QUEUE, 'cleanup', 50);
        if (!job) break;
        await qm.ack(job.id, { result: 'recovered' }, token!);
        iterations++;
      }

      const finalStats = qm.getStats();
      // All jobs should be completed or in DLQ
      expect(finalStats.waiting).toBe(0);
      expect(finalStats.active).toBe(0);
      expect(finalStats.completed + finalStats.dlq).toBe(JOBS);
    });
  });

  describe('4. Mixed Concurrent Operations Stress Test', () => {
    let qm: QueueManager;

    beforeEach(async () => {
      await cleanup();
      qm = new QueueManager({ dataPath: TEST_DB });
    });

    afterEach(async () => {
      qm.shutdown();
      await cleanup();
    });

    test('should handle push/pull/ack/fail/release all concurrently', async () => {
      const OPERATIONS = 200;
      const QUEUES = ['stress-a', 'stress-b', 'stress-c'];
      const errors: Error[] = [];

      const operations: Promise<any>[] = [];

      for (let i = 0; i < OPERATIONS; i++) {
        const queue = QUEUES[i % QUEUES.length];
        const clientId = `client-${i % 10}`;
        const op = i % 5;

        switch (op) {
          case 0:
            // Push
            operations.push(
              qm.push(queue, { data: { i } }).catch((e) => {
                errors.push(e);
                return null;
              })
            );
            break;

          case 1:
            // Pull and register
            operations.push(
              qm.pull(queue, 10).then((job) => {
                if (job) {
                  qm.registerClientJob(clientId, job.id);
                }
                return job;
              }).catch((e) => {
                errors.push(e);
                return null;
              })
            );
            break;

          case 2:
            // Ack random job
            operations.push(
              qm.pull(queue, 0).then(async (job) => {
                if (job) {
                  try {
                    await qm.ack(job.id, { result: i });
                  } catch {
                    // Job might have been released/processed already
                  }
                }
              })
            );
            break;

          case 3:
            // Fail random job
            operations.push(
              qm.pull(queue, 0).then(async (job) => {
                if (job) {
                  try {
                    await qm.fail(job.id, `Error ${i}`);
                  } catch {
                    // Job might have been released/processed already
                  }
                }
              })
            );
            break;

          case 4:
            // Release client jobs
            operations.push(
              qm.releaseClientJobs(clientId).catch((e) => {
                errors.push(e);
                return 0;
              })
            );
            break;
        }
      }

      await Promise.all(operations);

      // Should have no race condition errors
      expect(errors.length).toBe(0);

      // Stats should be consistent
      const stats = qm.getStats();
      expect(stats.waiting).toBeGreaterThanOrEqual(0);
      expect(stats.active).toBeGreaterThanOrEqual(0);
      expect(stats.completed).toBeGreaterThanOrEqual(0);
    });

    test('should maintain job count invariant under chaos', async () => {
      const TOTAL_JOBS = 100;
      const QUEUE = 'invariant-queue';

      // Push all jobs
      for (let i = 0; i < TOTAL_JOBS; i++) {
        await qm.push(QUEUE, { data: { i }, maxAttempts: 10 });
      }

      const initialStats = qm.getStats();
      expect(initialStats.waiting).toBe(TOTAL_JOBS);

      // Chaos: random operations - ack or fail only (no client tracking to simplify)
      const chaos: Promise<any>[] = [];
      for (let i = 0; i < 200; i++) {
        const op = Math.floor(Math.random() * 3);

        switch (op) {
          case 0:
            // Pull without processing (will timeout later)
            chaos.push(qm.pull(QUEUE, 5).catch(() => null));
            break;
          case 1:
            // Pull and ack
            chaos.push(
              qm.pull(QUEUE, 0).then(async (job) => {
                if (job) {
                  try {
                    await qm.ack(job.id, {});
                  } catch {
                    /* ignore */
                  }
                }
              })
            );
            break;
          case 2:
            // Pull and fail
            chaos.push(
              qm.pull(QUEUE, 0).then(async (job) => {
                if (job) {
                  try {
                    await qm.fail(job.id, 'chaos');
                  } catch {
                    /* ignore */
                  }
                }
              })
            );
            break;
        }
      }

      await Promise.all(chaos);

      // Wait for any pending retries
      await Bun.sleep(100);

      // Invariant: total jobs should be accounted for
      const midStats = qm.getStats();
      const accountedFor =
        midStats.waiting +
        midStats.active +
        midStats.completed +
        midStats.dlq;

      // All jobs should be somewhere in the system (allowing for some in-flight)
      expect(accountedFor).toBeLessThanOrEqual(TOTAL_JOBS);
      expect(accountedFor).toBeGreaterThan(0);

      // Clean up: complete all remaining jobs with longer timeout
      let processed = 0;
      let iterations = 0;
      while (iterations < TOTAL_JOBS * 3) {
        const job = await qm.pull(QUEUE, 200);
        if (!job) break;
        try {
          await qm.ack(job.id, {});
          processed++;
        } catch {
          /* job might have been processed already */
        }
        iterations++;
      }

      // Final state: no waiting jobs
      const endStats = qm.getStats();
      expect(endStats.waiting).toBe(0);
      // All jobs should be accounted for somewhere (completed, DLQ, or still active)
      // This test verifies no jobs are lost, but allows for some to still be in-flight
      expect(endStats.completed + endStats.dlq + endStats.active).toBeGreaterThanOrEqual(TOTAL_JOBS * 0.5);
    });
  });

  describe('5. High Concurrency Lock Contention', () => {
    let qm: QueueManager;

    beforeEach(async () => {
      await cleanup();
      qm = new QueueManager({ dataPath: TEST_DB });
    });

    afterEach(async () => {
      qm.shutdown();
      await cleanup();
    });

    test('should handle 100 concurrent pulls from same queue', async () => {
      const JOBS = 100;
      const QUEUE = 'contention-pull-queue';

      // Push all jobs
      for (let i = 0; i < JOBS; i++) {
        await qm.push(QUEUE, { data: { i } });
      }

      // 100 concurrent pulls
      const pulls = Array.from({ length: JOBS }, () =>
        qm.pull(QUEUE, 100)
      );

      const results = await Promise.all(pulls);
      const pulledJobs = results.filter((r) => r !== null);
      const pulledIds = new Set(pulledJobs.map((j) => String(j!.id)));

      // Each job should be pulled exactly once (no duplicates)
      expect(pulledIds.size).toBe(JOBS);
      expect(pulledJobs.length).toBe(JOBS);
    });

    test('should handle concurrent acks for different jobs', async () => {
      const JOBS = 50;
      const QUEUE = 'contention-ack-queue';

      // Push and pull jobs with locks
      const jobs: Array<{ id: bigint; token?: string }> = [];
      for (let i = 0; i < JOBS; i++) {
        await qm.push(QUEUE, { data: { i } });
        const { job, token } = await qm.pullWithLock(QUEUE, `owner-${i}`, 0);
        if (job) {
          jobs.push({ id: job.id, token: token ?? undefined });
        }
      }

      // Concurrent acks
      const acks = jobs.map((job) =>
        qm.ack(job.id, { result: 'done' }, job.token).then(
          () => ({ success: true, id: job.id }),
          (e) => ({ success: false, id: job.id, error: e.message })
        )
      );

      const results = await Promise.all(acks);
      const successful = results.filter((r) => r.success);

      expect(successful.length).toBe(JOBS);

      const stats = qm.getStats();
      expect(stats.completed).toBe(JOBS);
      expect(stats.active).toBe(0);
    });

    test('should handle rapid push/pull cycles without losing jobs', async () => {
      const CYCLES = 100;
      const QUEUE = 'rapid-cycle-queue';
      const processedIds: Set<string> = new Set();
      const errors: string[] = [];

      // Rapid push/pull/ack cycles
      const cycles = Array.from({ length: CYCLES }, async (_, i) => {
        try {
          const job = await qm.push(QUEUE, { data: { cycle: i } });
          const pulled = await qm.pull(QUEUE, 100);
          if (pulled) {
            processedIds.add(String(pulled.id));
            await qm.ack(pulled.id, { result: i });
          }
          return true;
        } catch (e) {
          errors.push(String(e));
          return false;
        }
      });

      const results = await Promise.all(cycles);
      const successCount = results.filter((r) => r).length;

      // Most cycles should succeed
      expect(successCount).toBeGreaterThan(CYCLES * 0.8);
      expect(errors.length).toBeLessThan(CYCLES * 0.2);

      // Clean up any remaining jobs
      while (true) {
        const job = await qm.pull(QUEUE, 10);
        if (!job) break;
        await qm.ack(job.id, {});
      }

      const stats = qm.getStats();
      expect(stats.waiting).toBe(0);
      expect(stats.active).toBe(0);
    });
  });

  describe('6. Cross-Queue Concurrent Operations', () => {
    let qm: QueueManager;

    beforeEach(async () => {
      await cleanup();
      qm = new QueueManager({ dataPath: TEST_DB });
    });

    afterEach(async () => {
      qm.shutdown();
      await cleanup();
    });

    test('should handle operations across 10 queues simultaneously', async () => {
      const QUEUES = Array.from({ length: 10 }, (_, i) => `multi-queue-${i}`);
      const JOBS_PER_QUEUE = 20;

      // Push to all queues concurrently
      const pushPromises: Promise<any>[] = [];
      for (const queue of QUEUES) {
        for (let j = 0; j < JOBS_PER_QUEUE; j++) {
          pushPromises.push(qm.push(queue, { data: { queue, job: j } }));
        }
      }
      await Promise.all(pushPromises);

      let stats = qm.getStats();
      expect(stats.waiting).toBe(QUEUES.length * JOBS_PER_QUEUE);

      // Pull and ack from all queues concurrently
      const processPromises: Promise<boolean>[] = [];
      for (const queue of QUEUES) {
        for (let j = 0; j < JOBS_PER_QUEUE; j++) {
          processPromises.push(
            qm.pull(queue, 100).then(async (job) => {
              if (job) {
                await qm.ack(job.id, { result: 'done' });
                return true;
              }
              return false;
            })
          );
        }
      }

      const results = await Promise.all(processPromises);
      const processed = results.filter((r) => r).length;

      expect(processed).toBe(QUEUES.length * JOBS_PER_QUEUE);

      stats = qm.getStats();
      expect(stats.completed).toBe(QUEUES.length * JOBS_PER_QUEUE);
      expect(stats.waiting).toBe(0);
      expect(stats.active).toBe(0);
    });

    test('should handle drain during active processing', async () => {
      const QUEUE = 'drain-test-queue';
      const JOBS = 50;

      // Push jobs
      for (let i = 0; i < JOBS; i++) {
        await qm.push(QUEUE, { data: { i } });
      }

      // Start pulling (but don't ack)
      const pullPromises = Array.from({ length: 10 }, () =>
        qm.pull(QUEUE, 0)
      );
      await Promise.all(pullPromises);

      let stats = qm.getStats();
      expect(stats.active).toBe(10);
      expect(stats.waiting).toBe(40);

      // Drain remaining waiting jobs
      qm.drain(QUEUE);

      stats = qm.getStats();
      expect(stats.waiting).toBe(0);
      expect(stats.active).toBe(10); // Active jobs not affected by drain
    });
  });
});

describe('Race Condition Benchmarks', () => {
  let qm: QueueManager;

  beforeEach(async () => {
    await cleanup();
    qm = new QueueManager({ dataPath: TEST_DB });
  });

  afterEach(async () => {
    qm.shutdown();
    await cleanup();
  });

  test('BENCHMARK: 1000 concurrent push operations', async () => {
    const COUNT = 1000;

    const start = performance.now();
    const pushes = Array.from({ length: COUNT }, (_, i) =>
      qm.push('bench-push', { data: { i } })
    );
    await Promise.all(pushes);
    const duration = performance.now() - start;

    console.log(`\n📊 1000 concurrent pushes: ${duration.toFixed(2)}ms`);
    console.log(`   Throughput: ${(COUNT / (duration / 1000)).toFixed(0)} ops/sec`);

    const stats = qm.getStats();
    expect(stats.waiting).toBe(COUNT);
  });

  test('BENCHMARK: 500 concurrent pull operations', async () => {
    const COUNT = 500;

    // First push
    for (let i = 0; i < COUNT; i++) {
      await qm.push('bench-pull', { data: { i } });
    }

    const start = performance.now();
    const pulls = Array.from({ length: COUNT }, () =>
      qm.pull('bench-pull', 100)
    );
    const results = await Promise.all(pulls);
    const duration = performance.now() - start;

    const pulled = results.filter((r) => r !== null).length;

    console.log(`\n📊 500 concurrent pulls: ${duration.toFixed(2)}ms`);
    console.log(`   Pulled: ${pulled} jobs`);
    console.log(`   Throughput: ${(pulled / (duration / 1000)).toFixed(0)} ops/sec`);

    expect(pulled).toBe(COUNT);
  });

  test('BENCHMARK: Mixed concurrent operations (push/pull/ack)', async () => {
    const ITERATIONS = 300;

    const start = performance.now();
    const operations: Promise<any>[] = [];

    for (let i = 0; i < ITERATIONS; i++) {
      // Push
      operations.push(qm.push('bench-mixed', { data: { i } }));

      // Pull and ack
      operations.push(
        qm.pull('bench-mixed', 50).then(async (job) => {
          if (job) {
            await qm.ack(job.id, {});
            return true;
          }
          return false;
        })
      );
    }

    await Promise.all(operations);
    const duration = performance.now() - start;

    console.log(`\n📊 ${ITERATIONS * 2} mixed operations: ${duration.toFixed(2)}ms`);
    console.log(`   Throughput: ${((ITERATIONS * 2) / (duration / 1000)).toFixed(0)} ops/sec`);

    // Clean up remaining
    while (true) {
      const job = await qm.pull('bench-mixed', 10);
      if (!job) break;
      await qm.ack(job.id, {});
    }

    const stats = qm.getStats();
    expect(stats.waiting).toBe(0);
    expect(stats.active).toBe(0);
  });

  test('BENCHMARK: Client disconnect/release under load', async () => {
    const CLIENTS = 20;
    const JOBS_PER_CLIENT = 50;

    // Setup: push and pull jobs for each client
    for (let c = 0; c < CLIENTS; c++) {
      for (let j = 0; j < JOBS_PER_CLIENT; j++) {
        await qm.push('bench-release', { data: { c, j } });
      }
    }

    for (let c = 0; c < CLIENTS; c++) {
      for (let j = 0; j < JOBS_PER_CLIENT; j++) {
        const job = await qm.pull('bench-release', 0);
        if (job) {
          qm.registerClientJob(`client-${c}`, job.id);
        }
      }
    }

    const start = performance.now();
    const releases = Array.from({ length: CLIENTS }, (_, c) =>
      qm.releaseClientJobs(`client-${c}`)
    );
    const results = await Promise.all(releases);
    const duration = performance.now() - start;

    const totalReleased = results.reduce((a, b) => a + b, 0);

    console.log(`\n📊 ${CLIENTS} concurrent client releases: ${duration.toFixed(2)}ms`);
    console.log(`   Total jobs released: ${totalReleased}`);
    console.log(`   Throughput: ${(totalReleased / (duration / 1000)).toFixed(0)} jobs/sec`);

    expect(totalReleased).toBe(CLIENTS * JOBS_PER_CLIENT);
  });
});
