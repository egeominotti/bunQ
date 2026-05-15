/**
 * Regression: TCP close handler must not leak clientJobs entries and must
 * trigger fast recovery of orphaned active jobs when releaseClientJobs fails
 * persistently.
 *
 * Bug history: src/infrastructure/server/tcp.ts called
 * releaseClientJobsWithRetry(qm, clientId, 3). If all 3 retries threw, the
 * handler only logged. Two consequences:
 *   1. ctx.clientJobs[clientId] was never cleared → Map leak across flapping
 *      TCP connections.
 *   2. Orphaned jobs in 'active' state stayed there until the stall detector
 *      finally noticed via stallTimeout — could be tens of seconds.
 *
 * Fix:
 *   - src/application/clientTracking.ts wraps the locked release block in
 *     try/finally so the clientJobs map entry is always deleted, even if the
 *     lock acquisition fails.
 *   - A new forceReleaseClientJobs() helper performs a best-effort
 *     lock-free cleanup: deletes the map entry and resets each orphaned
 *     job's lastHeartbeat to 0, so the next stall-detector tick recovers it.
 *   - src/infrastructure/server/tcp.ts calls forceReleaseClientJobs() in the
 *     catch branch of releaseClientJobsWithRetry.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { tcpLog } from '../src/shared/logger';

// Mirror of src/infrastructure/server/tcp.ts close-handler retry helper.
async function releaseClientJobsWithRetry(
  queueManager: QueueManager,
  clientId: string,
  maxRetries = 3
): Promise<number> {
  let lastError: Error | undefined;
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await queueManager.releaseClientJobs(clientId);
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));
      await Bun.sleep(100 * Math.pow(2, attempt));
    }
  }
  tcpLog.error('Failed to release client jobs after retries', {
    clientId,
    error: lastError?.message,
  });
  throw lastError ?? new Error('Failed to release client jobs after retries');
}

describe('TCP orphan recovery', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('releaseClientJobs clears the clientJobs map even when lock-acquisition throws', async () => {
    const CLIENT = 'flapping-client';
    await qm.push('q', { data: { v: 1 } });
    const job = await qm.pull('q');
    expect(job).not.toBeNull();
    qm.registerClientJob(CLIENT, job!.id);

    // Force every shard lock acquire to throw — simulates persistent contention.
    type CtxFactory = {
      contextFactory: {
        getLockContext: () => {
          shardLocks: { acquireWrite: (timeoutMs: number) => Promise<unknown> }[];
          clientJobs: Map<string, Set<unknown>>;
        };
      };
    };
    const ctx = (qm as unknown as CtxFactory).contextFactory.getLockContext();
    const originals: Array<(t: number) => Promise<unknown>> = [];
    for (const lock of ctx.shardLocks) {
      originals.push(lock.acquireWrite.bind(lock));
      lock.acquireWrite = async () => {
        throw new Error('lock timeout');
      };
    }

    await expect(qm.releaseClientJobs(CLIENT)).rejects.toThrow('lock timeout');

    // BUG FIX: clientJobs entry MUST be cleared even though the locked phase
    // threw. Without try/finally, this Map grew unbounded on flapping clients.
    expect(ctx.clientJobs.has(CLIENT)).toBe(false);

    // Restore for clean teardown
    ctx.shardLocks.forEach((lock, i) => {
      lock.acquireWrite = originals[i];
    });
  });

  test('forceReleaseClientJobs clears tracking, jobLocks, and expires heartbeat+startedAt for stall recovery', async () => {
    const CLIENT = 'dead-client';
    await qm.push('q', { data: { v: 1 } });
    await qm.push('q', { data: { v: 2 } });
    const j1 = await qm.pull('q');
    const j2 = await qm.pull('q');
    qm.registerClientJob(CLIENT, j1!.id);
    qm.registerClientJob(CLIENT, j2!.id);

    type CtxFactory = {
      contextFactory: {
        getLockContext: () => {
          clientJobs: Map<string, Set<unknown>>;
          jobLocks: Map<unknown, unknown>;
          jobIndex: Map<
            unknown,
            { type: string; shardIdx: number; queueName: string } | { type: string; queueName: string }
          >;
          processingShards: Map<unknown, { lastHeartbeat: number | null; startedAt: number | null }>[];
        };
      };
    };
    const ctx = (qm as unknown as CtxFactory).contextFactory.getLockContext();

    // Seed lock tokens for both jobs to verify they are dropped
    ctx.jobLocks.set(j1!.id, { token: 'tok1', expiresAt: Date.now() + 60000 });
    ctx.jobLocks.set(j2!.id, { token: 'tok2', expiresAt: Date.now() + 60000 });

    expect(ctx.clientJobs.get(CLIENT)?.size).toBe(2);
    expect(ctx.jobLocks.has(j1!.id)).toBe(true);
    expect(ctx.jobLocks.has(j2!.id)).toBe(true);

    const touched = qm.forceReleaseClientJobs(CLIENT);

    expect(touched).toBe(2);
    expect(ctx.clientJobs.has(CLIENT)).toBe(false);

    // Lock tokens must be dropped — otherwise verifyLock could return true
    // for an orphaned job until the lock TTL expires.
    expect(ctx.jobLocks.has(j1!.id)).toBe(false);
    expect(ctx.jobLocks.has(j2!.id)).toBe(false);

    // Both jobs should have lastHeartbeat=0 AND startedAt=0 so the stall
    // detector's gracePeriod check passes on the next eligible tick.
    for (const j of [j1!, j2!]) {
      const loc = ctx.jobIndex.get(j.id);
      expect(loc).toBeDefined();
      if (loc && 'shardIdx' in loc && loc.type === 'processing') {
        const live = ctx.processingShards[loc.shardIdx].get(j.id);
        expect(live?.lastHeartbeat).toBe(0);
        expect(live?.startedAt).toBe(0);
      }
    }
  });

  test('end-to-end: close-handler fallback recovers from persistent release failure', async () => {
    const CLIENT = 'flaky-tcp-client';
    await qm.push('q', { data: { v: 1 } });
    const job = await qm.pull('q');
    qm.registerClientJob(CLIENT, job!.id);
    expect(qm.getStats().active).toBe(1);

    // Force releaseClientJobs to throw deterministically
    const original = qm.releaseClientJobs.bind(qm);
    (qm as unknown as { releaseClientJobs: typeof qm.releaseClientJobs }).releaseClientJobs =
      async () => {
        throw new Error('persistent lock contention');
      };

    // Simulate tcp.ts close handler logic
    await releaseClientJobsWithRetry(qm, CLIENT).catch(() => {
      qm.forceReleaseClientJobs(CLIENT);
    });

    type CtxFactory = {
      contextFactory: {
        getLockContext: () => {
          clientJobs: Map<string, Set<unknown>>;
          jobIndex: Map<
            unknown,
            { type: string; shardIdx: number; queueName: string } | { type: string; queueName: string }
          >;
          processingShards: Map<unknown, { lastHeartbeat: number | null }>[];
        };
      };
    };
    const ctx = (qm as unknown as CtxFactory).contextFactory.getLockContext();

    // Map leak prevented
    expect(ctx.clientJobs.has(CLIENT)).toBe(false);

    // Heartbeat expired → stall detector will recover on next tick
    const loc = ctx.jobIndex.get(job!.id);
    if (loc && 'shardIdx' in loc && loc.type === 'processing') {
      const live = ctx.processingShards[loc.shardIdx].get(job!.id);
      expect(live?.lastHeartbeat).toBe(0);
    }

    // Restore for clean shutdown
    (qm as unknown as { releaseClientJobs: typeof qm.releaseClientJobs }).releaseClientJobs =
      original;
  });
});
