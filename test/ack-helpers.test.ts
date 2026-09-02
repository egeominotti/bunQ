/**
 * Tests for ackHelpers.ts
 * Covers: extractJobs, extractJobsWithResults, releaseResources, finalizeBatchAck
 */

import { describe, test, expect, beforeEach, mock } from 'bun:test';
import { jobId, type JobId, type Job } from '../src/domain/types/job';
import type { JobLocation, EventType } from '../src/domain/types/queue';
import { RWLock } from '../src/shared/lock';
import { SHARD_COUNT, processingShardIndex, shardIndex } from '../src/shared/hash';
import { DependencyCompletionTracker } from '../src/application/dependencyCompletions';
import { DEFAULT_CONFIG } from '../src/application/types';
import {
  extractJobs,
  extractJobsWithResults,
  releaseResources,
  finalizeBatchAck,
  groupByProcShard,
  type BatchContext,
  type FinalizeContext,
  type ExtractedJob,
} from '../src/application/operations/ackHelpers';

/** Create a fake Job with sensible defaults and optional overrides */
function fakeJob(overrides: Partial<Job> & { id: JobId; queue: string }): Job {
  const now = Date.now();
  return {
    data: {},
    priority: 0,
    createdAt: now,
    runAt: now,
    startedAt: null,
    completedAt: null,
    attempts: 0,
    maxAttempts: 3,
    backoff: 1000,
    backoffConfig: null,
    ttl: null,
    timeout: null,
    uniqueKey: null,
    customId: null,
    dependsOn: [],
    parentId: null,
    childrenIds: [],
    childrenCompleted: 0,
    tags: [],
    groupId: null,
    progress: 0,
    progressMessage: null,
    removeOnComplete: false,
    removeOnFail: false,
    repeat: null,
    lastHeartbeat: now,
    stallTimeout: null,
    stallCount: 0,
    lifo: false,
    stackTraceLimit: 10,
    keepLogs: null,
    sizeLimit: null,
    failParentOnFailure: false,
    removeDependencyOnFailure: false,
    continueParentOnFailure: false,
    ignoreDependencyOnFailure: false,
    deduplicationTtl: null,
    deduplicationExtend: false,
    deduplicationReplace: false,
    debounceId: null,
    debounceTtl: null,
    timeline: [],
    ...overrides,
  } as Job;
}

/** Create a BatchContext with real RWLocks and empty processing shards */
function createBatchContext(): BatchContext {
  const processingShards: Map<JobId, Job>[] = [];
  const processingLocks: RWLock[] = [];
  const shardLocks: RWLock[] = [];

  for (let i = 0; i < SHARD_COUNT; i++) {
    processingShards.push(new Map<JobId, Job>());
    processingLocks.push(new RWLock());
    shardLocks.push(new RWLock());
  }

  return {
    processingShards,
    processingLocks,
    shards: [] as any[], // populated per test when needed
    shardLocks,
  };
}

/** Create a mock Shard with a spy on releaseJobResources */
function createMockShard() {
  return {
    releaseJobResources: mock(() => {}),
  };
}

/** Create a FinalizeContext with no storage and mock callbacks */
function createFinalizeContext(overrides?: Partial<FinalizeContext>): FinalizeContext {
  return {
    storage: null,
    shards: [],
    completedJobs: new Set<JobId>() as any,
    completedJobsData: new Map<JobId, Job>() as any,
    depCompletions: new DependencyCompletionTracker(DEFAULT_CONFIG.maxCompletedJobs),
    maxDependencyCompletions: DEFAULT_CONFIG.maxCompletedJobs,
    jobResults: new Map<JobId, unknown>() as any,
    jobResultQueues: new Map<JobId, string>(),
    jobIndex: new Map<JobId, JobLocation>(),
    totalCompleted: { value: 0n },
    broadcast: mock(() => {}),
    onJobCompleted: mock(() => {}),
    ...overrides,
  };
}

// ============================================================================
// extractJobs
// ============================================================================

describe('extractJobs', () => {
  let ctx: BatchContext;

  beforeEach(() => {
    ctx = createBatchContext();
  });

  test('extracts jobs from processing shards and removes them', async () => {
    const id1 = jobId('job-extract-1');
    const job1 = fakeJob({ id: id1, queue: 'q1' });
    const procIdx = processingShardIndex(id1);

    ctx.processingShards[procIdx].set(id1, job1);
    expect(ctx.processingShards[procIdx].has(id1)).toBe(true);

    const byProcShard = groupByProcShard([id1]);
    const result = await extractJobs(byProcShard, ctx);

    expect(result).toHaveLength(1);
    expect(result[0].id).toBe(id1);
    expect(result[0].job).toBe(job1);
    expect(result[0].result).toBeUndefined();
    // Job should be removed from processing shard
    expect(ctx.processingShards[procIdx].has(id1)).toBe(false);
  });

  test('skips job IDs not found in processing shards', async () => {
    const id1 = jobId('job-found');
    const id2 = jobId('job-not-found');
    const job1 = fakeJob({ id: id1, queue: 'q1' });
    const procIdx1 = processingShardIndex(id1);

    ctx.processingShards[procIdx1].set(id1, job1);
    // id2 is NOT placed in any processing shard

    const byProcShard = groupByProcShard([id1, id2]);
    const result = await extractJobs(byProcShard, ctx);

    expect(result).toHaveLength(1);
    expect(result[0].id).toBe(id1);
  });

  test('works with empty input', async () => {
    const byProcShard = new Map<number, JobId[]>();
    const result = await extractJobs(byProcShard, ctx);

    expect(result).toHaveLength(0);
  });

  test('works across multiple processing shards', async () => {
    // Create jobs that will land in different processing shards
    const ids: JobId[] = [];
    const jobs: Job[] = [];
    for (let i = 0; i < 20; i++) {
      const id = jobId(`multi-shard-job-${i}`);
      const job = fakeJob({ id, queue: `q-${i % 3}` });
      const procIdx = processingShardIndex(id);
      ctx.processingShards[procIdx].set(id, job);
      ids.push(id);
      jobs.push(job);
    }

    const byProcShard = groupByProcShard(ids);
    // Verify we actually have multiple shards (with 20 IDs, extremely likely)
    expect(byProcShard.size).toBeGreaterThanOrEqual(1);

    const result = await extractJobs(byProcShard, ctx);

    expect(result).toHaveLength(20);
    // All IDs should be present in the result
    const extractedIds = new Set(result.map((r) => r.id));
    for (const id of ids) {
      expect(extractedIds.has(id)).toBe(true);
    }
    // All processing shards should be empty
    for (const shard of ctx.processingShards) {
      for (const id of ids) {
        expect(shard.has(id)).toBe(false);
      }
    }
  });

  test('does not remove jobs from shards that were not requested', async () => {
    const id1 = jobId('requested-job');
    const id2 = jobId('other-job');
    const job1 = fakeJob({ id: id1, queue: 'q1' });
    const job2 = fakeJob({ id: id2, queue: 'q2' });
    const procIdx1 = processingShardIndex(id1);
    const procIdx2 = processingShardIndex(id2);

    ctx.processingShards[procIdx1].set(id1, job1);
    ctx.processingShards[procIdx2].set(id2, job2);

    // Only request id1
    const byProcShard = groupByProcShard([id1]);
    await extractJobs(byProcShard, ctx);

    // id2 should still be in its shard
    expect(ctx.processingShards[procIdx2].has(id2)).toBe(true);
  });
});

// ============================================================================
// extractJobsWithResults
// ============================================================================

describe('extractJobsWithResults', () => {
  let ctx: BatchContext;

  beforeEach(() => {
    ctx = createBatchContext();
  });

  test('preserves result data alongside extracted jobs', async () => {
    const id1 = jobId('result-job-1');
    const id2 = jobId('result-job-2');
    const job1 = fakeJob({ id: id1, queue: 'q1' });
    const job2 = fakeJob({ id: id2, queue: 'q2' });

    const procIdx1 = processingShardIndex(id1);
    const procIdx2 = processingShardIndex(id2);
    ctx.processingShards[procIdx1].set(id1, job1);
    ctx.processingShards[procIdx2].set(id2, job2);

    const items = [
      { id: id1, result: { status: 'ok', count: 42 } },
      { id: id2, result: { status: 'done', count: 99 } },
    ];

    const byProcShard = new Map<number, typeof items>();
    for (const item of items) {
      const procIdx = processingShardIndex(item.id);
      let group = byProcShard.get(procIdx);
      if (!group) {
        group = [];
        byProcShard.set(procIdx, group);
      }
      group.push(item);
    }

    const result = await extractJobsWithResults(byProcShard, ctx);

    expect(result).toHaveLength(2);

    const r1 = result.find((r) => r.id === id1);
    const r2 = result.find((r) => r.id === id2);

    expect(r1).toBeDefined();
    expect(r1!.job).toBe(job1);
    expect(r1!.result).toEqual({ status: 'ok', count: 42 });

    expect(r2).toBeDefined();
    expect(r2!.job).toBe(job2);
    expect(r2!.result).toEqual({ status: 'done', count: 99 });

    // Jobs removed from shards
    expect(ctx.processingShards[procIdx1].has(id1)).toBe(false);
    expect(ctx.processingShards[procIdx2].has(id2)).toBe(false);
  });

  test('skips missing jobs but extracts found ones', async () => {
    const idFound = jobId('found-with-result');
    const idMissing = jobId('missing-with-result');
    const jobFound = fakeJob({ id: idFound, queue: 'q1' });

    const procIdxFound = processingShardIndex(idFound);
    ctx.processingShards[procIdxFound].set(idFound, jobFound);
    // idMissing is NOT in any processing shard

    const items = [
      { id: idFound, result: 'success' },
      { id: idMissing, result: 'should-not-appear' },
    ];

    const byProcShard = new Map<number, typeof items>();
    for (const item of items) {
      const procIdx = processingShardIndex(item.id);
      let group = byProcShard.get(procIdx);
      if (!group) {
        group = [];
        byProcShard.set(procIdx, group);
      }
      group.push(item);
    }

    const result = await extractJobsWithResults(byProcShard, ctx);

    expect(result).toHaveLength(1);
    expect(result[0].id).toBe(idFound);
    expect(result[0].result).toBe('success');
  });

  test('works with empty input', async () => {
    const byProcShard = new Map<number, Array<{ id: JobId; result: string }>>();
    const result = await extractJobsWithResults(byProcShard, ctx);
    expect(result).toHaveLength(0);
  });
});

// ============================================================================
// releaseResources
// ============================================================================

describe('releaseResources', () => {
  test('calls releaseJobResources for each job in the correct shard', async () => {
    const ctx = createBatchContext();
    const mockShards: ReturnType<typeof createMockShard>[] = [];
    for (let i = 0; i < SHARD_COUNT; i++) {
      mockShards.push(createMockShard());
    }
    ctx.shards = mockShards as any;

    const job1 = fakeJob({ id: jobId('rr-1'), queue: 'emails', uniqueKey: 'uk-1', groupId: 'g-1' });
    const job2 = fakeJob({ id: jobId('rr-2'), queue: 'emails', uniqueKey: null, groupId: null });
    const job3 = fakeJob({
      id: jobId('rr-3'),
      queue: 'payments',
      uniqueKey: 'uk-3',
      groupId: null,
    });

    const idx1 = shardIndex('emails');
    const idx3 = shardIndex('payments');

    const byQueueShard = new Map<number, Job[]>();
    // Group job1 and job2 under the 'emails' shard
    byQueueShard.set(idx1, [job1, job2]);
    // Group job3 under the 'payments' shard (may or may not be same as idx1)
    if (idx3 === idx1) {
      byQueueShard.get(idx1)!.push(job3);
    } else {
      byQueueShard.set(idx3, [job3]);
    }

    await releaseResources(byQueueShard, ctx);

    // Check that releaseJobResources was called with correct args
    const emailsShard = mockShards[idx1];
    expect(emailsShard.releaseJobResources).toHaveBeenCalledWith('emails', 'uk-1', 'g-1', 'rr-1');
    expect(emailsShard.releaseJobResources).toHaveBeenCalledWith('emails', null, null, 'rr-2');

    const paymentsShard = mockShards[idx3];
    expect(paymentsShard.releaseJobResources).toHaveBeenCalledWith(
      'payments',
      'uk-3',
      null,
      'rr-3'
    );
  });

  test('passes correct uniqueKey and groupId to releaseJobResources', async () => {
    const ctx = createBatchContext();
    const mockShards: ReturnType<typeof createMockShard>[] = [];
    for (let i = 0; i < SHARD_COUNT; i++) {
      mockShards.push(createMockShard());
    }
    ctx.shards = mockShards as any;

    const job = fakeJob({
      id: jobId('rr-unique'),
      queue: 'orders',
      uniqueKey: 'order-123',
      groupId: 'batch-A',
    });

    const idx = shardIndex('orders');
    const byQueueShard = new Map<number, Job[]>();
    byQueueShard.set(idx, [job]);

    await releaseResources(byQueueShard, ctx);

    expect(mockShards[idx].releaseJobResources).toHaveBeenCalledTimes(1);
    expect(mockShards[idx].releaseJobResources).toHaveBeenCalledWith(
      'orders',
      'order-123',
      'batch-A',
      'rr-unique'
    );
  });

  test('works with empty input', async () => {
    const ctx = createBatchContext();
    const mockShards: ReturnType<typeof createMockShard>[] = [];
    for (let i = 0; i < SHARD_COUNT; i++) {
      mockShards.push(createMockShard());
    }
    ctx.shards = mockShards as any;

    const byQueueShard = new Map<number, Job[]>();
    await releaseResources(byQueueShard, ctx);

    // No shard should have been called
    for (const shard of mockShards) {
      expect(shard.releaseJobResources).not.toHaveBeenCalled();
    }
  });
});

// ============================================================================
// finalizeBatchAck
// ============================================================================

describe('finalizeBatchAck', () => {
  let finalizeCtx: FinalizeContext;

  beforeEach(() => {
    finalizeCtx = createFinalizeContext();
  });

  test('updates totalCompleted counter', () => {
    const id1 = jobId('fc-1');
    const id2 = jobId('fc-2');
    const extracted: ExtractedJob[] = [
      { id: id1, job: fakeJob({ id: id1, queue: 'q1' }) },
      { id: id2, job: fakeJob({ id: id2, queue: 'q2' }) },
    ];

    finalizeBatchAck(extracted, finalizeCtx, false);

    expect(finalizeCtx.totalCompleted.value).toBe(2n);
  });

  test('increments totalCompleted across multiple calls', () => {
    const id1 = jobId('tc-1');
    const id2 = jobId('tc-2');
    const id3 = jobId('tc-3');

    finalizeBatchAck([{ id: id1, job: fakeJob({ id: id1, queue: 'q' }) }], finalizeCtx, false);
    finalizeBatchAck(
      [
        { id: id2, job: fakeJob({ id: id2, queue: 'q' }) },
        { id: id3, job: fakeJob({ id: id3, queue: 'q' }) },
      ],
      finalizeCtx,
      false
    );

    expect(finalizeCtx.totalCompleted.value).toBe(3n);
  });

  test('adds jobs to completedJobs set', () => {
    const id1 = jobId('cj-1');
    const id2 = jobId('cj-2');
    const extracted: ExtractedJob[] = [
      { id: id1, job: fakeJob({ id: id1, queue: 'q1' }) },
      { id: id2, job: fakeJob({ id: id2, queue: 'q2' }) },
    ];

    finalizeBatchAck(extracted, finalizeCtx, false);

    expect(finalizeCtx.completedJobs.has(id1)).toBe(true);
    expect(finalizeCtx.completedJobs.has(id2)).toBe(true);
  });

  test('sets jobIndex to completed location', () => {
    const id1 = jobId('ji-1');
    const extracted: ExtractedJob[] = [{ id: id1, job: fakeJob({ id: id1, queue: 'q1' }) }];

    finalizeBatchAck(extracted, finalizeCtx, false);

    const location = finalizeCtx.jobIndex.get(id1);
    expect(location).toBeDefined();
    expect(location!.type).toBe('completed');
  });

  test('stores results when includeResults=true', () => {
    const id1 = jobId('ir-1');
    const extracted: ExtractedJob<{ sent: boolean }>[] = [
      { id: id1, job: fakeJob({ id: id1, queue: 'q1' }), result: { sent: true } },
    ];

    finalizeBatchAck(extracted, finalizeCtx, true);

    expect(finalizeCtx.jobResults.has(id1)).toBe(true);
    expect(finalizeCtx.jobResults.get(id1)).toEqual({ sent: true });
  });

  test('does NOT store results when includeResults=false', () => {
    const id1 = jobId('nir-1');
    const extracted: ExtractedJob<string>[] = [
      { id: id1, job: fakeJob({ id: id1, queue: 'q1' }), result: 'some-result' },
    ];

    finalizeBatchAck(extracted, finalizeCtx, false);

    expect(finalizeCtx.jobResults.has(id1)).toBe(false);
  });

  test('does not store result when result is undefined even with includeResults=true', () => {
    const id1 = jobId('undef-result-1');
    const extracted: ExtractedJob[] = [
      { id: id1, job: fakeJob({ id: id1, queue: 'q1' }) },
      // result is undefined by default
    ];

    finalizeBatchAck(extracted, finalizeCtx, true);

    expect(finalizeCtx.jobResults.has(id1)).toBe(false);
    // But job should still be completed
    expect(finalizeCtx.completedJobs.has(id1)).toBe(true);
  });

  test('broadcasts completed events when needsBroadcast returns true', () => {
    const id1 = jobId('bc-1');
    const id2 = jobId('bc-2');

    finalizeCtx = createFinalizeContext({
      needsBroadcast: () => true,
    });

    const extracted: ExtractedJob<string>[] = [
      { id: id1, job: fakeJob({ id: id1, queue: 'emails' }), result: 'ok' },
      { id: id2, job: fakeJob({ id: id2, queue: 'payments' }) },
    ];

    finalizeBatchAck(extracted, finalizeCtx, true);

    expect(finalizeCtx.broadcast).toHaveBeenCalledTimes(2);

    const calls = (finalizeCtx.broadcast as any).mock.calls;
    // First call
    expect(calls[0][0].eventType).toBe('completed');
    expect(calls[0][0].queue).toBe('emails');
    expect(calls[0][0].jobId).toBe(id1);
    expect(calls[0][0].data).toBe('ok');
    // Second call
    expect(calls[1][0].eventType).toBe('completed');
    expect(calls[1][0].queue).toBe('payments');
    expect(calls[1][0].jobId).toBe(id2);
    expect(calls[1][0].data).toBeUndefined();
  });

  test('skips broadcast when needsBroadcast returns false', () => {
    const id1 = jobId('nb-1');

    finalizeCtx = createFinalizeContext({
      needsBroadcast: () => false,
    });

    const extracted: ExtractedJob[] = [{ id: id1, job: fakeJob({ id: id1, queue: 'q1' }) }];

    finalizeBatchAck(extracted, finalizeCtx, false);

    expect(finalizeCtx.broadcast).not.toHaveBeenCalled();
  });

  test('broadcasts by default when needsBroadcast is not provided', () => {
    const id1 = jobId('default-bc-1');

    // needsBroadcast is not set, should default to true
    const extracted: ExtractedJob[] = [{ id: id1, job: fakeJob({ id: id1, queue: 'q1' }) }];

    finalizeBatchAck(extracted, finalizeCtx, false);

    expect(finalizeCtx.broadcast).toHaveBeenCalledTimes(1);
  });

  test('calls onJobsCompleted with batch when available and hasPendingDeps', () => {
    const id1 = jobId('ojc-1');
    const id2 = jobId('ojc-2');

    const onJobsCompleted = mock(() => {});
    finalizeCtx = createFinalizeContext({
      onJobsCompleted,
      hasPendingDeps: () => true,
    });

    const extracted: ExtractedJob[] = [
      { id: id1, job: fakeJob({ id: id1, queue: 'q1' }) },
      { id: id2, job: fakeJob({ id: id2, queue: 'q2' }) },
    ];

    finalizeBatchAck(extracted, finalizeCtx, false);

    expect(onJobsCompleted).toHaveBeenCalledTimes(1);
    expect(onJobsCompleted).toHaveBeenCalledWith([id1, id2]);
    // Individual onJobCompleted should NOT be called
    expect(finalizeCtx.onJobCompleted).not.toHaveBeenCalled();
  });

  test('falls back to individual onJobCompleted calls when onJobsCompleted is not available', () => {
    const id1 = jobId('ind-1');
    const id2 = jobId('ind-2');

    finalizeCtx = createFinalizeContext({
      hasPendingDeps: () => true,
      // onJobsCompleted not set
    });

    const extracted: ExtractedJob[] = [
      { id: id1, job: fakeJob({ id: id1, queue: 'q1' }) },
      { id: id2, job: fakeJob({ id: id2, queue: 'q2' }) },
    ];

    finalizeBatchAck(extracted, finalizeCtx, false);

    expect(finalizeCtx.onJobCompleted).toHaveBeenCalledTimes(2);
    const calls = (finalizeCtx.onJobCompleted as any).mock.calls;
    expect(calls[0][0]).toBe(id1);
    expect(calls[1][0]).toBe(id2);
  });

  test('does not call onJobCompleted or onJobsCompleted when hasPendingDeps returns false', () => {
    const id1 = jobId('no-deps-1');

    const onJobsCompleted = mock(() => {});
    finalizeCtx = createFinalizeContext({
      onJobsCompleted,
      hasPendingDeps: () => false,
    });

    const extracted: ExtractedJob[] = [{ id: id1, job: fakeJob({ id: id1, queue: 'q1' }) }];

    finalizeBatchAck(extracted, finalizeCtx, false);

    expect(onJobsCompleted).not.toHaveBeenCalled();
    expect(finalizeCtx.onJobCompleted).not.toHaveBeenCalled();
  });

  test('defaults hasPendingDeps to true when not provided', () => {
    const id1 = jobId('def-deps-1');

    // hasPendingDeps not set, should default to true
    const extracted: ExtractedJob[] = [{ id: id1, job: fakeJob({ id: id1, queue: 'q1' }) }];

    finalizeBatchAck(extracted, finalizeCtx, false);

    // Should have called onJobCompleted since hasPendingDeps defaults to true
    // and onJobsCompleted is not provided
    expect(finalizeCtx.onJobCompleted).toHaveBeenCalledTimes(1);
    expect(finalizeCtx.onJobCompleted).toHaveBeenCalledWith(id1);
  });

  test('handles removeOnComplete=true (deletes from jobIndex)', () => {
    const id1 = jobId('roc-1');
    const job1 = fakeJob({ id: id1, queue: 'q1', removeOnComplete: true });

    // Pre-populate jobIndex to verify deletion
    finalizeCtx.jobIndex.set(id1, { type: 'processing', shardIdx: 0, queueName: 'q1' });

    const extracted: ExtractedJob[] = [{ id: id1, job: job1 }];

    finalizeBatchAck(extracted, finalizeCtx, false);

    // Should be deleted from jobIndex, not set to completed
    expect(finalizeCtx.jobIndex.has(id1)).toBe(false);
    // Should NOT be added to completedJobs
    expect(finalizeCtx.completedJobs.has(id1)).toBe(false);
    // The payload-free proof still allows a dependent to observe completion.
    expect(finalizeCtx.depCompletions?.has(id1)).toBe(true);
  });

  test('removeOnComplete=true does not store result even with includeResults=true', () => {
    const id1 = jobId('roc-noresult-1');
    const job1 = fakeJob({ id: id1, queue: 'q1', removeOnComplete: true });

    const extracted: ExtractedJob<string>[] = [
      { id: id1, job: job1, result: 'should-not-be-stored' },
    ];

    finalizeBatchAck(extracted, finalizeCtx, true);

    expect(finalizeCtx.jobResults.has(id1)).toBe(false);
    expect(finalizeCtx.depCompletions?.has(id1)).toBe(true);
  });

  test('cleans up customIdMap entries', () => {
    const id1 = jobId('cid-1');
    const id2 = jobId('cid-2');
    const job1 = fakeJob({ id: id1, queue: 'q1', customId: 'custom-abc' });
    const job2 = fakeJob({ id: id2, queue: 'q2', customId: null });

    const customIdMap = new Map<string, JobId>();
    customIdMap.set('custom-abc', id1);

    finalizeCtx = createFinalizeContext({
      customIdMap: customIdMap as any,
    });

    const extracted: ExtractedJob[] = [
      { id: id1, job: job1 },
      { id: id2, job: job2 },
    ];

    finalizeBatchAck(extracted, finalizeCtx, false);

    // custom-abc should be deleted
    expect(customIdMap.has('custom-abc')).toBe(false);
  });

  test('does not fail when customIdMap is undefined', () => {
    const id1 = jobId('no-cim-1');
    const job1 = fakeJob({ id: id1, queue: 'q1', customId: 'some-custom' });

    finalizeCtx = createFinalizeContext();
    // customIdMap is not set (undefined)

    const extracted: ExtractedJob[] = [{ id: id1, job: job1 }];

    // Should not throw
    expect(() => finalizeBatchAck(extracted, finalizeCtx, false)).not.toThrow();
  });

  test('handles empty input array', () => {
    const extracted: ExtractedJob[] = [];

    finalizeBatchAck(extracted, finalizeCtx, false);

    expect(finalizeCtx.totalCompleted.value).toBe(0n);
    expect(finalizeCtx.broadcast).not.toHaveBeenCalled();
    expect(finalizeCtx.onJobCompleted).not.toHaveBeenCalled();
  });

  test('mixed removeOnComplete jobs: some kept, some deleted', () => {
    const idKeep = jobId('mix-keep');
    const idRemove = jobId('mix-remove');
    const jobKeep = fakeJob({ id: idKeep, queue: 'q1', removeOnComplete: false });
    const jobRemove = fakeJob({ id: idRemove, queue: 'q2', removeOnComplete: true });

    const extracted: ExtractedJob<number>[] = [
      { id: idKeep, job: jobKeep, result: 42 },
      { id: idRemove, job: jobRemove, result: 99 },
    ];

    finalizeBatchAck(extracted, finalizeCtx, true);

    // kept job should be in completedJobs and jobIndex
    expect(finalizeCtx.completedJobs.has(idKeep)).toBe(true);
    expect(finalizeCtx.jobIndex.get(idKeep)).toEqual({ type: 'completed', queueName: 'q1' });
    expect(finalizeCtx.jobResults.get(idKeep)).toBe(42);

    // removed job should not be in completedJobs or jobIndex
    expect(finalizeCtx.completedJobs.has(idRemove)).toBe(false);
    expect(finalizeCtx.jobIndex.has(idRemove)).toBe(false);
    expect(finalizeCtx.jobResults.has(idRemove)).toBe(false);
    expect(finalizeCtx.depCompletions?.has(idRemove)).toBe(true);

    // totalCompleted should still count both
    expect(finalizeCtx.totalCompleted.value).toBe(2n);
  });

  test('broadcast includes result data only when includeResults is true', () => {
    const id1 = jobId('bd-1');

    finalizeCtx = createFinalizeContext({
      needsBroadcast: () => true,
    });

    const extracted: ExtractedJob<string>[] = [
      { id: id1, job: fakeJob({ id: id1, queue: 'q1' }), result: 'the-result' },
    ];

    // includeResults = false
    finalizeBatchAck(extracted, finalizeCtx, false);

    const call1 = (finalizeCtx.broadcast as any).mock.calls[0][0];
    expect(call1.data).toBeUndefined();

    // Reset and test with includeResults = true
    finalizeCtx = createFinalizeContext({
      needsBroadcast: () => true,
    });

    finalizeBatchAck(extracted, finalizeCtx, true);

    const call2 = (finalizeCtx.broadcast as any).mock.calls[0][0];
    expect(call2.data).toBe('the-result');
  });
});
