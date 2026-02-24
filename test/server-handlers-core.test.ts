/**
 * Server Handlers Tests
 * Tests for core.ts, query.ts, management.ts, and advanced.ts command handlers
 * Tests handlers by calling them directly with a real QueueManager
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import type { HandlerContext } from '../src/infrastructure/server/types';

// Core handlers
import {
  handlePush,
  handlePushBatch,
  handlePull,
  handlePullBatch,
  handleAck,
  handleAckBatch,
  handleFail,
} from '../src/infrastructure/server/handlers/core';

// Query handlers
import {
  handleGetJob,
  handleGetState,
  handleGetResult,
  handleGetJobCounts,
  handleGetJobByCustomId,
  handleGetJobs,
} from '../src/infrastructure/server/handlers/query';

// Management handlers
import {
  handleCancel,
  handleProgress,
  handleGetProgress,
  handlePause,
  handleResume,
  handleDrain,
  handleStats,
  handleMetrics,
} from '../src/infrastructure/server/handlers/management';

// Advanced handlers
import {
  handleUpdate,
  handleChangePriority,
  handlePromote,
  handleDiscard,
  handleCount,
} from '../src/infrastructure/server/handlers/advanced';

// Monitoring handlers
import { handlePing } from '../src/infrastructure/server/handlers/monitoring';

// Handler router
import { handleCommand } from '../src/infrastructure/server/handler';

function createContext(qm: QueueManager): HandlerContext {
  return {
    queueManager: qm,
    authTokens: new Set<string>(),
    authenticated: false,
    clientId: 'test-client-1',
  };
}

// ============================================================
// CORE HANDLERS
// ============================================================

describe('Core Handlers', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  // ---- PUSH ----

  describe('handlePush', () => {
    test('should push a job and return ok with id', async () => {
      const res = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { to: 'user@test.com' } },
        ctx
      );
      expect(res.ok).toBe(true);
      expect((res as any).id).toBeDefined();
    });

    test('should push with priority', async () => {
      const res = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'hi' }, priority: 10 },
        ctx
      );
      expect(res.ok).toBe(true);
      const id = (res as any).id;
      const job = await qm.getJob(id);
      expect(job?.priority).toBe(10);
    });

    test('should push with delay', async () => {
      const before = Date.now();
      const res = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'hi' }, delay: 5000 },
        ctx
      );
      expect(res.ok).toBe(true);
      const id = (res as any).id;
      const job = await qm.getJob(id);
      expect(job?.runAt).toBeGreaterThanOrEqual(before + 5000);
    });

    test('should push with custom jobId', async () => {
      const res = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'hi' }, jobId: 'custom-123' },
        ctx
      );
      expect(res.ok).toBe(true);
      // Should be retrievable by custom id
      const job = qm.getJobByCustomId('custom-123');
      expect(job).toBeDefined();
    });

    test('should push with tags', async () => {
      const res = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'hi' }, tags: ['urgent', 'vip'] },
        ctx
      );
      expect(res.ok).toBe(true);
      const id = (res as any).id;
      const job = await qm.getJob(id);
      expect(job?.tags).toEqual(['urgent', 'vip']);
    });

    test('should push with maxAttempts', async () => {
      const res = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'hi' }, maxAttempts: 5 },
        ctx
      );
      expect(res.ok).toBe(true);
      const id = (res as any).id;
      const job = await qm.getJob(id);
      expect(job?.maxAttempts).toBe(5);
    });

    test('should return error for empty queue name', async () => {
      const res = await handlePush(
        { cmd: 'PUSH', queue: '', data: { msg: 'hi' } },
        ctx
      );
      expect(res.ok).toBe(false);
      expect((res as any).error).toBeDefined();
    });

    test('should return error for invalid queue name characters', async () => {
      const res = await handlePush(
        { cmd: 'PUSH', queue: 'invalid queue!', data: { msg: 'hi' } },
        ctx
      );
      expect(res.ok).toBe(false);
    });

    test('should return error for queue name exceeding max length', async () => {
      const res = await handlePush(
        { cmd: 'PUSH', queue: 'a'.repeat(257), data: { msg: 'hi' } },
        ctx
      );
      expect(res.ok).toBe(false);
    });

    test('should return error for negative delay', async () => {
      const res = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'hi' }, delay: -1 },
        ctx
      );
      expect(res.ok).toBe(false);
    });

    test('should return error for non-integer priority when validation enforces integer', async () => {
      const res = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'hi' }, priority: 1.5 },
        ctx
      );
      expect(res.ok).toBe(false);
    });

    test('should return error for maxAttempts < 1', async () => {
      const res = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'hi' }, maxAttempts: 0 },
        ctx
      );
      expect(res.ok).toBe(false);
    });

    test('should propagate reqId in response', async () => {
      const res = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'hi' }, reqId: 'req-42' },
        ctx,
        'req-42'
      );
      expect(res.ok).toBe(true);
      expect(res.reqId).toBe('req-42');
    });

    test('should return error for dependsOn referencing non-existent job', async () => {
      const res = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'hi' }, dependsOn: ['non-existent-id'] },
        ctx
      );
      expect(res.ok).toBe(false);
      expect((res as any).error).toContain('Dependency job not found');
    });

    test('should push with uniqueKey and deduplicate', async () => {
      const res1 = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'first' }, uniqueKey: 'dedup-1' },
        ctx
      );
      expect(res1.ok).toBe(true);
      const id1 = (res1 as any).id;

      const res2 = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'second' }, uniqueKey: 'dedup-1' },
        ctx
      );
      expect(res2.ok).toBe(true);
      const id2 = (res2 as any).id;
      expect(id2).toBe(id1);
    });
  });

  // ---- PUSH BATCH ----

  describe('handlePushBatch', () => {
    test('should push multiple jobs and return ids', async () => {
      const res = await handlePushBatch(
        {
          cmd: 'PUSHB',
          queue: 'emails',
          jobs: [
            { data: { id: 1 } },
            { data: { id: 2 } },
            { data: { id: 3 } },
          ],
        },
        ctx
      );
      expect(res.ok).toBe(true);
      expect((res as any).ids).toHaveLength(3);
    });

    test('should return error for invalid queue name', async () => {
      const res = await handlePushBatch(
        { cmd: 'PUSHB', queue: '', jobs: [{ data: { id: 1 } }] },
        ctx
      );
      expect(res.ok).toBe(false);
    });

    test('should push batch with priorities', async () => {
      const res = await handlePushBatch(
        {
          cmd: 'PUSHB',
          queue: 'emails',
          jobs: [
            { data: { id: 1 }, priority: 10 },
            { data: { id: 2 }, priority: 5 },
          ],
        },
        ctx
      );
      expect(res.ok).toBe(true);
      expect((res as any).ids).toHaveLength(2);
    });

    test('should propagate reqId', async () => {
      const res = await handlePushBatch(
        { cmd: 'PUSHB', queue: 'emails', jobs: [{ data: { id: 1 } }], reqId: 'req-batch' },
        ctx,
        'req-batch'
      );
      expect(res.reqId).toBe('req-batch');
    });

    test('should deduplicate jobs with same customId in batch', async () => {
      const res = await handlePushBatch(
        {
          cmd: 'PUSHB',
          queue: 'dedup-test',
          jobs: [
            { data: { id: 1 }, customId: 'same-key' },
            { data: { id: 2 }, customId: 'same-key' },
            { data: { id: 3 }, customId: 'same-key' },
          ],
        },
        ctx
      );
      expect(res.ok).toBe(true);
      const ids = (res as any).ids as string[];
      expect(ids).toHaveLength(3);
      // All IDs should be the same (deduplicated)
      expect(ids[0]).toBe(ids[1]);
      expect(ids[1]).toBe(ids[2]);
    });

    test('should deduplicate across separate PUSHB calls with same customId', async () => {
      const res1 = await handlePushBatch(
        {
          cmd: 'PUSHB',
          queue: 'dedup-test-2',
          jobs: [{ data: { id: 1 }, customId: 'cross-batch-key' }],
        },
        ctx
      );
      const res2 = await handlePushBatch(
        {
          cmd: 'PUSHB',
          queue: 'dedup-test-2',
          jobs: [{ data: { id: 2 }, customId: 'cross-batch-key' }],
        },
        ctx
      );
      expect(res1.ok).toBe(true);
      expect(res2.ok).toBe(true);
      const ids1 = (res1 as any).ids as string[];
      const ids2 = (res2 as any).ids as string[];
      expect(ids1[0]).toBe(ids2[0]);
    });
  });

  // ---- PULL ----

  describe('handlePull', () => {
    test('should pull a job from queue', async () => {
      await handlePush({ cmd: 'PUSH', queue: 'emails', data: { msg: 'test' } }, ctx);
      const res = await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).job).toBeDefined();
      expect((res as any).job).not.toBeNull();
    });

    test('should return null job when queue is empty', async () => {
      const res = await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).job).toBeNull();
    });

    test('should return error for invalid queue name', async () => {
      const res = await handlePull({ cmd: 'PULL', queue: '' }, ctx);
      expect(res.ok).toBe(false);
    });

    test('should return error for negative timeout', async () => {
      const res = await handlePull({ cmd: 'PULL', queue: 'emails', timeout: -1 }, ctx);
      expect(res.ok).toBe(false);
    });

    test('should return error for timeout exceeding max', async () => {
      const res = await handlePull({ cmd: 'PULL', queue: 'emails', timeout: 70000 }, ctx);
      expect(res.ok).toBe(false);
    });

    test('should pull with lock when owner is specified', async () => {
      await handlePush({ cmd: 'PUSH', queue: 'emails', data: { msg: 'lock-test' } }, ctx);
      const res = await handlePull(
        { cmd: 'PULL', queue: 'emails', owner: 'worker-1' },
        ctx
      );
      expect(res.ok).toBe(true);
      expect((res as any).job).not.toBeNull();
      expect((res as any).token).toBeDefined();
    });

    test('should propagate reqId', async () => {
      const res = await handlePull(
        { cmd: 'PULL', queue: 'emails', reqId: 'req-pull' },
        ctx,
        'req-pull'
      );
      expect(res.reqId).toBe('req-pull');
    });
  });

  // ---- PULL BATCH ----

  describe('handlePullBatch', () => {
    test('should pull multiple jobs', async () => {
      for (let i = 0; i < 5; i++) {
        await handlePush({ cmd: 'PUSH', queue: 'emails', data: { id: i } }, ctx);
      }
      const res = await handlePullBatch(
        { cmd: 'PULLB', queue: 'emails', count: 3 },
        ctx
      );
      expect(res.ok).toBe(true);
      expect((res as any).jobs).toHaveLength(3);
    });

    test('should return empty array when queue is empty', async () => {
      const res = await handlePullBatch(
        { cmd: 'PULLB', queue: 'emails', count: 5 },
        ctx
      );
      expect(res.ok).toBe(true);
      expect((res as any).jobs).toHaveLength(0);
    });

    test('should return error for invalid queue name', async () => {
      const res = await handlePullBatch(
        { cmd: 'PULLB', queue: '', count: 5 },
        ctx
      );
      expect(res.ok).toBe(false);
    });

    test('should return error for count < 1', async () => {
      const res = await handlePullBatch(
        { cmd: 'PULLB', queue: 'emails', count: 0 },
        ctx
      );
      expect(res.ok).toBe(false);
    });

    test('should return error for count > 1000', async () => {
      const res = await handlePullBatch(
        { cmd: 'PULLB', queue: 'emails', count: 1001 },
        ctx
      );
      expect(res.ok).toBe(false);
    });

    test('should pull batch with lock when owner is specified', async () => {
      for (let i = 0; i < 3; i++) {
        await handlePush({ cmd: 'PUSH', queue: 'emails', data: { id: i } }, ctx);
      }
      const res = await handlePullBatch(
        { cmd: 'PULLB', queue: 'emails', count: 2, owner: 'worker-1' },
        ctx
      );
      expect(res.ok).toBe(true);
      expect((res as any).jobs).toHaveLength(2);
      expect((res as any).tokens).toHaveLength(2);
    });

    test('should return fewer jobs than requested when queue has less', async () => {
      await handlePush({ cmd: 'PUSH', queue: 'emails', data: { id: 1 } }, ctx);
      const res = await handlePullBatch(
        { cmd: 'PULLB', queue: 'emails', count: 10 },
        ctx
      );
      expect(res.ok).toBe(true);
      expect((res as any).jobs).toHaveLength(1);
    });
  });

  // ---- ACK ----

  describe('handleAck', () => {
    test('should ack a pulled job', async () => {
      await handlePush({ cmd: 'PUSH', queue: 'emails', data: { msg: 'test' } }, ctx);
      const pullRes = await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
      const jobId = (pullRes as any).job.id;

      const res = await handleAck({ cmd: 'ACK', id: jobId }, ctx);
      expect(res.ok).toBe(true);
    });

    test('should ack with result', async () => {
      await handlePush({ cmd: 'PUSH', queue: 'emails', data: { msg: 'test' } }, ctx);
      const pullRes = await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
      const jobId = (pullRes as any).job.id;

      const res = await handleAck(
        { cmd: 'ACK', id: jobId, result: { sent: true } },
        ctx
      );
      expect(res.ok).toBe(true);

      // Result should be stored
      const result = qm.getResult(jobId);
      expect(result).toEqual({ sent: true });
    });

    test('should return error for non-existent job', async () => {
      const res = await handleAck({ cmd: 'ACK', id: 'non-existent' }, ctx);
      expect(res.ok).toBe(false);
    });

    test('should propagate reqId', async () => {
      await handlePush({ cmd: 'PUSH', queue: 'emails', data: { msg: 'test' } }, ctx);
      const pullRes = await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
      const jobId = (pullRes as any).job.id;

      const res = await handleAck(
        { cmd: 'ACK', id: jobId, reqId: 'req-ack' },
        ctx,
        'req-ack'
      );
      expect(res.reqId).toBe('req-ack');
    });
  });

  // ---- ACK BATCH ----

  describe('handleAckBatch', () => {
    test('should ack multiple pulled jobs', async () => {
      for (let i = 0; i < 3; i++) {
        await handlePush({ cmd: 'PUSH', queue: 'emails', data: { id: i } }, ctx);
      }
      const pullRes = await handlePullBatch(
        { cmd: 'PULLB', queue: 'emails', count: 3 },
        ctx
      );
      const ids = (pullRes as any).jobs.map((j: any) => j.id);

      const res = await handleAckBatch({ cmd: 'ACKB', ids }, ctx);
      expect(res.ok).toBe(true);
    });

    test('should ack batch with results', async () => {
      for (let i = 0; i < 2; i++) {
        await handlePush({ cmd: 'PUSH', queue: 'emails', data: { id: i } }, ctx);
      }
      const pullRes = await handlePullBatch(
        { cmd: 'PULLB', queue: 'emails', count: 2 },
        ctx
      );
      const ids = (pullRes as any).jobs.map((j: any) => j.id);

      const res = await handleAckBatch(
        { cmd: 'ACKB', ids, results: [{ success: true }, { success: false }] },
        ctx
      );
      expect(res.ok).toBe(true);

      // Results should be stored
      const result0 = qm.getResult(ids[0]);
      expect(result0).toEqual({ success: true });
      const result1 = qm.getResult(ids[1]);
      expect(result1).toEqual({ success: false });
    });

    test('should propagate reqId', async () => {
      await handlePush({ cmd: 'PUSH', queue: 'emails', data: { msg: 'test' } }, ctx);
      const pullRes = await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
      const jobId = (pullRes as any).job.id;

      const res = await handleAckBatch(
        { cmd: 'ACKB', ids: [jobId], reqId: 'req-ackb' },
        ctx,
        'req-ackb'
      );
      expect(res.reqId).toBe('req-ackb');
    });
  });

  // ---- FAIL ----

  describe('handleFail', () => {
    test('should fail a pulled job', async () => {
      await handlePush({ cmd: 'PUSH', queue: 'emails', data: { msg: 'test' } }, ctx);
      const pullRes = await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
      const jobId = (pullRes as any).job.id;

      const res = await handleFail(
        { cmd: 'FAIL', id: jobId, error: 'Something went wrong' },
        ctx
      );
      expect(res.ok).toBe(true);
    });

    test('should return error for non-existent job', async () => {
      const res = await handleFail(
        { cmd: 'FAIL', id: 'non-existent', error: 'Oops' },
        ctx
      );
      expect(res.ok).toBe(false);
    });

    test('should fail without error message', async () => {
      await handlePush({ cmd: 'PUSH', queue: 'emails', data: { msg: 'test' } }, ctx);
      const pullRes = await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
      const jobId = (pullRes as any).job.id;

      const res = await handleFail({ cmd: 'FAIL', id: jobId }, ctx);
      expect(res.ok).toBe(true);
    });

    test('should propagate reqId', async () => {
      await handlePush({ cmd: 'PUSH', queue: 'emails', data: { msg: 'test' } }, ctx);
      const pullRes = await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
      const jobId = (pullRes as any).job.id;

      const res = await handleFail(
        { cmd: 'FAIL', id: jobId, reqId: 'req-fail' },
        ctx,
        'req-fail'
      );
      expect(res.reqId).toBe('req-fail');
    });
  });
});

// ============================================================
// QUERY HANDLERS
// ============================================================

describe('Query Handlers', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  // ---- GetJob ----

  describe('handleGetJob', () => {
    test('should return job by id', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'hello' } },
        ctx
      );
      const jobId = (pushRes as any).id;

      const res = await handleGetJob({ cmd: 'GetJob', id: jobId }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).job).toBeDefined();
      expect((res as any).job.id).toBe(jobId);
      expect((res as any).job.data).toEqual({ msg: 'hello' });
    });

    test('should return error for non-existent job', async () => {
      const res = await handleGetJob({ cmd: 'GetJob', id: 'non-existent' }, ctx);
      expect(res.ok).toBe(false);
      expect((res as any).error).toBe('Job not found');
    });

    test('should propagate reqId', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'hello' } },
        ctx
      );
      const jobId = (pushRes as any).id;

      const res = await handleGetJob(
        { cmd: 'GetJob', id: jobId, reqId: 'req-getjob' },
        ctx,
        'req-getjob'
      );
      expect(res.reqId).toBe('req-getjob');
    });
  });

  // ---- GetState ----

  describe('handleGetState', () => {
    test('should return waiting state for pushed job', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'hello' } },
        ctx
      );
      const jobId = (pushRes as any).id;

      const res = await handleGetState({ cmd: 'GetState', id: jobId }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).state).toBe('waiting');
    });

    test('should return active state for pulled job', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'hello' } },
        ctx
      );
      const jobId = (pushRes as any).id;

      await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);

      const res = await handleGetState({ cmd: 'GetState', id: jobId }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).state).toBe('active');
    });

    test('should return completed state for acked job', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'hello' } },
        ctx
      );
      const jobId = (pushRes as any).id;

      await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
      await handleAck({ cmd: 'ACK', id: jobId }, ctx);

      const res = await handleGetState({ cmd: 'GetState', id: jobId }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).state).toBe('completed');
    });

    test('should propagate reqId', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: {} },
        ctx
      );
      const jobId = (pushRes as any).id;

      const res = await handleGetState(
        { cmd: 'GetState', id: jobId, reqId: 'req-state' },
        ctx,
        'req-state'
      );
      expect(res.reqId).toBe('req-state');
    });
  });

  // ---- GetResult ----

  describe('handleGetResult', () => {
    test('should return result for completed job', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'test' } },
        ctx
      );
      const jobId = (pushRes as any).id;

      await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
      await handleAck({ cmd: 'ACK', id: jobId, result: { sent: true } }, ctx);

      const res = handleGetResult({ cmd: 'GetResult', id: jobId }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).result).toEqual({ sent: true });
    });

    test('should return undefined result for job without result', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'test' } },
        ctx
      );
      const jobId = (pushRes as any).id;

      const res = handleGetResult({ cmd: 'GetResult', id: jobId }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).result).toBeUndefined();
    });

    test('should propagate reqId', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: {} },
        ctx
      );
      const res = handleGetResult(
        { cmd: 'GetResult', id: (pushRes as any).id, reqId: 'req-result' },
        ctx,
        'req-result'
      );
      expect(res.reqId).toBe('req-result');
    });
  });

  // ---- GetJobCounts ----

  describe('handleGetJobCounts', () => {
    test('should return zero counts for empty queue', () => {
      const res = handleGetJobCounts(
        { cmd: 'GetJobCounts', queue: 'emails' },
        ctx
      );
      expect(res.ok).toBe(true);
      const counts = (res as any).counts;
      expect(counts.waiting).toBe(0);
      expect(counts.active).toBe(0);
      expect(counts.completed).toBe(0);
      expect(counts.failed).toBe(0);
      expect(counts.delayed).toBe(0);
    });

    test('should return correct counts after push and pull', async () => {
      // Push 3 jobs
      for (let i = 0; i < 3; i++) {
        await handlePush({ cmd: 'PUSH', queue: 'emails', data: { id: i } }, ctx);
      }

      let res = handleGetJobCounts({ cmd: 'GetJobCounts', queue: 'emails' }, ctx);
      expect((res as any).counts.waiting).toBe(3);

      // Pull 1
      await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
      res = handleGetJobCounts({ cmd: 'GetJobCounts', queue: 'emails' }, ctx);
      expect((res as any).counts.waiting).toBe(2);
      expect((res as any).counts.active).toBe(1);
    });

    test('should propagate reqId', () => {
      const res = handleGetJobCounts(
        { cmd: 'GetJobCounts', queue: 'emails', reqId: 'req-counts' },
        ctx,
        'req-counts'
      );
      expect(res.reqId).toBe('req-counts');
    });
  });

  // ---- GetJobByCustomId ----

  describe('handleGetJobByCustomId', () => {
    test('should return job by custom id', async () => {
      await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'hello' }, jobId: 'my-custom-id' },
        ctx
      );

      const res = handleGetJobByCustomId(
        { cmd: 'GetJobByCustomId', customId: 'my-custom-id' },
        ctx
      );
      expect(res.ok).toBe(true);
      expect((res as any).job).toBeDefined();
      expect((res as any).job.data).toEqual({ msg: 'hello' });
    });

    test('should return error for non-existent custom id', () => {
      const res = handleGetJobByCustomId(
        { cmd: 'GetJobByCustomId', customId: 'non-existent' },
        ctx
      );
      expect(res.ok).toBe(false);
      expect((res as any).error).toBe('Job not found');
    });
  });

  // ---- GetJobs ----

  describe('handleGetJobs', () => {
    test('should return jobs for queue', async () => {
      for (let i = 0; i < 5; i++) {
        await handlePush({ cmd: 'PUSH', queue: 'emails', data: { id: i } }, ctx);
      }

      const res = handleGetJobs(
        { cmd: 'GetJobs', queue: 'emails' },
        ctx
      );
      expect(res.ok).toBe(true);
      expect((res as any).jobs.length).toBe(5);
    });

    test('should filter by state', async () => {
      for (let i = 0; i < 3; i++) {
        await handlePush({ cmd: 'PUSH', queue: 'emails', data: { id: i } }, ctx);
      }
      // Pull one job to make it active
      await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);

      const waitingRes = handleGetJobs(
        { cmd: 'GetJobs', queue: 'emails', state: 'waiting' },
        ctx
      );
      expect((waitingRes as any).jobs.length).toBe(2);

      const activeRes = handleGetJobs(
        { cmd: 'GetJobs', queue: 'emails', state: 'active' },
        ctx
      );
      expect((activeRes as any).jobs.length).toBe(1);
    });

    test('should paginate with offset and limit', async () => {
      for (let i = 0; i < 10; i++) {
        await handlePush({ cmd: 'PUSH', queue: 'emails', data: { id: i } }, ctx);
      }

      const res = handleGetJobs(
        { cmd: 'GetJobs', queue: 'emails', offset: 2, limit: 3 },
        ctx
      );
      expect(res.ok).toBe(true);
      expect((res as any).jobs.length).toBe(3);
    });

    test('should return empty array for empty queue', () => {
      const res = handleGetJobs(
        { cmd: 'GetJobs', queue: 'non-existent' },
        ctx
      );
      expect(res.ok).toBe(true);
      expect((res as any).jobs).toHaveLength(0);
    });

    test('should propagate reqId', () => {
      const res = handleGetJobs(
        { cmd: 'GetJobs', queue: 'emails', reqId: 'req-getjobs' },
        ctx,
        'req-getjobs'
      );
      expect(res.reqId).toBe('req-getjobs');
    });
  });

  // ---- Count ----

  describe('handleCount', () => {
    test('should return 0 for empty queue', () => {
      const res = handleCount({ cmd: 'Count', queue: 'emails' }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).count).toBe(0);
    });

    test('should return correct count after pushing', async () => {
      for (let i = 0; i < 4; i++) {
        await handlePush({ cmd: 'PUSH', queue: 'emails', data: { id: i } }, ctx);
      }
      const res = handleCount({ cmd: 'Count', queue: 'emails' }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).count).toBe(4);
    });

    test('should propagate reqId', () => {
      const res = handleCount(
        { cmd: 'Count', queue: 'emails', reqId: 'req-count' },
        ctx,
        'req-count'
      );
      expect(res.reqId).toBe('req-count');
    });
  });

  // ---- GetProgress ----

  describe('handleGetProgress', () => {
    test('should return progress for active job', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'test' } },
        ctx
      );
      const jobId = (pushRes as any).id;

      await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);

      await handleProgress(
        { cmd: 'Progress', id: jobId, progress: 42, message: 'Step 2' },
        ctx
      );

      const res = handleGetProgress({ cmd: 'GetProgress', id: jobId }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).progress).toBe(42);
      expect((res as any).message).toBe('Step 2');
    });

    test('should return error for non-active job', () => {
      const res = handleGetProgress(
        { cmd: 'GetProgress', id: 'non-existent' },
        ctx
      );
      expect(res.ok).toBe(false);
    });

    test('should propagate reqId', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: {} },
        ctx
      );
      const jobId = (pushRes as any).id;
      await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
      await handleProgress({ cmd: 'Progress', id: jobId, progress: 10 }, ctx);

      const res = handleGetProgress(
        { cmd: 'GetProgress', id: jobId, reqId: 'req-prog' },
        ctx,
        'req-prog'
      );
      expect(res.reqId).toBe('req-prog');
    });
  });
});

// ============================================================
// MANAGEMENT HANDLERS
// ============================================================

describe('Management Handlers', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  // ---- Cancel ----

  describe('handleCancel', () => {
    test('should cancel a waiting job', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'cancel me' } },
        ctx
      );
      const jobId = (pushRes as any).id;

      const res = await handleCancel({ cmd: 'Cancel', id: jobId }, ctx);
      expect(res.ok).toBe(true);

      // Job should no longer be found
      const getRes = await handleGetJob({ cmd: 'GetJob', id: jobId }, ctx);
      expect(getRes.ok).toBe(false);
    });

    test('should return error for non-existent job', async () => {
      const res = await handleCancel({ cmd: 'Cancel', id: 'non-existent' }, ctx);
      expect(res.ok).toBe(false);
      expect((res as any).error).toContain('cannot be cancelled');
    });

    test('should propagate reqId', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: {} },
        ctx
      );
      const res = await handleCancel(
        { cmd: 'Cancel', id: (pushRes as any).id, reqId: 'req-cancel' },
        ctx,
        'req-cancel'
      );
      expect(res.reqId).toBe('req-cancel');
    });
  });

  // ---- Progress ----

  describe('handleProgress', () => {
    test('should update progress on active job', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'test' } },
        ctx
      );
      const jobId = (pushRes as any).id;
      await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);

      const res = await handleProgress(
        { cmd: 'Progress', id: jobId, progress: 75, message: 'Almost done' },
        ctx
      );
      expect(res.ok).toBe(true);
    });

    test('should return error for non-active job', async () => {
      const res = await handleProgress(
        { cmd: 'Progress', id: 'non-existent', progress: 50 },
        ctx
      );
      expect(res.ok).toBe(false);
    });

    test('should propagate reqId', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: {} },
        ctx
      );
      await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);

      const res = await handleProgress(
        { cmd: 'Progress', id: (pushRes as any).id, progress: 50, reqId: 'req-p' },
        ctx,
        'req-p'
      );
      expect(res.reqId).toBe('req-p');
    });
  });

  // ---- Pause / Resume ----

  describe('handlePause and handleResume', () => {
    test('should pause queue and prevent pulls', async () => {
      await handlePush({ cmd: 'PUSH', queue: 'emails', data: { msg: 'test' } }, ctx);

      const pauseRes = handlePause({ cmd: 'Pause', queue: 'emails' }, ctx);
      expect(pauseRes.ok).toBe(true);

      // Pull should return null for paused queue
      const pullRes = await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
      expect((pullRes as any).job).toBeNull();
    });

    test('should resume queue and allow pulls', async () => {
      await handlePush({ cmd: 'PUSH', queue: 'emails', data: { msg: 'test' } }, ctx);

      handlePause({ cmd: 'Pause', queue: 'emails' }, ctx);
      handleResume({ cmd: 'Resume', queue: 'emails' }, ctx);

      const pullRes = await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
      expect((pullRes as any).job).not.toBeNull();
    });

    test('should propagate reqId', () => {
      const pauseRes = handlePause(
        { cmd: 'Pause', queue: 'emails', reqId: 'req-pause' },
        ctx,
        'req-pause'
      );
      expect(pauseRes.reqId).toBe('req-pause');

      const resumeRes = handleResume(
        { cmd: 'Resume', queue: 'emails', reqId: 'req-resume' },
        ctx,
        'req-resume'
      );
      expect(resumeRes.reqId).toBe('req-resume');
    });
  });

  // ---- Drain ----

  describe('handleDrain', () => {
    test('should drain all waiting jobs from queue', async () => {
      for (let i = 0; i < 5; i++) {
        await handlePush({ cmd: 'PUSH', queue: 'emails', data: { id: i } }, ctx);
      }

      const res = handleDrain({ cmd: 'Drain', queue: 'emails' }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).count).toBe(5);

      // Queue should be empty
      const countRes = handleCount({ cmd: 'Count', queue: 'emails' }, ctx);
      expect((countRes as any).count).toBe(0);
    });

    test('should return 0 for empty queue', () => {
      const res = handleDrain({ cmd: 'Drain', queue: 'emails' }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).count).toBe(0);
    });

    test('should propagate reqId', () => {
      const res = handleDrain(
        { cmd: 'Drain', queue: 'emails', reqId: 'req-drain' },
        ctx,
        'req-drain'
      );
      expect(res.reqId).toBe('req-drain');
    });
  });

  // ---- Stats ----

  describe('handleStats', () => {
    test('should return stats with all fields', async () => {
      const res = handleStats(ctx);
      expect(res.ok).toBe(true);
      const stats = (res as any).stats;
      expect(stats).toBeDefined();
      expect(typeof stats.queued).toBe('number');
      expect(typeof stats.processing).toBe('number');
      expect(typeof stats.delayed).toBe('number');
      expect(typeof stats.completed).toBe('number');
      expect(typeof stats.uptime).toBe('number');
    });

    test('should reflect pushed jobs in stats', async () => {
      for (let i = 0; i < 3; i++) {
        await handlePush({ cmd: 'PUSH', queue: 'emails', data: { id: i } }, ctx);
      }
      const res = handleStats(ctx);
      expect((res as any).stats.queued).toBe(3);
    });

    test('should propagate reqId', () => {
      const res = handleStats(ctx, 'req-stats');
      expect(res.reqId).toBe('req-stats');
    });
  });

  // ---- Metrics ----

  describe('handleMetrics', () => {
    test('should return metrics with all fields', () => {
      const res = handleMetrics(ctx);
      expect(res.ok).toBe(true);
      const m = (res as any).metrics;
      expect(m).toBeDefined();
      expect(typeof m.totalPushed).toBe('number');
      expect(typeof m.totalPulled).toBe('number');
      expect(typeof m.totalCompleted).toBe('number');
      expect(typeof m.totalFailed).toBe('number');
      expect(typeof m.memoryUsageMb).toBe('number');
    });

    test('should propagate reqId', () => {
      const res = handleMetrics(ctx, 'req-metrics');
      expect(res.reqId).toBe('req-metrics');
    });
  });
});

// ============================================================
// ADVANCED HANDLERS (management operations from advanced.ts)
// ============================================================

describe('Advanced Handlers', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  // ---- Update ----

  describe('handleUpdate', () => {
    test('should update job data for waiting job', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'original' } },
        ctx
      );
      const jobId = (pushRes as any).id;

      const res = await handleUpdate(
        { cmd: 'Update', id: jobId, data: { msg: 'updated' } },
        ctx
      );
      expect(res.ok).toBe(true);

      // Verify data was updated
      const getRes = await handleGetJob({ cmd: 'GetJob', id: jobId }, ctx);
      expect((getRes as any).job.data).toEqual({ msg: 'updated' });
    });

    test('should return error for non-existent job', async () => {
      const res = await handleUpdate(
        { cmd: 'Update', id: 'non-existent', data: { msg: 'fail' } },
        ctx
      );
      expect(res.ok).toBe(false);
    });

    test('should propagate reqId', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: {} },
        ctx
      );
      const res = await handleUpdate(
        { cmd: 'Update', id: (pushRes as any).id, data: {}, reqId: 'req-upd' },
        ctx,
        'req-upd'
      );
      expect(res.reqId).toBe('req-upd');
    });
  });

  // ---- ChangePriority ----

  describe('handleChangePriority', () => {
    test('should change priority of waiting job', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'test' }, priority: 1 },
        ctx
      );
      const jobId = (pushRes as any).id;

      const res = await handleChangePriority(
        { cmd: 'ChangePriority', id: jobId, priority: 100 },
        ctx
      );
      expect(res.ok).toBe(true);

      // Verify priority was changed
      const getRes = await handleGetJob({ cmd: 'GetJob', id: jobId }, ctx);
      expect((getRes as any).job.priority).toBe(100);
    });

    test('should return error for non-existent job', async () => {
      const res = await handleChangePriority(
        { cmd: 'ChangePriority', id: 'non-existent', priority: 10 },
        ctx
      );
      expect(res.ok).toBe(false);
    });

    test('should propagate reqId', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: {} },
        ctx
      );
      const res = await handleChangePriority(
        { cmd: 'ChangePriority', id: (pushRes as any).id, priority: 5, reqId: 'req-cp' },
        ctx,
        'req-cp'
      );
      expect(res.reqId).toBe('req-cp');
    });
  });

  // ---- Promote ----

  describe('handlePromote', () => {
    test('should promote delayed job to waiting', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'delayed' }, delay: 60000 },
        ctx
      );
      const jobId = (pushRes as any).id;

      // Verify it's delayed
      const stateRes1 = await handleGetState({ cmd: 'GetState', id: jobId }, ctx);
      expect((stateRes1 as any).state).toBe('delayed');

      // Promote
      const res = await handlePromote({ cmd: 'Promote', id: jobId }, ctx);
      expect(res.ok).toBe(true);

      // Verify it's now waiting
      const stateRes2 = await handleGetState({ cmd: 'GetState', id: jobId }, ctx);
      expect((stateRes2 as any).state).toBe('waiting');
    });

    test('should return error for non-existent job', async () => {
      const res = await handlePromote({ cmd: 'Promote', id: 'non-existent' }, ctx);
      expect(res.ok).toBe(false);
    });

    test('should return error for non-delayed job', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'waiting' } },
        ctx
      );
      const jobId = (pushRes as any).id;

      const res = await handlePromote({ cmd: 'Promote', id: jobId }, ctx);
      expect(res.ok).toBe(false);
    });

    test('should propagate reqId', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: {}, delay: 60000 },
        ctx
      );
      const res = await handlePromote(
        { cmd: 'Promote', id: (pushRes as any).id, reqId: 'req-prom' },
        ctx,
        'req-prom'
      );
      expect(res.reqId).toBe('req-prom');
    });
  });

  // ---- Discard ----

  describe('handleDiscard', () => {
    test('should discard a waiting job', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: { msg: 'discard me' } },
        ctx
      );
      const jobId = (pushRes as any).id;

      const res = await handleDiscard({ cmd: 'Discard', id: jobId }, ctx);
      expect(res.ok).toBe(true);
    });

    test('should return error for non-existent job', async () => {
      const res = await handleDiscard({ cmd: 'Discard', id: 'non-existent' }, ctx);
      expect(res.ok).toBe(false);
      expect((res as any).error).toContain('Job not found');
    });

    test('should propagate reqId', async () => {
      const pushRes = await handlePush(
        { cmd: 'PUSH', queue: 'emails', data: {} },
        ctx
      );
      const res = await handleDiscard(
        { cmd: 'Discard', id: (pushRes as any).id, reqId: 'req-disc' },
        ctx,
        'req-disc'
      );
      expect(res.reqId).toBe('req-disc');
    });
  });
});

// ============================================================
// HANDLER ROUTER (handleCommand)
// ============================================================

describe('Handler Router', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should route PUSH command', async () => {
    const res = await handleCommand(
      { cmd: 'PUSH', queue: 'emails', data: { msg: 'routed' } },
      ctx
    );
    expect(res.ok).toBe(true);
    expect((res as any).id).toBeDefined();
  });

  test('should route PULL command', async () => {
    await handleCommand(
      { cmd: 'PUSH', queue: 'emails', data: { msg: 'test' } },
      ctx
    );
    const res = await handleCommand(
      { cmd: 'PULL', queue: 'emails' },
      ctx
    );
    expect(res.ok).toBe(true);
  });

  test('should route GetJob command', async () => {
    const pushRes = await handleCommand(
      { cmd: 'PUSH', queue: 'emails', data: { msg: 'test' } },
      ctx
    );
    const jobId = (pushRes as any).id;

    const res = await handleCommand({ cmd: 'GetJob', id: jobId }, ctx);
    expect(res.ok).toBe(true);
    expect((res as any).job.id).toBe(jobId);
  });

  test('should route Stats command', async () => {
    const res = await handleCommand({ cmd: 'Stats' }, ctx);
    expect(res.ok).toBe(true);
    expect((res as any).stats).toBeDefined();
  });

  test('should route Cancel command', async () => {
    const pushRes = await handleCommand(
      { cmd: 'PUSH', queue: 'emails', data: {} },
      ctx
    );
    const res = await handleCommand(
      { cmd: 'Cancel', id: (pushRes as any).id },
      ctx
    );
    expect(res.ok).toBe(true);
  });

  test('should return error for unknown command', async () => {
    const res = await handleCommand(
      { cmd: 'UnknownCommand' } as any,
      ctx
    );
    expect(res.ok).toBe(false);
    expect((res as any).error).toContain('Unknown command');
  });

  test('should require authentication when auth tokens are configured', async () => {
    ctx.authTokens = new Set(['secret-token']);
    ctx.authenticated = false;

    const res = await handleCommand(
      { cmd: 'PUSH', queue: 'emails', data: {} },
      ctx
    );
    expect(res.ok).toBe(false);
    expect((res as any).error).toContain('Not authenticated');
  });

  test('should allow Auth command without authentication', async () => {
    ctx.authTokens = new Set(['secret-token']);
    ctx.authenticated = false;

    const res = await handleCommand(
      { cmd: 'Auth', token: 'secret-token' },
      ctx
    );
    expect(res.ok).toBe(true);
    expect(ctx.authenticated).toBe(true);
  });

  test('should reject invalid auth token', async () => {
    ctx.authTokens = new Set(['secret-token']);
    ctx.authenticated = false;

    const res = await handleCommand(
      { cmd: 'Auth', token: 'wrong-token' },
      ctx
    );
    expect(res.ok).toBe(false);
    expect((res as any).error).toContain('Invalid token');
  });

  test('should allow commands after successful authentication', async () => {
    ctx.authTokens = new Set(['secret-token']);
    ctx.authenticated = false;

    await handleCommand({ cmd: 'Auth', token: 'secret-token' }, ctx);

    const res = await handleCommand(
      { cmd: 'PUSH', queue: 'emails', data: {} },
      ctx
    );
    expect(res.ok).toBe(true);
  });

  test('should route Pause and Resume commands', async () => {
    const pauseRes = await handleCommand(
      { cmd: 'Pause', queue: 'emails' },
      ctx
    );
    expect(pauseRes.ok).toBe(true);

    const resumeRes = await handleCommand(
      { cmd: 'Resume', queue: 'emails' },
      ctx
    );
    expect(resumeRes.ok).toBe(true);
  });

  test('should route Drain command', async () => {
    const res = await handleCommand(
      { cmd: 'Drain', queue: 'emails' },
      ctx
    );
    expect(res.ok).toBe(true);
  });

  test('should route Metrics command', async () => {
    const res = await handleCommand({ cmd: 'Metrics' }, ctx);
    expect(res.ok).toBe(true);
    expect((res as any).metrics).toBeDefined();
  });
});

// ============================================================
// END-TO-END FLOWS
// ============================================================

describe('End-to-End Flows', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('full lifecycle: push -> pull -> progress -> ack -> getResult', async () => {
    // Push
    const pushRes = await handlePush(
      { cmd: 'PUSH', queue: 'emails', data: { to: 'user@test.com' } },
      ctx
    );
    expect(pushRes.ok).toBe(true);
    const jobId = (pushRes as any).id;

    // Verify state is waiting
    const state1 = await handleGetState({ cmd: 'GetState', id: jobId }, ctx);
    expect((state1 as any).state).toBe('waiting');

    // Pull
    const pullRes = await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
    expect((pullRes as any).job.id).toBe(jobId);

    // Verify state is active
    const state2 = await handleGetState({ cmd: 'GetState', id: jobId }, ctx);
    expect((state2 as any).state).toBe('active');

    // Update progress
    const progRes = await handleProgress(
      { cmd: 'Progress', id: jobId, progress: 50, message: 'Sending...' },
      ctx
    );
    expect(progRes.ok).toBe(true);

    // Get progress
    const getProgRes = handleGetProgress({ cmd: 'GetProgress', id: jobId }, ctx);
    expect((getProgRes as any).progress).toBe(50);

    // Ack with result
    const ackRes = await handleAck(
      { cmd: 'ACK', id: jobId, result: { sent: true, timestamp: 12345 } },
      ctx
    );
    expect(ackRes.ok).toBe(true);

    // Verify state is completed
    const state3 = await handleGetState({ cmd: 'GetState', id: jobId }, ctx);
    expect((state3 as any).state).toBe('completed');

    // Get result
    const resultRes = handleGetResult({ cmd: 'GetResult', id: jobId }, ctx);
    expect((resultRes as any).result).toEqual({ sent: true, timestamp: 12345 });
  });

  test('push -> pull -> fail -> retry cycle', async () => {
    // Push with maxAttempts
    const pushRes = await handlePush(
      { cmd: 'PUSH', queue: 'emails', data: { msg: 'flaky' }, maxAttempts: 3 },
      ctx
    );
    const jobId = (pushRes as any).id;

    // Pull and fail
    await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
    const failRes = await handleFail(
      { cmd: 'FAIL', id: jobId, error: 'Connection timeout' },
      ctx
    );
    expect(failRes.ok).toBe(true);

    // Job should be back in waiting (retry)
    // Give it a moment for the retry to process
    const state = await handleGetState({ cmd: 'GetState', id: jobId }, ctx);
    // After fail with retries left, it could be waiting or delayed (backoff)
    expect(['waiting', 'delayed']).toContain((state as any).state);
  });

  test('batch operations: pushBatch -> pullBatch -> ackBatch', async () => {
    // Push batch
    const pushRes = await handlePushBatch(
      {
        cmd: 'PUSHB',
        queue: 'emails',
        jobs: [
          { data: { id: 1 } },
          { data: { id: 2 } },
          { data: { id: 3 } },
        ],
      },
      ctx
    );
    expect((pushRes as any).ids).toHaveLength(3);

    // Verify counts
    const countsRes = handleGetJobCounts({ cmd: 'GetJobCounts', queue: 'emails' }, ctx);
    expect((countsRes as any).counts.waiting).toBe(3);

    // Pull batch
    const pullRes = await handlePullBatch(
      { cmd: 'PULLB', queue: 'emails', count: 3 },
      ctx
    );
    expect((pullRes as any).jobs).toHaveLength(3);

    // Verify active counts
    const countsRes2 = handleGetJobCounts({ cmd: 'GetJobCounts', queue: 'emails' }, ctx);
    expect((countsRes2 as any).counts.active).toBe(3);
    expect((countsRes2 as any).counts.waiting).toBe(0);

    // Ack batch
    const ids = (pullRes as any).jobs.map((j: any) => j.id);
    const ackRes = await handleAckBatch({ cmd: 'ACKB', ids }, ctx);
    expect(ackRes.ok).toBe(true);
  });

  test('push -> cancel -> verify removed', async () => {
    const pushRes = await handlePush(
      { cmd: 'PUSH', queue: 'emails', data: { msg: 'test' } },
      ctx
    );
    const jobId = (pushRes as any).id;

    // Cancel
    const cancelRes = await handleCancel({ cmd: 'Cancel', id: jobId }, ctx);
    expect(cancelRes.ok).toBe(true);

    // Try to get the job
    const getRes = await handleGetJob({ cmd: 'GetJob', id: jobId }, ctx);
    expect(getRes.ok).toBe(false);

    // Count should be 0
    const countRes = handleCount({ cmd: 'Count', queue: 'emails' }, ctx);
    expect((countRes as any).count).toBe(0);
  });

  test('push delayed -> promote -> pull', async () => {
    const pushRes = await handlePush(
      { cmd: 'PUSH', queue: 'emails', data: { msg: 'delayed' }, delay: 60000 },
      ctx
    );
    const jobId = (pushRes as any).id;

    // Verify delayed state
    const state1 = await handleGetState({ cmd: 'GetState', id: jobId }, ctx);
    expect((state1 as any).state).toBe('delayed');

    // Can't pull delayed job
    const pullRes1 = await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
    expect((pullRes1 as any).job).toBeNull();

    // Promote
    const promRes = await handlePromote({ cmd: 'Promote', id: jobId }, ctx);
    expect(promRes.ok).toBe(true);

    // Now we can pull it
    const pullRes2 = await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
    expect((pullRes2 as any).job).not.toBeNull();
    expect((pullRes2 as any).job.id).toBe(jobId);
  });

  test('push -> update data -> pull -> verify updated data', async () => {
    const pushRes = await handlePush(
      { cmd: 'PUSH', queue: 'emails', data: { msg: 'original' } },
      ctx
    );
    const jobId = (pushRes as any).id;

    // Update data
    await handleUpdate(
      { cmd: 'Update', id: jobId, data: { msg: 'modified', extra: true } },
      ctx
    );

    // Pull and verify data
    const pullRes = await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
    expect((pullRes as any).job.data).toEqual({ msg: 'modified', extra: true });
  });

  test('push -> changePriority -> verify priority order', async () => {
    // Push two jobs with same priority
    const push1 = await handlePush(
      { cmd: 'PUSH', queue: 'emails', data: { msg: 'first' }, priority: 1 },
      ctx
    );
    const push2 = await handlePush(
      { cmd: 'PUSH', queue: 'emails', data: { msg: 'second' }, priority: 1 },
      ctx
    );
    const id1 = (push1 as any).id;

    // Change first job to higher priority
    await handleChangePriority(
      { cmd: 'ChangePriority', id: id1, priority: 100 },
      ctx
    );

    // Pull should return the higher priority job first
    const pullRes = await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
    expect((pullRes as any).job.id).toBe(id1);
  });

  test('pause -> push -> pull returns null -> resume -> pull returns job', async () => {
    handlePause({ cmd: 'Pause', queue: 'emails' }, ctx);

    await handlePush({ cmd: 'PUSH', queue: 'emails', data: { msg: 'test' } }, ctx);

    // Pull while paused
    const pullRes1 = await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
    expect((pullRes1 as any).job).toBeNull();

    // Resume
    handleResume({ cmd: 'Resume', queue: 'emails' }, ctx);

    // Pull after resume
    const pullRes2 = await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
    expect((pullRes2 as any).job).not.toBeNull();
  });

  test('drain removes all waiting jobs but not active ones', async () => {
    // Push 5 jobs
    for (let i = 0; i < 5; i++) {
      await handlePush({ cmd: 'PUSH', queue: 'emails', data: { id: i } }, ctx);
    }

    // Pull 2 (make them active)
    await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);
    await handlePull({ cmd: 'PULL', queue: 'emails' }, ctx);

    // Drain should only remove waiting jobs
    const drainRes = handleDrain({ cmd: 'Drain', queue: 'emails' }, ctx);
    expect((drainRes as any).count).toBe(3);

    // 2 active jobs should remain
    const countsRes = handleGetJobCounts({ cmd: 'GetJobCounts', queue: 'emails' }, ctx);
    expect((countsRes as any).counts.active).toBe(2);
    expect((countsRes as any).counts.waiting).toBe(0);
  });
});

// ============================================================
// MONITORING HANDLERS (Ping)
// ============================================================

describe('Monitoring Handlers', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  describe('handlePing', () => {
    test('handlePing response should match client expected structure', () => {
      const res = handlePing({ cmd: 'Ping' } as any, ctx, 'req-42');
      // Client checks: response.data.pong === true
      expect(res.ok).toBe(true);
      expect((res as any).data).toBeDefined();
      expect((res as any).data.pong).toBe(true);
      expect((res as any).data.time).toBeDefined();
      expect(typeof (res as any).data.time).toBe('number');
      expect((res as any).reqId).toBe('req-42');
    });
  });
});
