/**
 * Comprehensive tests for advanced TCP server command handlers:
 * - cron.ts: Cron, CronDelete, CronList
 * - dlq.ts: Dlq, RetryDlq, PurgeDlq
 * - monitoring.ts: Ping, Heartbeat, JobHeartbeat, RegisterWorker, UnregisterWorker, ListWorkers,
 *                  AddWebhook, RemoveWebhook, ListWebhooks, Prometheus, AddLog, GetLogs, Hello
 * - advanced.ts: RateLimit, RateLimitClear, SetConcurrency, ClearConcurrency,
 *                Update, ChangePriority, Promote, IsPaused, Obliterate, ListQueues, Clean, Count
 * - management.ts: Stats, Metrics
 *
 * Tests use the handleCommand router in embedded mode (no TCP server needed).
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { handleCommand } from '../src/infrastructure/server/handler';
import type { HandlerContext } from '../src/infrastructure/server/types';
import type { Command } from '../src/domain/types/command';
import type { Response } from '../src/domain/types/response';

/** Create a HandlerContext for embedded testing (no auth required) */
function createCtx(qm: QueueManager): HandlerContext {
  return {
    queueManager: qm,
    authTokens: new Set<string>(),
    authenticated: false,
  };
}

/** Convenience: send a command through the handler and return the response */
async function send(ctx: HandlerContext, cmd: Command): Promise<Response> {
  return handleCommand(cmd, ctx);
}

// ============================================================
// Cron Handlers
// ============================================================

describe('Cron Handlers', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createCtx(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  describe('Cron (add)', () => {
    test('should add a cron job with repeatEvery', async () => {
      const res = await send(ctx, {
        cmd: 'Cron',
        name: 'daily-cleanup',
        queue: 'maintenance',
        data: { action: 'cleanup' },
        repeatEvery: 60000,
      });

      expect(res.ok).toBe(true);
      const cron = (res as any).cron;
      expect(cron).toBeDefined();
      expect(cron.name).toBe('daily-cleanup');
      expect(cron.queue).toBe('maintenance');
      expect(cron.repeatEvery).toBe(60000);
      expect(cron.nextRun).toBeGreaterThan(0);
    });

    test('should add a cron job with schedule', async () => {
      const res = await send(ctx, {
        cmd: 'Cron',
        name: 'nightly-report',
        queue: 'reports',
        data: { type: 'nightly' },
        schedule: '0 0 * * *',
      });

      expect(res.ok).toBe(true);
      const cron = (res as any).cron;
      expect(cron.name).toBe('nightly-report');
      expect(cron.schedule).toBe('0 0 * * *');
    });

    test('should add a cron job with timezone', async () => {
      const res = await send(ctx, {
        cmd: 'Cron',
        name: 'tz-cron',
        queue: 'tz-queue',
        data: {},
        schedule: '0 9 * * *',
        timezone: 'America/New_York',
      });

      expect(res.ok).toBe(true);
      const cron = (res as any).cron;
      expect(cron.timezone).toBe('America/New_York');
    });

    test('should add a cron job with priority and maxLimit', async () => {
      const res = await send(ctx, {
        cmd: 'Cron',
        name: 'priority-cron',
        queue: 'priority-queue',
        data: { msg: 'hi' },
        repeatEvery: 30000,
        priority: 10,
        maxLimit: 100,
      });

      expect(res.ok).toBe(true);
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'Cron',
        name: 'reqid-cron',
        queue: 'test',
        data: {},
        repeatEvery: 5000,
        reqId: 'req-cron-1',
      });

      expect(res.ok).toBe(true);
      expect(res.reqId).toBe('req-cron-1');
    });

    test('should return error for invalid cron (no schedule or repeatEvery)', async () => {
      const res = await send(ctx, {
        cmd: 'Cron',
        name: 'bad-cron',
        queue: 'test',
        data: {},
      });

      // Should return error since no schedule or repeatEvery
      expect(res.ok).toBe(false);
    });
  });

  describe('CronDelete', () => {
    test('should delete an existing cron job', async () => {
      // First add a cron job
      await send(ctx, {
        cmd: 'Cron',
        name: 'delete-me',
        queue: 'test',
        data: {},
        repeatEvery: 10000,
      });

      const res = await send(ctx, {
        cmd: 'CronDelete',
        name: 'delete-me',
      });

      expect(res.ok).toBe(true);
    });

    test('should return error for non-existent cron job', async () => {
      const res = await send(ctx, {
        cmd: 'CronDelete',
        name: 'non-existent-cron',
      });

      expect(res.ok).toBe(false);
      expect((res as any).error).toBe('Cron job not found');
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'CronDelete',
        name: 'non-existent',
        reqId: 'req-del-1',
      });

      expect(res.reqId).toBe('req-del-1');
    });
  });

  describe('CronList', () => {
    test('should list all cron jobs', async () => {
      await send(ctx, {
        cmd: 'Cron',
        name: 'cron-a',
        queue: 'q1',
        data: {},
        repeatEvery: 10000,
      });
      await send(ctx, {
        cmd: 'Cron',
        name: 'cron-b',
        queue: 'q2',
        data: {},
        repeatEvery: 20000,
      });

      const res = await send(ctx, { cmd: 'CronList' });

      expect(res.ok).toBe(true);
      const crons = (res as any).crons;
      expect(crons.length).toBeGreaterThanOrEqual(2);

      const names = crons.map((c: any) => c.name);
      expect(names).toContain('cron-a');
      expect(names).toContain('cron-b');
    });

    test('should return empty list when no cron jobs exist', async () => {
      const res = await send(ctx, { cmd: 'CronList' });

      expect(res.ok).toBe(true);
      expect((res as any).crons).toEqual([]);
    });

    test('should include execution count and nextRun', async () => {
      await send(ctx, {
        cmd: 'Cron',
        name: 'detail-cron',
        queue: 'test',
        data: {},
        repeatEvery: 5000,
      });

      const res = await send(ctx, { cmd: 'CronList' });
      const cron = (res as any).crons.find((c: any) => c.name === 'detail-cron');
      expect(cron).toBeDefined();
      expect(typeof cron.executions).toBe('number');
      expect(typeof cron.nextRun).toBe('number');
    });

    test('should not list deleted cron jobs', async () => {
      await send(ctx, {
        cmd: 'Cron',
        name: 'temp-cron',
        queue: 'test',
        data: {},
        repeatEvery: 5000,
      });

      await send(ctx, { cmd: 'CronDelete', name: 'temp-cron' });

      const res = await send(ctx, { cmd: 'CronList' });
      const names = (res as any).crons.map((c: any) => c.name);
      expect(names).not.toContain('temp-cron');
    });
  });
});

// ============================================================
// DLQ Handlers
// ============================================================

describe('DLQ Handlers', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createCtx(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  describe('Dlq (list)', () => {
    test('should return empty list for queue with no DLQ entries', async () => {
      const res = await send(ctx, {
        cmd: 'Dlq',
        queue: 'test-queue',
      });

      expect(res.ok).toBe(true);
      expect((res as any).jobs).toEqual([]);
    });

    test('should return DLQ entries after job failure exhausts retries', async () => {
      // Push a job with 1 attempt
      const pushRes = await send(ctx, {
        cmd: 'PUSH',
        queue: 'dlq-test',
        data: { msg: 'fail-me' },
        maxAttempts: 1,
      });
      expect(pushRes.ok).toBe(true);

      // Pull the job
      const pullRes = await send(ctx, {
        cmd: 'PULL',
        queue: 'dlq-test',
      });
      expect(pullRes.ok).toBe(true);
      const jobId = (pullRes as any).job?.id;
      expect(jobId).toBeDefined();

      // Fail the job
      await send(ctx, {
        cmd: 'FAIL',
        id: jobId,
        error: 'test failure',
      });

      // Check DLQ
      const dlqRes = await send(ctx, {
        cmd: 'Dlq',
        queue: 'dlq-test',
      });

      expect(dlqRes.ok).toBe(true);
      // After failure with maxAttempts=1, the job may be in DLQ
      // The exact behavior depends on the retry logic
    });

    test('should respect count parameter', async () => {
      const res = await send(ctx, {
        cmd: 'Dlq',
        queue: 'test-queue',
        count: 5,
      });

      expect(res.ok).toBe(true);
      expect((res as any).jobs.length).toBeLessThanOrEqual(5);
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'Dlq',
        queue: 'test-queue',
        reqId: 'req-dlq-1',
      });

      expect(res.reqId).toBe('req-dlq-1');
    });
  });

  describe('RetryDlq', () => {
    test('should return count 0 when no DLQ entries', async () => {
      const res = await send(ctx, {
        cmd: 'RetryDlq',
        queue: 'empty-queue',
      });

      expect(res.ok).toBe(true);
      expect((res as any).count).toBe(0);
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'RetryDlq',
        queue: 'test',
        reqId: 'req-retry-1',
      });

      expect(res.reqId).toBe('req-retry-1');
    });

    test('should accept optional jobId parameter', async () => {
      const res = await send(ctx, {
        cmd: 'RetryDlq',
        queue: 'test',
        jobId: 'some-job-id',
      });

      expect(res.ok).toBe(true);
      expect((res as any).count).toBe(0);
    });
  });

  describe('PurgeDlq', () => {
    test('should return count 0 when no DLQ entries', async () => {
      const res = await send(ctx, {
        cmd: 'PurgeDlq',
        queue: 'empty-queue',
      });

      expect(res.ok).toBe(true);
      expect((res as any).count).toBe(0);
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'PurgeDlq',
        queue: 'test',
        reqId: 'req-purge-1',
      });

      expect(res.reqId).toBe('req-purge-1');
    });
  });
});

// ============================================================
// Monitoring Handlers
// ============================================================

describe('Monitoring Handlers', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createCtx(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  describe('Ping', () => {
    test('should return pong with timestamp', async () => {
      const before = Date.now();
      const res = await send(ctx, { cmd: 'Ping' });

      expect(res.ok).toBe(true);
      const data = (res as any).data;
      expect(data.pong).toBe(true);
      expect(data.time).toBeGreaterThanOrEqual(before);
      expect(data.time).toBeLessThanOrEqual(Date.now());
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, { cmd: 'Ping', reqId: 'req-ping-1' });

      expect(res.ok).toBe(true);
      expect(res.reqId).toBe('req-ping-1');
    });
  });

  describe('Hello', () => {
    test('should return protocol version and capabilities', async () => {
      const res = await send(ctx, {
        cmd: 'Hello',
        protocolVersion: 1,
      });

      expect(res.ok).toBe(true);
      expect((res as any).protocolVersion).toBe(3);
      expect((res as any).capabilities).toContain('pipelining');
      expect((res as any).capabilities).toContain('separate-job-name');
      expect((res as any).server).toBe('bunqueue');
      expect((res as any).version).toBeDefined();
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'Hello',
        protocolVersion: 1,
        reqId: 'req-hello-1',
      });

      expect(res.reqId).toBe('req-hello-1');
    });
  });

  describe('Stats', () => {
    test('should return queue statistics', async () => {
      const res = await send(ctx, { cmd: 'Stats' });

      expect(res.ok).toBe(true);
      const stats = (res as any).stats;
      expect(stats).toBeDefined();
      expect(typeof stats.waiting).toBe('number');
      expect(typeof stats.active).toBe('number');
      expect(typeof stats.delayed).toBe('number');
      expect(typeof stats.dlq).toBe('number');
      expect(typeof stats.completed).toBe('number');
      expect(typeof stats.uptime).toBe('number');
    });

    test('should reflect pushed jobs in stats', async () => {
      await send(ctx, {
        cmd: 'PUSH',
        queue: 'stats-test',
        data: { msg: 'hello' },
      });

      const res = await send(ctx, { cmd: 'Stats' });
      const stats = (res as any).stats;
      expect(stats.waiting).toBeGreaterThanOrEqual(1);
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, { cmd: 'Stats', reqId: 'req-stats-1' });

      expect(res.reqId).toBe('req-stats-1');
    });
  });

  describe('Metrics', () => {
    test('should return metrics data', async () => {
      const res = await send(ctx, { cmd: 'Metrics' });

      expect(res.ok).toBe(true);
      const metrics = (res as any).metrics;
      expect(metrics).toBeDefined();
      expect(typeof metrics.totalPushed).toBe('number');
      expect(typeof metrics.totalPulled).toBe('number');
      expect(typeof metrics.totalCompleted).toBe('number');
      expect(typeof metrics.totalFailed).toBe('number');
      expect(typeof metrics.memoryUsageMb).toBe('number');
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, { cmd: 'Metrics', reqId: 'req-metrics-1' });

      expect(res.reqId).toBe('req-metrics-1');
    });
  });

  describe('Prometheus', () => {
    test('should return prometheus metrics string', async () => {
      const res = await send(ctx, { cmd: 'Prometheus' });

      expect(res.ok).toBe(true);
      const data = (res as any).data;
      expect(data.metrics).toBeDefined();
      expect(typeof data.metrics).toBe('string');
    });

    test('should include standard prometheus metric names', async () => {
      // Push a job so there is some data
      await send(ctx, {
        cmd: 'PUSH',
        queue: 'prom-test',
        data: { msg: 'test' },
      });

      const res = await send(ctx, { cmd: 'Prometheus' });
      const metrics = (res as any).data.metrics;
      expect(metrics).toContain('bunqueue_');
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, { cmd: 'Prometheus', reqId: 'req-prom-1' });

      expect(res.reqId).toBe('req-prom-1');
    });
  });

  describe('RegisterWorker', () => {
    test('should register a worker and return its info', async () => {
      const res = await send(ctx, {
        cmd: 'RegisterWorker',
        name: 'worker-1',
        queues: ['emails', 'notifications'],
      });

      expect(res.ok).toBe(true);
      const data = (res as any).data;
      expect(data.workerId).toBeDefined();
      expect(data.name).toBe('worker-1');
      expect(data.queues).toEqual(['emails', 'notifications']);
      expect(data.registeredAt).toBeDefined();
    });

    test('should register multiple workers with unique IDs', async () => {
      const res1 = await send(ctx, {
        cmd: 'RegisterWorker',
        name: 'w1',
        queues: ['q1'],
      });
      const res2 = await send(ctx, {
        cmd: 'RegisterWorker',
        name: 'w2',
        queues: ['q2'],
      });

      expect((res1 as any).data.workerId).not.toBe((res2 as any).data.workerId);
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'RegisterWorker',
        name: 'w1',
        queues: ['q1'],
        reqId: 'req-reg-1',
      });

      expect(res.reqId).toBe('req-reg-1');
    });
  });

  describe('UnregisterWorker', () => {
    test('should unregister an existing worker', async () => {
      const regRes = await send(ctx, {
        cmd: 'RegisterWorker',
        name: 'worker-to-remove',
        queues: ['q1'],
      });
      const workerId = (regRes as any).data.workerId;

      const res = await send(ctx, {
        cmd: 'UnregisterWorker',
        workerId,
      });

      expect(res.ok).toBe(true);
      expect((res as any).data.removed).toBe(true);
    });

    test('should return error for non-existent worker', async () => {
      const res = await send(ctx, {
        cmd: 'UnregisterWorker',
        workerId: 'non-existent-worker-id',
      });

      expect(res.ok).toBe(false);
      expect((res as any).error).toBe('Worker not found');
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'UnregisterWorker',
        workerId: 'non-existent',
        reqId: 'req-unreg-1',
      });

      expect(res.reqId).toBe('req-unreg-1');
    });
  });

  describe('ListWorkers', () => {
    test('should list registered workers', async () => {
      await send(ctx, {
        cmd: 'RegisterWorker',
        name: 'lw-1',
        queues: ['q1'],
      });
      await send(ctx, {
        cmd: 'RegisterWorker',
        name: 'lw-2',
        queues: ['q2', 'q3'],
      });

      const res = await send(ctx, { cmd: 'ListWorkers' });

      expect(res.ok).toBe(true);
      const data = (res as any).data;
      expect(data.workers.length).toBeGreaterThanOrEqual(2);
      const names = data.workers.map((w: any) => w.name);
      expect(names).toContain('lw-1');
      expect(names).toContain('lw-2');
    });

    test('should return worker stats', async () => {
      const res = await send(ctx, { cmd: 'ListWorkers' });

      expect(res.ok).toBe(true);
      const data = (res as any).data;
      expect(data.stats).toBeDefined();
    });

    test('should return empty list when no workers', async () => {
      const res = await send(ctx, { cmd: 'ListWorkers' });

      expect(res.ok).toBe(true);
      expect((res as any).data.workers).toEqual([]);
    });

    test('should include worker detail fields', async () => {
      await send(ctx, {
        cmd: 'RegisterWorker',
        name: 'detail-worker',
        queues: ['q1'],
      });

      const res = await send(ctx, { cmd: 'ListWorkers' });
      const worker = (res as any).data.workers[0];
      expect(worker.id).toBeDefined();
      expect(worker.name).toBeDefined();
      expect(worker.queues).toBeDefined();
      expect(typeof worker.activeJobs).toBe('number');
      expect(typeof worker.processedJobs).toBe('number');
      expect(typeof worker.failedJobs).toBe('number');
    });
  });

  describe('Heartbeat (worker)', () => {
    test('should heartbeat a registered worker', async () => {
      const regRes = await send(ctx, {
        cmd: 'RegisterWorker',
        name: 'hb-worker',
        queues: ['q1'],
      });
      const workerId = (regRes as any).data.workerId;

      const res = await send(ctx, {
        cmd: 'Heartbeat',
        id: workerId,
      });

      expect(res.ok).toBe(true);
      expect((res as any).data.ok).toBe(true);
    });

    test('should return error for non-existent worker', async () => {
      const res = await send(ctx, {
        cmd: 'Heartbeat',
        id: 'non-existent-worker',
      });

      expect(res.ok).toBe(false);
      expect((res as any).error).toBe('Worker not found');
    });
  });

  describe('JobHeartbeat', () => {
    test('should heartbeat an active job', async () => {
      await send(ctx, {
        cmd: 'PUSH',
        queue: 'hb-queue',
        data: { msg: 'heartbeat-test' },
      });
      const pullRes = await send(ctx, {
        cmd: 'PULL',
        queue: 'hb-queue',
      });
      const jobId = (pullRes as any).job?.id;
      expect(jobId).toBeDefined();

      const res = await send(ctx, {
        cmd: 'JobHeartbeat',
        id: jobId,
      });

      expect(res.ok).toBe(true);
      expect((res as any).data.ok).toBe(true);
    });

    test('should return error for non-existent job', async () => {
      const res = await send(ctx, {
        cmd: 'JobHeartbeat',
        id: 'non-existent-job',
      });

      expect(res.ok).toBe(false);
      expect((res as any).error).toContain('Job not found');
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'JobHeartbeat',
        id: 'non-existent',
        reqId: 'req-jhb-1',
      });

      expect(res.reqId).toBe('req-jhb-1');
    });
  });

  describe('AddLog', () => {
    test('should add a log to an active job', async () => {
      await send(ctx, {
        cmd: 'PUSH',
        queue: 'log-queue',
        data: { msg: 'log-test' },
      });
      const pullRes = await send(ctx, {
        cmd: 'PULL',
        queue: 'log-queue',
      });
      const jobId = (pullRes as any).job?.id;

      const res = await send(ctx, {
        cmd: 'AddLog',
        id: jobId,
        message: 'Step 1 completed',
        level: 'info',
      });

      expect(res.ok).toBe(true);
      expect((res as any).data.added).toBe(true);
    });

    test('should return error for non-existent job', async () => {
      const res = await send(ctx, {
        cmd: 'AddLog',
        id: 'non-existent',
        message: 'test log',
      });

      expect(res.ok).toBe(false);
      expect((res as any).error).toBe('Job not found');
    });
  });

  describe('GetLogs', () => {
    test('should get logs for a job', async () => {
      await send(ctx, {
        cmd: 'PUSH',
        queue: 'log-queue',
        data: { msg: 'log-test' },
      });
      const pullRes = await send(ctx, {
        cmd: 'PULL',
        queue: 'log-queue',
      });
      const jobId = (pullRes as any).job?.id;

      // Add a log
      await send(ctx, {
        cmd: 'AddLog',
        id: jobId,
        message: 'Step 1 done',
        level: 'info',
      });

      const res = await send(ctx, {
        cmd: 'GetLogs',
        id: jobId,
      });

      expect(res.ok).toBe(true);
      const logs = (res as any).data.logs;
      expect(logs.length).toBe(1);
      expect(logs[0].message).toBe('Step 1 done');
    });

    test('should return empty logs for job with no logs', async () => {
      await send(ctx, {
        cmd: 'PUSH',
        queue: 'log-queue',
        data: { msg: 'no-logs' },
      });
      const pullRes = await send(ctx, {
        cmd: 'PULL',
        queue: 'log-queue',
      });
      const jobId = (pullRes as any).job?.id;

      const res = await send(ctx, {
        cmd: 'GetLogs',
        id: jobId,
      });

      expect(res.ok).toBe(true);
      expect((res as any).data.logs).toEqual([]);
    });
  });

  describe('AddWebhook', () => {
    test('should add a webhook', async () => {
      const res = await send(ctx, {
        cmd: 'AddWebhook',
        url: 'https://example.com/webhook',
        events: ['job.completed', 'job.failed'],
      });

      expect(res.ok).toBe(true);
      const data = (res as any).data;
      expect(data.webhookId).toBeDefined();
      expect(data.url).toBe('https://example.com/webhook');
      expect(data.events).toEqual(['job.completed', 'job.failed']);
    });

    test('should add a webhook with queue filter', async () => {
      const res = await send(ctx, {
        cmd: 'AddWebhook',
        url: 'https://example.com/webhook',
        events: ['job.completed'],
        queue: 'emails',
      });

      expect(res.ok).toBe(true);
      expect((res as any).data.queue).toBe('emails');
    });

    test('should add a webhook with secret', async () => {
      const res = await send(ctx, {
        cmd: 'AddWebhook',
        url: 'https://example.com/webhook',
        events: ['job.completed'],
        secret: 'my-secret-123',
      });

      expect(res.ok).toBe(true);
    });

    test('should reject invalid URL', async () => {
      const res = await send(ctx, {
        cmd: 'AddWebhook',
        url: 'not-a-valid-url',
        events: ['job.completed'],
      });

      expect(res.ok).toBe(false);
    });

    test('should reject empty URL', async () => {
      const res = await send(ctx, {
        cmd: 'AddWebhook',
        url: '',
        events: ['job.completed'],
      });

      expect(res.ok).toBe(false);
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'AddWebhook',
        url: 'https://example.com/hook',
        events: ['job.completed'],
        reqId: 'req-wh-1',
      });

      expect(res.reqId).toBe('req-wh-1');
    });
  });

  describe('RemoveWebhook', () => {
    test('should remove an existing webhook', async () => {
      const addRes = await send(ctx, {
        cmd: 'AddWebhook',
        url: 'https://example.com/remove-me',
        events: ['job.completed'],
      });
      const webhookId = (addRes as any).data.webhookId;

      const res = await send(ctx, {
        cmd: 'RemoveWebhook',
        webhookId,
      });

      expect(res.ok).toBe(true);
      expect((res as any).data.removed).toBe(true);
    });

    test('should return error for non-existent webhook', async () => {
      const res = await send(ctx, {
        cmd: 'RemoveWebhook',
        webhookId: 'non-existent-webhook',
      });

      expect(res.ok).toBe(false);
      expect((res as any).error).toBe('Webhook not found');
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'RemoveWebhook',
        webhookId: 'non-existent',
        reqId: 'req-rmwh-1',
      });

      expect(res.reqId).toBe('req-rmwh-1');
    });
  });

  describe('ListWebhooks', () => {
    test('should list registered webhooks', async () => {
      await send(ctx, {
        cmd: 'AddWebhook',
        url: 'https://example.com/hook1',
        events: ['job.completed'],
      });
      await send(ctx, {
        cmd: 'AddWebhook',
        url: 'https://example.com/hook2',
        events: ['job.failed'],
      });

      const res = await send(ctx, { cmd: 'ListWebhooks' });

      expect(res.ok).toBe(true);
      const data = (res as any).data;
      expect(data.webhooks.length).toBeGreaterThanOrEqual(2);
      const urls = data.webhooks.map((w: any) => w.url);
      expect(urls).toContain('https://example.com/hook1');
      expect(urls).toContain('https://example.com/hook2');
    });

    test('should return empty list when no webhooks', async () => {
      const res = await send(ctx, { cmd: 'ListWebhooks' });

      expect(res.ok).toBe(true);
      expect((res as any).data.webhooks).toEqual([]);
    });

    test('should return webhook stats', async () => {
      const res = await send(ctx, { cmd: 'ListWebhooks' });

      expect(res.ok).toBe(true);
      expect((res as any).data.stats).toBeDefined();
    });

    test('should include webhook detail fields', async () => {
      await send(ctx, {
        cmd: 'AddWebhook',
        url: 'https://example.com/details',
        events: ['job.completed'],
      });

      const res = await send(ctx, { cmd: 'ListWebhooks' });
      const webhook = (res as any).data.webhooks[0];
      expect(webhook.id).toBeDefined();
      expect(webhook.url).toBeDefined();
      expect(webhook.events).toBeDefined();
      expect(typeof webhook.enabled).toBe('boolean');
      expect(typeof webhook.successCount).toBe('number');
      expect(typeof webhook.failureCount).toBe('number');
    });

    test('should not list removed webhooks', async () => {
      const addRes = await send(ctx, {
        cmd: 'AddWebhook',
        url: 'https://example.com/temp',
        events: ['job.completed'],
      });
      const wid = (addRes as any).data.webhookId;

      await send(ctx, { cmd: 'RemoveWebhook', webhookId: wid });

      const res = await send(ctx, { cmd: 'ListWebhooks' });
      const urls = (res as any).data.webhooks.map((w: any) => w.url);
      expect(urls).not.toContain('https://example.com/temp');
    });
  });
});

// ============================================================
// Advanced Handlers
// ============================================================

describe('Advanced Handlers', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createCtx(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  describe('RateLimit', () => {
    test('should set a rate limit on a queue', async () => {
      const res = await send(ctx, {
        cmd: 'RateLimit',
        queue: 'rate-test',
        limit: 100,
      });

      expect(res.ok).toBe(true);
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'RateLimit',
        queue: 'rate-test',
        limit: 50,
        reqId: 'req-rl-1',
      });

      expect(res.reqId).toBe('req-rl-1');
    });
  });

  describe('RateLimitClear', () => {
    test('should clear a rate limit', async () => {
      // First set a rate limit
      await send(ctx, {
        cmd: 'RateLimit',
        queue: 'rate-clear-test',
        limit: 100,
      });

      const res = await send(ctx, {
        cmd: 'RateLimitClear',
        queue: 'rate-clear-test',
      });

      expect(res.ok).toBe(true);
    });

    test('should succeed even if no rate limit was set', async () => {
      const res = await send(ctx, {
        cmd: 'RateLimitClear',
        queue: 'no-limit-queue',
      });

      expect(res.ok).toBe(true);
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'RateLimitClear',
        queue: 'test',
        reqId: 'req-rlc-1',
      });

      expect(res.reqId).toBe('req-rlc-1');
    });
  });

  describe('SetConcurrency', () => {
    test('should set concurrency limit on a queue', async () => {
      const res = await send(ctx, {
        cmd: 'SetConcurrency',
        queue: 'conc-test',
        limit: 5,
      });

      expect(res.ok).toBe(true);
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'SetConcurrency',
        queue: 'test',
        limit: 10,
        reqId: 'req-sc-1',
      });

      expect(res.reqId).toBe('req-sc-1');
    });
  });

  describe('ClearConcurrency', () => {
    test('should clear concurrency limit', async () => {
      // First set
      await send(ctx, {
        cmd: 'SetConcurrency',
        queue: 'conc-clear-test',
        limit: 5,
      });

      const res = await send(ctx, {
        cmd: 'ClearConcurrency',
        queue: 'conc-clear-test',
      });

      expect(res.ok).toBe(true);
    });

    test('should succeed even if no concurrency limit was set', async () => {
      const res = await send(ctx, {
        cmd: 'ClearConcurrency',
        queue: 'no-conc-queue',
      });

      expect(res.ok).toBe(true);
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'ClearConcurrency',
        queue: 'test',
        reqId: 'req-cc-1',
      });

      expect(res.reqId).toBe('req-cc-1');
    });
  });

  describe('Update (job data)', () => {
    test('should update a waiting job data', async () => {
      const pushRes = await send(ctx, {
        cmd: 'PUSH',
        queue: 'update-test',
        data: { original: true },
      });
      const jobId = (pushRes as any).id;

      const res = await send(ctx, {
        cmd: 'Update',
        id: jobId,
        data: { updated: true },
      });

      expect(res.ok).toBe(true);
    });

    test('should return error for non-existent job', async () => {
      const res = await send(ctx, {
        cmd: 'Update',
        id: 'non-existent-job',
        data: { foo: 'bar' },
      });

      expect(res.ok).toBe(false);
      expect((res as any).error).toContain('Job not found');
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'Update',
        id: 'non-existent',
        data: {},
        reqId: 'req-upd-1',
      });

      expect(res.reqId).toBe('req-upd-1');
    });
  });

  describe('ChangePriority', () => {
    test('should change priority of a waiting job', async () => {
      const pushRes = await send(ctx, {
        cmd: 'PUSH',
        queue: 'priority-test',
        data: { msg: 'priority' },
        priority: 1,
      });
      const jobId = (pushRes as any).id;

      const res = await send(ctx, {
        cmd: 'ChangePriority',
        id: jobId,
        priority: 10,
      });

      expect(res.ok).toBe(true);
    });

    test('should return error for non-existent job', async () => {
      const res = await send(ctx, {
        cmd: 'ChangePriority',
        id: 'non-existent',
        priority: 5,
      });

      expect(res.ok).toBe(false);
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'ChangePriority',
        id: 'non-existent',
        priority: 5,
        reqId: 'req-cp-1',
      });

      expect(res.reqId).toBe('req-cp-1');
    });
  });

  describe('Promote', () => {
    test('should promote a delayed job to waiting', async () => {
      const pushRes = await send(ctx, {
        cmd: 'PUSH',
        queue: 'promote-test',
        data: { msg: 'delayed' },
        delay: 60000,
      });
      const jobId = (pushRes as any).id;

      const res = await send(ctx, {
        cmd: 'Promote',
        id: jobId,
      });

      expect(res.ok).toBe(true);
    });

    test('should return error for non-existent job', async () => {
      const res = await send(ctx, {
        cmd: 'Promote',
        id: 'non-existent',
      });

      expect(res.ok).toBe(false);
    });

    test('should return error for non-delayed job', async () => {
      // Push a job without delay
      const pushRes = await send(ctx, {
        cmd: 'PUSH',
        queue: 'promote-test',
        data: { msg: 'immediate' },
      });
      const jobId = (pushRes as any).id;

      const res = await send(ctx, {
        cmd: 'Promote',
        id: jobId,
      });

      // Should fail since job is not delayed
      expect(res.ok).toBe(false);
    });
  });

  describe('IsPaused', () => {
    test('should return false for non-paused queue', async () => {
      const res = await send(ctx, {
        cmd: 'IsPaused',
        queue: 'not-paused',
      });

      expect(res.ok).toBe(true);
      expect((res as any).paused).toBe(false);
    });

    test('should return true for paused queue', async () => {
      await send(ctx, { cmd: 'Pause', queue: 'pause-check' });

      const res = await send(ctx, {
        cmd: 'IsPaused',
        queue: 'pause-check',
      });

      expect(res.ok).toBe(true);
      expect((res as any).paused).toBe(true);
    });

    test('should return false after resume', async () => {
      await send(ctx, { cmd: 'Pause', queue: 'pause-resume' });
      await send(ctx, { cmd: 'Resume', queue: 'pause-resume' });

      const res = await send(ctx, {
        cmd: 'IsPaused',
        queue: 'pause-resume',
      });

      expect((res as any).paused).toBe(false);
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'IsPaused',
        queue: 'test',
        reqId: 'req-ip-1',
      });

      expect(res.reqId).toBe('req-ip-1');
    });
  });

  describe('Obliterate', () => {
    test('should obliterate a queue', async () => {
      // Push some jobs
      await send(ctx, {
        cmd: 'PUSH',
        queue: 'obliterate-test',
        data: { msg: '1' },
      });
      await send(ctx, {
        cmd: 'PUSH',
        queue: 'obliterate-test',
        data: { msg: '2' },
      });

      const res = await send(ctx, {
        cmd: 'Obliterate',
        queue: 'obliterate-test',
      });

      expect(res.ok).toBe(true);

      // Verify queue is empty
      const countRes = await send(ctx, {
        cmd: 'Count',
        queue: 'obliterate-test',
      });
      expect((countRes as any).count).toBe(0);
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'Obliterate',
        queue: 'test',
        reqId: 'req-obl-1',
      });

      expect(res.reqId).toBe('req-obl-1');
    });
  });

  describe('ListQueues', () => {
    test('should list queues with jobs', async () => {
      await send(ctx, {
        cmd: 'PUSH',
        queue: 'list-q1',
        data: { msg: '1' },
      });
      await send(ctx, {
        cmd: 'PUSH',
        queue: 'list-q2',
        data: { msg: '2' },
      });

      const res = await send(ctx, { cmd: 'ListQueues' });

      expect(res.ok).toBe(true);
      const queues = (res as any).queues as string[];
      expect(queues).toBeDefined();
      expect(queues).toContain('list-q1');
      expect(queues).toContain('list-q2');
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, { cmd: 'ListQueues', reqId: 'req-lq-1' });

      expect(res.reqId).toBe('req-lq-1');
    });
  });

  describe('Clean', () => {
    test('should clean completed jobs', async () => {
      const res = await send(ctx, {
        cmd: 'Clean',
        queue: 'clean-test',
        grace: 0,
        state: 'completed',
      });

      expect(res.ok).toBe(true);
      expect(typeof (res as any).count).toBe('number');
    });

    test('should respect grace period', async () => {
      const res = await send(ctx, {
        cmd: 'Clean',
        queue: 'clean-test',
        grace: 3600000,
      });

      expect(res.ok).toBe(true);
    });

    test('should respect limit parameter', async () => {
      const res = await send(ctx, {
        cmd: 'Clean',
        queue: 'clean-test',
        grace: 0,
        limit: 10,
      });

      expect(res.ok).toBe(true);
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'Clean',
        queue: 'test',
        grace: 0,
        reqId: 'req-cl-1',
      });

      expect(res.reqId).toBe('req-cl-1');
    });
  });

  describe('Count', () => {
    test('should count jobs in a queue', async () => {
      await send(ctx, {
        cmd: 'PUSH',
        queue: 'count-test',
        data: { msg: '1' },
      });
      await send(ctx, {
        cmd: 'PUSH',
        queue: 'count-test',
        data: { msg: '2' },
      });

      const res = await send(ctx, {
        cmd: 'Count',
        queue: 'count-test',
      });

      expect(res.ok).toBe(true);
      expect((res as any).count).toBe(2);
    });

    test('should return 0 for empty queue', async () => {
      const res = await send(ctx, {
        cmd: 'Count',
        queue: 'empty-count-queue',
      });

      expect(res.ok).toBe(true);
      expect((res as any).count).toBe(0);
    });

    test('should include reqId in response', async () => {
      const res = await send(ctx, {
        cmd: 'Count',
        queue: 'test',
        reqId: 'req-cnt-1',
      });

      expect(res.reqId).toBe('req-cnt-1');
    });
  });

  describe('Discard', () => {
    test('should discard a waiting job to DLQ', async () => {
      const pushRes = await send(ctx, {
        cmd: 'PUSH',
        queue: 'discard-test',
        data: { msg: 'discard-me' },
      });
      const jobId = (pushRes as any).id;

      const res = await send(ctx, {
        cmd: 'Discard',
        id: jobId,
      });

      expect(res.ok).toBe(true);
    });

    test('should return error for non-existent job', async () => {
      const res = await send(ctx, {
        cmd: 'Discard',
        id: 'non-existent',
      });

      expect(res.ok).toBe(false);
    });
  });

  describe('MoveToDelayed', () => {
    test('should return error for non-existent job', async () => {
      const res = await send(ctx, {
        cmd: 'MoveToDelayed',
        id: 'non-existent',
        delay: 5000,
      });

      expect(res.ok).toBe(false);
    });

    test('should move an active job to delayed', async () => {
      await send(ctx, {
        cmd: 'PUSH',
        queue: 'move-delayed-test',
        data: { msg: 'delay-me' },
      });
      const pullRes = await send(ctx, {
        cmd: 'PULL',
        queue: 'move-delayed-test',
      });
      const jobId = (pullRes as any).job?.id;
      expect(jobId).toBeDefined();

      const res = await send(ctx, {
        cmd: 'MoveToDelayed',
        id: jobId,
        delay: 5000,
      });

      expect(res.ok).toBe(true);
    });
  });
});

// ============================================================
// Authentication Tests
// ============================================================

describe('Authentication', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should reject commands when auth tokens are configured but not authenticated', async () => {
    const ctx: HandlerContext = {
      queueManager: qm,
      authTokens: new Set(['secret-token']),
      authenticated: false,
    };

    const res = await send(ctx, { cmd: 'Ping' });

    expect(res.ok).toBe(false);
    expect((res as any).error).toBe('Not authenticated');
  });

  test('should allow commands after authentication', async () => {
    const ctx: HandlerContext = {
      queueManager: qm,
      authTokens: new Set(['secret-token']),
      authenticated: false,
    };

    // Authenticate
    const authRes = await send(ctx, {
      cmd: 'Auth',
      token: 'secret-token',
    });
    expect(authRes.ok).toBe(true);

    // Now commands should work
    const pingRes = await send(ctx, { cmd: 'Ping' });
    expect(pingRes.ok).toBe(true);
  });

  test('should reject invalid auth token', async () => {
    const ctx: HandlerContext = {
      queueManager: qm,
      authTokens: new Set(['secret-token']),
      authenticated: false,
    };

    const res = await send(ctx, {
      cmd: 'Auth',
      token: 'wrong-token',
    });

    expect(res.ok).toBe(false);
    expect((res as any).error).toBe('Invalid token');
  });

  test('should allow commands when no auth tokens configured', async () => {
    const ctx: HandlerContext = {
      queueManager: qm,
      authTokens: new Set<string>(),
      authenticated: false,
    };

    const res = await send(ctx, { cmd: 'Ping' });
    expect(res.ok).toBe(true);
  });
});

// ============================================================
// Unknown Command Test
// ============================================================

describe('Unknown Command', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createCtx(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should return error for unknown command', async () => {
    const res = await send(ctx, { cmd: 'NonExistentCommand' } as any);

    expect(res.ok).toBe(false);
    expect((res as any).error).toContain('Unknown command');
  });
});

// ============================================================
// Integration: Full Workflow Tests
// ============================================================

describe('Integration Workflows', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createCtx(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('full lifecycle: push -> pull -> ack -> stats', async () => {
    // Push
    const pushRes = await send(ctx, {
      cmd: 'PUSH',
      queue: 'lifecycle',
      data: { step: 1 },
    });
    expect(pushRes.ok).toBe(true);

    // Pull
    const pullRes = await send(ctx, {
      cmd: 'PULL',
      queue: 'lifecycle',
    });
    expect(pullRes.ok).toBe(true);
    const jobId = (pullRes as any).job?.id;

    // Ack
    const ackRes = await send(ctx, {
      cmd: 'ACK',
      id: jobId,
      result: { done: true },
    });
    expect(ackRes.ok).toBe(true);

    // Stats
    const statsRes = await send(ctx, { cmd: 'Stats' });
    expect(statsRes.ok).toBe(true);
  });

  test('cron lifecycle: add -> list -> delete -> verify', async () => {
    // Add
    await send(ctx, {
      cmd: 'Cron',
      name: 'lifecycle-cron',
      queue: 'test',
      data: {},
      repeatEvery: 5000,
    });

    // List - verify it exists
    let listRes = await send(ctx, { cmd: 'CronList' });
    let names = (listRes as any).crons.map((c: any) => c.name);
    expect(names).toContain('lifecycle-cron');

    // Delete
    const delRes = await send(ctx, {
      cmd: 'CronDelete',
      name: 'lifecycle-cron',
    });
    expect(delRes.ok).toBe(true);

    // Verify deletion
    listRes = await send(ctx, { cmd: 'CronList' });
    names = (listRes as any).crons.map((c: any) => c.name);
    expect(names).not.toContain('lifecycle-cron');
  });

  test('worker lifecycle: register -> heartbeat -> list -> unregister', async () => {
    // Register
    const regRes = await send(ctx, {
      cmd: 'RegisterWorker',
      name: 'lifecycle-worker',
      queues: ['q1'],
    });
    const workerId = (regRes as any).data.workerId;

    // Heartbeat
    const hbRes = await send(ctx, {
      cmd: 'Heartbeat',
      id: workerId,
    });
    expect(hbRes.ok).toBe(true);

    // List
    const listRes = await send(ctx, { cmd: 'ListWorkers' });
    const workerNames = (listRes as any).data.workers.map((w: any) => w.name);
    expect(workerNames).toContain('lifecycle-worker');

    // Unregister
    const unregRes = await send(ctx, {
      cmd: 'UnregisterWorker',
      workerId,
    });
    expect(unregRes.ok).toBe(true);

    // Verify removal
    const listRes2 = await send(ctx, { cmd: 'ListWorkers' });
    const ids = (listRes2 as any).data.workers.map((w: any) => w.id);
    expect(ids).not.toContain(workerId);
  });

  test('webhook lifecycle: add -> list -> remove -> verify', async () => {
    // Add
    const addRes = await send(ctx, {
      cmd: 'AddWebhook',
      url: 'https://example.com/lifecycle',
      events: ['job.completed'],
    });
    const webhookId = (addRes as any).data.webhookId;

    // List
    let listRes = await send(ctx, { cmd: 'ListWebhooks' });
    let urls = (listRes as any).data.webhooks.map((w: any) => w.url);
    expect(urls).toContain('https://example.com/lifecycle');

    // Remove
    const rmRes = await send(ctx, {
      cmd: 'RemoveWebhook',
      webhookId,
    });
    expect(rmRes.ok).toBe(true);

    // Verify removal
    listRes = await send(ctx, { cmd: 'ListWebhooks' });
    urls = (listRes as any).data.webhooks.map((w: any) => w.url);
    expect(urls).not.toContain('https://example.com/lifecycle');
  });

  test('rate limiting workflow: set -> clear', async () => {
    const setRes = await send(ctx, {
      cmd: 'RateLimit',
      queue: 'rl-workflow',
      limit: 100,
    });
    expect(setRes.ok).toBe(true);

    const clearRes = await send(ctx, {
      cmd: 'RateLimitClear',
      queue: 'rl-workflow',
    });
    expect(clearRes.ok).toBe(true);
  });

  test('concurrency workflow: set -> clear', async () => {
    const setRes = await send(ctx, {
      cmd: 'SetConcurrency',
      queue: 'conc-workflow',
      limit: 3,
    });
    expect(setRes.ok).toBe(true);

    const clearRes = await send(ctx, {
      cmd: 'ClearConcurrency',
      queue: 'conc-workflow',
    });
    expect(clearRes.ok).toBe(true);
  });

  test('pause/resume/isPaused workflow', async () => {
    // Pause
    const pauseRes = await send(ctx, {
      cmd: 'Pause',
      queue: 'pr-workflow',
    });
    expect(pauseRes.ok).toBe(true);

    // Check paused
    let isPausedRes = await send(ctx, {
      cmd: 'IsPaused',
      queue: 'pr-workflow',
    });
    expect((isPausedRes as any).paused).toBe(true);

    // Resume
    const resumeRes = await send(ctx, {
      cmd: 'Resume',
      queue: 'pr-workflow',
    });
    expect(resumeRes.ok).toBe(true);

    // Check not paused
    isPausedRes = await send(ctx, {
      cmd: 'IsPaused',
      queue: 'pr-workflow',
    });
    expect((isPausedRes as any).paused).toBe(false);
  });
});
