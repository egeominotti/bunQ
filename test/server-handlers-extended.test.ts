/** Direct tests for dlq.ts, cron.ts, and monitoring.ts handlers */
import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import type { HandlerContext } from '../src/infrastructure/server/types';
import { handlePush, handlePull, handleFail } from '../src/infrastructure/server/handlers/core';
import { handleDlq, handleRetryDlq, handlePurgeDlq, handleRetryCompleted } from '../src/infrastructure/server/handlers/dlq';
import { handleCron, handleCronDelete, handleCronList } from '../src/infrastructure/server/handlers/cron';
import { handlePing, handleHeartbeat, handleJobHeartbeat, handleRegisterWorker,
  handleUnregisterWorker, handleListWorkers, handleAddWebhook, handleRemoveWebhook,
  handleListWebhooks, handlePrometheus } from '../src/infrastructure/server/handlers/monitoring';

function createContext(qm: QueueManager): HandlerContext {
  return { queueManager: qm, authTokens: new Set<string>(), authenticated: false, clientId: 'test-1' };
}

describe('DLQ Handlers (direct)', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;
  beforeEach(() => { qm = new QueueManager(); ctx = createContext(qm); });
  afterEach(() => { qm.shutdown(); });

  describe('handleDlq', () => {
    test('should return empty list for queue with no DLQ entries', () => {
      const res = handleDlq({ cmd: 'Dlq', queue: 'test-queue' }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).jobs).toEqual([]);
    });
    test('should respect count parameter', () => {
      const res = handleDlq({ cmd: 'Dlq', queue: 'test-queue', count: 5 }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).jobs.length).toBeLessThanOrEqual(5);
    });
    test('should propagate reqId', () => {
      const res = handleDlq({ cmd: 'Dlq', queue: 'test', reqId: 'r1' }, ctx, 'r1');
      expect(res.reqId).toBe('r1');
    });
    test('should handle DLQ after job exhausts retries', async () => {
      await handlePush({ cmd: 'PUSH', queue: 'dlq-t', data: { msg: 'fail' }, maxAttempts: 1 }, ctx);
      const pullRes = await handlePull({ cmd: 'PULL', queue: 'dlq-t' }, ctx);
      await handleFail({ cmd: 'FAIL', id: (pullRes as any).job?.id, error: 'boom' }, ctx);
      const res = handleDlq({ cmd: 'Dlq', queue: 'dlq-t' }, ctx);
      expect(res.ok).toBe(true);
    });
  });

  describe('handleRetryDlq', () => {
    test('should return count 0 when no DLQ entries', () => {
      const res = handleRetryDlq({ cmd: 'RetryDlq', queue: 'empty' }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).count).toBe(0);
    });
    test('should accept optional jobId and propagate reqId', () => {
      const res = handleRetryDlq({ cmd: 'RetryDlq', queue: 'q', jobId: 'x', reqId: 'r2' }, ctx, 'r2');
      expect(res.ok).toBe(true);
      expect((res as any).count).toBe(0);
      expect(res.reqId).toBe('r2');
    });
  });

  describe('handlePurgeDlq', () => {
    test('should return count 0 when no DLQ entries', () => {
      const res = handlePurgeDlq({ cmd: 'PurgeDlq', queue: 'empty' }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).count).toBe(0);
    });
    test('should propagate reqId', () => {
      const res = handlePurgeDlq({ cmd: 'PurgeDlq', queue: 'q', reqId: 'r3' }, ctx, 'r3');
      expect(res.reqId).toBe('r3');
    });
  });

  describe('handleRetryCompleted', () => {
    test('should return count 0 and accept optional id', () => {
      const res = handleRetryCompleted({ cmd: 'RetryCompleted', queue: 'e', id: 'x' }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).count).toBe(0);
    });
    test('should propagate reqId', () => {
      const res = handleRetryCompleted({ cmd: 'RetryCompleted', queue: 'q', reqId: 'r4' }, ctx, 'r4');
      expect(res.reqId).toBe('r4');
    });
  });
});

describe('Cron Handlers (direct)', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;
  beforeEach(() => { qm = new QueueManager(); ctx = createContext(qm); });
  afterEach(() => { qm.shutdown(); });

  describe('handleCron', () => {
    test('should add a cron job with repeatEvery', () => {
      const res = handleCron({ cmd: 'Cron', name: 'r-cron', queue: 'q1', data: {}, repeatEvery: 60000 }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).cron.name).toBe('r-cron');
      expect((res as any).cron.repeatEvery).toBe(60000);
      expect((res as any).cron.nextRun).toBeGreaterThan(0);
    });
    test('should add a cron job with schedule expression', () => {
      const res = handleCron({ cmd: 'Cron', name: 's-cron', queue: 'q1', data: {}, schedule: '0 0 * * *' }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).cron.schedule).toBe('0 0 * * *');
    });
    test('should add a cron job with timezone', () => {
      const res = handleCron(
        { cmd: 'Cron', name: 'tz-cron', queue: 'q1', data: {}, schedule: '0 9 * * *', timezone: 'America/New_York' }, ctx
      );
      expect(res.ok).toBe(true);
      expect((res as any).cron.timezone).toBe('America/New_York');
    });
    test('should return error for invalid cron (no schedule or repeatEvery)', () => {
      const res = handleCron({ cmd: 'Cron', name: 'bad', queue: 'q1', data: {} }, ctx);
      expect(res.ok).toBe(false);
      expect((res as any).error).toBeDefined();
    });
    test('should propagate reqId', () => {
      const res = handleCron({ cmd: 'Cron', name: 'rq', queue: 'q1', data: {}, repeatEvery: 5000, reqId: 'c1' }, ctx, 'c1');
      expect(res.reqId).toBe('c1');
    });
  });

  describe('handleCronDelete', () => {
    test('should delete an existing cron job', () => {
      handleCron({ cmd: 'Cron', name: 'del', queue: 'q1', data: {}, repeatEvery: 10000 }, ctx);
      const res = handleCronDelete({ cmd: 'CronDelete', name: 'del' }, ctx);
      expect(res.ok).toBe(true);
    });
    test('should return error for non-existent cron job', () => {
      const res = handleCronDelete({ cmd: 'CronDelete', name: 'nope' }, ctx);
      expect(res.ok).toBe(false);
      expect((res as any).error).toBe('Cron job not found');
    });
    test('should propagate reqId', () => {
      const res = handleCronDelete({ cmd: 'CronDelete', name: 'nope', reqId: 'd1' }, ctx, 'd1');
      expect(res.reqId).toBe('d1');
    });
  });

  describe('handleCronList', () => {
    test('should return empty list when no cron jobs exist', () => {
      const res = handleCronList({ cmd: 'CronList' }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).crons).toEqual([]);
    });
    test('should list cron jobs with detail fields', () => {
      handleCron({ cmd: 'Cron', name: 'a', queue: 'q1', data: {}, repeatEvery: 10000 }, ctx);
      handleCron({ cmd: 'Cron', name: 'b', queue: 'q2', data: {}, repeatEvery: 20000 }, ctx);
      const crons = (handleCronList({ cmd: 'CronList' }, ctx) as any).crons;
      expect(crons.map((c: any) => c.name)).toContain('a');
      expect(crons.map((c: any) => c.name)).toContain('b');
      expect(typeof crons[0].executions).toBe('number');
      expect(typeof crons[0].nextRun).toBe('number');
    });
    test('should propagate reqId', () => {
      const res = handleCronList({ cmd: 'CronList', reqId: 'l1' }, ctx, 'l1');
      expect(res.reqId).toBe('l1');
    });
  });
});

describe('Monitoring Handlers (direct)', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;
  beforeEach(() => { qm = new QueueManager(); ctx = createContext(qm); });
  afterEach(() => { qm.shutdown(); });

  describe('handlePing', () => {
    test('should return pong with timestamp and propagate reqId', () => {
      const before = Date.now();
      const res = handlePing({ cmd: 'Ping', reqId: 'p1' }, ctx, 'p1');
      expect(res.ok).toBe(true);
      expect((res as any).data.pong).toBe(true);
      expect((res as any).data.time).toBeGreaterThanOrEqual(before);
      expect(res.reqId).toBe('p1');
    });
  });

  describe('handleHeartbeat', () => {
    test('should heartbeat a registered worker', () => {
      const reg = handleRegisterWorker({ cmd: 'RegisterWorker', name: 'hb', queues: ['q1'] }, ctx);
      const res = handleHeartbeat({ cmd: 'Heartbeat', id: (reg as any).data.workerId }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).data.ok).toBe(true);
    });
    test('should return error for non-existent worker', () => {
      const res = handleHeartbeat({ cmd: 'Heartbeat', id: 'fake' }, ctx);
      expect(res.ok).toBe(false);
      expect((res as any).error).toBe('Worker not found');
    });
  });

  describe('handleJobHeartbeat', () => {
    test('should heartbeat an active job', async () => {
      await handlePush({ cmd: 'PUSH', queue: 'jh', data: {} }, ctx);
      const pull = await handlePull({ cmd: 'PULL', queue: 'jh' }, ctx);
      const res = handleJobHeartbeat({ cmd: 'JobHeartbeat', id: (pull as any).job.id }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).data.ok).toBe(true);
    });
    test('should return error for non-existent job and propagate reqId', () => {
      const res = handleJobHeartbeat({ cmd: 'JobHeartbeat', id: 'fake', reqId: 'j1' }, ctx, 'j1');
      expect(res.ok).toBe(false);
      expect((res as any).error).toContain('Job not found');
      expect(res.reqId).toBe('j1');
    });
  });

  describe('handleRegisterWorker / handleUnregisterWorker', () => {
    test('should register and return worker info', () => {
      const res = handleRegisterWorker({ cmd: 'RegisterWorker', name: 'w1', queues: ['emails', 'sms'] }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).data.workerId).toBeDefined();
      expect((res as any).data.name).toBe('w1');
      expect((res as any).data.queues).toEqual(['emails', 'sms']);
    });
    test('should unregister an existing worker', () => {
      const reg = handleRegisterWorker({ cmd: 'RegisterWorker', name: 'w2', queues: ['q1'] }, ctx);
      const res = handleUnregisterWorker({ cmd: 'UnregisterWorker', workerId: (reg as any).data.workerId }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).data.removed).toBe(true);
    });
    test('should return error when unregistering non-existent worker', () => {
      const res = handleUnregisterWorker({ cmd: 'UnregisterWorker', workerId: 'nope' }, ctx);
      expect(res.ok).toBe(false);
      expect((res as any).error).toBe('Worker not found');
    });
  });

  describe('handleListWorkers', () => {
    test('should return empty list or list workers with detail fields', () => {
      const empty = handleListWorkers({ cmd: 'ListWorkers' }, ctx);
      expect(empty.ok).toBe(true);
      expect((empty as any).data.workers).toEqual([]);
      expect((empty as any).data.stats).toBeDefined();
      handleRegisterWorker({ cmd: 'RegisterWorker', name: 'lw1', queues: ['q1'] }, ctx);
      const w = (handleListWorkers({ cmd: 'ListWorkers' }, ctx) as any).data.workers[0];
      expect(w.id).toBeDefined();
      expect(w.name).toBe('lw1');
      expect(typeof w.activeJobs).toBe('number');
      expect(typeof w.failedJobs).toBe('number');
    });
  });

  describe('handleAddWebhook / handleRemoveWebhook / handleListWebhooks', () => {
    test('should add a webhook and return its info', () => {
      const res = handleAddWebhook({ cmd: 'AddWebhook', url: 'https://example.com/hook', events: ['job.completed'] }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).data.webhookId).toBeDefined();
      expect((res as any).data.url).toBe('https://example.com/hook');
    });
    test('should reject invalid and empty webhook URLs', () => {
      const r1 = handleAddWebhook({ cmd: 'AddWebhook', url: 'not-a-url', events: ['job.completed'] }, ctx);
      expect(r1.ok).toBe(false);
      const r2 = handleAddWebhook({ cmd: 'AddWebhook', url: '', events: ['job.completed'] }, ctx);
      expect(r2.ok).toBe(false);
    });
    test('should remove an existing webhook', () => {
      const add = handleAddWebhook({ cmd: 'AddWebhook', url: 'https://example.com/rm', events: ['job.failed'] }, ctx);
      const res = handleRemoveWebhook({ cmd: 'RemoveWebhook', webhookId: (add as any).data.webhookId }, ctx);
      expect(res.ok).toBe(true);
      expect((res as any).data.removed).toBe(true);
    });
    test('should return error when removing non-existent webhook', () => {
      const res = handleRemoveWebhook({ cmd: 'RemoveWebhook', webhookId: 'nope' }, ctx);
      expect(res.ok).toBe(false);
      expect((res as any).error).toBe('Webhook not found');
    });
    test('should list webhooks with detail fields, stats, or empty list', () => {
      const empty = handleListWebhooks({ cmd: 'ListWebhooks' }, ctx);
      expect(empty.ok).toBe(true);
      expect((empty as any).data.webhooks).toEqual([]);
      handleAddWebhook({ cmd: 'AddWebhook', url: 'https://example.com/a', events: ['job.completed'] }, ctx);
      const res = handleListWebhooks({ cmd: 'ListWebhooks' }, ctx);
      const wh = (res as any).data.webhooks[0];
      expect(wh.id).toBeDefined();
      expect(typeof wh.enabled).toBe('boolean');
      expect(typeof wh.successCount).toBe('number');
      expect((res as any).data.stats).toBeDefined();
    });
  });

  describe('handlePrometheus', () => {
    test('should return prometheus metrics string with bunqueue prefix', async () => {
      await handlePush({ cmd: 'PUSH', queue: 'prom', data: {} }, ctx);
      const res = handlePrometheus({ cmd: 'Prometheus' }, ctx);
      expect(res.ok).toBe(true);
      expect(typeof (res as any).data.metrics).toBe('string');
      expect((res as any).data.metrics).toContain('bunqueue_');
    });
    test('should propagate reqId', () => {
      const res = handlePrometheus({ cmd: 'Prometheus', reqId: 'pr1' }, ctx, 'pr1');
      expect(res.reqId).toBe('pr1');
    });
  });
});
