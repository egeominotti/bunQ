/**
 * HTTP Queue & Resource Endpoints Tests
 * Tests for queue control, DLQ, crons, webhooks, workers, and monitoring
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { handleCommand } from '../src/infrastructure/server/handler';
import type { HandlerContext } from '../src/infrastructure/server/types';

function createCtx(qm: QueueManager): HandlerContext {
  return { queueManager: qm, authTokens: new Set<string>(), authenticated: false, clientId: 'test-http' };
}

describe('HTTP Queue Endpoints', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => { qm = new QueueManager(); ctx = createCtx(qm); });
  afterEach(async () => { qm.shutdown(); await Bun.sleep(50); });

  describe('Drain', () => {
    test('drains all jobs', async () => {
      for (let i = 0; i < 5; i++) await handleCommand({ cmd: 'PUSH', queue: 'q', data: { i } }, ctx);
      const r = await handleCommand({ cmd: 'Drain', queue: 'q' }, ctx);
      expect(r.ok).toBe(true);
      expect((r as { count: number }).count).toBe(5);

      const counts = await handleCommand({ cmd: 'GetJobCounts', queue: 'q' }, ctx);
      expect((counts as { counts: { waiting: number } }).counts.waiting).toBe(0);
    });

    test('drain empty queue returns 0', async () => {
      const r = await handleCommand({ cmd: 'Drain', queue: 'empty' }, ctx);
      expect((r as { count: number }).count).toBe(0);
    });
  });

  describe('Clean', () => {
    test('cleans with grace 0', async () => {
      for (let i = 0; i < 3; i++) await handleCommand({ cmd: 'PUSH', queue: 'q', data: { i } }, ctx);
      const r = await handleCommand({ cmd: 'Clean', queue: 'q', grace: 0 }, ctx);
      expect((r as { count: number }).count).toBe(3);
    });

    test('high grace removes nothing', async () => {
      await handleCommand({ cmd: 'PUSH', queue: 'q', data: {} }, ctx);
      const r = await handleCommand({ cmd: 'Clean', queue: 'q', grace: 3600000 }, ctx);
      expect((r as { count: number }).count).toBe(0);
    });

    test('limit respected', async () => {
      for (let i = 0; i < 5; i++) await handleCommand({ cmd: 'PUSH', queue: 'q', data: { i } }, ctx);
      const r = await handleCommand({ cmd: 'Clean', queue: 'q', grace: 0, limit: 2 }, ctx);
      expect((r as { count: number }).count).toBe(2);
    });
  });

  describe('Pause / Resume', () => {
    test('pause and resume queue', async () => {
      await handleCommand({ cmd: 'PUSH', queue: 'q-pr', data: {} }, ctx);
      const pause = await handleCommand({ cmd: 'Pause', queue: 'q-pr' }, ctx);
      expect(pause.ok).toBe(true);

      const isPaused = await handleCommand({ cmd: 'IsPaused', queue: 'q-pr' }, ctx);
      expect((isPaused as { paused: boolean }).paused).toBe(true);

      const resume = await handleCommand({ cmd: 'Resume', queue: 'q-pr' }, ctx);
      expect(resume.ok).toBe(true);

      const isResumed = await handleCommand({ cmd: 'IsPaused', queue: 'q-pr' }, ctx);
      expect((isResumed as { paused: boolean }).paused).toBe(false);
    });
  });

  describe('Obliterate', () => {
    test('obliterates a queue', async () => {
      for (let i = 0; i < 3; i++) await handleCommand({ cmd: 'PUSH', queue: 'q-obl', data: { i } }, ctx);
      const r = await handleCommand({ cmd: 'Obliterate', queue: 'q-obl' }, ctx);
      expect(r.ok).toBe(true);

      const counts = await handleCommand({ cmd: 'GetJobCounts', queue: 'q-obl' }, ctx);
      expect((counts as { counts: { waiting: number } }).counts.waiting).toBe(0);
    });
  });

  describe('ListQueues', () => {
    test('lists queues', async () => {
      await handleCommand({ cmd: 'PUSH', queue: 'alpha', data: {} }, ctx);
      await handleCommand({ cmd: 'PUSH', queue: 'beta', data: {} }, ctx);

      const r = await handleCommand({ cmd: 'ListQueues' }, ctx);
      expect(r.ok).toBe(true);
      const queues = (r as { queues: string[] }).queues;
      expect(queues).toContain('alpha');
      expect(queues).toContain('beta');
    });
  });

  describe('GetJobCounts / Count', () => {
    test('returns correct counts', async () => {
      for (let i = 0; i < 3; i++) await handleCommand({ cmd: 'PUSH', queue: 'q-cnt', data: {} }, ctx);
      await handleCommand({ cmd: 'PUSH', queue: 'q-cnt', data: {}, delay: 60000 }, ctx);

      const counts = await handleCommand({ cmd: 'GetJobCounts', queue: 'q-cnt' }, ctx);
      const c = (counts as { counts: { waiting: number; delayed: number } }).counts;
      expect(c.waiting).toBe(3);
      expect(c.delayed).toBe(1);

      const count = await handleCommand({ cmd: 'Count', queue: 'q-cnt' }, ctx);
      expect((count as { count: number }).count).toBe(4);
    });
  });

  describe('GetJobs', () => {
    test('lists jobs by state', async () => {
      for (let i = 0; i < 3; i++) await handleCommand({ cmd: 'PUSH', queue: 'q-list', data: { i } }, ctx);
      const r = await handleCommand({ cmd: 'GetJobs', queue: 'q-list', state: 'waiting', limit: 10 } as Parameters<typeof handleCommand>[0], ctx);
      expect(r.ok).toBe(true);
      expect((r as { jobs: unknown[] }).jobs).toHaveLength(3);
    });

    test('limit and offset work correctly', async () => {
      for (let i = 0; i < 5; i++) await handleCommand({ cmd: 'PUSH', queue: 'q-pag', data: { i } }, ctx);
      const r = await handleCommand({ cmd: 'GetJobs', queue: 'q-pag', state: 'waiting', limit: 2, offset: 1 } as Parameters<typeof handleCommand>[0], ctx);
      expect(r.ok).toBe(true);
      expect((r as { jobs: unknown[] }).jobs).toHaveLength(2);
    });
  });

  describe('PUSH maxAttempts', () => {
    test('maxAttempts is correctly passed', async () => {
      const push = await handleCommand({ cmd: 'PUSH', queue: 'q-ma', data: {}, maxAttempts: 7 }, ctx);
      expect(push.ok).toBe(true);
      const id = (push as { id: string }).id;
      const get = await handleCommand({ cmd: 'GetJob', id }, ctx);
      expect((get as { job: { maxAttempts: number } }).job.maxAttempts).toBe(7);
    });
  });

  describe('Bulk Push', () => {
    test('pushes multiple jobs at once', async () => {
      const r = await handleCommand({
        cmd: 'PUSHB',
        queue: 'q-bulk',
        jobs: [{ data: { a: 1 } }, { data: { b: 2 } }, { data: { c: 3 } }],
      } as Parameters<typeof handleCommand>[0], ctx);
      expect(r.ok).toBe(true);
      expect((r as { ids: string[] }).ids).toHaveLength(3);
    });
  });

  describe('PromoteJobs (batch)', () => {
    test('promotes multiple delayed jobs', async () => {
      for (let i = 0; i < 3; i++) {
        await handleCommand({ cmd: 'PUSH', queue: 'q-pj', data: { i }, delay: 60000 }, ctx);
      }
      const r = await handleCommand({ cmd: 'PromoteJobs', queue: 'q-pj' } as Parameters<typeof handleCommand>[0], ctx);
      expect(r.ok).toBe(true);
      expect((r as { count: number }).count).toBe(3);
    });
  });

  describe('DLQ', () => {
    test('list, retry, purge DLQ', async () => {
      const push = await handleCommand({ cmd: 'PUSH', queue: 'q-dlq', data: {} }, ctx);
      const id = (push as { id: string }).id;
      await handleCommand({ cmd: 'Discard', id }, ctx);

      const list = await handleCommand({ cmd: 'Dlq', queue: 'q-dlq', count: 10 }, ctx);
      expect(list.ok).toBe(true);
      expect((list as { jobs: unknown[] }).jobs.length).toBeGreaterThan(0);

      const purge = await handleCommand({ cmd: 'PurgeDlq', queue: 'q-dlq' }, ctx);
      expect(purge.ok).toBe(true);
    });
  });

  describe('Rate Limit', () => {
    test('set and clear rate limit', async () => {
      await handleCommand({ cmd: 'PUSH', queue: 'q-rl', data: {} }, ctx);
      const set = await handleCommand({ cmd: 'RateLimit', queue: 'q-rl', limit: 10 } as Parameters<typeof handleCommand>[0], ctx);
      expect(set.ok).toBe(true);

      const clear = await handleCommand({ cmd: 'RateLimitClear', queue: 'q-rl' }, ctx);
      expect(clear.ok).toBe(true);
    });
  });

  describe('Concurrency', () => {
    test('set and clear concurrency', async () => {
      await handleCommand({ cmd: 'PUSH', queue: 'q-cc', data: {} }, ctx);
      const set = await handleCommand({ cmd: 'SetConcurrency', queue: 'q-cc', limit: 5 } as Parameters<typeof handleCommand>[0], ctx);
      expect(set.ok).toBe(true);

      const clear = await handleCommand({ cmd: 'ClearConcurrency', queue: 'q-cc' }, ctx);
      expect(clear.ok).toBe(true);
    });
  });

  describe('Batch Pull (PULLB)', () => {
    test('pulls multiple jobs at once', async () => {
      for (let i = 0; i < 3; i++) await handleCommand({ cmd: 'PUSH', queue: 'q-pb', data: { i } }, ctx);
      const r = await handleCommand({
        cmd: 'PULLB', queue: 'q-pb', count: 3,
      } as Parameters<typeof handleCommand>[0], ctx);
      expect(r.ok).toBe(true);
      expect((r as { jobs: unknown[] }).jobs).toHaveLength(3);
    });
  });

  describe('Batch ACK (ACKB)', () => {
    test('acks multiple jobs at once', async () => {
      const ids: string[] = [];
      for (let i = 0; i < 3; i++) {
        const push = await handleCommand({ cmd: 'PUSH', queue: 'q-ab', data: { i } }, ctx);
        ids.push((push as { id: string }).id);
      }
      await handleCommand({ cmd: 'PULLB', queue: 'q-ab', count: 3 } as Parameters<typeof handleCommand>[0], ctx);
      const r = await handleCommand({
        cmd: 'ACKB', ids,
      } as Parameters<typeof handleCommand>[0], ctx);
      expect(r.ok).toBe(true);
    });
  });

  describe('MoveToWait', () => {
    test('moves delayed job to waiting', async () => {
      const push = await handleCommand({ cmd: 'PUSH', queue: 'q-mtw', data: {}, delay: 60000 }, ctx);
      const id = (push as { id: string }).id;
      const r = await handleCommand({ cmd: 'MoveToWait', id } as Parameters<typeof handleCommand>[0], ctx);
      expect(r.ok).toBe(true);
    });
  });
});

describe('HTTP Resource Endpoints', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => { qm = new QueueManager(); ctx = createCtx(qm); });
  afterEach(async () => { qm.shutdown(); await Bun.sleep(50); });

  describe('Crons', () => {
    test('add, get, list, and delete cron', async () => {
      const add = await handleCommand({
        cmd: 'Cron', name: 'test-cron', queue: 'cron-q', data: { task: true },
        schedule: '*/5 * * * *',
      } as Parameters<typeof handleCommand>[0], ctx);
      expect(add.ok).toBe(true);

      const get = await handleCommand({ cmd: 'CronGet', name: 'test-cron' }, ctx);
      expect(get.ok).toBe(true);

      const list = await handleCommand({ cmd: 'CronList' }, ctx);
      expect(list.ok).toBe(true);
      expect((list as { crons: unknown[] }).crons.length).toBeGreaterThan(0);

      const del = await handleCommand({ cmd: 'CronDelete', name: 'test-cron' }, ctx);
      expect(del.ok).toBe(true);
    });
  });

  describe('Workers', () => {
    test('register, list, and unregister worker', async () => {
      const reg = await handleCommand({
        cmd: 'RegisterWorker', name: 'test-worker', queues: ['q1', 'q2'],
      } as Parameters<typeof handleCommand>[0], ctx);
      expect(reg.ok).toBe(true);
      const workerId = (reg as { data: { workerId: string } }).data.workerId;

      const list = await handleCommand({ cmd: 'ListWorkers' }, ctx);
      expect(list.ok).toBe(true);

      const unreg = await handleCommand({ cmd: 'UnregisterWorker', workerId } as Parameters<typeof handleCommand>[0], ctx);
      expect(unreg.ok).toBe(true);
    });
  });

  describe('Webhooks', () => {
    test('add, list, toggle, and remove webhook', async () => {
      const add = await handleCommand({
        cmd: 'AddWebhook', url: 'https://example.com/hook', events: ['completed'],
      } as Parameters<typeof handleCommand>[0], ctx);
      expect(add.ok).toBe(true);
      const webhookId = (add as { data: { webhookId: string } }).data.webhookId;

      const list = await handleCommand({ cmd: 'ListWebhooks' }, ctx);
      expect(list.ok).toBe(true);

      const toggle = await handleCommand({
        cmd: 'SetWebhookEnabled', id: webhookId, enabled: false,
      } as Parameters<typeof handleCommand>[0], ctx);
      expect(toggle.ok).toBe(true);

      const remove = await handleCommand({
        cmd: 'RemoveWebhook', webhookId,
      } as Parameters<typeof handleCommand>[0], ctx);
      expect(remove.ok).toBe(true);
    });
  });

  describe('Ping', () => {
    test('returns pong', async () => {
      const r = await handleCommand({ cmd: 'Ping' }, ctx);
      expect(r.ok).toBe(true);
      expect((r as { data: { pong: boolean } }).data.pong).toBe(true);
    });
  });

  describe('StorageStatus', () => {
    test('returns storage info', async () => {
      const r = await handleCommand({ cmd: 'StorageStatus' }, ctx);
      expect(r.ok).toBe(true);
    });
  });
});
