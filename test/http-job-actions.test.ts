/**
 * HTTP Job Action Endpoints Tests
 * Tests for all /jobs/:id/* HTTP endpoints
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { handleCommand } from '../src/infrastructure/server/handler';
import type { HandlerContext } from '../src/infrastructure/server/types';

function createCtx(qm: QueueManager): HandlerContext {
  return { queueManager: qm, authTokens: new Set<string>(), authenticated: false, clientId: 'test-http' };
}

describe('HTTP Job Endpoints', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => { qm = new QueueManager(); ctx = createCtx(qm); });
  afterEach(async () => { qm.shutdown(); await Bun.sleep(50); });

  describe('Promote', () => {
    test('promotes delayed job to waiting', async () => {
      const push = await handleCommand({ cmd: 'PUSH', queue: 'q', data: { x: 1 }, delay: 60000 }, ctx);
      const id = (push as { id: string }).id;

      const state = await handleCommand({ cmd: 'GetState', id }, ctx);
      expect((state as { state: string }).state).toBe('delayed');

      const r = await handleCommand({ cmd: 'Promote', id }, ctx);
      expect(r.ok).toBe(true);

      const pull = await handleCommand({ cmd: 'PULL', queue: 'q' }, ctx);
      expect((pull as { job: { id: string } }).job.id).toBe(id);
    });

    test('fails on non-delayed job', async () => {
      const push = await handleCommand({ cmd: 'PUSH', queue: 'q', data: {} }, ctx);
      const r = await handleCommand({ cmd: 'Promote', id: (push as { id: string }).id }, ctx);
      expect(r.ok).toBe(false);
    });

    test('fails on non-existent job', async () => {
      const r = await handleCommand({ cmd: 'Promote', id: 'nope' }, ctx);
      expect(r.ok).toBe(false);
    });
  });

  describe('Update Data', () => {
    test('updates waiting job data', async () => {
      const push = await handleCommand({ cmd: 'PUSH', queue: 'q', data: { old: 1 } }, ctx);
      const id = (push as { id: string }).id;

      const r = await handleCommand({ cmd: 'Update', id, data: { new: 2 } }, ctx);
      expect(r.ok).toBe(true);

      const pull = await handleCommand({ cmd: 'PULL', queue: 'q' }, ctx);
      expect((pull as { job: { data: unknown } }).job.data).toEqual({ new: 2 });
    });

    test('updates delayed job data', async () => {
      const push = await handleCommand({ cmd: 'PUSH', queue: 'q', data: { old: 1 }, delay: 60000 }, ctx);
      const id = (push as { id: string }).id;

      await handleCommand({ cmd: 'Update', id, data: { updated: true } }, ctx);

      const get = await handleCommand({ cmd: 'GetJob', id }, ctx);
      expect((get as { job: { data: unknown } }).job.data).toEqual({ updated: true });
    });

    test('fails on non-existent job', async () => {
      const r = await handleCommand({ cmd: 'Update', id: 'nope', data: {} }, ctx);
      expect(r.ok).toBe(false);
    });
  });

  describe('GetState', () => {
    test('returns waiting state', async () => {
      const push = await handleCommand({ cmd: 'PUSH', queue: 'q', data: {} }, ctx);
      const r = await handleCommand({ cmd: 'GetState', id: (push as { id: string }).id }, ctx);
      expect(r.ok).toBe(true);
      expect((r as { state: string }).state).toBe('waiting');
    });

    test('returns delayed state', async () => {
      const push = await handleCommand({ cmd: 'PUSH', queue: 'q', data: {}, delay: 60000 }, ctx);
      const r = await handleCommand({ cmd: 'GetState', id: (push as { id: string }).id }, ctx);
      expect((r as { state: string }).state).toBe('delayed');
    });
  });

  describe('GetResult', () => {
    test('returns result after ack', async () => {
      const push = await handleCommand({ cmd: 'PUSH', queue: 'q', data: {} }, ctx);
      const id = (push as { id: string }).id;
      await handleCommand({ cmd: 'PULL', queue: 'q' }, ctx);
      await handleCommand({ cmd: 'ACK', id, result: { done: true } }, ctx);

      const r = await handleCommand({ cmd: 'GetResult', id }, ctx);
      expect(r.ok).toBe(true);
      expect((r as { result: unknown }).result).toEqual({ done: true });
    });
  });

  describe('ChangePriority', () => {
    test('changes job priority', async () => {
      const push = await handleCommand({ cmd: 'PUSH', queue: 'q', data: {}, priority: 1 }, ctx);
      const id = (push as { id: string }).id;

      const r = await handleCommand({ cmd: 'ChangePriority', id, priority: 10 }, ctx);
      expect(r.ok).toBe(true);

      const get = await handleCommand({ cmd: 'GetJob', id }, ctx);
      expect((get as { job: { priority: number } }).job.priority).toBe(10);
    });
  });

  describe('Discard', () => {
    test('moves job to DLQ', async () => {
      const push = await handleCommand({ cmd: 'PUSH', queue: 'q-discard', data: { x: 1 } }, ctx);
      const id = (push as { id: string }).id;

      const r = await handleCommand({ cmd: 'Discard', id }, ctx);
      expect(r.ok).toBe(true);

      // Job should no longer be pullable
      const pull = await handleCommand({ cmd: 'PULL', queue: 'q-discard' }, ctx);
      expect((pull as { job: null }).job).toBeNull();
    });
  });

  describe('Progress', () => {
    test('updates and gets job progress', async () => {
      const push = await handleCommand({ cmd: 'PUSH', queue: 'q', data: {} }, ctx);
      const id = (push as { id: string }).id;
      await handleCommand({ cmd: 'PULL', queue: 'q' }, ctx);

      const r = await handleCommand({ cmd: 'Progress', id, progress: 50 }, ctx);
      expect(r.ok).toBe(true);

      const get = await handleCommand({ cmd: 'GetProgress', id }, ctx);
      expect(get.ok).toBe(true);
      expect((get as { progress: number }).progress).toBe(50);
    });
  });

  describe('Job Logs', () => {
    test('add, get, and clear logs', async () => {
      const push = await handleCommand({ cmd: 'PUSH', queue: 'q', data: {} }, ctx);
      const id = (push as { id: string }).id;

      await handleCommand({ cmd: 'AddLog', id, message: 'test log', level: 'info' }, ctx);
      await handleCommand({ cmd: 'AddLog', id, message: 'warning', level: 'warn' }, ctx);

      const logs = await handleCommand({ cmd: 'GetLogs', id }, ctx);
      expect(logs.ok).toBe(true);
      expect((logs as { data: { logs: unknown[] } }).data.logs).toHaveLength(2);

      const clear = await handleCommand({ cmd: 'ClearLogs', id }, ctx);
      expect(clear.ok).toBe(true);

      const logsAfter = await handleCommand({ cmd: 'GetLogs', id }, ctx);
      expect((logsAfter as { data: { logs: unknown[] } }).data.logs).toHaveLength(0);
    });
  });

  describe('MoveToDelayed', () => {
    test('moves active job to delayed', async () => {
      const push = await handleCommand({ cmd: 'PUSH', queue: 'q-mtd', data: {} }, ctx);
      const id = (push as { id: string }).id;
      await handleCommand({ cmd: 'PULL', queue: 'q-mtd' }, ctx);

      const r = await handleCommand({ cmd: 'MoveToDelayed', id, delay: 60000 }, ctx);
      expect(r.ok).toBe(true);

      const state = await handleCommand({ cmd: 'GetState', id }, ctx);
      expect((state as { state: string }).state).toBe('delayed');
    });
  });
});
