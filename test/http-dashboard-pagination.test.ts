/**
 * HTTP Dashboard Pagination Tests
 * Verifies that dashboard endpoints return paginated results
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createHttpServer } from '../src/infrastructure/server/http';
import { handleCommand } from '../src/infrastructure/server/handler';
import type { HandlerContext } from '../src/infrastructure/server/types';

describe('HTTP Dashboard Pagination', () => {
  let qm: QueueManager;
  let baseUrl: string;
  let stop: () => void;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    const httpServer = createHttpServer(qm, { port: 0 });
    baseUrl = `http://localhost:${httpServer.server.port}`;
    stop = () => httpServer.stop();
    ctx = { queueManager: qm, authTokens: new Set<string>(), authenticated: false, clientId: 'test' };
  });

  afterEach(() => {
    stop();
    qm.shutdown();
  });

  describe('/dashboard/queues pagination', () => {
    test('returns pagination metadata', async () => {
      // Create 5 queues
      for (let i = 0; i < 5; i++) {
        await handleCommand({ cmd: 'PUSH', queue: `queue-${i}`, data: { i } }, ctx);
      }

      const res = await fetch(`${baseUrl}/dashboard/queues`);
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.total).toBe(5);
      expect(body.limit).toBe(100);
      expect(body.offset).toBe(0);
      expect(body.queues).toHaveLength(5);
    });

    test('limit restricts number of queues returned', async () => {
      for (let i = 0; i < 10; i++) {
        await handleCommand({ cmd: 'PUSH', queue: `q-${String(i).padStart(2, '0')}`, data: {} }, ctx);
      }

      const res = await fetch(`${baseUrl}/dashboard/queues?limit=3`);
      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.queues).toHaveLength(3);
      expect(body.total).toBe(10);
      expect(body.limit).toBe(3);
      expect(body.offset).toBe(0);
    });

    test('offset skips queues', async () => {
      for (let i = 0; i < 10; i++) {
        await handleCommand({ cmd: 'PUSH', queue: `q-${String(i).padStart(2, '0')}`, data: {} }, ctx);
      }

      const res = await fetch(`${baseUrl}/dashboard/queues?limit=3&offset=7`);
      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.queues).toHaveLength(3);
      expect(body.total).toBe(10);
      expect(body.offset).toBe(7);
    });

    test('offset beyond total returns empty', async () => {
      for (let i = 0; i < 3; i++) {
        await handleCommand({ cmd: 'PUSH', queue: `q-${i}`, data: {} }, ctx);
      }

      const res = await fetch(`${baseUrl}/dashboard/queues?offset=100`);
      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.queues).toHaveLength(0);
      expect(body.total).toBe(3);
    });

    test('limit capped at 500', async () => {
      await handleCommand({ cmd: 'PUSH', queue: 'q', data: {} }, ctx);

      const res = await fetch(`${baseUrl}/dashboard/queues?limit=9999`);
      const body = await res.json();
      expect(body.limit).toBeLessThanOrEqual(500);
    });

    test('negative/invalid params use defaults', async () => {
      await handleCommand({ cmd: 'PUSH', queue: 'q', data: {} }, ctx);

      const res = await fetch(`${baseUrl}/dashboard/queues?limit=-5&offset=-10`);
      const body = await res.json();
      expect(body.limit).toBeGreaterThan(0);
      expect(body.offset).toBe(0);
    });

    test('non-numeric params use defaults', async () => {
      await handleCommand({ cmd: 'PUSH', queue: 'q', data: {} }, ctx);

      const res = await fetch(`${baseUrl}/dashboard/queues?limit=abc&offset=xyz`);
      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.limit).toBe(100);
      expect(body.offset).toBe(0);
    });
  });

  describe('/dashboard overview capping', () => {
    test('workers and crons include truncated flag', async () => {
      await handleCommand({ cmd: 'PUSH', queue: 'q', data: {} }, ctx);

      const res = await fetch(`${baseUrl}/dashboard`);
      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.workers).toHaveProperty('truncated');
      expect(body.workers.truncated).toBe(false);
      expect(body.crons).toHaveProperty('truncated');
      expect(body.crons.truncated).toBe(false);
      expect(body.crons).toHaveProperty('total');
    });
  });
});
