/**
 * HTTP parseJsonBody Tests
 * Verifies that invalid JSON returns 400 instead of silently becoming {}
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createHttpServer } from '../src/infrastructure/server/http';

describe('HTTP parseJsonBody', () => {
  let qm: QueueManager;
  let baseUrl: string;
  let stop: () => void;

  beforeEach(() => {
    qm = new QueueManager();
    const httpServer = createHttpServer(qm, { port: 0 });
    baseUrl = `http://localhost:${httpServer.server.port}`;
    stop = () => httpServer.stop();
  });

  afterEach(() => {
    stop();
    qm.shutdown();
  });

  describe('invalid JSON returns 400', () => {
    test('POST /queues/:queue/clean with invalid JSON', async () => {
      const res = await fetch(`${baseUrl}/queues/q/clean`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{invalid json!!!',
      });
      expect(res.status).toBe(400);
      const body = await res.json();
      expect(body.ok).toBe(false);
      expect(body.error).toBe('Invalid JSON body');
    });

    test('POST /jobs/:id/ack with invalid JSON', async () => {
      const res = await fetch(`${baseUrl}/jobs/fake-id/ack`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: 'not-json',
      });
      expect(res.status).toBe(400);
      const body = await res.json();
      expect(body.ok).toBe(false);
      expect(body.error).toBe('Invalid JSON body');
    });

    test('POST /jobs/:id/fail with invalid JSON', async () => {
      const res = await fetch(`${baseUrl}/jobs/fake-id/fail`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{{{{',
      });
      expect(res.status).toBe(400);
      const body = await res.json();
      expect(body.ok).toBe(false);
      expect(body.error).toBe('Invalid JSON body');
    });

    test('PUT /jobs/:id/priority with invalid JSON', async () => {
      const res = await fetch(`${baseUrl}/jobs/fake-id/priority`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: 'broken{',
      });
      expect(res.status).toBe(400);
      const body = await res.json();
      expect(body.ok).toBe(false);
      expect(body.error).toBe('Invalid JSON body');
    });

    test('PUT /queues/:queue/rate-limit with invalid JSON', async () => {
      const res = await fetch(`${baseUrl}/queues/q/rate-limit`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: '][',
      });
      expect(res.status).toBe(400);
      const body = await res.json();
      expect(body.ok).toBe(false);
      expect(body.error).toBe('Invalid JSON body');
    });

    test('PUT /webhooks/:id/enabled with invalid JSON', async () => {
      const res = await fetch(`${baseUrl}/webhooks/wh-1/enabled`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: '{bad}',
      });
      expect(res.status).toBe(400);
      const body = await res.json();
      expect(body.ok).toBe(false);
      expect(body.error).toBe('Invalid JSON body');
    });
  });

  describe('empty body still works (backward compat)', () => {
    test('POST /jobs/:id/ack with empty body', async () => {
      // Push a job first, pull it, then ack with empty body
      await fetch(`${baseUrl}/queues/q/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ data: { x: 1 } }),
      });
      const pullRes = await fetch(`${baseUrl}/queues/q/jobs`);
      const pullBody = await pullRes.json();
      const jobId = pullBody.job?.id;

      if (jobId) {
        const res = await fetch(`${baseUrl}/jobs/${jobId}/ack`, { method: 'POST' });
        // Should not return 400 — empty body is allowed
        expect(res.status).not.toBe(400);
      }
    });

    test('POST /jobs/:id/fail with empty body', async () => {
      await fetch(`${baseUrl}/queues/q/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ data: { x: 1 } }),
      });
      const pullRes = await fetch(`${baseUrl}/queues/q/jobs`);
      const pullBody = await pullRes.json();
      const jobId = pullBody.job?.id;

      if (jobId) {
        const res = await fetch(`${baseUrl}/jobs/${jobId}/fail`, { method: 'POST' });
        expect(res.status).not.toBe(400);
      }
    });

    test('POST /queues/:queue/clean with empty body uses defaults', async () => {
      const res = await fetch(`${baseUrl}/queues/q/clean`, { method: 'POST' });
      expect(res.status).not.toBe(400);
    });
  });

  describe('valid JSON works normally', () => {
    test('POST /queues/:queue/jobs with valid JSON', async () => {
      const res = await fetch(`${baseUrl}/queues/q/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ data: { email: 'test@test.com' }, priority: 5 }),
      });
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.id).toBeTruthy();
    });

    test('explicit try/catch routes still work (POST /crons)', async () => {
      const res = await fetch(`${baseUrl}/crons`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{invalid',
      });
      // Crons uses explicit try/catch, not parseJsonBody
      expect(res.status).toBe(400);
      const body = await res.json();
      expect(body.error).toBe('Invalid JSON body');
    });
  });
});
