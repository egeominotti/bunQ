/**
 * Webhook Delivery Tests
 * Tests actual HTTP delivery using a local mock server (Bun.serve)
 */
import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { WebhookManager } from '../src/application/webhookManager';
import type { WebhookPayload } from '../src/domain/types/webhook';

interface ReceivedRequest {
  body: WebhookPayload;
  headers: Record<string, string>;
}

function createMockServer(statusCode = 200) {
  const received: ReceivedRequest[] = [];
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const body = (await req.json()) as WebhookPayload;
      const headers: Record<string, string> = {};
      req.headers.forEach((v, k) => { headers[k] = v; });
      received.push({ body, headers });
      return new Response('OK', { status: statusCode });
    },
  });
  return {
    url: `http://localhost:${server.port}/webhook`,
    received,
    stop: () => server.stop(true),
  };
}

async function waitFor(arr: unknown[], count: number, ms = 5000) {
  const t = Date.now();
  while (arr.length < count && Date.now() - t < ms) await Bun.sleep(50);
}

describe('Webhook Delivery', () => {
  let manager: WebhookManager;
  let mock: ReturnType<typeof createMockServer>;

  beforeEach(() => {
    manager = new WebhookManager({ validateUrls: false });
    mock = createMockServer();
  });

  afterEach(() => { mock.stop(); });

  describe('registration', () => {
    test('should register with all properties', () => {
      const wh = manager.add(mock.url, ['job.completed'], 'q1', 'secret');
      expect(wh.id).toBeDefined();
      expect(wh.url).toBe(mock.url);
      expect(wh.events).toEqual(['job.completed']);
      expect(wh.queue).toBe('q1');
      expect(wh.secret).toBe('secret');
      expect(wh.enabled).toBe(true);
      expect(wh.successCount).toBe(0);
      expect(wh.failureCount).toBe(0);
    });

    test('should default queue to null and support multiple events', () => {
      const wh = manager.add(mock.url, ['job.completed', 'job.failed', 'job.pushed']);
      expect(wh.queue).toBeNull();
      expect(wh.events).toHaveLength(3);
    });
  });

  describe('delivery on job completion', () => {
    test('should deliver payload with correct fields', async () => {
      manager.add(mock.url, ['job.completed']);
      await manager.trigger('job.completed', 'job-1', 'emails', { data: { to: 'u@t.com' } });
      await waitFor(mock.received, 1);

      expect(mock.received).toHaveLength(1);
      const p = mock.received[0].body;
      expect(p.event).toBe('job.completed');
      expect(p.jobId).toBe('job-1');
      expect(p.queue).toBe('emails');
      expect(p.data).toEqual({ to: 'u@t.com' });
      expect(typeof p.timestamp).toBe('number');
      expect(p.timestamp).toBeGreaterThan(0);
    });
  });

  describe('delivery on job failure', () => {
    test('should deliver error field on job.failed', async () => {
      manager.add(mock.url, ['job.failed']);
      await manager.trigger('job.failed', 'job-2', 'emails', { error: 'SMTP refused' });
      await waitFor(mock.received, 1);

      expect(mock.received).toHaveLength(1);
      const p = mock.received[0].body;
      expect(p.event).toBe('job.failed');
      expect(p.jobId).toBe('job-2');
      expect(p.error).toBe('SMTP refused');
    });
  });

  describe('payload structure', () => {
    test('should include correct HTTP headers', async () => {
      manager.add(mock.url, ['job.completed']);
      await manager.trigger('job.completed', 'job-3', 'tasks');
      await waitFor(mock.received, 1);

      const h = mock.received[0].headers;
      expect(h['content-type']).toBe('application/json');
      expect(h['x-webhook-event']).toBe('job.completed');
      expect(h['x-webhook-timestamp']).toBeDefined();
    });

    test('should include valid HMAC signature when secret is set', async () => {
      const secret = 'my-secret';
      manager.add(mock.url, ['job.completed'], undefined, secret);
      await manager.trigger('job.completed', 'job-4', 'tasks');
      await waitFor(mock.received, 1);

      const sig = mock.received[0].headers['x-webhook-signature'];
      expect(sig).toBeDefined();
      expect(sig.length).toBe(64);
      const body = JSON.stringify(mock.received[0].body);
      const expected = new Bun.CryptoHasher('sha256', secret).update(body).digest('hex');
      expect(sig).toBe(expected);
    });

    test('should omit signature when no secret', async () => {
      manager.add(mock.url, ['job.completed']);
      await manager.trigger('job.completed', 'job-5', 'tasks');
      await waitFor(mock.received, 1);
      expect(mock.received[0].headers['x-webhook-signature']).toBeUndefined();
    });

    test('should include progress field when provided', async () => {
      manager.add(mock.url, ['job.progress']);
      await manager.trigger('job.progress', 'job-6', 'tasks', { progress: 75 });
      await waitFor(mock.received, 1);
      expect(mock.received[0].body.progress).toBe(75);
    });
  });

  describe('retry logic', () => {
    test('should retry on HTTP 500 and track failure', async () => {
      const failMock = createMockServer(500);
      try {
        const wh = manager.add(failMock.url, ['job.completed']);
        await manager.trigger('job.completed', 'job-r', 'tasks');
        await Bun.sleep(8000);

        expect(wh.failureCount).toBe(1);
        expect(wh.successCount).toBe(0);
        expect(failMock.received.length).toBeGreaterThanOrEqual(2);
      } finally {
        failMock.stop();
      }
    }, 15000);

    test('should track success count and lastTriggered on success', async () => {
      const wh = manager.add(mock.url, ['job.completed']);
      expect(wh.lastTriggered).toBeNull();
      const before = Date.now();

      await manager.trigger('job.completed', 'job-ok', 'tasks');
      await waitFor(mock.received, 1);
      await Bun.sleep(200);

      expect(wh.successCount).toBe(1);
      expect(wh.failureCount).toBe(0);
      expect(wh.lastTriggered).toBeGreaterThanOrEqual(before);
    });
  });

  describe('enable/disable toggle', () => {
    test('should not deliver to disabled webhooks', async () => {
      const wh = manager.add(mock.url, ['job.completed']);
      manager.setEnabled(wh.id, false);
      await manager.trigger('job.completed', 'job-off', 'tasks');
      await Bun.sleep(500);
      expect(mock.received).toHaveLength(0);
    });

    test('should deliver again after re-enabling', async () => {
      const wh = manager.add(mock.url, ['job.completed']);
      manager.setEnabled(wh.id, false);
      await manager.trigger('job.completed', 'j1', 'tasks');
      await Bun.sleep(300);
      expect(mock.received).toHaveLength(0);

      manager.setEnabled(wh.id, true);
      await manager.trigger('job.completed', 'j2', 'tasks');
      await waitFor(mock.received, 1);
      expect(mock.received).toHaveLength(1);
      expect(mock.received[0].body.jobId).toBe('j2');
    });

    test('hasEnabledWebhooks should reflect state', () => {
      const wh = manager.add(mock.url, ['job.completed']);
      expect(manager.hasEnabledWebhooks()).toBe(true);
      manager.setEnabled(wh.id, false);
      expect(manager.hasEnabledWebhooks()).toBe(false);
      manager.setEnabled(wh.id, true);
      expect(manager.hasEnabledWebhooks()).toBe(true);
    });
  });

  describe('removal', () => {
    test('should not deliver after removal and clear from list', async () => {
      const wh = manager.add(mock.url, ['job.completed']);
      expect(manager.list()).toHaveLength(1);

      manager.remove(wh.id);
      expect(manager.list()).toHaveLength(0);
      expect(manager.get(wh.id)).toBeUndefined();

      await manager.trigger('job.completed', 'job-rm', 'tasks');
      await Bun.sleep(500);
      expect(mock.received).toHaveLength(0);
    });
  });

  describe('multiple webhooks', () => {
    test('should deliver to all matching webhooks', async () => {
      const mock2 = createMockServer();
      try {
        manager.add(mock.url, ['job.completed']);
        manager.add(mock2.url, ['job.completed']);
        await manager.trigger('job.completed', 'job-m', 'tasks');
        await waitFor(mock.received, 1);
        await waitFor(mock2.received, 1);
        expect(mock.received).toHaveLength(1);
        expect(mock2.received).toHaveLength(1);
        expect(mock.received[0].body.jobId).toBe('job-m');
        expect(mock2.received[0].body.jobId).toBe('job-m');
      } finally {
        mock2.stop();
      }
    });

    test('should deliver only to webhooks matching the queue', async () => {
      const mock2 = createMockServer();
      try {
        manager.add(mock.url, ['job.completed'], 'emails');
        manager.add(mock2.url, ['job.completed'], 'tasks');
        await manager.trigger('job.completed', 'job-q', 'emails');
        await waitFor(mock.received, 1);
        await Bun.sleep(300);
        expect(mock.received).toHaveLength(1);
        expect(mock2.received).toHaveLength(0);
      } finally {
        mock2.stop();
      }
    });

    test('null queue webhook receives events from all queues', async () => {
      manager.add(mock.url, ['job.completed']);
      await manager.trigger('job.completed', 'a', 'emails');
      await manager.trigger('job.completed', 'b', 'tasks');
      await waitFor(mock.received, 2);
      expect(mock.received).toHaveLength(2);
    });
  });

  describe('event type filtering', () => {
    test('should only deliver matching event types', async () => {
      manager.add(mock.url, ['job.failed']);
      await manager.trigger('job.completed', 'j1', 'q');
      await Bun.sleep(300);
      expect(mock.received).toHaveLength(0);

      await manager.trigger('job.failed', 'j2', 'q');
      await waitFor(mock.received, 1);
      expect(mock.received).toHaveLength(1);
      expect(mock.received[0].body.event).toBe('job.failed');
    });

    test('should deliver for multiple subscribed events', async () => {
      manager.add(mock.url, ['job.completed', 'job.failed']);
      await manager.trigger('job.completed', 'c1', 'q');
      await manager.trigger('job.failed', 'f1', 'q');
      await waitFor(mock.received, 2);
      expect(mock.received).toHaveLength(2);
      const events = mock.received.map((r) => r.body.event);
      expect(events).toContain('job.completed');
      expect(events).toContain('job.failed');
    });

    test('should not deliver for unsubscribed events', async () => {
      manager.add(mock.url, ['job.pushed']);
      await manager.trigger('job.completed', 'j1', 'q');
      await manager.trigger('job.failed', 'j2', 'q');
      await manager.trigger('job.started', 'j3', 'q');
      await Bun.sleep(500);
      expect(mock.received).toHaveLength(0);
    });
  });
});
