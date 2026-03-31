/**
 * MCP HTTP Handler Tests
 * Tests for bunqueue_register_handler, bunqueue_unregister_handler, bunqueue_list_handlers
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { HttpHandlerRegistry } from '../src/mcp/httpHandler';
import { getSharedManager, shutdownManager } from '../src/client/manager';

describe('HTTP Handler Registry', () => {
  let registry: HttpHandlerRegistry;

  beforeEach(() => {
    shutdownManager();
    registry = new HttpHandlerRegistry();
  });

  afterEach(async () => {
    registry.shutdown();
    await Bun.sleep(100);
    shutdownManager();
  });

  describe('register', () => {
    test('registers a handler and spawns a worker', () => {
      registry.register('test-queue', {
        url: 'https://httpbin.org/get',
        method: 'GET',
      });

      const handlers = registry.list();
      expect(handlers).toHaveLength(1);
      expect(handlers[0].queue).toBe('test-queue');
      expect(handlers[0].handler.url).toBe('https://httpbin.org/get');
      expect(handlers[0].handler.method).toBe('GET');
      expect(handlers[0].active).toBe(true);
    });

    test('registers handler with all options', () => {
      registry.register('full-queue', {
        url: 'https://httpbin.org/post',
        method: 'POST',
        headers: { 'X-Custom': 'value', Authorization: 'Bearer token123' },
        body: { template: true },
        timeoutMs: 5000,
      });

      const handlers = registry.list();
      expect(handlers).toHaveLength(1);
      expect(handlers[0].handler.method).toBe('POST');
      expect(handlers[0].handler.headers).toEqual({
        'X-Custom': 'value',
        Authorization: 'Bearer token123',
      });
      expect(handlers[0].handler.body).toEqual({ template: true });
      expect(handlers[0].handler.timeoutMs).toBe(5000);
    });

    test('replaces existing handler on same queue', () => {
      registry.register('test-queue', {
        url: 'https://httpbin.org/get',
        method: 'GET',
      });

      registry.register('test-queue', {
        url: 'https://httpbin.org/post',
        method: 'POST',
      });

      const handlers = registry.list();
      expect(handlers).toHaveLength(1);
      expect(handlers[0].handler.url).toBe('https://httpbin.org/post');
      expect(handlers[0].handler.method).toBe('POST');
    });

    test('registers handlers on multiple queues', () => {
      registry.register('queue-a', {
        url: 'https://httpbin.org/get',
        method: 'GET',
      });
      registry.register('queue-b', {
        url: 'https://httpbin.org/post',
        method: 'POST',
      });
      registry.register('queue-c', {
        url: 'https://httpbin.org/put',
        method: 'PUT',
      });

      const handlers = registry.list();
      expect(handlers).toHaveLength(3);

      const queues = handlers.map((h) => h.queue).sort();
      expect(queues).toEqual(['queue-a', 'queue-b', 'queue-c']);
    });
  });

  describe('unregister', () => {
    test('removes handler and stops worker', () => {
      registry.register('test-queue', {
        url: 'https://httpbin.org/get',
        method: 'GET',
      });

      const removed = registry.unregister('test-queue');
      expect(removed).toBe(true);
      expect(registry.list()).toHaveLength(0);
    });

    test('returns false for non-existent queue', () => {
      const removed = registry.unregister('no-such-queue');
      expect(removed).toBe(false);
    });

    test('only removes specified queue', () => {
      registry.register('queue-a', {
        url: 'https://httpbin.org/get',
        method: 'GET',
      });
      registry.register('queue-b', {
        url: 'https://httpbin.org/post',
        method: 'POST',
      });

      registry.unregister('queue-a');

      const handlers = registry.list();
      expect(handlers).toHaveLength(1);
      expect(handlers[0].queue).toBe('queue-b');
    });
  });

  describe('list', () => {
    test('returns empty array when no handlers', () => {
      expect(registry.list()).toEqual([]);
    });

    test('returns all registered handlers', () => {
      registry.register('q1', { url: 'https://a.com', method: 'GET' });
      registry.register('q2', { url: 'https://b.com', method: 'POST' });

      const handlers = registry.list();
      expect(handlers).toHaveLength(2);

      for (const h of handlers) {
        expect(h.queue).toBeDefined();
        expect(h.handler.url).toBeDefined();
        expect(h.handler.method).toBeDefined();
        expect(typeof h.active).toBe('boolean');
      }
    });
  });

  describe('shutdown', () => {
    test('clears all handlers', () => {
      registry.register('q1', { url: 'https://a.com', method: 'GET' });
      registry.register('q2', { url: 'https://b.com', method: 'POST' });

      registry.shutdown();

      expect(registry.list()).toHaveLength(0);
    });

    test('can register after shutdown', () => {
      registry.register('q1', { url: 'https://a.com', method: 'GET' });
      registry.shutdown();

      registry.register('q2', { url: 'https://b.com', method: 'POST' });
      expect(registry.list()).toHaveLength(1);
      expect(registry.list()[0].queue).toBe('q2');
    });
  });

  describe('job processing via HTTP handler', () => {
    test('worker auto-processes job with GET handler', async () => {
      // Start a local HTTP server to verify the call
      let receivedRequest = false;
      const server = Bun.serve({
        port: 0,
        fetch() {
          receivedRequest = true;
          return Response.json({ weather: 'sunny', temp: 22 });
        },
      });

      try {
        const url = `http://localhost:${server.port}/weather`;
        registry.register('http-test', { url, method: 'GET' });

        // Push a job to the queue
        const manager = getSharedManager();
        await manager.push('http-test', { data: { city: 'Milan' } });

        // Wait for the worker to process it
        await Bun.sleep(500);

        expect(receivedRequest).toBe(true);
      } finally {
        server.stop(true);
      }
    });

    test('worker auto-processes job with POST handler', async () => {
      let receivedBody: unknown = null;
      const server = Bun.serve({
        port: 0,
        async fetch(req) {
          receivedBody = await req.json();
          return Response.json({ ok: true });
        },
      });

      try {
        const url = `http://localhost:${server.port}/webhook`;
        registry.register('post-test', { url, method: 'POST' });

        const manager = getSharedManager();
        await manager.push('post-test', {
          data: { event: 'user_signup', userId: '123' },
        });

        await Bun.sleep(500);

        expect(receivedBody).toBeDefined();
        expect((receivedBody as Record<string, unknown>).event).toBe('user_signup');
      } finally {
        server.stop(true);
      }
    });

    test('worker handles HTTP errors gracefully', async () => {
      const server = Bun.serve({
        port: 0,
        fetch() {
          return new Response('Internal Server Error', { status: 500 });
        },
      });

      try {
        const url = `http://localhost:${server.port}/fail`;
        registry.register('error-test', { url, method: 'GET' });

        const manager = getSharedManager();
        await manager.push('error-test', { data: { test: true } });

        // Wait for processing attempt
        await Bun.sleep(500);

        // Job should have been attempted (worker handles the error)
        const counts = manager.getQueueJobCounts('error-test');
        // Job should not be in waiting anymore (it was pulled)
        expect(counts.waiting).toBe(0);
      } finally {
        server.stop(true);
      }
    });

    test('worker processes multiple jobs sequentially', async () => {
      const receivedRequests: number[] = [];
      const server = Bun.serve({
        port: 0,
        async fetch(req) {
          const body = (await req.json()) as Record<string, unknown>;
          receivedRequests.push(body.index as number);
          return Response.json({ processed: body.index });
        },
      });

      try {
        const url = `http://localhost:${server.port}/batch`;
        registry.register('batch-test', { url, method: 'POST' });

        const manager = getSharedManager();
        for (let i = 0; i < 5; i++) {
          await manager.push('batch-test', { data: { index: i } });
        }

        await Bun.sleep(1500);

        expect(receivedRequests.length).toBe(5);
      } finally {
        server.stop(true);
      }
    });

    test('worker sends custom headers', async () => {
      let receivedHeaders: Record<string, string> = {};
      const server = Bun.serve({
        port: 0,
        fetch(req) {
          receivedHeaders = Object.fromEntries(req.headers.entries());
          return Response.json({ ok: true });
        },
      });

      try {
        const url = `http://localhost:${server.port}/headers`;
        registry.register('header-test', {
          url,
          method: 'GET',
          headers: { 'X-Api-Key': 'secret-123', 'X-Custom': 'value' },
        });

        const manager = getSharedManager();
        await manager.push('header-test', { data: {} });

        await Bun.sleep(500);

        expect(receivedHeaders['x-api-key']).toBe('secret-123');
        expect(receivedHeaders['x-custom']).toBe('value');
      } finally {
        server.stop(true);
      }
    });

    test('worker uses template body for POST instead of job data', async () => {
      let receivedBody: unknown = null;
      const server = Bun.serve({
        port: 0,
        async fetch(req) {
          receivedBody = await req.json();
          return Response.json({ ok: true });
        },
      });

      try {
        const url = `http://localhost:${server.port}/template`;
        registry.register('template-test', {
          url,
          method: 'POST',
          body: { fixed: 'payload', source: 'handler' },
        });

        const manager = getSharedManager();
        await manager.push('template-test', { data: { should: 'be-ignored' } });

        await Bun.sleep(500);

        expect(receivedBody).toEqual({ fixed: 'payload', source: 'handler' });
      } finally {
        server.stop(true);
      }
    });

    test('unregistering stops job processing', async () => {
      let callCount = 0;
      const server = Bun.serve({
        port: 0,
        fetch() {
          callCount++;
          return Response.json({ ok: true });
        },
      });

      try {
        const url = `http://localhost:${server.port}/stop`;
        registry.register('stop-test', { url, method: 'GET' });

        const manager = getSharedManager();
        await manager.push('stop-test', { data: {} });
        await Bun.sleep(500);

        const countBefore = callCount;
        expect(countBefore).toBeGreaterThan(0);

        // Unregister and add more jobs
        registry.unregister('stop-test');
        await manager.push('stop-test', { data: {} });
        await Bun.sleep(500);

        // No additional calls should have been made
        expect(callCount).toBe(countBefore);
      } finally {
        server.stop(true);
      }
    });

    test('handler with cron creates recurring auto-processed jobs', async () => {
      let callCount = 0;
      const server = Bun.serve({
        port: 0,
        fetch() {
          callCount++;
          return Response.json({ temp: 22, humidity: 65 });
        },
      });

      try {
        const url = `http://localhost:${server.port}/meteo`;
        registry.register('meteo', { url, method: 'GET' });

        // Add a repeating cron (every 500ms via repeatEvery)
        const manager = getSharedManager();
        manager.addCron({
          name: 'check-meteo',
          queue: 'meteo',
          data: { city: 'Milan' },
          repeatEvery: 500,
        });

        // Wait for a few cron ticks + processing
        await Bun.sleep(2500);

        // Should have been called multiple times
        expect(callCount).toBeGreaterThanOrEqual(3);

        // Cleanup
        manager.removeCron('check-meteo');
      } finally {
        server.stop(true);
      }
    });
  });
});
