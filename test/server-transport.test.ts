/**
 * Server Transport Layer Tests
 * Tests for sseHandler.ts, wsHandler.ts, httpEndpoints.ts, and handlerRoutes.ts
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import type { HandlerContext } from '../src/infrastructure/server/types';
import type { JobEvent } from '../src/domain/types/queue';
import { EventType } from '../src/domain/types/queue';

// SSE Handler
import { SseHandler, type SseClient } from '../src/infrastructure/server/sseHandler';

// WS Handler
import { WsHandler, type WsData } from '../src/infrastructure/server/wsHandler';

// HTTP Endpoints
import {
  jsonResponse,
  corsResponse,
  healthEndpoint,
  gcEndpoint,
  statsEndpoint,
  metricsEndpoint,
} from '../src/infrastructure/server/httpEndpoints';

// Handler Routes
import {
  routeCoreCommand,
  routeQueryCommand,
  routeManagementCommand,
  routeQueueControlCommand,
  routeDlqCommand,
  routeRateLimitCommand,
  routeCronCommand,
  routeMonitoringCommand,
} from '../src/infrastructure/server/handlerRoutes';

// Handler router (main entry)
import { handleCommand } from '../src/infrastructure/server/handler';

function createContext(qm: QueueManager): HandlerContext {
  return {
    queueManager: qm,
    authTokens: new Set<string>(),
    authenticated: false,
    clientId: 'test-client-transport',
  };
}

function createJobEvent(overrides: Partial<JobEvent> = {}): JobEvent {
  return {
    eventType: EventType.Pushed,
    queue: 'test-queue',
    jobId: 'job-1',
    timestamp: Date.now(),
    ...overrides,
  };
}

// ============================================================
// SSE HANDLER
// ============================================================

describe('SseHandler', () => {
  let sseHandler: SseHandler;

  beforeEach(() => {
    sseHandler = new SseHandler();
  });

  afterEach(() => {
    sseHandler.closeAll();
  });

  describe('client registration', () => {
    test('should start with zero clients', () => {
      expect(sseHandler.size).toBe(0);
    });

    test('should register a client when createResponse is called', async () => {
      const response = sseHandler.createResponse(null, '*');
      expect(sseHandler.size).toBe(1);
      expect(response).toBeInstanceOf(Response);

      // Verify response headers
      expect(response.headers.get('Content-Type')).toBe('text/event-stream');
      expect(response.headers.get('Cache-Control')).toBe('no-cache');
      expect(response.headers.get('Connection')).toBe('keep-alive');
      expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*');
    });

    test('should register multiple clients', () => {
      sseHandler.createResponse(null, '*');
      sseHandler.createResponse(null, '*');
      sseHandler.createResponse(null, '*');
      expect(sseHandler.size).toBe(3);
    });

    test('should set queue filter on client', () => {
      sseHandler.createResponse('emails', '*');
      const clients = sseHandler.getClients();
      expect(clients.size).toBe(1);
      const client = Array.from(clients.values())[0];
      expect(client.queueFilter).toBe('emails');
    });

    test('should set null queue filter for global subscription', () => {
      sseHandler.createResponse(null, '*');
      const clients = sseHandler.getClients();
      const client = Array.from(clients.values())[0];
      expect(client.queueFilter).toBeNull();
    });

    test('should use provided CORS origin in response', () => {
      const response = sseHandler.createResponse(null, 'https://example.com');
      expect(response.headers.get('Access-Control-Allow-Origin')).toBe('https://example.com');
    });

    test('should assign unique IDs to each client', () => {
      sseHandler.createResponse(null, '*');
      sseHandler.createResponse(null, '*');
      const clients = sseHandler.getClients();
      const ids = Array.from(clients.keys());
      expect(ids[0]).not.toBe(ids[1]);
    });
  });

  describe('SSE stream content', () => {
    test('should send connected message on stream start', async () => {
      const response = sseHandler.createResponse(null, '*');
      const reader = response.body!.getReader();
      const { value } = await reader.read();
      const text = new TextDecoder().decode(value);
      expect(text).toContain('"connected":true');
      expect(text).toContain('"clientId"');
      expect(text).toContain('retry:');
      expect(text).toContain('data: ');
      expect(text).toEndWith('\n\n');
      reader.cancel();
    });
  });

  describe('event broadcasting', () => {
    test('should broadcast event to all clients without queue filter', async () => {
      const response1 = sseHandler.createResponse(null, '*');
      const response2 = sseHandler.createResponse(null, '*');

      const event = createJobEvent({ queue: 'emails' });
      sseHandler.broadcast(event);

      // Both clients should receive the event
      // Read from first response
      const reader1 = response1.body!.getReader();
      // First read gets the connected message
      await reader1.read();
      const { value: val1 } = await reader1.read();
      const text1 = new TextDecoder().decode(val1);
      expect(text1).toContain('"queue":"emails"');
      reader1.cancel();

      const reader2 = response2.body!.getReader();
      await reader2.read();
      const { value: val2 } = await reader2.read();
      const text2 = new TextDecoder().decode(val2);
      expect(text2).toContain('"queue":"emails"');
      reader2.cancel();
    });

    test('should only broadcast to clients with matching queue filter', async () => {
      const emailResponse = sseHandler.createResponse('emails', '*');
      const orderResponse = sseHandler.createResponse('orders', '*');

      const event = createJobEvent({ queue: 'emails' });
      sseHandler.broadcast(event);

      // Email client should receive it
      const emailReader = emailResponse.body!.getReader();
      await emailReader.read(); // connected message
      const { value: emailVal } = await emailReader.read();
      expect(new TextDecoder().decode(emailVal)).toContain('"queue":"emails"');
      emailReader.cancel();

      // Orders client should NOT receive it - broadcasting only to matching
      // We can verify by checking the orders stream only has the connected message
      const orderReader = orderResponse.body!.getReader();
      const { value: connectVal } = await orderReader.read();
      expect(new TextDecoder().decode(connectVal)).toContain('"connected":true');
      orderReader.cancel();
    });

    test('should broadcast to client with null queue filter (global)', async () => {
      const globalResponse = sseHandler.createResponse(null, '*');

      const event = createJobEvent({ queue: 'any-queue' });
      sseHandler.broadcast(event);

      const reader = globalResponse.body!.getReader();
      await reader.read(); // connected
      const { value } = await reader.read();
      expect(new TextDecoder().decode(value)).toContain('"queue":"any-queue"');
      reader.cancel();
    });

    test('should format broadcast as SSE data message with event ID', async () => {
      const response = sseHandler.createResponse(null, '*');

      const event = createJobEvent();
      sseHandler.broadcast(event);

      const reader = response.body!.getReader();
      await reader.read(); // connected
      const { value } = await reader.read();
      const text = new TextDecoder().decode(value);
      expect(text).toContain('id: ');
      expect(text).toContain('data: ');
      expect(text).toEndWith('\n\n');
      // Extract JSON from 'data: ' line
      const dataLine = text.split('\n').find((l: string) => l.startsWith('data: '));
      const jsonStr = dataLine!.replace('data: ', '');
      const parsed = JSON.parse(jsonStr);
      expect(parsed.queue).toBe('test-queue');
      reader.cancel();
    });
  });

  describe('client disconnection', () => {
    test('should remove client on stream cancel', async () => {
      const response = sseHandler.createResponse(null, '*');
      expect(sseHandler.size).toBe(1);

      const reader = response.body!.getReader();
      await reader.cancel();

      // The cancel callback should remove the client
      // Wait a moment for async cleanup
      await Bun.sleep(10);
      expect(sseHandler.size).toBe(0);
    });

    test('should remove disconnected clients during broadcast', () => {
      // Manually add a client with a closed controller to simulate disconnection
      const clients = sseHandler.getClients();
      const stream = new ReadableStream({
        start(controller) {
          clients.set('bad-client', {
            id: 'bad-client',
            controller,
            queueFilter: null,
          });
          // Close the controller to simulate disconnection
          controller.close();
        },
      });
      // Start reading to activate the stream
      void stream.getReader().read();

      expect(sseHandler.size).toBe(1);

      // Broadcasting should detect the closed controller and remove the client
      const event = createJobEvent();
      sseHandler.broadcast(event);

      expect(sseHandler.size).toBe(0);
    });
  });

  describe('closeAll', () => {
    test('should close all connections and clear clients', () => {
      sseHandler.createResponse(null, '*');
      sseHandler.createResponse('queue1', '*');
      sseHandler.createResponse('queue2', '*');
      expect(sseHandler.size).toBe(3);

      sseHandler.closeAll();
      expect(sseHandler.size).toBe(0);
    });

    test('should handle closeAll on empty handler', () => {
      expect(() => sseHandler.closeAll()).not.toThrow();
      expect(sseHandler.size).toBe(0);
    });
  });

  describe('getClients', () => {
    test('should return the clients map', () => {
      const clients = sseHandler.getClients();
      expect(clients).toBeInstanceOf(Map);
      expect(clients.size).toBe(0);

      sseHandler.createResponse(null, '*');
      expect(clients.size).toBe(1);
    });
  });
});

// ============================================================
// WS HANDLER
// ============================================================

describe('WsHandler', () => {
  let wsHandler: WsHandler;

  beforeEach(() => {
    wsHandler = new WsHandler();
  });

  /** Create a mock WebSocket object */
  function createMockWs(overrides: Partial<WsData> = {}): any {
    const messages: string[] = [];
    return {
      data: {
        id: overrides.id ?? `ws-${Date.now()}-${Math.random()}`,
        authenticated: overrides.authenticated ?? false,
        queueFilter: overrides.queueFilter ?? null,
        subscriptions: overrides.subscriptions ?? null,
      },
      send(msg: string) {
        messages.push(msg);
      },
      _messages: messages,
    };
  }

  describe('client management', () => {
    test('should start with zero clients', () => {
      expect(wsHandler.size).toBe(0);
    });

    test('should register client on open', () => {
      const ws = createMockWs({ id: 'ws-1' });
      wsHandler.onOpen(ws);
      expect(wsHandler.size).toBe(1);
    });

    test('should register multiple clients', () => {
      wsHandler.onOpen(createMockWs({ id: 'ws-1' }));
      wsHandler.onOpen(createMockWs({ id: 'ws-2' }));
      wsHandler.onOpen(createMockWs({ id: 'ws-3' }));
      expect(wsHandler.size).toBe(3);
    });

    test('should remove client on close', () => {
      const ws = createMockWs({ id: 'ws-1' });
      wsHandler.onOpen(ws);
      expect(wsHandler.size).toBe(1);

      wsHandler.onClose(ws);
      expect(wsHandler.size).toBe(0);
    });

    test('should handle closing non-existent client gracefully', () => {
      const ws = createMockWs({ id: 'non-existent' });
      expect(() => wsHandler.onClose(ws)).not.toThrow();
    });
  });

  describe('event broadcasting', () => {
    test('should broadcast event to all connected clients', () => {
      const ws1 = createMockWs({ id: 'ws-1' });
      const ws2 = createMockWs({ id: 'ws-2' });
      wsHandler.onOpen(ws1);
      wsHandler.onOpen(ws2);

      const event = createJobEvent({ queue: 'emails' });
      wsHandler.broadcast(event);

      expect(ws1._messages.length).toBe(1);
      expect(ws2._messages.length).toBe(1);
      const parsed1 = JSON.parse(ws1._messages[0]);
      expect(parsed1.queue).toBe('emails');
    });

    test('should only broadcast to clients with matching queue filter', () => {
      const wsEmails = createMockWs({ id: 'ws-emails', queueFilter: 'emails' });
      const wsOrders = createMockWs({ id: 'ws-orders', queueFilter: 'orders' });
      wsHandler.onOpen(wsEmails);
      wsHandler.onOpen(wsOrders);

      const event = createJobEvent({ queue: 'emails' });
      wsHandler.broadcast(event);

      expect(wsEmails._messages.length).toBe(1);
      expect(wsOrders._messages.length).toBe(0);
    });

    test('should broadcast to client with null queue filter (global)', () => {
      const wsGlobal = createMockWs({ id: 'ws-global', queueFilter: null });
      wsHandler.onOpen(wsGlobal);

      const event = createJobEvent({ queue: 'some-queue' });
      wsHandler.broadcast(event);

      expect(wsGlobal._messages.length).toBe(1);
    });

    test('should broadcast to no clients when none are connected', () => {
      // Should not throw
      expect(() => wsHandler.broadcast(createJobEvent())).not.toThrow();
    });
  });

  describe('message handling', () => {
    let qm: QueueManager;
    let ctx: HandlerContext;

    beforeEach(() => {
      qm = new QueueManager();
      ctx = createContext(qm);
    });

    afterEach(() => {
      qm.shutdown();
    });

    test('should handle valid command as string', async () => {
      const ws = createMockWs({ id: 'ws-test', authenticated: false });
      wsHandler.onOpen(ws);

      const message = JSON.stringify({ cmd: 'Ping' });
      await wsHandler.onMessage(ws, message, ctx);

      expect(ws._messages.length).toBe(1);
      const response = JSON.parse(ws._messages[0]);
      expect(response.ok).toBe(true);
    });

    test('should handle valid command as Buffer', async () => {
      const ws = createMockWs({ id: 'ws-test' });
      wsHandler.onOpen(ws);

      const message = Buffer.from(JSON.stringify({ cmd: 'Ping' }));
      await wsHandler.onMessage(ws, message, ctx);

      expect(ws._messages.length).toBe(1);
      const response = JSON.parse(ws._messages[0]);
      expect(response.ok).toBe(true);
    });

    test('should send error for invalid command format', async () => {
      const ws = createMockWs({ id: 'ws-test' });
      wsHandler.onOpen(ws);

      await wsHandler.onMessage(ws, 'not valid json', ctx);

      expect(ws._messages.length).toBe(1);
      const response = JSON.parse(ws._messages[0]);
      expect(response.ok).toBe(false);
      expect(response.error).toBe('Invalid JSON');
    });

    test('should send error for command missing cmd field', async () => {
      const ws = createMockWs({ id: 'ws-test' });
      wsHandler.onOpen(ws);

      await wsHandler.onMessage(ws, JSON.stringify({ queue: 'test' }), ctx);

      expect(ws._messages.length).toBe(1);
      const response = JSON.parse(ws._messages[0]);
      expect(response.ok).toBe(false);
      expect(response.error).toBe('Invalid command');
    });

    test('should handle PUSH command via WebSocket', async () => {
      const ws = createMockWs({ id: 'ws-test' });
      wsHandler.onOpen(ws);

      const message = JSON.stringify({
        cmd: 'PUSH',
        queue: 'emails',
        data: { to: 'user@test.com' },
      });
      await wsHandler.onMessage(ws, message, ctx);

      expect(ws._messages.length).toBe(1);
      const response = JSON.parse(ws._messages[0]);
      expect(response.ok).toBe(true);
      expect(response.id).toBeDefined();
    });

    test('should update authentication state on successful Auth', async () => {
      const authCtx: HandlerContext = {
        queueManager: qm,
        authTokens: new Set(['secret-token']),
        authenticated: false,
        clientId: 'ws-auth-test',
      };

      const ws = createMockWs({ id: 'ws-auth', authenticated: false });
      wsHandler.onOpen(ws);

      const message = JSON.stringify({ cmd: 'Auth', token: 'secret-token' });
      await wsHandler.onMessage(ws, message, authCtx);

      expect(ws._messages.length).toBe(1);
      const response = JSON.parse(ws._messages[0]);
      expect(response.ok).toBe(true);
      expect(ws.data.authenticated).toBe(true);
    });

    test('should not update authentication on failed Auth', async () => {
      const authCtx: HandlerContext = {
        queueManager: qm,
        authTokens: new Set(['secret-token']),
        authenticated: false,
        clientId: 'ws-auth-test',
      };

      const ws = createMockWs({ id: 'ws-auth', authenticated: false });
      wsHandler.onOpen(ws);

      const message = JSON.stringify({ cmd: 'Auth', token: 'wrong-token' });
      await wsHandler.onMessage(ws, message, authCtx);

      expect(ws._messages.length).toBe(1);
      const response = JSON.parse(ws._messages[0]);
      expect(response.ok).toBe(false);
      expect(ws.data.authenticated).toBe(false);
    });
  });

  describe('getClients', () => {
    test('should return the clients map', () => {
      const clients = wsHandler.getClients();
      expect(clients).toBeInstanceOf(Map);
      expect(clients.size).toBe(0);

      wsHandler.onOpen(createMockWs({ id: 'ws-1' }));
      expect(clients.size).toBe(1);
    });
  });
});

// ============================================================
// HTTP ENDPOINTS
// ============================================================

describe('HTTP Endpoints', () => {
  describe('jsonResponse', () => {
    test('should return JSON response with default 200 status', async () => {
      const res = jsonResponse({ ok: true, data: 'test' });
      expect(res.status).toBe(200);
      expect(res.headers.get('Content-Type')).toBe('application/json');

      const body = await res.json();
      expect(body).toEqual({ ok: true, data: 'test' });
    });

    test('should return JSON response with custom status', async () => {
      const res = jsonResponse({ error: 'not found' }, 404);
      expect(res.status).toBe(404);
    });

    test('should include CORS header when corsOrigins is provided', () => {
      const corsOrigins = new Set(['*']);
      const res = jsonResponse({ ok: true }, 200, corsOrigins);
      expect(res.headers.get('Access-Control-Allow-Origin')).toBe('*');
    });

    test('should join multiple CORS origins', () => {
      const corsOrigins = new Set(['https://a.com', 'https://b.com']);
      const res = jsonResponse({ ok: true }, 200, corsOrigins);
      const origin = res.headers.get('Access-Control-Allow-Origin')!;
      expect(origin).toContain('https://a.com');
      expect(origin).toContain('https://b.com');
    });

    test('should not include CORS header when corsOrigins is not provided', () => {
      const res = jsonResponse({ ok: true });
      expect(res.headers.get('Access-Control-Allow-Origin')).toBeNull();
    });
  });

  describe('corsResponse', () => {
    test('should return 204 status with CORS headers', () => {
      const corsOrigins = new Set(['*']);
      const res = corsResponse(corsOrigins);
      expect(res.status).toBe(204);
      expect(res.headers.get('Access-Control-Allow-Origin')).toBe('*');
      expect(res.headers.get('Access-Control-Allow-Methods')).toBe(
        'GET, POST, PUT, DELETE, OPTIONS'
      );
      expect(res.headers.get('Access-Control-Allow-Headers')).toBe(
        'Content-Type, Authorization'
      );
      expect(res.headers.get('Access-Control-Max-Age')).toBe('86400');
    });

    test('should return null body', async () => {
      const corsOrigins = new Set(['*']);
      const res = corsResponse(corsOrigins);
      expect(res.body).toBeNull();
    });

    test('should join multiple CORS origins', () => {
      const corsOrigins = new Set(['https://a.com', 'https://b.com']);
      const res = corsResponse(corsOrigins);
      const origin = res.headers.get('Access-Control-Allow-Origin')!;
      expect(origin).toContain('https://a.com');
      expect(origin).toContain('https://b.com');
    });
  });

  describe('healthEndpoint', () => {
    let qm: QueueManager;

    beforeEach(() => {
      qm = new QueueManager();
    });

    afterEach(() => {
      qm.shutdown();
    });

    test('should return health status with all expected fields', async () => {
      const res = healthEndpoint(qm, 2, 3);
      expect(res.status).toBe(200);

      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.status).toBe('healthy');
      expect(typeof body.uptime).toBe('number');
      expect(body.version).toBeDefined();
    });

    test('should include queue stats', async () => {
      const res = healthEndpoint(qm, 0, 0);
      const body = await res.json();
      expect(body.queues).toBeDefined();
      expect(typeof body.queues.waiting).toBe('number');
      expect(typeof body.queues.active).toBe('number');
      expect(typeof body.queues.delayed).toBe('number');
      expect(typeof body.queues.completed).toBe('number');
      expect(typeof body.queues.dlq).toBe('number');
    });

    test('should include connection counts', async () => {
      const res = healthEndpoint(qm, 5, 10);
      const body = await res.json();
      expect(body.connections.ws).toBe(5);
      expect(body.connections.sse).toBe(10);
      expect(body.connections.tcp).toBe(0);
    });

    test('should include memory stats in MB', async () => {
      const res = healthEndpoint(qm, 0, 0);
      const body = await res.json();
      expect(body.memory).toBeDefined();
      expect(typeof body.memory.heapUsed).toBe('number');
      expect(typeof body.memory.heapTotal).toBe('number');
      expect(typeof body.memory.rss).toBe('number');
    });
  });

  describe('gcEndpoint', () => {
    let qm: QueueManager;

    beforeEach(() => {
      qm = new QueueManager();
    });

    afterEach(() => {
      qm.shutdown();
    });

    test('should return before/after memory stats', async () => {
      const res = gcEndpoint(qm);
      expect(res.status).toBe(200);

      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.before).toBeDefined();
      expect(body.after).toBeDefined();
      expect(typeof body.before.heapUsed).toBe('number');
      expect(typeof body.before.heapTotal).toBe('number');
      expect(typeof body.before.rss).toBe('number');
      expect(typeof body.after.heapUsed).toBe('number');
      expect(typeof body.after.heapTotal).toBe('number');
      expect(typeof body.after.rss).toBe('number');
    });
  });

  describe('statsEndpoint', () => {
    let qm: QueueManager;

    beforeEach(() => {
      qm = new QueueManager();
    });

    afterEach(() => {
      qm.shutdown();
    });

    test('should return stats with all expected fields', async () => {
      const res = statsEndpoint(qm);
      expect(res.status).toBe(200);

      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.stats).toBeDefined();
      expect(typeof body.stats.totalPushed).toBe('number');
      expect(typeof body.stats.totalPulled).toBe('number');
      expect(typeof body.stats.totalCompleted).toBe('number');
      expect(typeof body.stats.totalFailed).toBe('number');
    });

    test('should include memory stats', async () => {
      const res = statsEndpoint(qm);
      const body = await res.json();
      expect(body.memory).toBeDefined();
      expect(typeof body.memory.heapUsed).toBe('number');
      expect(typeof body.memory.heapTotal).toBe('number');
      expect(typeof body.memory.rss).toBe('number');
      expect(typeof body.memory.external).toBe('number');
      expect(typeof body.memory.arrayBuffers).toBe('number');
    });

    test('should include collections stats', async () => {
      const res = statsEndpoint(qm);
      const body = await res.json();
      expect(body.collections).toBeDefined();
    });

    test('should include CORS headers when corsOrigins provided', () => {
      const corsOrigins = new Set(['*']);
      const res = statsEndpoint(qm, corsOrigins);
      expect(res.headers.get('Access-Control-Allow-Origin')).toBe('*');
    });
  });

  describe('metricsEndpoint', () => {
    let qm: QueueManager;

    beforeEach(() => {
      qm = new QueueManager();
    });

    afterEach(() => {
      qm.shutdown();
    });

    test('should return metrics with all expected fields', async () => {
      const res = metricsEndpoint(qm);
      expect(res.status).toBe(200);

      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.metrics).toBeDefined();
      expect(typeof body.metrics.totalPushed).toBe('number');
      expect(typeof body.metrics.totalPulled).toBe('number');
      expect(typeof body.metrics.totalCompleted).toBe('number');
      expect(typeof body.metrics.totalFailed).toBe('number');
    });

    test('should include CORS headers when corsOrigins provided', () => {
      const corsOrigins = new Set(['https://example.com']);
      const res = metricsEndpoint(qm, corsOrigins);
      expect(res.headers.get('Access-Control-Allow-Origin')).toBe('https://example.com');
    });
  });
});

// ============================================================
// HANDLER ROUTES
// ============================================================

describe('Handler Routes', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  // ---- routeCoreCommand ----

  describe('routeCoreCommand', () => {
    test('should route PUSH command', async () => {
      const result = await routeCoreCommand(
        { cmd: 'PUSH', queue: 'test', data: { msg: 'hello' } },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route PUSHB command', async () => {
      const result = await routeCoreCommand(
        { cmd: 'PUSHB', queue: 'test', jobs: [{ data: { id: 1 } }] },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route PULL command', async () => {
      const result = await routeCoreCommand(
        { cmd: 'PULL', queue: 'test' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route PULLB command', async () => {
      const result = await routeCoreCommand(
        { cmd: 'PULLB', queue: 'test', count: 5 },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route ACK command', async () => {
      // Push and pull first to have an active job
      await handleCommand({ cmd: 'PUSH', queue: 'test', data: {} }, ctx);
      const pullRes = await handleCommand({ cmd: 'PULL', queue: 'test' }, ctx);
      const jobId = (pullRes as any).job.id;

      const result = await routeCoreCommand(
        { cmd: 'ACK', id: jobId },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route ACKB command', async () => {
      await handleCommand({ cmd: 'PUSH', queue: 'test', data: {} }, ctx);
      const pullRes = await handleCommand({ cmd: 'PULL', queue: 'test' }, ctx);
      const jobId = (pullRes as any).job.id;

      const result = await routeCoreCommand(
        { cmd: 'ACKB', ids: [jobId] },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route FAIL command', async () => {
      await handleCommand({ cmd: 'PUSH', queue: 'test', data: {} }, ctx);
      const pullRes = await handleCommand({ cmd: 'PULL', queue: 'test' }, ctx);
      const jobId = (pullRes as any).job.id;

      const result = await routeCoreCommand(
        { cmd: 'FAIL', id: jobId, error: 'test error' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should return null for unknown command', async () => {
      const result = await routeCoreCommand(
        { cmd: 'Stats' } as any,
        ctx
      );
      expect(result).toBeNull();
    });

    test('should propagate reqId', async () => {
      const result = await routeCoreCommand(
        { cmd: 'PUSH', queue: 'test', data: {}, reqId: 'req-123' },
        ctx,
        'req-123'
      );
      expect(result!.reqId).toBe('req-123');
    });
  });

  // ---- routeQueryCommand ----

  describe('routeQueryCommand', () => {
    test('should route GetJob command', async () => {
      const pushRes = await handleCommand(
        { cmd: 'PUSH', queue: 'test', data: {} },
        ctx
      );
      const jobId = (pushRes as any).id;

      const result = await routeQueryCommand(
        { cmd: 'GetJob', id: jobId },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route GetState command', async () => {
      const pushRes = await handleCommand(
        { cmd: 'PUSH', queue: 'test', data: {} },
        ctx
      );
      const jobId = (pushRes as any).id;

      const result = await routeQueryCommand(
        { cmd: 'GetState', id: jobId },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
      expect((result as any).state).toBe('waiting');
    });

    test('should route GetResult command', async () => {
      const pushRes = await handleCommand(
        { cmd: 'PUSH', queue: 'test', data: {} },
        ctx
      );
      const jobId = (pushRes as any).id;

      const result = await routeQueryCommand(
        { cmd: 'GetResult', id: jobId },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route GetJobCounts command', async () => {
      const result = await routeQueryCommand(
        { cmd: 'GetJobCounts', queue: 'test' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
      expect((result as any).counts).toBeDefined();
    });

    test('should route GetCountsPerPriority command', async () => {
      const result = await routeQueryCommand(
        { cmd: 'GetCountsPerPriority', queue: 'test' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route GetJobByCustomId command', async () => {
      const result = await routeQueryCommand(
        { cmd: 'GetJobByCustomId', customId: 'non-existent' },
        ctx
      );
      expect(result).not.toBeNull();
      // Will be an error response since the job doesn't exist
      expect(result!.ok).toBe(false);
    });

    test('should route GetJobs command', async () => {
      const result = await routeQueryCommand(
        { cmd: 'GetJobs', queue: 'test' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route Count command', async () => {
      const result = await routeQueryCommand(
        { cmd: 'Count', queue: 'test' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
      expect((result as any).count).toBe(0);
    });

    test('should route GetProgress command', async () => {
      const result = await routeQueryCommand(
        { cmd: 'GetProgress', id: 'non-existent' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(false);
    });

    test('should return null for unknown command', async () => {
      const result = await routeQueryCommand(
        { cmd: 'PUSH' } as any,
        ctx
      );
      expect(result).toBeNull();
    });
  });

  // ---- routeManagementCommand ----

  describe('routeManagementCommand', () => {
    test('should route Cancel command', async () => {
      const pushRes = await handleCommand(
        { cmd: 'PUSH', queue: 'test', data: {} },
        ctx
      );
      const jobId = (pushRes as any).id;

      const result = await routeManagementCommand(
        { cmd: 'Cancel', id: jobId },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route Progress command', async () => {
      await handleCommand({ cmd: 'PUSH', queue: 'test', data: {} }, ctx);
      const pullRes = await handleCommand({ cmd: 'PULL', queue: 'test' }, ctx);
      const jobId = (pullRes as any).job.id;

      const result = await routeManagementCommand(
        { cmd: 'Progress', id: jobId, progress: 50 },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route Update command', async () => {
      const pushRes = await handleCommand(
        { cmd: 'PUSH', queue: 'test', data: {} },
        ctx
      );
      const jobId = (pushRes as any).id;

      const result = await routeManagementCommand(
        { cmd: 'Update', id: jobId, data: { updated: true } },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route ChangePriority command', async () => {
      const pushRes = await handleCommand(
        { cmd: 'PUSH', queue: 'test', data: {} },
        ctx
      );
      const jobId = (pushRes as any).id;

      const result = await routeManagementCommand(
        { cmd: 'ChangePriority', id: jobId, priority: 10 },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route Promote command', async () => {
      const pushRes = await handleCommand(
        { cmd: 'PUSH', queue: 'test', data: {}, delay: 60000 },
        ctx
      );
      const jobId = (pushRes as any).id;

      const result = await routeManagementCommand(
        { cmd: 'Promote', id: jobId },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route MoveToDelayed command', async () => {
      const pushRes = await handleCommand(
        { cmd: 'PUSH', queue: 'test', data: {} },
        ctx
      );
      const jobId = (pushRes as any).id;

      const result = await routeManagementCommand(
        { cmd: 'MoveToDelayed', id: jobId, delay: 5000 },
        ctx
      );
      expect(result).not.toBeNull();
    });

    test('should route Discard command', async () => {
      const pushRes = await handleCommand(
        { cmd: 'PUSH', queue: 'test', data: {} },
        ctx
      );
      const jobId = (pushRes as any).id;

      const result = await routeManagementCommand(
        { cmd: 'Discard', id: jobId },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route WaitJob command', async () => {
      const result = await routeManagementCommand(
        { cmd: 'WaitJob', id: 'non-existent', timeout: 100 },
        ctx
      );
      expect(result).not.toBeNull();
    });

    test('should return null for unknown command', async () => {
      const result = await routeManagementCommand(
        { cmd: 'PUSH' } as any,
        ctx
      );
      expect(result).toBeNull();
    });
  });

  // ---- routeQueueControlCommand ----

  describe('routeQueueControlCommand', () => {
    test('should route Pause command', () => {
      const result = routeQueueControlCommand(
        { cmd: 'Pause', queue: 'test' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route Resume command', () => {
      const result = routeQueueControlCommand(
        { cmd: 'Resume', queue: 'test' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route IsPaused command', () => {
      const result = routeQueueControlCommand(
        { cmd: 'IsPaused', queue: 'test' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route Drain command', () => {
      const result = routeQueueControlCommand(
        { cmd: 'Drain', queue: 'test' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route Obliterate command', () => {
      const result = routeQueueControlCommand(
        { cmd: 'Obliterate', queue: 'test' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route ListQueues command', () => {
      const result = routeQueueControlCommand(
        { cmd: 'ListQueues' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route Clean command', () => {
      const result = routeQueueControlCommand(
        { cmd: 'Clean', queue: 'test', grace: 60000 },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should return null for unknown command', () => {
      const result = routeQueueControlCommand(
        { cmd: 'PUSH' } as any,
        ctx
      );
      expect(result).toBeNull();
    });

    test('should propagate reqId', () => {
      const result = routeQueueControlCommand(
        { cmd: 'Pause', queue: 'test', reqId: 'req-pause' },
        ctx,
        'req-pause'
      );
      expect(result!.reqId).toBe('req-pause');
    });
  });

  // ---- routeDlqCommand ----

  describe('routeDlqCommand', () => {
    test('should route Dlq command', () => {
      const result = routeDlqCommand(
        { cmd: 'Dlq', queue: 'test' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route RetryDlq command', () => {
      const result = routeDlqCommand(
        { cmd: 'RetryDlq', queue: 'test' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route PurgeDlq command', () => {
      const result = routeDlqCommand(
        { cmd: 'PurgeDlq', queue: 'test' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route RemoveDlqJob command', () => {
      const result = routeDlqCommand(
        { cmd: 'RemoveDlqJob', queue: 'test', jobId: 'missing' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
      expect((result as any).data.removed).toBe(false);
    });

    test('should route RetryCompleted command', () => {
      const result = routeDlqCommand(
        { cmd: 'RetryCompleted', queue: 'test' },
        ctx
      );
      expect(result).not.toBeNull();
    });

    test('should return null for unknown command', () => {
      const result = routeDlqCommand(
        { cmd: 'PUSH' } as any,
        ctx
      );
      expect(result).toBeNull();
    });
  });

  // ---- routeRateLimitCommand ----

  describe('routeRateLimitCommand', () => {
    test('should route RateLimit command', () => {
      const result = routeRateLimitCommand(
        { cmd: 'RateLimit', queue: 'test', limit: 100 },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route RateLimitClear command', () => {
      const result = routeRateLimitCommand(
        { cmd: 'RateLimitClear', queue: 'test' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route SetConcurrency command', () => {
      const result = routeRateLimitCommand(
        { cmd: 'SetConcurrency', queue: 'test', limit: 10 },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route ClearConcurrency command', () => {
      const result = routeRateLimitCommand(
        { cmd: 'ClearConcurrency', queue: 'test' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should return null for unknown command', () => {
      const result = routeRateLimitCommand(
        { cmd: 'PUSH' } as any,
        ctx
      );
      expect(result).toBeNull();
    });
  });

  // ---- routeCronCommand ----

  describe('routeCronCommand', () => {
    test('should route Cron command', () => {
      const result = routeCronCommand(
        { cmd: 'Cron', name: 'test-cron', queue: 'test', data: {}, repeatEvery: 60000 },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route CronDelete command', () => {
      // First create a cron
      routeCronCommand(
        { cmd: 'Cron', name: 'delete-me', queue: 'test', data: {}, repeatEvery: 60000 },
        ctx
      );
      const result = routeCronCommand(
        { cmd: 'CronDelete', name: 'delete-me' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route CronList command', () => {
      const result = routeCronCommand(
        { cmd: 'CronList' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should return null for unknown command', () => {
      const result = routeCronCommand(
        { cmd: 'PUSH' } as any,
        ctx
      );
      expect(result).toBeNull();
    });
  });

  // ---- routeMonitoringCommand ----

  describe('routeMonitoringCommand', () => {
    test('should route Stats command', () => {
      const result = routeMonitoringCommand(
        { cmd: 'Stats' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
      expect((result as any).stats).toBeDefined();
    });

    test('should route Metrics command', () => {
      const result = routeMonitoringCommand(
        { cmd: 'Metrics' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
      expect((result as any).metrics).toBeDefined();
    });

    test('should route Prometheus command', () => {
      const result = routeMonitoringCommand(
        { cmd: 'Prometheus' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route Ping command', () => {
      const result = routeMonitoringCommand(
        { cmd: 'Ping' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route Hello command', () => {
      const result = routeMonitoringCommand(
        { cmd: 'Hello', protocolVersion: 1 },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route RegisterWorker command', () => {
      const result = routeMonitoringCommand(
        { cmd: 'RegisterWorker', name: 'worker-1', queues: ['test'] },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route UnregisterWorker command', () => {
      // First register - returns DataResponse with data.workerId
      const regResult = routeMonitoringCommand(
        { cmd: 'RegisterWorker', name: 'worker-1', queues: ['test'] },
        ctx
      );
      const workerId = (regResult as any).data.workerId;

      const result = routeMonitoringCommand(
        { cmd: 'UnregisterWorker', workerId },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route ListWorkers command', () => {
      const result = routeMonitoringCommand(
        { cmd: 'ListWorkers' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route Heartbeat command', () => {
      // Register a worker first - returns DataResponse with data.workerId
      const regResult = routeMonitoringCommand(
        { cmd: 'RegisterWorker', name: 'worker-hb', queues: ['test'] },
        ctx
      );
      const workerId = (regResult as any).data.workerId;

      const result = routeMonitoringCommand(
        { cmd: 'Heartbeat', id: workerId },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route AddLog command', async () => {
      const pushRes = await handleCommand(
        { cmd: 'PUSH', queue: 'test', data: {} },
        ctx
      );
      const jobId = (pushRes as any).id;

      const result = routeMonitoringCommand(
        { cmd: 'AddLog', id: jobId, message: 'test log' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route GetLogs command', async () => {
      const pushRes = await handleCommand(
        { cmd: 'PUSH', queue: 'test', data: {} },
        ctx
      );
      const jobId = (pushRes as any).id;

      const result = routeMonitoringCommand(
        { cmd: 'GetLogs', id: jobId },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should route ListWebhooks command', () => {
      const result = routeMonitoringCommand(
        { cmd: 'ListWebhooks' },
        ctx
      );
      expect(result).not.toBeNull();
      expect(result!.ok).toBe(true);
    });

    test('should return null for unknown command', () => {
      const result = routeMonitoringCommand(
        { cmd: 'PUSH' } as any,
        ctx
      );
      expect(result).toBeNull();
    });

    test('should propagate reqId', () => {
      const result = routeMonitoringCommand(
        { cmd: 'Ping', reqId: 'req-ping' },
        ctx,
        'req-ping'
      );
      expect(result!.reqId).toBe('req-ping');
    });
  });

  // ---- handleCommand (main router) unknown command handling ----

  describe('handleCommand - unknown command handling', () => {
    test('should return error for completely unknown command', async () => {
      const result = await handleCommand(
        { cmd: 'CompletelyUnknownCmd' } as any,
        ctx
      );
      expect(result.ok).toBe(false);
      expect((result as any).error).toContain('Unknown command');
      expect((result as any).error).toContain('CompletelyUnknownCmd');
    });

    test('should include command name in error message', async () => {
      const result = await handleCommand(
        { cmd: 'DoSomethingWeird' } as any,
        ctx
      );
      expect(result.ok).toBe(false);
      expect((result as any).error).toContain('DoSomethingWeird');
    });
  });

  // ---- Route registration completeness ----

  describe('route registration completeness', () => {
    test('all core commands should be routable', async () => {
      const coreCommands = ['PUSH', 'PUSHB', 'PULL', 'PULLB'];
      for (const cmd of coreCommands) {
        const result = await routeCoreCommand(
          { cmd, queue: 'test', data: {}, count: 1, jobs: [{ data: {} }] } as any,
          ctx
        );
        expect(result).not.toBeNull();
      }
    });

    test('all queue control commands should be routable', () => {
      const queueCommands = ['Pause', 'Resume', 'IsPaused', 'Drain', 'Obliterate', 'ListQueues'];
      for (const cmd of queueCommands) {
        const result = routeQueueControlCommand(
          { cmd, queue: 'test', grace: 60000 } as any,
          ctx
        );
        expect(result).not.toBeNull();
      }
    });

    test('all DLQ commands should be routable', () => {
      const dlqCommands = ['Dlq', 'RetryDlq', 'PurgeDlq', 'RemoveDlqJob', 'RetryCompleted'];
      for (const cmd of dlqCommands) {
        const result = routeDlqCommand(
          { cmd, queue: 'test', jobId: 'missing' } as any,
          ctx
        );
        expect(result).not.toBeNull();
      }
    });

    test('all rate limit commands should be routable', () => {
      const rlCommands = ['RateLimit', 'RateLimitClear', 'SetConcurrency', 'ClearConcurrency'];
      for (const cmd of rlCommands) {
        const result = routeRateLimitCommand(
          { cmd, queue: 'test', limit: 100 } as any,
          ctx
        );
        expect(result).not.toBeNull();
      }
    });

    test('all cron commands should be routable', () => {
      const cronCommands = ['CronList'];
      for (const cmd of cronCommands) {
        const result = routeCronCommand(
          { cmd, name: 'cron-test', queue: 'test', data: {}, repeatEvery: 60000 } as any,
          ctx
        );
        expect(result).not.toBeNull();
      }
    });

    test('all monitoring commands should be routable', () => {
      const monitorCommands = ['Stats', 'Metrics', 'Prometheus', 'Ping', 'ListWorkers', 'ListWebhooks'];
      for (const cmd of monitorCommands) {
        const result = routeMonitoringCommand(
          { cmd, protocolVersion: 1 } as any,
          ctx
        );
        expect(result).not.toBeNull();
      }
    });
  });
});

// ============================================================
// HTTP SERVER INTEGRATION (using createHttpServer)
// ============================================================

describe('HTTP Server Integration', () => {
  let qm: QueueManager;
  let server: ReturnType<typeof import('../src/infrastructure/server/http').createHttpServer>;
  let baseUrl: string;

  beforeEach(async () => {
    qm = new QueueManager();
    const { createHttpServer } = await import('../src/infrastructure/server/http');
    server = createHttpServer(qm, {
      port: 0, // Random port
      hostname: '127.0.0.1',
    });
    const addr = server.server.url;
    baseUrl = addr.toString().replace(/\/$/, '');
  });

  afterEach(() => {
    server.stop();
    qm.shutdown();
  });

  describe('/health endpoint', () => {
    test('should return 200 with health status', async () => {
      const res = await fetch(`${baseUrl}/health`);
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.status).toBe('healthy');
    });

    test('should include version', async () => {
      const res = await fetch(`${baseUrl}/health`);
      const body = await res.json();
      expect(body.version).toBeDefined();
      expect(typeof body.version).toBe('string');
    });

    test('should include connections info', async () => {
      const res = await fetch(`${baseUrl}/health`);
      const body = await res.json();
      expect(body.connections).toBeDefined();
      expect(typeof body.connections.ws).toBe('number');
      expect(typeof body.connections.sse).toBe('number');
    });

    test('should not require authentication', async () => {
      // Stop existing server, create one with auth
      server.stop();
      qm.shutdown();

      qm = new QueueManager();
      const { createHttpServer } = await import('../src/infrastructure/server/http');
      server = createHttpServer(qm, {
        port: 0,
        hostname: '127.0.0.1',
        authTokens: ['secret-token'],
      });
      baseUrl = server.server.url.toString().replace(/\/$/, '');

      const res = await fetch(`${baseUrl}/health`);
      expect(res.status).toBe(200);
    });
  });

  describe('/healthz and /live endpoints', () => {
    test('/healthz should return OK', async () => {
      const res = await fetch(`${baseUrl}/healthz`);
      expect(res.status).toBe(200);
      const text = await res.text();
      expect(text).toBe('OK');
    });

    test('/live should return OK', async () => {
      const res = await fetch(`${baseUrl}/live`);
      expect(res.status).toBe(200);
      const text = await res.text();
      expect(text).toBe('OK');
    });
  });

  describe('/ready endpoint', () => {
    test('should return ready status', async () => {
      const res = await fetch(`${baseUrl}/ready`);
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.ready).toBe(true);
    });
  });

  describe('/gc endpoint', () => {
    test('should return memory before/after on POST', async () => {
      const res = await fetch(`${baseUrl}/gc`, { method: 'POST' });
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.before).toBeDefined();
      expect(body.after).toBeDefined();
    });

    test('should not accept GET', async () => {
      const res = await fetch(`${baseUrl}/gc`);
      expect(res.status).toBe(404);
    });
  });

  describe('/heapstats endpoint', () => {
    test('should return heap stats on GET', async () => {
      const res = await fetch(`${baseUrl}/heapstats`);
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.memory).toBeDefined();
      expect(body.heap).toBeDefined();
      expect(typeof body.heap.objectCount).toBe('number');
    });
  });

  describe('/stats endpoint', () => {
    test('should return stats', async () => {
      const res = await fetch(`${baseUrl}/stats`);
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.stats).toBeDefined();
      expect(body.memory).toBeDefined();
      expect(body.collections).toBeDefined();
    });

    test('should require authentication when configured', async () => {
      server.stop();
      qm.shutdown();

      qm = new QueueManager();
      const { createHttpServer } = await import('../src/infrastructure/server/http');
      server = createHttpServer(qm, {
        port: 0,
        hostname: '127.0.0.1',
        authTokens: ['secret-token'],
      });
      baseUrl = server.server.url.toString().replace(/\/$/, '');

      const res = await fetch(`${baseUrl}/stats`);
      // Server returns 401 for unauthenticated requests
      const body = await res.json();
      expect(body.ok).toBe(false);
      expect(body.error).toBe('Unauthorized');
    });
  });

  describe('/metrics endpoint', () => {
    test('should return metrics', async () => {
      const res = await fetch(`${baseUrl}/metrics`);
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.metrics).toBeDefined();
    });
  });

  describe('/prometheus endpoint', () => {
    test('should return prometheus text metrics', async () => {
      const res = await fetch(`${baseUrl}/prometheus`);
      expect(res.status).toBe(200);
      expect(res.headers.get('Content-Type')).toContain('text/plain');
      const text = await res.text();
      expect(text.length).toBeGreaterThan(0);
    });
  });

  describe('CORS', () => {
    test('OPTIONS should return CORS preflight response', async () => {
      const res = await fetch(`${baseUrl}/health`, { method: 'OPTIONS' });
      expect(res.status).toBe(204);
      expect(res.headers.get('Access-Control-Allow-Origin')).toBe('*');
      expect(res.headers.get('Access-Control-Allow-Methods')).toContain('GET');
      expect(res.headers.get('Access-Control-Allow-Methods')).toContain('POST');
      expect(res.headers.get('Access-Control-Allow-Headers')).toContain('Authorization');
    });
  });

  describe('Authentication', () => {
    beforeEach(async () => {
      server.stop();
      qm.shutdown();

      qm = new QueueManager();
      const { createHttpServer } = await import('../src/infrastructure/server/http');
      server = createHttpServer(qm, {
        port: 0,
        hostname: '127.0.0.1',
        authTokens: ['my-secret-token'],
      });
      baseUrl = server.server.url.toString().replace(/\/$/, '');
    });

    test('should reject unauthenticated requests to protected endpoints', async () => {
      const res = await fetch(`${baseUrl}/stats`);
      const body = await res.json();
      expect(body.ok).toBe(false);
      expect(body.error).toBe('Unauthorized');
    });

    test('should allow authenticated requests with Bearer token', async () => {
      const res = await fetch(`${baseUrl}/stats`, {
        headers: { Authorization: 'Bearer my-secret-token' },
      });
      const body = await res.json();
      expect(body.ok).toBe(true);
    });

    test('should reject invalid tokens', async () => {
      const res = await fetch(`${baseUrl}/stats`, {
        headers: { Authorization: 'Bearer wrong-token' },
      });
      const body = await res.json();
      expect(body.ok).toBe(false);
    });
  });

  describe('REST API Routes', () => {
    test('POST /queues/:queue/jobs should push a job', async () => {
      const res = await fetch(`${baseUrl}/queues/emails/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ data: { to: 'user@test.com' } }),
      });
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.id).toBeDefined();
    });

    test('GET /queues/:queue/jobs should pull a job', async () => {
      // Push first
      await fetch(`${baseUrl}/queues/emails/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ data: { to: 'user@test.com' } }),
      });

      const res = await fetch(`${baseUrl}/queues/emails/jobs`);
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.job).toBeDefined();
    });

    test('GET /jobs/:id supports UUID-style job IDs', async () => {
      const pushRes = await fetch(`${baseUrl}/queues/emails/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ data: { msg: 'test' } }),
      });
      const pushBody = await pushRes.json();
      const jobId = pushBody.id;

      const res = await fetch(`${baseUrl}/jobs/${jobId}`);
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.job.id).toBe(jobId);
    });

    test('DELETE /jobs/:id supports UUID-style job IDs', async () => {
      const pushRes = await fetch(`${baseUrl}/queues/emails/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ data: { msg: 'cancel me' } }),
      });
      const pushBody = await pushRes.json();
      const jobId = pushBody.id;

      const res = await fetch(`${baseUrl}/jobs/${jobId}`, { method: 'DELETE' });
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
    });

    test('POST /jobs/:id/ack supports UUID-style job IDs', async () => {
      // Push
      await fetch(`${baseUrl}/queues/emails/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ data: { msg: 'ack me' } }),
      });

      // Pull
      const pullRes = await fetch(`${baseUrl}/queues/emails/jobs`);
      const pullBody = await pullRes.json();
      const jobId = pullBody.job.id;

      const res = await fetch(`${baseUrl}/jobs/${jobId}/ack`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ result: { sent: true } }),
      });
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
    });

    test('POST /jobs/:id/fail supports UUID-style job IDs', async () => {
      // Push
      await fetch(`${baseUrl}/queues/emails/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ data: { msg: 'fail me' } }),
      });

      // Pull
      const pullRes = await fetch(`${baseUrl}/queues/emails/jobs`);
      const pullBody = await pullRes.json();
      const jobId = pullBody.job.id;

      const res = await fetch(`${baseUrl}/jobs/${jobId}/fail`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ error: 'something went wrong' }),
      });
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
    });

    test('should return 404 for unknown routes', async () => {
      const res = await fetch(`${baseUrl}/unknown/route`);
      const body = await res.json();
      expect(body.ok).toBe(false);
      expect(body.error).toBe('Not found');
    });

    test('POST /queues/:queue/jobs should return 400 for invalid JSON', async () => {
      const res = await fetch(`${baseUrl}/queues/emails/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: 'not valid json',
      });
      expect(res.status).toBe(400);
      const body = await res.json();
      expect(body.ok).toBe(false);
      expect(body.error).toContain('Invalid JSON');
    });
  });

  describe('SSE endpoint', () => {
    test('/events should establish SSE connection', async () => {
      const res = await fetch(`${baseUrl}/events`);
      expect(res.status).toBe(200);
      expect(res.headers.get('Content-Type')).toBe('text/event-stream');
      expect(res.headers.get('Cache-Control')).toBe('no-cache');

      // Read the connected message
      const reader = res.body!.getReader();
      const { value } = await reader.read();
      const text = new TextDecoder().decode(value);
      expect(text).toContain('"connected":true');
      reader.cancel();
    });

    test('/events should track SSE client count', async () => {
      const res1 = await fetch(`${baseUrl}/events`);
      // Need to start reading to trigger the stream
      const reader1 = res1.body!.getReader();
      await reader1.read();

      // Check health shows SSE count
      const healthRes = await fetch(`${baseUrl}/health`);
      const healthBody = await healthRes.json();
      expect(healthBody.connections.sse).toBeGreaterThanOrEqual(1);

      reader1.cancel();
    });
  });
});
