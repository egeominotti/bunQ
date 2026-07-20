/**
 * HTTP Server
 * REST API and WebSocket support
 */

import type { Server, ServerWebSocket } from 'bun';
import type { QueueManager } from '../../application/queueManager';
import type { HandlerContext } from './types';
import { constantTimeEqual, uuid } from '../../shared/hash';
import type { JobEvent } from '../../domain/types/queue';
import { httpLog } from '../../shared/logger';
import { getRateLimiter } from './rateLimiter';
import { SseHandler } from './sseHandler';
import { WsHandler, type WsData } from './wsHandler';
import {
  jsonResponse,
  corsResponse,
  healthEndpoint,
  readinessEndpoint,
  gcEndpoint,
  heapStatsEndpoint,
} from './httpEndpoints';
import { loadTlsOptions, type TlsServerOptions } from './tls';
import { routeHttpRequest } from './httpRouter';

/**
 * Validate auth token against valid tokens set
 */
function validateAuthToken(token: string, authTokens: Set<string>): boolean {
  for (const validToken of authTokens) {
    if (constantTimeEqual(token, validToken)) {
      return true;
    }
  }
  return false;
}

/** Check auth and return 401 response if invalid, or null if OK */
function checkAuth(req: Request, authTokens: Set<string>): Response | null {
  if (authTokens.size === 0) return null;
  const token = req.headers.get('Authorization')?.replace('Bearer ', '') ?? '';
  if (!validateAuthToken(token, authTokens)) {
    return jsonResponse({ ok: false, error: 'Unauthorized' }, 401);
  }
  return null;
}

/** HTTP Server configuration */
export interface HttpServerConfig {
  port?: number;
  hostname?: string;
  socketPath?: string;
  authTokens?: string[];
  corsOrigins?: string[];
  requireAuthForMetrics?: boolean;
  /** Current TCP client count, supplied by the TCP server when available. */
  getTcpConnectionCount?: () => number;
  /** Native TLS termination (https/wss). Protocol and routes are unchanged. */
  tls?: TlsServerOptions;
}

/**
 * Create and start HTTP server
 */
export function createHttpServer(queueManager: QueueManager, config: HttpServerConfig) {
  const authTokens = new Set(config.authTokens ?? []);
  const corsOrigins = new Set(config.corsOrigins ?? ['*']);
  const wsHandler = new WsHandler();
  const sseHandler = new SseHandler();

  // Subscribe to queue events for broadcast
  const unsubscribe = queueManager.subscribe((event: JobEvent) => {
    wsHandler.broadcast(event);
    sseHandler.broadcast(event);
  });

  // Start periodic broadcasts
  wsHandler.startBroadcasts(queueManager);
  sseHandler.startBroadcasts(queueManager);

  // Register dashboard event emitter for non-job events (worker, queue, dlq)
  queueManager.setDashboardEmit((event, data) => {
    wsHandler.emitEvent(event, data);
    sseHandler.emitEvent(event, data);
  });

  // Helper to get CORS origin string
  const getCorsOrigin = () => (corsOrigins.has('*') ? '*' : Array.from(corsOrigins).join(', '));
  // Attach CORS to responses built outside the routeRequest pipeline (health,
  // ready, prometheus, debug) so browser dashboards can read them cross-origin
  // (audit #16-20). Response headers are mutable for normally-constructed
  // Responses; this never overwrites an existing value set by the endpoint.
  const withCors = async (r: Response | Promise<Response>): Promise<Response> => {
    const res = await r;
    if (!res.headers.has('Access-Control-Allow-Origin')) {
      res.headers.set('Access-Control-Allow-Origin', getCorsOrigin());
    }
    return res;
  };

  // Fetch handler
  const fetch = async (req: Request, server: Server<WsData>) => {
    const url = new URL(req.url);
    const path = url.pathname;

    // CORS preflight
    if (req.method === 'OPTIONS') {
      return corsResponse(corsOrigins);
    }

    // Health endpoints (no auth, no rate limit)
    if (path === '/health') {
      return withCors(
        healthEndpoint(
          queueManager,
          wsHandler.size,
          sseHandler.size,
          config.getTcpConnectionCount?.() ?? 0
        )
      );
    }
    if (path === '/healthz' || path === '/live') {
      return withCors(new Response('OK', { status: 200 }));
    }
    if (path === '/ready') {
      return readinessEndpoint(queueManager, corsOrigins);
    }

    // Debug endpoints (require auth)
    if (path === '/gc' && req.method === 'POST') {
      const denied = checkAuth(req, authTokens);
      if (denied) return denied;
      return withCors(gcEndpoint(queueManager));
    }
    if (path === '/heapstats' && req.method === 'GET') {
      const denied = checkAuth(req, authTokens);
      if (denied) return denied;
      return withCors(heapStatsEndpoint(queueManager));
    }

    // Rate limiting
    const clientIp =
      req.headers.get('x-forwarded-for')?.split(',')[0]?.trim() ??
      req.headers.get('x-real-ip') ??
      'unknown';
    if (!getRateLimiter().isAllowed(clientIp)) {
      queueManager.emitDashboardEvent('ratelimit:hit', { clientId: clientIp });
      return jsonResponse({ ok: false, error: 'Rate limit exceeded' }, 429);
    }

    // WebSocket upgrade
    if (path === '/ws' || path.startsWith('/ws/')) {
      const denied = checkAuth(req, authTokens);
      if (denied) return denied;
      if (!wsHandler.canAccept()) {
        return jsonResponse({ ok: false, error: 'Too many WebSocket connections' }, 503);
      }
      const queueFilter = path.startsWith('/ws/queues/') ? path.slice('/ws/queues/'.length) : null;
      const upgraded = server.upgrade(req, {
        data: { id: uuid(), authenticated: true, queueFilter, subscriptions: null },
      });
      return upgraded ? undefined : new Response('WebSocket upgrade failed', { status: 400 });
    }

    // SSE endpoint
    if (path === '/events' || path.startsWith('/events/')) {
      const denied = checkAuth(req, authTokens);
      if (denied) return denied;
      const queueFilter = path.startsWith('/events/queues/')
        ? path.slice('/events/queues/'.length)
        : null;
      const lastEventId = req.headers.get('Last-Event-ID') ?? undefined;
      return sseHandler.createResponse(queueFilter, getCorsOrigin(), lastEventId);
    }

    // Prometheus metrics
    if (path === '/prometheus' && req.method === 'GET') {
      if (config.requireAuthForMetrics) {
        if (authTokens.size === 0) {
          return jsonResponse(
            { ok: false, error: 'Metrics authentication is enabled but no tokens are configured' },
            503,
            corsOrigins
          );
        }
        const denied = checkAuth(req, authTokens);
        if (denied) return denied;
      }
      return new Response(queueManager.getPrometheusMetrics(), {
        headers: {
          'Content-Type': 'text/plain; version=0.0.4; charset=utf-8',
          'Access-Control-Allow-Origin': getCorsOrigin(),
        },
      });
    }

    // Check authentication for other endpoints
    {
      const denied = checkAuth(req, authTokens);
      if (denied) {
        queueManager.emitDashboardEvent('auth:failed', { transport: 'http' });
        return denied;
      }
    }

    // HTTP is stateless — no clientId. Job ownership tracking is only for persistent
    // connections (TCP/WebSocket). Orphaned HTTP jobs are handled by stall detection.
    const ctx: HandlerContext = { queueManager, authTokens, authenticated: true };

    try {
      return await routeHttpRequest(req, path, ctx, corsOrigins);
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Internal error';
      return jsonResponse({ ok: false, error: message }, 500);
    }
  };

  // WebSocket handlers
  const websocket = {
    // Idle timeout: Bun sends ping automatically and closes if no pong received.
    // 120s is generous — detects dead clients within 2 minutes.
    idleTimeout: 120,
    // Max 1MB per message (prevents memory exhaustion from large payloads)
    maxPayloadLength: 1024 * 1024,
    open(ws: ServerWebSocket<WsData>) {
      wsHandler.onOpen(ws);
    },
    async message(ws: ServerWebSocket<WsData>, message: string | Buffer) {
      const ctx: HandlerContext = {
        queueManager,
        authTokens,
        authenticated: ws.data.authenticated,
        clientId: ws.data.id,
      };
      await wsHandler.onMessage(ws, message, ctx);
    },
    close(ws: ServerWebSocket<WsData>) {
      const clientId = ws.data.id;
      wsHandler.onClose(ws);
      getRateLimiter().removeClient(clientId);
      queueManager.unregisterWorkersByClientId(clientId);
      queueManager
        .releaseClientJobs(clientId)
        .then(() => undefined)
        .catch((err: unknown) => {
          httpLog.error('Failed to release WebSocket client jobs', {
            clientId,
            error: String(err),
          });
        });
    },
  };

  // Create server (validate TLS files BEFORE binding, fail fast on bad paths)
  const tlsOptions = config.tls ? loadTlsOptions(config.tls) : undefined;
  let server: Server<WsData>;
  if (config.socketPath) {
    server = Bun.serve<WsData>({
      unix: config.socketPath,
      ...(tlsOptions && { tls: tlsOptions }),
      fetch,
      websocket,
    });
  } else {
    server = Bun.serve<WsData>({
      hostname: config.hostname ?? '0.0.0.0',
      port: config.port ?? 6790,
      ...(tlsOptions && { tls: tlsOptions }),
      fetch,
      websocket,
    });
  }

  return {
    server,
    wsClients: wsHandler.getClients(),
    sseClients: sseHandler.getClients(),
    getWsClientCount: () => wsHandler.size,
    getSseClientCount: () => sseHandler.size,
    stop(): void {
      unsubscribe();
      wsHandler.stopBroadcasts();
      sseHandler.closeAll();
      void server.stop();
    },
  };
}

export type HttpServer = ReturnType<typeof createHttpServer>;
