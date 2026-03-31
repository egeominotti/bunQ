/**
 * HTTP Debug Endpoints Authentication Tests
 *
 * Verifies that /gc and /heapstats require auth when AUTH_TOKENS are configured,
 * while /health remains publicly accessible.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createHttpServer, type HttpServerConfig } from '../src/infrastructure/server/http';

describe('HTTP Debug Endpoints Auth', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  /** Helper to create an HTTP server and return its base URL and stop function */
  function startServer(config: HttpServerConfig) {
    const httpServer = createHttpServer(qm, config);
    const port = httpServer.server.port;
    const baseUrl = `http://localhost:${port}`;
    return { baseUrl, stop: () => httpServer.stop() };
  }

  describe('with auth tokens configured', () => {
    const AUTH_TOKEN = 'test-secret-token-12345';
    const AUTH_TOKENS = [AUTH_TOKEN];
    let baseUrl: string;
    let stop: () => void;

    beforeEach(() => {
      const server = startServer({ port: 0, authTokens: AUTH_TOKENS });
      baseUrl = server.baseUrl;
      stop = server.stop;
    });

    afterEach(() => {
      stop();
    });

    // --- POST /gc ---

    test('/gc returns 401 when no token provided', async () => {
      const res = await fetch(`${baseUrl}/gc`, { method: 'POST' });

      expect(res.status).toBe(401);
      const body = await res.json();
      expect(body.ok).toBe(false);
      expect(body.error).toBe('Unauthorized');
    });

    test('/gc returns 401 when wrong token provided', async () => {
      const res = await fetch(`${baseUrl}/gc`, {
        method: 'POST',
        headers: { Authorization: 'Bearer wrong-token' },
      });

      expect(res.status).toBe(401);
      const body = await res.json();
      expect(body.ok).toBe(false);
      expect(body.error).toBe('Unauthorized');
    });

    test('/gc returns 200 when valid token provided', async () => {
      const res = await fetch(`${baseUrl}/gc`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${AUTH_TOKEN}` },
      });

      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
    });

    // --- GET /heapstats ---

    test('/heapstats returns 401 when no token provided', async () => {
      const res = await fetch(`${baseUrl}/heapstats`, { method: 'GET' });

      expect(res.status).toBe(401);
      const body = await res.json();
      expect(body.ok).toBe(false);
      expect(body.error).toBe('Unauthorized');
    });

    test('/heapstats returns 401 when wrong token provided', async () => {
      const res = await fetch(`${baseUrl}/heapstats`, {
        method: 'GET',
        headers: { Authorization: 'Bearer invalid-token-xyz' },
      });

      expect(res.status).toBe(401);
      const body = await res.json();
      expect(body.ok).toBe(false);
      expect(body.error).toBe('Unauthorized');
    });

    test('/heapstats returns 200 when valid token provided', async () => {
      const res = await fetch(`${baseUrl}/heapstats`, {
        method: 'GET',
        headers: { Authorization: `Bearer ${AUTH_TOKEN}` },
      });

      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
    });

    // --- GET /health (always public) ---

    test('/health returns 200 without auth (public endpoint)', async () => {
      const res = await fetch(`${baseUrl}/health`, { method: 'GET' });

      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
    });
  });

  describe('without auth tokens configured', () => {
    let baseUrl: string;
    let stop: () => void;

    beforeEach(() => {
      const server = startServer({ port: 0, authTokens: [] });
      baseUrl = server.baseUrl;
      stop = server.stop;
    });

    afterEach(() => {
      stop();
    });

    test('/gc works without auth when no tokens configured', async () => {
      const res = await fetch(`${baseUrl}/gc`, { method: 'POST' });

      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
    });

    test('/heapstats works without auth when no tokens configured', async () => {
      const res = await fetch(`${baseUrl}/heapstats`, { method: 'GET' });

      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
    });

    test('/health works without auth when no tokens configured', async () => {
      const res = await fetch(`${baseUrl}/health`, { method: 'GET' });

      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
    });
  });

  describe('auth edge cases', () => {
    const AUTH_TOKEN = 'edge-case-token';
    let baseUrl: string;
    let stop: () => void;

    beforeEach(() => {
      const server = startServer({ port: 0, authTokens: [AUTH_TOKEN] });
      baseUrl = server.baseUrl;
      stop = server.stop;
    });

    afterEach(() => {
      stop();
    });

    test('/gc rejects empty Bearer token', async () => {
      const res = await fetch(`${baseUrl}/gc`, {
        method: 'POST',
        headers: { Authorization: 'Bearer ' },
      });

      expect(res.status).toBe(401);
    });

    test('/heapstats rejects empty Bearer token', async () => {
      const res = await fetch(`${baseUrl}/heapstats`, {
        method: 'GET',
        headers: { Authorization: 'Bearer ' },
      });

      expect(res.status).toBe(401);
    });

    test('/gc accepts token without Bearer prefix (server strips prefix if present)', async () => {
      // The server uses .replace('Bearer ', '') which is a no-op when
      // the prefix is absent, so the raw token value still matches.
      const res = await fetch(`${baseUrl}/gc`, {
        method: 'POST',
        headers: { Authorization: AUTH_TOKEN },
      });

      expect(res.status).toBe(200);
    });

    test('/heapstats accepts token without Bearer prefix (server strips prefix if present)', async () => {
      const res = await fetch(`${baseUrl}/heapstats`, {
        method: 'GET',
        headers: { Authorization: AUTH_TOKEN },
      });

      expect(res.status).toBe(200);
    });
  });

  describe('multiple auth tokens', () => {
    const TOKEN_A = 'token-alpha';
    const TOKEN_B = 'token-beta';
    let baseUrl: string;
    let stop: () => void;

    beforeEach(() => {
      const server = startServer({ port: 0, authTokens: [TOKEN_A, TOKEN_B] });
      baseUrl = server.baseUrl;
      stop = server.stop;
    });

    afterEach(() => {
      stop();
    });

    test('/gc accepts first token', async () => {
      const res = await fetch(`${baseUrl}/gc`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${TOKEN_A}` },
      });

      expect(res.status).toBe(200);
    });

    test('/gc accepts second token', async () => {
      const res = await fetch(`${baseUrl}/gc`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${TOKEN_B}` },
      });

      expect(res.status).toBe(200);
    });

    test('/heapstats accepts first token', async () => {
      const res = await fetch(`${baseUrl}/heapstats`, {
        method: 'GET',
        headers: { Authorization: `Bearer ${TOKEN_A}` },
      });

      expect(res.status).toBe(200);
    });

    test('/heapstats accepts second token', async () => {
      const res = await fetch(`${baseUrl}/heapstats`, {
        method: 'GET',
        headers: { Authorization: `Bearer ${TOKEN_B}` },
      });

      expect(res.status).toBe(200);
    });

    test('/gc rejects token not in the configured list', async () => {
      const res = await fetch(`${baseUrl}/gc`, {
        method: 'POST',
        headers: { Authorization: 'Bearer token-gamma' },
      });

      expect(res.status).toBe(401);
    });
  });
});
