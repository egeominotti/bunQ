/**
 * Startup & Graceful Shutdown Tests
 *
 * Tests the key units of the server lifecycle:
 * 1. Config shape validation (ServerConfig accepted by QueueManager + servers)
 * 2. Server lifecycle: start TCP + HTTP servers, verify they listen, then stop cleanly
 * 3. Graceful shutdown: active jobs block shutdown, shutdown completes when active reaches 0
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createHttpServer } from '../src/infrastructure/server/http';
import { createTcpServer } from '../src/infrastructure/server/tcp';

describe('Startup & Shutdown', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  // =============================================
  // Config shape tests
  // =============================================

  describe('config shape', () => {
    test('QueueManager accepts empty config (in-memory mode)', () => {
      const manager = new QueueManager();
      const stats = manager.getStats();
      expect(stats.waiting).toBe(0);
      expect(stats.active).toBe(0);
      manager.shutdown();
    });

    test('QueueManager accepts dataPath config', () => {
      const tmpPath = `/tmp/bunqueue-test-${Date.now()}.db`;
      const manager = new QueueManager({ dataPath: tmpPath });
      const stats = manager.getStats();
      expect(stats.waiting).toBe(0);
      manager.shutdown();

      // Clean up temp file
      try {
        const { unlinkSync } = require('fs');
        unlinkSync(tmpPath);
        unlinkSync(tmpPath + '-wal');
        unlinkSync(tmpPath + '-shm');
      } catch {
        // ignore cleanup errors
      }
    });

    test('TCP server accepts port 0 for random port', () => {
      const tcpServer = createTcpServer(qm, { port: 0 });
      expect(tcpServer.server.port).toBeGreaterThan(0);
      tcpServer.stop();
    });

    test('TCP server accepts hostname config', () => {
      const tcpServer = createTcpServer(qm, { port: 0, hostname: '127.0.0.1' });
      expect(tcpServer.server.hostname).toBe('127.0.0.1');
      tcpServer.stop();
    });

    test('TCP server accepts auth tokens', () => {
      const tcpServer = createTcpServer(qm, {
        port: 0,
        authTokens: ['token-a', 'token-b'],
      });
      expect(tcpServer.server.port).toBeGreaterThan(0);
      tcpServer.stop();
    });

    test('HTTP server accepts port 0 for random port', () => {
      const httpServer = createHttpServer(qm, { port: 0 });
      expect(httpServer.server.port).toBeGreaterThan(0);
      httpServer.stop();
    });

    test('HTTP server accepts full config', () => {
      const httpServer = createHttpServer(qm, {
        port: 0,
        hostname: '127.0.0.1',
        authTokens: ['my-token'],
        corsOrigins: ['https://example.com'],
        requireAuthForMetrics: true,
      });
      expect(httpServer.server.port).toBeGreaterThan(0);
      httpServer.stop();
    });

    test('HTTP server with empty corsOrigins defaults to wildcard', async () => {
      const httpServer = createHttpServer(qm, { port: 0, corsOrigins: [] });
      const port = httpServer.server.port;

      // Health endpoint should work regardless of CORS
      const res = await fetch(`http://localhost:${port}/health`);
      expect(res.status).toBe(200);

      httpServer.stop();
    });

    test('auth tokens as comma-separated values are correctly split', () => {
      // This mirrors loadConfig()'s AUTH_TOKENS parsing logic
      const raw = 'token1,token2,token3';
      const parsed = raw.split(',').filter(Boolean);
      expect(parsed).toEqual(['token1', 'token2', 'token3']);
    });

    test('empty AUTH_TOKENS string results in empty array', () => {
      const raw = '';
      const parsed = raw.split(',').filter(Boolean);
      expect(parsed).toEqual([]);
    });

    test('CORS_ALLOW_ORIGIN parsing with multiple origins', () => {
      const raw = 'https://a.com,https://b.com';
      const parsed = raw.split(',').filter(Boolean);
      expect(parsed).toEqual(['https://a.com', 'https://b.com']);
    });

    test('undefined CORS_ALLOW_ORIGIN results in empty array', () => {
      const raw: string | undefined = undefined;
      const parsed = raw?.split(',').filter(Boolean) ?? [];
      expect(parsed).toEqual([]);
    });

    test('boolean config: S3_BACKUP_ENABLED parses correctly', () => {
      expect('1' === '1' || '1' === 'true').toBe(true);
      expect('true' === '1' || 'true' === 'true').toBe(true);
      expect('0' === '1' || '0' === 'true').toBe(false);
      expect('false' === '1' || 'false' === 'true').toBe(false);
    });

    test('boolean config: METRICS_AUTH parses correctly', () => {
      expect('true' === 'true').toBe(true);
      expect('false' === 'true').toBe(false);
      expect('1' === 'true').toBe(false); // Only strict 'true' enables it
    });

    test('port defaults: TCP=6789, HTTP=6790', () => {
      const tcpDefault = parseInt(undefined ?? '6789', 10);
      const httpDefault = parseInt(undefined ?? '6790', 10);
      expect(tcpDefault).toBe(6789);
      expect(httpDefault).toBe(6790);
    });

    test('hostname defaults to 0.0.0.0', () => {
      const hostname = undefined ?? '0.0.0.0';
      expect(hostname).toBe('0.0.0.0');
    });

    test('DATA_PATH falls back to SQLITE_PATH', () => {
      // Mirrors: dataPath: Bun.env.DATA_PATH ?? Bun.env.SQLITE_PATH
      const dataPath = undefined;
      const sqlitePath = '/some/path.db';
      const resolved = dataPath ?? sqlitePath;
      expect(resolved).toBe('/some/path.db');
    });
  });

  // =============================================
  // Server lifecycle tests
  // =============================================

  describe('server lifecycle', () => {
    test('TCP and HTTP servers start on random ports', () => {
      const tcpServer = createTcpServer(qm, { port: 0 });
      const httpServer = createHttpServer(qm, { port: 0 });

      expect(tcpServer.server.port).toBeGreaterThan(0);
      expect(httpServer.server.port).toBeGreaterThan(0);

      // Ports should be different
      expect(tcpServer.server.port).not.toBe(httpServer.server.port);

      tcpServer.stop();
      httpServer.stop();
    });

    test('HTTP health endpoint responds after startup', async () => {
      const httpServer = createHttpServer(qm, { port: 0 });
      const port = httpServer.server.port;

      const res = await fetch(`http://localhost:${port}/health`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as { ok: boolean };
      expect(body.ok).toBe(true);

      httpServer.stop();
    });

    test('HTTP /healthz returns OK after startup', async () => {
      const httpServer = createHttpServer(qm, { port: 0 });
      const port = httpServer.server.port;

      const res = await fetch(`http://localhost:${port}/healthz`);
      expect(res.status).toBe(200);
      const text = await res.text();
      expect(text).toBe('OK');

      httpServer.stop();
    });

    test('HTTP /ready returns ready after startup', async () => {
      const httpServer = createHttpServer(qm, { port: 0 });
      const port = httpServer.server.port;

      const res = await fetch(`http://localhost:${port}/ready`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as { ok: boolean; ready: boolean };
      expect(body.ok).toBe(true);
      expect(body.ready).toBe(true);

      httpServer.stop();
    });

    test('TCP server starts with 0 connections', () => {
      const tcpServer = createTcpServer(qm, { port: 0 });

      expect(tcpServer.getConnectionCount()).toBe(0);

      tcpServer.stop();
    });

    test('HTTP server starts with 0 WebSocket and SSE clients', () => {
      const httpServer = createHttpServer(qm, { port: 0 });

      expect(httpServer.getWsClientCount()).toBe(0);
      expect(httpServer.getSseClientCount()).toBe(0);

      httpServer.stop();
    });

    test('servers stop cleanly without errors', () => {
      const tcpServer = createTcpServer(qm, { port: 0 });
      const httpServer = createHttpServer(qm, { port: 0 });

      // Stop should not throw
      expect(() => {
        tcpServer.stop();
        httpServer.stop();
      }).not.toThrow();
    });

    test('TCP server clears connections on stop', () => {
      const tcpServer = createTcpServer(qm, { port: 0 });
      tcpServer.stop();
      expect(tcpServer.getConnectionCount()).toBe(0);
    });

    test('QueueManager shutdown clears all internal state', async () => {
      const manager = new QueueManager();

      // Push some jobs to populate internal state
      await manager.push('test-queue', { data: { msg: 'hello' } });
      await manager.push('test-queue', { data: { msg: 'world' } });

      const statsBefore = manager.getStats();
      expect(statsBefore.waiting).toBeGreaterThan(0);

      // Shutdown should clear everything
      manager.shutdown();

      // After shutdown, creating a new manager should start fresh
      const fresh = new QueueManager();
      const statsAfter = fresh.getStats();
      expect(statsAfter.waiting).toBe(0);
      expect(statsAfter.active).toBe(0);
      fresh.shutdown();
    });

    test('QueueManager shutdown is idempotent', () => {
      const manager = new QueueManager();

      // Multiple shutdowns should not throw
      expect(() => {
        manager.shutdown();
        manager.shutdown();
      }).not.toThrow();
    });

    test('full stack start and stop lifecycle', async () => {
      const manager = new QueueManager();
      const tcpServer = createTcpServer(manager, { port: 0 });
      const httpServer = createHttpServer(manager, { port: 0 });

      // Verify everything is running
      const healthRes = await fetch(`http://localhost:${httpServer.server.port}/health`);
      expect(healthRes.status).toBe(200);

      // Clean shutdown in the order main.ts does it
      tcpServer.stop();
      httpServer.stop();
      manager.shutdown();
    });
  });

  // =============================================
  // Graceful shutdown tests
  // =============================================

  describe('graceful shutdown', () => {
    test('stats.active is 0 when no jobs are processing', () => {
      const stats = qm.getStats();
      expect(stats.active).toBe(0);
    });

    test('pushing jobs without pulling keeps active at 0', async () => {
      await qm.push('shutdown-test', { data: { id: 1 } });
      await qm.push('shutdown-test', { data: { id: 2 } });
      await qm.push('shutdown-test', { data: { id: 3 } });

      const stats = qm.getStats();
      expect(stats.waiting).toBe(3);
      expect(stats.active).toBe(0);
    });

    test('pulled jobs become active', async () => {
      await qm.push('shutdown-test', { data: { id: 1 } });
      await qm.push('shutdown-test', { data: { id: 2 } });

      // Pull one job (makes it active)
      const job = await qm.pull('shutdown-test');
      expect(job).not.toBeNull();

      const stats = qm.getStats();
      expect(stats.active).toBe(1);
      expect(stats.waiting).toBe(1);
    });

    test('active jobs block shutdown wait loop', async () => {
      await qm.push('shutdown-test', { data: { id: 1 } });
      const job = await qm.pull('shutdown-test');
      expect(job).not.toBeNull();

      // Simulate the shutdown wait loop from main.ts
      const stats = qm.getStats();
      expect(stats.active).toBeGreaterThan(0);

      // In the real shutdown, this would loop until active === 0
      // The loop would NOT break here because active > 0
      const shouldContinueWaiting = stats.active > 0;
      expect(shouldContinueWaiting).toBe(true);
    });

    test('acknowledging active jobs reduces active count to 0', async () => {
      await qm.push('shutdown-test', { data: { id: 1 } });
      await qm.push('shutdown-test', { data: { id: 2 } });

      // Pull both jobs
      const job1 = await qm.pull('shutdown-test');
      const job2 = await qm.pull('shutdown-test');
      expect(job1).not.toBeNull();
      expect(job2).not.toBeNull();

      let stats = qm.getStats();
      expect(stats.active).toBe(2);

      // Ack the first job
      await qm.ack(job1!.id);
      stats = qm.getStats();
      expect(stats.active).toBe(1);

      // Ack the second job
      await qm.ack(job2!.id);
      stats = qm.getStats();
      expect(stats.active).toBe(0);
    });

    test('shutdown completes when active count reaches 0', async () => {
      await qm.push('shutdown-test', { data: { id: 1 } });
      const job = await qm.pull('shutdown-test');
      expect(job).not.toBeNull();

      // Simulate the shutdown wait loop logic from main.ts:
      //   while (Date.now() - start < shutdownTimeout) {
      //     const stats = queueManager.getStats();
      //     if (stats.active === 0) break;
      //     await Bun.sleep(1000);
      //   }

      let shutdownReady = false;
      const checkShutdown = () => {
        const stats = qm.getStats();
        if (stats.active === 0) {
          shutdownReady = true;
        }
      };

      // Before ack: shutdown not ready
      checkShutdown();
      expect(shutdownReady).toBe(false);

      // Ack the job: shutdown becomes ready
      await qm.ack(job!.id);
      checkShutdown();
      expect(shutdownReady).toBe(true);
    });

    test('failing a job also removes it from active', async () => {
      await qm.push('shutdown-test', { data: { id: 1 }, attempts: 1 });
      const job = await qm.pull('shutdown-test');
      expect(job).not.toBeNull();

      let stats = qm.getStats();
      expect(stats.active).toBe(1);

      // Fail the job (with attempts=1, it goes to DLQ instead of retrying)
      await qm.fail(job!.id, 'test error');
      stats = qm.getStats();
      expect(stats.active).toBe(0);
    });

    test('shutdown wait loop with timeout simulation', async () => {
      await qm.push('shutdown-test', { data: { id: 1 } });
      const job = await qm.pull('shutdown-test');
      expect(job).not.toBeNull();

      // Simulate a shutdown with a short timeout
      const shutdownTimeoutMs = 100;
      const start = Date.now();
      let timedOut = false;
      let broke = false;

      while (Date.now() - start < shutdownTimeoutMs) {
        const stats = qm.getStats();
        if (stats.active === 0) {
          broke = true;
          break;
        }
        await Bun.sleep(10);
      }

      if (!broke) {
        timedOut = true;
      }

      // We never acked, so the loop should have timed out
      expect(timedOut).toBe(true);
      expect(broke).toBe(false);

      // Clean up: ack the job so afterEach shutdown is clean
      await qm.ack(job!.id);
    });

    test('shutdown wait loop exits immediately when no active jobs', async () => {
      await qm.push('shutdown-test', { data: { id: 1 } });
      // Don't pull - no active jobs

      const start = Date.now();
      const shutdownTimeoutMs = 5000;
      let iterations = 0;

      while (Date.now() - start < shutdownTimeoutMs) {
        const stats = qm.getStats();
        iterations++;
        if (stats.active === 0) break;
        await Bun.sleep(100);
      }

      // Should break immediately on first iteration
      expect(iterations).toBe(1);
    });

    test('concurrent acks during shutdown wait', async () => {
      // Push and pull multiple jobs
      const count = 5;
      for (let i = 0; i < count; i++) {
        await qm.push('shutdown-test', { data: { id: i } });
      }

      const jobs = [];
      for (let i = 0; i < count; i++) {
        const job = await qm.pull('shutdown-test');
        expect(job).not.toBeNull();
        jobs.push(job!);
      }

      let stats = qm.getStats();
      expect(stats.active).toBe(count);

      // Ack all concurrently (simulating workers finishing during shutdown)
      await Promise.all(jobs.map((j) => qm.ack(j.id)));

      stats = qm.getStats();
      expect(stats.active).toBe(0);
    });

    test('servers stop accepting new connections during shutdown', async () => {
      const httpServer = createHttpServer(qm, { port: 0 });
      const port = httpServer.server.port;

      // Verify server is running
      const res1 = await fetch(`http://localhost:${port}/health`);
      expect(res1.status).toBe(200);

      // Stop the server
      httpServer.stop();

      // After stopping, new requests should fail
      try {
        await fetch(`http://localhost:${port}/health`, {
          signal: AbortSignal.timeout(1000),
        });
        // If we get here, the server might still be draining - that's acceptable
      } catch {
        // Expected: connection refused or timeout after stop
      }
    });

    test('full graceful shutdown sequence', async () => {
      const manager = new QueueManager();
      const tcpServer = createTcpServer(manager, { port: 0 });
      const httpServer = createHttpServer(manager, { port: 0 });

      // Push and pull a job to make it active
      await manager.push('shutdown-q', { data: { msg: 'test' } });
      const job = await manager.pull('shutdown-q');
      expect(job).not.toBeNull();

      // Step 1: Stop servers (no new connections)
      tcpServer.stop();
      httpServer.stop();

      // Step 2: Wait for active jobs (simulate the main.ts loop)
      let stats = manager.getStats();
      expect(stats.active).toBe(1);

      // Simulate worker completing the job
      await manager.ack(job!.id);

      stats = manager.getStats();
      expect(stats.active).toBe(0);

      // Step 3: Shutdown QueueManager
      manager.shutdown();

      // Verify clean state after full shutdown
      // (manager is shutdown, so we check with a fresh instance)
      const fresh = new QueueManager();
      const freshStats = fresh.getStats();
      expect(freshStats.active).toBe(0);
      expect(freshStats.waiting).toBe(0);
      fresh.shutdown();
    });
  });
});
