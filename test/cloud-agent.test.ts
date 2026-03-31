/**
 * Tests for bunqueue Cloud Agent
 * Covers: config parsing, instance ID, circuit breaker, buffer, snapshot collection,
 * HTTP sender, WS sender, and the agent orchestrator.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { CircuitBreaker } from '../src/infrastructure/cloud/circuitBreaker';
import { SnapshotBuffer } from '../src/infrastructure/cloud/buffer';
import { getInstanceId } from '../src/infrastructure/cloud/instanceId';
import { loadCloudConfig } from '../src/infrastructure/cloud/config';
import type { CloudSnapshot } from '../src/infrastructure/cloud/types';
import { existsSync, mkdirSync, readFileSync, rmSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';

// ─── Config ───────────────────────────────────────────────────────

describe('loadCloudConfig', () => {
  const originalEnv: Record<string, string | undefined> = {};

  beforeEach(() => {
    // Save and clear all BUNQUEUE_CLOUD_* env vars
    for (const key of Object.keys(Bun.env)) {
      if (key.startsWith('BUNQUEUE_CLOUD_')) {
        originalEnv[key] = Bun.env[key];
        delete Bun.env[key];
      }
    }
  });

  afterEach(() => {
    // Restore env
    for (const key of Object.keys(Bun.env)) {
      if (key.startsWith('BUNQUEUE_CLOUD_')) {
        delete Bun.env[key];
      }
    }
    for (const [key, val] of Object.entries(originalEnv)) {
      if (val !== undefined) Bun.env[key] = val;
    }
  });

  test('returns null when URL is missing', () => {
    Bun.env.BUNQUEUE_CLOUD_API_KEY = 'dk_test_123';
    expect(loadCloudConfig()).toBeNull();
  });

  test('returns null when API key is missing', () => {
    Bun.env.BUNQUEUE_CLOUD_URL = 'https://cloud.bunqueue.io';
    expect(loadCloudConfig()).toBeNull();
  });

  test('returns null when both are missing', () => {
    expect(loadCloudConfig()).toBeNull();
  });

  test('returns config when both URL and API key are set', () => {
    Bun.env.BUNQUEUE_CLOUD_URL = 'https://cloud.bunqueue.io';
    Bun.env.BUNQUEUE_CLOUD_API_KEY = 'dk_test_123';

    const config = loadCloudConfig();
    expect(config).not.toBeNull();
    expect(config!.url).toBe('https://cloud.bunqueue.io');
    expect(config!.apiKey).toBe('dk_test_123');
  });

  test('strips trailing slashes from URL', () => {
    Bun.env.BUNQUEUE_CLOUD_URL = 'https://cloud.bunqueue.io///';
    Bun.env.BUNQUEUE_CLOUD_API_KEY = 'dk_test_123';

    const config = loadCloudConfig()!;
    expect(config.url).toBe('https://cloud.bunqueue.io');
  });

  test('parses all optional config with defaults', () => {
    Bun.env.BUNQUEUE_CLOUD_URL = 'https://cloud.bunqueue.io';
    Bun.env.BUNQUEUE_CLOUD_API_KEY = 'dk_test_123';

    const config = loadCloudConfig()!;
    expect(config.signingSecret).toBeNull();
    expect(config.intervalMs).toBe(15000);
    expect(config.includeJobData).toBe(false);
    expect(config.redactFields).toEqual([]);
    expect(config.eventFilter).toEqual([]);
    expect(config.bufferSize).toBe(720);
    expect(config.circuitBreakerThreshold).toBe(5);
    expect(config.circuitBreakerResetMs).toBe(60000);
    expect(config.useWebSocket).toBe(true);
    expect(config.useHttp).toBe(true);
    expect(config.dataPath).toBeNull();
  });

  test('parses custom values', () => {
    Bun.env.BUNQUEUE_CLOUD_URL = 'https://cloud.bunqueue.io';
    Bun.env.BUNQUEUE_CLOUD_API_KEY = 'dk_test_123';
    Bun.env.BUNQUEUE_CLOUD_SIGNING_SECRET = 'my-secret';
    Bun.env.BUNQUEUE_CLOUD_INSTANCE_NAME = 'prod-1';
    Bun.env.BUNQUEUE_CLOUD_INTERVAL_MS = '10000';
    Bun.env.BUNQUEUE_CLOUD_INCLUDE_JOB_DATA = 'true';
    Bun.env.BUNQUEUE_CLOUD_REDACT_FIELDS = 'email,password';
    Bun.env.BUNQUEUE_CLOUD_EVENTS = 'failed,stalled';
    Bun.env.BUNQUEUE_CLOUD_BUFFER_SIZE = '100';
    Bun.env.BUNQUEUE_CLOUD_USE_WEBSOCKET = 'false';

    const config = loadCloudConfig('/data/db')!;
    expect(config.signingSecret).toBe('my-secret');
    expect(config.instanceName).toBe('prod-1');
    expect(config.intervalMs).toBe(10000);
    expect(config.includeJobData).toBe(true);
    expect(config.redactFields).toEqual(['email', 'password']);
    expect(config.eventFilter).toEqual(['failed', 'stalled']);
    expect(config.bufferSize).toBe(100);
    expect(config.useWebSocket).toBe(false);
    expect(config.dataPath).toBe('/data/db');
  });
});

// ─── Instance ID ──────────────────────────────────────────────────

describe('getInstanceId', () => {
  test('returns UUID when no dataPath', () => {
    const id = getInstanceId(null);
    expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-/);
  });

  test('generates and persists ID to disk', () => {
    const dir = join(tmpdir(), `bq-cloud-test-${Date.now()}`);
    mkdirSync(dir, { recursive: true });
    const dataPath = join(dir, 'db.sqlite');

    try {
      const id1 = getInstanceId(dataPath);
      expect(id1).toMatch(/^[0-9a-f]{8}-/);

      // Second call returns same ID
      const id2 = getInstanceId(dataPath);
      expect(id2).toBe(id1);

      // Verify file exists
      const filePath = join(dir, 'cloud-instance-id');
      expect(existsSync(filePath)).toBe(true);
      expect(readFileSync(filePath, 'utf-8').trim()).toBe(id1);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ─── Circuit Breaker ──────────────────────────────────────────────

describe('CircuitBreaker', () => {
  test('starts closed', () => {
    const cb = new CircuitBreaker(3, 1000);
    expect(cb.getState()).toBe('closed');
    expect(cb.canExecute()).toBe(true);
  });

  test('opens after threshold failures', () => {
    const cb = new CircuitBreaker(3, 1000);
    cb.onFailure();
    cb.onFailure();
    expect(cb.getState()).toBe('closed');
    cb.onFailure(); // 3rd failure
    expect(cb.getState()).toBe('open');
    expect(cb.canExecute()).toBe(false);
  });

  test('resets on success', () => {
    const cb = new CircuitBreaker(3, 1000);
    cb.onFailure();
    cb.onFailure();
    cb.onSuccess();
    expect(cb.getState()).toBe('closed');
    // Should need 3 more failures to open
    cb.onFailure();
    cb.onFailure();
    expect(cb.getState()).toBe('closed');
  });

  test('transitions from open to half_open after resetMs', async () => {
    const cb = new CircuitBreaker(1, 50); // 50ms reset
    cb.onFailure(); // Opens immediately
    expect(cb.getState()).toBe('open');
    expect(cb.canExecute()).toBe(false);

    await Bun.sleep(60);

    expect(cb.canExecute()).toBe(true); // Now half_open
    expect(cb.getState()).toBe('half_open');
  });

  test('half_open goes back to closed on success', async () => {
    const cb = new CircuitBreaker(1, 50);
    cb.onFailure();
    await Bun.sleep(60);
    cb.canExecute(); // Transitions to half_open
    cb.onSuccess();
    expect(cb.getState()).toBe('closed');
  });

  test('half_open goes back to open on failure', async () => {
    const cb = new CircuitBreaker(1, 50);
    cb.onFailure();
    await Bun.sleep(60);
    cb.canExecute(); // Transitions to half_open
    cb.onFailure();
    expect(cb.getState()).toBe('open');
  });
});

// ─── Snapshot Buffer ──────────────────────────────────────────────

function makeSnapshot(seq: number): CloudSnapshot {
  return {
    instanceId: 'test',
    instanceName: 'test',
    version: '1.0.0',
    hostname: 'localhost',
    pid: 1,
    startedAt: 0,
    timestamp: Date.now(),
    sequenceId: seq,
    stats: { waiting: 0, delayed: 0, active: 0, dlq: 0, completed: 0, totalPushed: '0', totalPulled: '0', totalCompleted: '0', totalFailed: '0', uptime: 0, cronJobs: 0, cronPending: 0 },
    throughput: { pushPerSec: 0, pullPerSec: 0, completePerSec: 0, failPerSec: 0 },
    latency: { averages: { pushMs: 0, pullMs: 0, ackMs: 0 }, percentiles: { push: { p50: 0, p95: 0, p99: 0 }, pull: { p50: 0, p95: 0, p99: 0 }, ack: { p50: 0, p95: 0, p99: 0 } } },
    memory: { heapUsed: 0, heapTotal: 0, rss: 0, external: 0 },
    collections: { jobIndex: 0, completedJobs: 0, jobResults: 0, jobLogs: 0, customIdMap: 0, jobLocks: 0, processingTotal: 0, queuedTotal: 0, temporalIndexTotal: 0, delayedHeapTotal: 0 },
    queues: [],
    workers: { total: 0, active: 0, totalProcessed: 0, totalFailed: 0, activeJobs: 0 },
    crons: [],
    storage: { diskFull: false, error: null },
    taskErrors: {},
  };
}

describe('SnapshotBuffer', () => {
  test('push and drain', () => {
    const buf = new SnapshotBuffer(10);
    expect(buf.isEmpty).toBe(true);
    expect(buf.size).toBe(0);

    buf.push(makeSnapshot(1));
    buf.push(makeSnapshot(2));
    buf.push(makeSnapshot(3));

    expect(buf.size).toBe(3);
    expect(buf.isEmpty).toBe(false);

    const batch = buf.drain(2);
    expect(batch.length).toBe(2);
    expect(batch[0].sequenceId).toBe(1);
    expect(batch[1].sequenceId).toBe(2);
    expect(buf.size).toBe(1);
  });

  test('drops oldest when full', () => {
    const buf = new SnapshotBuffer(3);
    buf.push(makeSnapshot(1));
    buf.push(makeSnapshot(2));
    buf.push(makeSnapshot(3));
    buf.push(makeSnapshot(4)); // Should drop seq 1

    expect(buf.size).toBe(3);
    const all = buf.drain(10);
    expect(all[0].sequenceId).toBe(2);
    expect(all[2].sequenceId).toBe(4);
  });

  test('drain with count larger than buffer', () => {
    const buf = new SnapshotBuffer(10);
    buf.push(makeSnapshot(1));
    const batch = buf.drain(100);
    expect(batch.length).toBe(1);
    expect(buf.isEmpty).toBe(true);
  });

  test('drain empty buffer returns empty array', () => {
    const buf = new SnapshotBuffer(10);
    const batch = buf.drain(10);
    expect(batch.length).toBe(0);
  });
});

// ─── HTTP Sender with mock server ─────────────────────────────────

describe('HttpSender', () => {
  let server: ReturnType<typeof Bun.serve> | null = null;
  let received: unknown[] = [];
  let statusCode = 200;

  beforeEach(() => {
    received = [];
    statusCode = 200;
    server = Bun.serve({
      port: 0,
      fetch: async (req) => {
        const { unpack } = await import('msgpackr');
        const raw = Buffer.from(await req.arrayBuffer());
        const encoding = req.headers.get('content-encoding');
        const buf = encoding === 'zstd' ? Buffer.from(await Bun.zstdDecompress(raw)) : raw;
        const body = unpack(buf);
        received.push({ url: new URL(req.url).pathname, body, headers: Object.fromEntries(req.headers) });
        return new Response('ok', { status: statusCode });
      },
    });
  });

  afterEach(() => {
    server?.stop(true);
    server = null;
  });

  test('sends snapshot to /api/v1/ingest', async () => {
    const { HttpSender } = await import('../src/infrastructure/cloud/httpSender');

    const sender = new HttpSender({
      url: `http://localhost:${server!.port}`,
      apiKey: 'dk_test_123',
      signingSecret: null,
      instanceName: 'test',
      intervalMs: 15000,
      includeJobData: false,
      redactFields: [],
      eventFilter: [],
      bufferSize: 10,
      circuitBreakerThreshold: 5,
      circuitBreakerResetMs: 60000,
      useWebSocket: false,
      useHttp: true,
      dataPath: null,
    });

    const snapshot = makeSnapshot(1);
    await sender.send(snapshot);

    expect(received.length).toBe(1);
    const r = received[0] as { url: string; body: unknown; headers: Record<string, string> };
    expect(r.url).toBe('/api/v1/ingest');
    expect(r.headers.authorization).toBe('Bearer dk_test_123');
    expect(r.headers['content-type']).toBe('application/x-msgpack');
    expect(r.headers['x-timestamp']).toBeDefined();
  });

  test('buffers on failure and flushes on success', async () => {
    const { HttpSender } = await import('../src/infrastructure/cloud/httpSender');

    const sender = new HttpSender({
      url: `http://localhost:${server!.port}`,
      apiKey: 'dk_test_123',
      signingSecret: null,
      instanceName: 'test',
      intervalMs: 15000,
      includeJobData: false,
      redactFields: [],
      eventFilter: [],
      bufferSize: 10,
      circuitBreakerThreshold: 5,
      circuitBreakerResetMs: 60000,
      useWebSocket: false,
      useHttp: true,
      dataPath: null,
    });

    // Cause failures
    statusCode = 500;
    await sender.send(makeSnapshot(1)); // POST fails, buffered
    await sender.send(makeSnapshot(2)); // flush attempt + POST fail, both buffered
    expect(sender.getBufferSize()).toBe(2);

    // Fix server — next send should flush buffer then send new
    statusCode = 200;
    received = [];
    await sender.send(makeSnapshot(3));

    // Should have flushed buffer + sent new snapshot
    expect(sender.getBufferSize()).toBe(0);
  });

  test('includes HMAC signature when signing secret is set', async () => {
    const { HttpSender } = await import('../src/infrastructure/cloud/httpSender');

    const sender = new HttpSender({
      url: `http://localhost:${server!.port}`,
      apiKey: 'dk_test_123',
      signingSecret: 'test-secret-key',
      instanceName: 'test',
      intervalMs: 15000,
      includeJobData: false,
      redactFields: [],
      eventFilter: [],
      bufferSize: 10,
      circuitBreakerThreshold: 5,
      circuitBreakerResetMs: 60000,
      useWebSocket: false,
      useHttp: true,
      dataPath: null,
    });

    await sender.send(makeSnapshot(1));

    const r = received[0] as { headers: Record<string, string> };
    expect(r.headers['x-signature']).toBeDefined();
    expect(r.headers['x-signature'].length).toBe(64); // SHA-256 hex
  });

  test('circuit breaker stops sending after threshold', async () => {
    const { HttpSender } = await import('../src/infrastructure/cloud/httpSender');

    const sender = new HttpSender({
      url: `http://localhost:${server!.port}`,
      apiKey: 'dk_test_123',
      signingSecret: null,
      instanceName: 'test',
      intervalMs: 15000,
      includeJobData: false,
      redactFields: [],
      eventFilter: [],
      bufferSize: 100,
      circuitBreakerThreshold: 2,
      circuitBreakerResetMs: 60000,
      useWebSocket: false,
      useHttp: true,
      dataPath: null,
    });

    statusCode = 500;
    await sender.send(makeSnapshot(1)); // failure 1
    await sender.send(makeSnapshot(2)); // failure 2, circuit opens

    const countBefore = received.length;
    await sender.send(makeSnapshot(3)); // should be buffered without HTTP call

    expect(received.length).toBe(countBefore); // No new request
    expect(sender.getCircuitState()).toBe('open');
    expect(sender.getBufferSize()).toBe(3);
  });
});

// ─── CloudAgent redaction ─────────────────────────────────────────

describe('CloudAgent redaction', () => {
  test('redacts specified fields', async () => {
    // Access the private method via prototype for testing
    const { CloudAgent } = await import('../src/infrastructure/cloud/cloudAgent');

    // Create a minimal mock to test redactData
    const agent = Object.create(CloudAgent.prototype);
    agent.config = { redactFields: ['email', 'password'] };

    const data = { email: 'test@test.com', password: 'secret', name: 'John' };
    const redacted = agent.redactData(data) as Record<string, unknown>;

    expect(redacted.email).toBe('[REDACTED]');
    expect(redacted.password).toBe('[REDACTED]');
    expect(redacted.name).toBe('John');
  });

  test('returns data unchanged when no redact fields', async () => {
    const { CloudAgent } = await import('../src/infrastructure/cloud/cloudAgent');
    const agent = Object.create(CloudAgent.prototype);
    agent.config = { redactFields: [] };

    const data = { email: 'test@test.com' };
    expect(agent.redactData(data)).toBe(data); // Same reference
  });

  test('handles null/undefined data', async () => {
    const { CloudAgent } = await import('../src/infrastructure/cloud/cloudAgent');
    const agent = Object.create(CloudAgent.prototype);
    agent.config = { redactFields: ['email'] };

    expect(agent.redactData(null)).toBeNull();
    expect(agent.redactData(undefined)).toBeUndefined();
  });
});
