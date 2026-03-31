/**
 * Tests for missing TCP command handlers
 * These commands are sent by the client SDK/MCP adapter but had no server-side handler.
 * Commands: ClearLogs, ExtendLock, ExtendLocks, ChangeDelay, SetWebhookEnabled,
 *           CompactMemory, MoveToWait, PromoteJobs
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import type { HandlerContext } from '../src/infrastructure/server/types';
import { handleCommand } from '../src/infrastructure/server/handler';
import type { Command } from '../src/domain/types/command';

function createContext(qm: QueueManager): HandlerContext {
  return {
    queueManager: qm,
    authTokens: new Set<string>(),
    authenticated: false,
    clientId: 'test-client-1',
  };
}

// ============================================================
// ClearLogs
// ============================================================

describe('ClearLogs command', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should clear logs for a job via handleCommand', async () => {
    const job = await qm.push('test-queue', { data: { x: 1 } });
    qm.addLog(job.id, 'test log message');
    expect(qm.getLogs(job.id).length).toBe(1);

    const res = await handleCommand(
      { cmd: 'ClearLogs', id: String(job.id) } as Command,
      ctx
    );

    expect(res.ok).toBe(true);
    expect(qm.getLogs(job.id).length).toBe(0);
  });

  test('should support keepLogs parameter', async () => {
    const job = await qm.push('test-queue', { data: { x: 1 } });
    qm.addLog(job.id, 'log 1');
    qm.addLog(job.id, 'log 2');
    qm.addLog(job.id, 'log 3');
    expect(qm.getLogs(job.id).length).toBe(3);

    const res = await handleCommand(
      { cmd: 'ClearLogs', id: String(job.id), keepLogs: 1 } as Command,
      ctx
    );

    expect(res.ok).toBe(true);
    expect(qm.getLogs(job.id).length).toBe(1);
  });
});

// ============================================================
// ExtendLock
// ============================================================

describe('ExtendLock command', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should extend lock for an active job via handleCommand', async () => {
    await qm.push('test-queue', { data: { x: 1 } });
    const { job, token } = await qm.pullWithLock('test-queue', 'worker-1');
    expect(job).not.toBeNull();
    expect(token).not.toBeNull();

    const res = await handleCommand(
      { cmd: 'ExtendLock', id: String(job!.id), token, duration: 60000 } as Command,
      ctx
    );

    expect(res.ok).toBe(true);
  });

  test('should fail for invalid token', async () => {
    await qm.push('test-queue', { data: { x: 1 } });
    const { job } = await qm.pullWithLock('test-queue', 'worker-1');

    const res = await handleCommand(
      { cmd: 'ExtendLock', id: String(job!.id), token: 'invalid-token', duration: 60000 } as Command,
      ctx
    );

    expect(res.ok).toBe(false);
  });
});

// ============================================================
// ExtendLocks (batch)
// ============================================================

describe('ExtendLocks command', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should extend locks for multiple active jobs via handleCommand', async () => {
    await qm.push('test-queue', { data: { x: 1 } });
    await qm.push('test-queue', { data: { x: 2 } });
    const r1 = await qm.pullWithLock('test-queue', 'worker-1');
    const r2 = await qm.pullWithLock('test-queue', 'worker-1');

    const res = await handleCommand(
      {
        cmd: 'ExtendLocks',
        ids: [String(r1.job!.id), String(r2.job!.id)],
        tokens: [r1.token!, r2.token!],
        durations: [60000, 60000],
      } as Command,
      ctx
    );

    expect(res.ok).toBe(true);
    const data = res as Record<string, unknown>;
    expect(data.count).toBe(2);
  });
});

// ============================================================
// ChangeDelay
// ============================================================

describe('ChangeDelay command', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should change delay for a job via handleCommand', async () => {
    await qm.push('test-queue', { data: { x: 1 } });
    const job = await qm.pull('test-queue');
    expect(job).not.toBeNull();

    const res = await handleCommand(
      { cmd: 'ChangeDelay', id: String(job!.id), delay: 5000 } as Command,
      ctx
    );

    expect(res.ok).toBe(true);
  });
});

// ============================================================
// SetWebhookEnabled
// ============================================================

describe('SetWebhookEnabled command', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should enable/disable a webhook via handleCommand', async () => {
    const webhook = qm.webhookManager.add('https://example.com/hook', ['job.completed']);

    // Disable
    const res1 = await handleCommand(
      { cmd: 'SetWebhookEnabled', id: webhook.id, enabled: false } as Command,
      ctx
    );
    expect(res1.ok).toBe(true);

    // Verify disabled
    const list = qm.webhookManager.list();
    const found = list.find((w) => w.id === webhook.id);
    expect(found?.enabled).toBe(false);

    // Re-enable
    const res2 = await handleCommand(
      { cmd: 'SetWebhookEnabled', id: webhook.id, enabled: true } as Command,
      ctx
    );
    expect(res2.ok).toBe(true);
  });

  test('should fail for non-existent webhook', async () => {
    const res = await handleCommand(
      { cmd: 'SetWebhookEnabled', id: 'non-existent', enabled: false } as Command,
      ctx
    );
    expect(res.ok).toBe(false);
  });
});

// ============================================================
// CompactMemory
// ============================================================

describe('CompactMemory command', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should compact memory via handleCommand', async () => {
    const res = await handleCommand(
      { cmd: 'CompactMemory' } as Command,
      ctx
    );

    expect(res.ok).toBe(true);
  });
});

// ============================================================
// MoveToWait
// ============================================================

describe('MoveToWait command', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should move a delayed job to waiting via handleCommand', async () => {
    const job = await qm.push('test-queue', { data: { x: 1 }, delay: 999999 });

    const res = await handleCommand(
      { cmd: 'MoveToWait', id: String(job.id) } as Command,
      ctx
    );

    expect(res.ok).toBe(true);
  });

  test('should fail for non-existent job', async () => {
    const res = await handleCommand(
      { cmd: 'MoveToWait', id: '99999' } as Command,
      ctx
    );
    expect(res.ok).toBe(false);
  });
});

// ============================================================
// PromoteJobs (batch)
// ============================================================

describe('PromoteJobs command', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should promote delayed jobs in a queue via handleCommand', async () => {
    await qm.push('test-queue', { data: { x: 1 }, delay: 999999 });
    await qm.push('test-queue', { data: { x: 2 }, delay: 999999 });

    const res = await handleCommand(
      { cmd: 'PromoteJobs', queue: 'test-queue' } as Command,
      ctx
    );

    expect(res.ok).toBe(true);
    const data = res as Record<string, unknown>;
    expect(typeof data.count).toBe('number');
    expect(data.count).toBeGreaterThanOrEqual(0);
  });
});
