/**
 * Issue #46: setStallConfig and setDlqConfig don't work in TCP mode
 *
 * Tests that SetStallConfig, GetStallConfig, SetDlqConfig, GetDlqConfig
 * commands are handled by the server and the client sends them correctly
 * in TCP mode.
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
// SetStallConfig
// ============================================================

describe('SetStallConfig command', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should set stall config via handleCommand', async () => {
    const res = await handleCommand(
      {
        cmd: 'SetStallConfig',
        queue: 'test-queue',
        config: { stallInterval: 60000, maxStalls: 5 },
      } as Command,
      ctx
    );

    expect(res.ok).toBe(true);
  });

  test('should persist stall config that can be retrieved', async () => {
    await handleCommand(
      {
        cmd: 'SetStallConfig',
        queue: 'stall-persist',
        config: { stallInterval: 15000, maxStalls: 2, gracePeriod: 3000 },
      } as Command,
      ctx
    );

    const res = await handleCommand(
      { cmd: 'GetStallConfig', queue: 'stall-persist' } as Command,
      ctx
    );

    expect(res.ok).toBe(true);
    const config = (res as Record<string, unknown>).config as Record<string, unknown>;
    expect(config.stallInterval).toBe(15000);
    expect(config.maxStalls).toBe(2);
    expect(config.gracePeriod).toBe(3000);
  });
});

// ============================================================
// GetStallConfig
// ============================================================

describe('GetStallConfig command', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should return default stall config via handleCommand', async () => {
    const res = await handleCommand(
      { cmd: 'GetStallConfig', queue: 'test-queue' } as Command,
      ctx
    );

    expect(res.ok).toBe(true);
    const config = (res as Record<string, unknown>).config as Record<string, unknown>;
    expect(config.enabled).toBe(true);
    expect(config.stallInterval).toBe(30000);
    expect(config.maxStalls).toBe(3);
    expect(config.gracePeriod).toBe(5000);
  });
});

// ============================================================
// SetDlqConfig
// ============================================================

describe('SetDlqConfig command', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should set DLQ config via handleCommand', async () => {
    const res = await handleCommand(
      {
        cmd: 'SetDlqConfig',
        queue: 'test-queue',
        config: { autoRetry: true, maxAutoRetries: 5 },
      } as Command,
      ctx
    );

    expect(res.ok).toBe(true);
  });

  test('should persist DLQ config that can be retrieved', async () => {
    await handleCommand(
      {
        cmd: 'SetDlqConfig',
        queue: 'dlq-persist',
        config: { autoRetry: true, maxAutoRetries: 10, maxAge: 86400000 },
      } as Command,
      ctx
    );

    const res = await handleCommand(
      { cmd: 'GetDlqConfig', queue: 'dlq-persist' } as Command,
      ctx
    );

    expect(res.ok).toBe(true);
    const config = (res as Record<string, unknown>).config as Record<string, unknown>;
    expect(config.autoRetry).toBe(true);
    expect(config.maxAutoRetries).toBe(10);
    expect(config.maxAge).toBe(86400000);
  });
});

// ============================================================
// GetDlqConfig
// ============================================================

describe('GetDlqConfig command', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should return default DLQ config via handleCommand', async () => {
    const res = await handleCommand(
      { cmd: 'GetDlqConfig', queue: 'test-queue' } as Command,
      ctx
    );

    expect(res.ok).toBe(true);
    const config = (res as Record<string, unknown>).config as Record<string, unknown>;
    expect(config.autoRetry).toBe(false);
    expect(config.maxEntries).toBe(10000);
    expect(config.maxAutoRetries).toBe(3);
  });
});
