/**
 * Tests for new TCP commands: CronGet, GetChildrenValues, StorageStatus
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import type { HandlerContext } from '../src/infrastructure/server/types';
import { handleCronGet } from '../src/infrastructure/server/handlers/cron';
import { handleGetChildrenValues } from '../src/infrastructure/server/handlers/query';
import { handleStorageStatus } from '../src/infrastructure/server/handlers/management';
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
// CronGet
// ============================================================

describe('CronGet command', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should return cron job by name', () => {
    qm.addCron({
      name: 'daily-cleanup',
      queue: 'maintenance',
      data: { type: 'cleanup' },
      schedule: '0 0 * * *',
    });

    const res = handleCronGet(
      { cmd: 'CronGet', name: 'daily-cleanup' } as Extract<Command, { cmd: 'CronGet' }>,
      ctx
    );

    expect(res.ok).toBe(true);
    const cron = (res as Record<string, unknown>).cron as Record<string, unknown>;
    expect(cron.name).toBe('daily-cleanup');
    expect(cron.queue).toBe('maintenance');
    expect(cron.schedule).toBe('0 0 * * *');
    expect(cron.nextRun).toBeGreaterThan(0);
  });

  test('should return error for non-existent cron', () => {
    const res = handleCronGet(
      { cmd: 'CronGet', name: 'does-not-exist' } as Extract<Command, { cmd: 'CronGet' }>,
      ctx
    );

    expect(res.ok).toBe(false);
    expect((res as Record<string, unknown>).error).toBe('Cron job not found');
  });

  test('should include executions and timezone fields', () => {
    qm.addCron({
      name: 'tz-cron',
      queue: 'emails',
      data: {},
      schedule: '0 9 * * *',
      timezone: 'Europe/Rome',
    });

    const res = handleCronGet(
      { cmd: 'CronGet', name: 'tz-cron' } as Extract<Command, { cmd: 'CronGet' }>,
      ctx
    );

    expect(res.ok).toBe(true);
    const cron = (res as Record<string, unknown>).cron as Record<string, unknown>;
    expect(cron.executions).toBe(0);
    expect(cron.timezone).toBe('Europe/Rome');
  });

  test('should be routable via handleCommand', async () => {
    qm.addCron({
      name: 'routed-cron',
      queue: 'test',
      data: {},
      schedule: '*/5 * * * *',
    });

    const res = await handleCommand(
      { cmd: 'CronGet', name: 'routed-cron' } as Command,
      ctx
    );

    expect(res.ok).toBe(true);
    const cron = (res as Record<string, unknown>).cron as Record<string, unknown>;
    expect(cron.name).toBe('routed-cron');
  });
});

// ============================================================
// GetChildrenValues
// ============================================================

describe('GetChildrenValues command', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should return empty values for job without children', async () => {
    const job = await qm.push('test-queue', { data: { name: 'solo' } });

    const res = await handleGetChildrenValues(
      { cmd: 'GetChildrenValues', id: String(job.id) } as Extract<
        Command,
        { cmd: 'GetChildrenValues' }
      >,
      ctx
    );

    expect(res.ok).toBe(true);
    const data = (res as Record<string, unknown>).data as Record<string, unknown>;
    expect(data.values).toEqual({});
  });

  test('should return empty values for non-existent job', async () => {
    const res = await handleGetChildrenValues(
      { cmd: 'GetChildrenValues', id: '99999' } as Extract<
        Command,
        { cmd: 'GetChildrenValues' }
      >,
      ctx
    );

    expect(res.ok).toBe(true);
    const data = (res as Record<string, unknown>).data as Record<string, unknown>;
    expect(data.values).toEqual({});
  });

  test('should be routable via handleCommand', async () => {
    const job = await qm.push('test-queue', { data: { x: 1 } });

    const res = await handleCommand(
      { cmd: 'GetChildrenValues', id: String(job.id) } as Command,
      ctx
    );

    expect(res.ok).toBe(true);
  });
});

// ============================================================
// StorageStatus
// ============================================================

describe('StorageStatus command', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should return real storage status', () => {
    const res = handleStorageStatus(
      { cmd: 'StorageStatus' } as Extract<Command, { cmd: 'StorageStatus' }>,
      ctx
    );

    expect(res.ok).toBe(true);
    const data = (res as Record<string, unknown>).data as Record<string, unknown>;
    expect(typeof data.diskFull).toBe('boolean');
    expect(data.diskFull).toBe(false); // Normal conditions
    expect(data.error).toBeNull();
  });

  test('should be routable via handleCommand', async () => {
    const res = await handleCommand(
      { cmd: 'StorageStatus' } as Command,
      ctx
    );

    expect(res.ok).toBe(true);
    const data = (res as Record<string, unknown>).data as Record<string, unknown>;
    expect(typeof data.diskFull).toBe('boolean');
  });
});
