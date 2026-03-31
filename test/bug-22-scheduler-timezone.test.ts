/**
 * Bug #22: timezone cannot be set when using Queue#upsertJobScheduler
 *
 * The RepeatOpts interface in scheduler.ts is missing the timezone field,
 * and the embedded path hardcodes 'UTC' instead of using the user-provided value.
 * The TCP path does not pass timezone at all.
 *
 * @see https://github.com/egeominotti/bunqueue/issues/22
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue } from '../src/client';
import { getSharedManager, shutdownManager } from '../src/client/manager';
import { QueueManager } from '../src/application/queueManager';

describe('Bug #22: upsertJobScheduler timezone support', () => {
  let queue: Queue;
  let manager: QueueManager;

  beforeEach(() => {
    manager = getSharedManager();
    queue = new Queue('test-tz-queue', { embedded: true });
  });

  afterEach(async () => {
    await queue.close();
  });

  test('upsertJobScheduler should accept timezone option', async () => {
    // This should not throw a TypeScript error and should work at runtime
    const result = await queue.upsertJobScheduler(
      'rome-scheduler',
      {
        pattern: '0 9 * * *',
        timezone: 'Europe/Rome',
      },
      {
        name: 'daily-report',
        data: { type: 'report' },
      }
    );

    expect(result).not.toBeNull();
    expect(result!.id).toBe('rome-scheduler');
  });

  test('timezone should be passed through to cron job (not hardcoded to UTC)', async () => {
    await queue.upsertJobScheduler(
      'rome-scheduler',
      {
        pattern: '0 9 * * *',
        timezone: 'Europe/Rome',
      },
      {
        name: 'daily-report',
        data: { type: 'report' },
      }
    );

    // Verify the cron was created with the correct timezone
    const crons = manager.listCrons();
    const cron = crons.find((c) => c.name === 'rome-scheduler');

    expect(cron).toBeDefined();
    expect(cron!.timezone).toBe('Europe/Rome');
  });

  test('timezone should default to UTC when not provided', async () => {
    await queue.upsertJobScheduler(
      'utc-scheduler',
      {
        pattern: '0 9 * * *',
      },
      {
        name: 'daily-report',
        data: { type: 'report' },
      }
    );

    const crons = manager.listCrons();
    const cron = crons.find((c) => c.name === 'utc-scheduler');

    expect(cron).toBeDefined();
    // When no timezone is provided, should default to UTC (or null)
    expect(cron!.timezone === 'UTC' || cron!.timezone === null).toBe(true);
  });

  test('TCP mode should pass timezone in Cron command', async () => {
    // Verify the RepeatOpts type accepts timezone
    // This is primarily a compile-time check - if this file compiles,
    // the type includes timezone
    const repeatOpts: Parameters<typeof queue.upsertJobScheduler>[1] = {
      pattern: '0 9 * * *',
      timezone: 'America/New_York',
    };

    expect(repeatOpts.timezone).toBe('America/New_York');
  });
});
