/**
 * Cron Timezone & DST Edge Cases (Embedded Mode)
 *
 * Tests cron scheduling with timezone handling using QueueManager directly.
 * Covers: timezone scheduling, UTC, multi-timezone, repeat_every, deletion,
 * list, maxLimit, name uniqueness, persistence via load, and DST offsets.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { getNextCronRun } from '../src/infrastructure/scheduler/cronParser';
import type { CronJob } from '../src/domain/types/cron';

/** Access private tick() for deterministic testing */
function tickScheduler(mgr: QueueManager): Promise<void> {
  return (mgr as any).cronScheduler.tick();
}

describe('Cron Timezone & DST Edge Cases', () => {
  let manager: QueueManager;

  beforeEach(() => { manager = new QueueManager(); });
  afterEach(() => { manager.shutdown(); });

  // 1. Cron with specific timezone (America/New_York)
  test('cron with America/New_York schedules at correct local hour', () => {
    const cron = manager.addCron({
      name: 'ny-morning', queue: 'notifications',
      data: { message: 'good morning' },
      schedule: '0 9 * * *', timezone: 'America/New_York',
    });
    expect(cron.timezone).toBe('America/New_York');
    expect(cron.nextRun).toBeGreaterThan(Date.now());
    const nyHour = new Intl.DateTimeFormat('en-US', {
      timeZone: 'America/New_York', hour: 'numeric', hour12: false,
    }).format(new Date(cron.nextRun));
    expect(parseInt(nyHour)).toBe(9);
  });

  // 2. Cron with UTC timezone
  test('cron with UTC timezone schedules at UTC hours', () => {
    const cron = manager.addCron({
      name: 'utc-noon', queue: 'reports',
      data: {}, schedule: '0 12 * * *', timezone: 'UTC',
    });
    expect(cron.timezone).toBe('UTC');
    const d = new Date(cron.nextRun);
    expect(d.getUTCHours()).toBe(12);
    expect(d.getUTCMinutes()).toBe(0);
  });

  test('cron without timezone defaults to null', () => {
    const cron = manager.addCron({
      name: 'no-tz', queue: 'tasks', data: {}, schedule: '0 8 * * *',
    });
    expect(cron.timezone).toBeNull();
    expect(cron.nextRun).toBeGreaterThan(Date.now());
  });

  // 3. Multiple crons with different timezones
  test('multiple crons with different timezones have distinct next_run', () => {
    const cronNY = manager.addCron({
      name: 'ny', queue: 'mtz', data: {}, schedule: '0 9 * * *', timezone: 'America/New_York',
    });
    const cronTokyo = manager.addCron({
      name: 'tokyo', queue: 'mtz', data: {}, schedule: '0 9 * * *', timezone: 'Asia/Tokyo',
    });
    const cronLondon = manager.addCron({
      name: 'london', queue: 'mtz', data: {}, schedule: '0 9 * * *', timezone: 'Europe/London',
    });
    const runs = new Set([cronNY.nextRun, cronTokyo.nextRun, cronLondon.nextRun]);
    expect(runs.size).toBeGreaterThanOrEqual(2);
    expect(cronNY.timezone).toBe('America/New_York');
    expect(cronTokyo.timezone).toBe('Asia/Tokyo');
    expect(cronLondon.timezone).toBe('Europe/London');
  });

  test('getNextCronRun produces different UTC times per timezone', () => {
    const ref = new Date('2024-01-15T00:00:00Z').getTime();
    expect(new Date(getNextCronRun('0 9 * * *', ref)).getUTCHours()).toBe(9);            // UTC
    expect(new Date(getNextCronRun('0 9 * * *', ref, 'America/New_York')).getUTCHours()).toBe(14); // EST
    expect(new Date(getNextCronRun('0 9 * * *', ref, 'Asia/Tokyo')).getUTCHours()).toBe(0);        // JST
  });

  // 4. Cron with repeat_every (interval-based)
  test('repeat_every schedules next run at fixed interval', () => {
    const before = Date.now();
    const cron = manager.addCron({
      name: 'interval-5s', queue: 'tasks', data: {}, repeatEvery: 5000,
    });
    expect(cron.repeatEvery).toBe(5000);
    expect(cron.schedule).toBeNull();
    expect(cron.nextRun).toBeGreaterThanOrEqual(before + 5000);
    expect(cron.nextRun).toBeLessThanOrEqual(Date.now() + 5100);
  });

  test('repeat_every stores timezone but uses absolute interval', () => {
    const cron = manager.addCron({
      name: 'interval-tz', queue: 'tasks', data: {},
      repeatEvery: 10000, timezone: 'Asia/Tokyo',
    });
    expect(cron.timezone).toBe('Asia/Tokyo');
    const diff = cron.nextRun - Date.now();
    expect(diff).toBeGreaterThan(9800);
    expect(diff).toBeLessThanOrEqual(10200);
  });

  // 5. Cron deletion stops future scheduling
  test('deleting a cron removes it from scheduler', () => {
    manager.addCron({
      name: 'to-delete', queue: 'tasks', data: {},
      schedule: '*/5 * * * *', timezone: 'Europe/Berlin',
    });
    expect(manager.getCron('to-delete')).toBeDefined();
    expect(manager.removeCron('to-delete')).toBe(true);
    expect(manager.getCron('to-delete')).toBeUndefined();
  });

  test('deleted cron does not produce jobs on tick', async () => {
    manager.addCron({ name: 'del-tick', queue: 'del-q', data: {}, repeatEvery: 10 });
    manager.removeCron('del-tick');
    await new Promise((r) => setTimeout(r, 30));
    await tickScheduler(manager);
    expect(manager.count('del-q')).toBe(0);
  });

  test('deleting non-existent cron returns false', () => {
    expect(manager.removeCron('ghost')).toBe(false);
  });

  // 6. Cron list returns all registered crons with correct next_run
  test('listCrons returns all registered crons with correct metadata', () => {
    manager.addCron({ name: 'a', queue: 'q1', data: {}, schedule: '0 * * * *', timezone: 'UTC' });
    manager.addCron({ name: 'b', queue: 'q2', data: {}, schedule: '0 0 * * *', timezone: 'America/Chicago' });
    manager.addCron({ name: 'c', queue: 'q3', data: {}, repeatEvery: 60000 });
    const crons = manager.listCrons();
    expect(crons.length).toBe(3);
    expect(crons.map((c) => c.name).sort()).toEqual(['a', 'b', 'c']);
    const now = Date.now();
    for (const c of crons) expect(c.nextRun).toBeGreaterThan(now);
  });

  test('listCrons reflects removals', () => {
    manager.addCron({ name: 'x', queue: 'q', data: {}, repeatEvery: 1000 });
    manager.addCron({ name: 'y', queue: 'q', data: {}, repeatEvery: 1000 });
    expect(manager.listCrons().length).toBe(2);
    manager.removeCron('x');
    expect(manager.listCrons().length).toBe(1);
    expect(manager.listCrons()[0].name).toBe('y');
  });

  // 7. Cron with maxLimit stops after N executions
  test('cron with maxLimit=1 produces exactly one job', async () => {
    manager.addCron({
      name: 'one-shot', queue: 'lim-q1', data: {}, repeatEvery: 10, maxLimit: 1,
    });
    await new Promise((r) => setTimeout(r, 30));
    await tickScheduler(manager);
    expect(manager.count('lim-q1')).toBe(1);
    // Tick again - no more jobs
    await new Promise((r) => setTimeout(r, 30));
    await tickScheduler(manager);
    expect(manager.count('lim-q1')).toBe(1);
  });

  test('cron with maxLimit=3 stops after 3 executions', async () => {
    manager.addCron({
      name: 'three-shot', queue: 'lim-q3', data: {}, repeatEvery: 10, maxLimit: 3,
    });
    for (let i = 0; i < 5; i++) {
      await new Promise((r) => setTimeout(r, 20));
      await tickScheduler(manager);
    }
    const cron = manager.getCron('three-shot');
    if (cron) expect(cron.executions).toBe(3);
    expect(manager.count('lim-q3')).toBe(3);
  });

  // 8. Cron name uniqueness (re-adding same name updates it)
  test('re-adding cron with same name replaces the old one', () => {
    manager.addCron({
      name: 'unique', queue: 'old-q', data: { v: 1 },
      schedule: '0 6 * * *', timezone: 'UTC',
    });
    manager.addCron({
      name: 'unique', queue: 'new-q', data: { v: 2 },
      schedule: '0 18 * * *', timezone: 'Asia/Tokyo',
    });
    expect(manager.listCrons().length).toBe(1);
    const cron = manager.getCron('unique')!;
    expect(cron.queue).toBe('new-q');
    expect(cron.data).toEqual({ v: 2 });
    expect(cron.timezone).toBe('Asia/Tokyo');
  });

  test('re-adding cron resets execution count', () => {
    manager.addCron({ name: 'reset', queue: 'q', data: {}, repeatEvery: 60000 });
    const c2 = manager.addCron({ name: 'reset', queue: 'q', data: {}, repeatEvery: 30000 });
    expect(c2.executions).toBe(0);
    expect(c2.repeatEvery).toBe(30000);
  });

  // 9. Cron survives scheduler restart (persistence via load)
  test('crons can be saved and reloaded via load() to survive restart', () => {
    manager.addCron({
      name: 'pa', queue: 'pq', data: { k: 'alpha' },
      schedule: '0 8 * * *', timezone: 'America/New_York',
    });
    manager.addCron({ name: 'pb', queue: 'pq', data: { k: 'beta' }, repeatEvery: 60000 });
    const snapshot: CronJob[] = manager.listCrons().map((c) => ({ ...c }));
    manager.shutdown();

    const newMgr = new QueueManager();
    (newMgr as any).cronScheduler.load(snapshot);
    expect(newMgr.listCrons().length).toBe(2);
    const a = newMgr.getCron('pa')!;
    expect(a.data).toEqual({ k: 'alpha' });
    expect(a.timezone).toBe('America/New_York');
    const b = newMgr.getCron('pb')!;
    expect(b.repeatEvery).toBe(60000);
    newMgr.shutdown();
  });

  test('loaded crons with past nextRun fire on next tick', async () => {
    const newMgr = new QueueManager();
    (newMgr as any).cronScheduler.load([{
      name: 'overdue', queue: 'overdue-q', data: { restored: true },
      schedule: '* * * * *', repeatEvery: null, priority: 0,
      timezone: 'UTC', nextRun: Date.now() - 60000, executions: 5, maxLimit: null,
    }]);
    await tickScheduler(newMgr);
    expect(newMgr.count('overdue-q')).toBe(1);
    const cron = newMgr.getCron('overdue')!;
    expect(cron.executions).toBe(6);
    expect(cron.nextRun).toBeGreaterThan(Date.now());
    newMgr.shutdown();
  });

  // DST-adjacent: timezone offset differences (EST vs EDT, GMT vs BST)
  test('same cron yields different UTC times for EST vs EDT periods', () => {
    const winter = new Date('2024-01-15T00:00:00Z').getTime();
    const summer = new Date('2024-07-15T00:00:00Z').getTime();
    // 9am EST = 14:00 UTC, 9am EDT = 13:00 UTC
    expect(new Date(getNextCronRun('0 9 * * *', winter, 'America/New_York')).getUTCHours()).toBe(14);
    expect(new Date(getNextCronRun('0 9 * * *', summer, 'America/New_York')).getUTCHours()).toBe(13);
  });

  test('Europe/London switches between GMT and BST correctly', () => {
    const winter = new Date('2024-01-15T00:00:00Z').getTime();
    const summer = new Date('2024-07-15T00:00:00Z').getTime();
    // 10am GMT = 10:00 UTC, 10am BST = 09:00 UTC
    expect(new Date(getNextCronRun('0 10 * * *', winter, 'Europe/London')).getUTCHours()).toBe(10);
    expect(new Date(getNextCronRun('0 10 * * *', summer, 'Europe/London')).getUTCHours()).toBe(9);
  });
});
