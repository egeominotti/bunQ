/**
 * Cron Scheduler Tests
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { CronScheduler } from '../src/infrastructure/scheduler/cronScheduler';
import {
  validateCronExpression,
  getNextCronRun,
  expandCronShortcut,
} from '../src/infrastructure/scheduler/cronParser';

describe('CronParser', () => {
  test('should validate valid cron expressions', () => {
    expect(validateCronExpression('* * * * *')).toBeNull();
    expect(validateCronExpression('0 0 * * *')).toBeNull();
    expect(validateCronExpression('*/5 * * * *')).toBeNull();
    expect(validateCronExpression('0 9-17 * * 1-5')).toBeNull();
  });

  test('should reject invalid cron expressions', () => {
    expect(validateCronExpression('invalid')).not.toBeNull();
    expect(validateCronExpression('* * *')).not.toBeNull();
    expect(validateCronExpression('60 * * * *')).not.toBeNull();
  });

  test('should expand shortcuts', () => {
    expect(expandCronShortcut('@daily')).toBe('0 0 * * *');
    expect(expandCronShortcut('@hourly')).toBe('0 * * * *');
    expect(expandCronShortcut('@weekly')).toBe('0 0 * * 0');
    expect(expandCronShortcut('@monthly')).toBe('0 0 1 * *');
    expect(expandCronShortcut('@yearly')).toBe('0 0 1 1 *');
  });

  test('should calculate next run time', () => {
    const now = Date.now();
    const nextRun = getNextCronRun('* * * * *', now);

    expect(nextRun).toBeGreaterThan(now);
    expect(nextRun).toBeLessThanOrEqual(now + 60000);
  });
});

describe('CronScheduler', () => {
  let scheduler: CronScheduler;
  const pushedJobs: Array<{ queue: string; data: unknown }> = [];

  beforeEach(() => {
    pushedJobs.length = 0;
    scheduler = new CronScheduler({ checkIntervalMs: 100 });
    scheduler.setPushCallback(async (queue, input) => {
      pushedJobs.push({ queue, data: input.data });
    });
  });

  afterEach(() => {
    scheduler.stop();
  });

  test('should add cron job with schedule', () => {
    const cron = scheduler.add({
      name: 'test-cron',
      queue: 'emails',
      data: { type: 'reminder' },
      schedule: '* * * * *',
    });

    expect(cron.name).toBe('test-cron');
    expect(cron.queue).toBe('emails');
    expect(cron.nextRun).toBeGreaterThan(Date.now());
  });

  test('should add cron job with repeatEvery', () => {
    const cron = scheduler.add({
      name: 'interval-job',
      queue: 'tasks',
      data: { type: 'cleanup' },
      repeatEvery: 5000,
    });

    expect(cron.repeatEvery).toBe(5000);
  });

  test('should reject cron without schedule or repeatEvery', () => {
    expect(() => {
      scheduler.add({
        name: 'invalid',
        queue: 'test',
        data: {},
      });
    }).toThrow();
  });

  test('should reject invalid cron expression', () => {
    expect(() => {
      scheduler.add({
        name: 'invalid',
        queue: 'test',
        data: {},
        schedule: 'not-valid',
      });
    }).toThrow();
  });

  test('should remove cron job', () => {
    scheduler.add({
      name: 'to-remove',
      queue: 'test',
      data: {},
      schedule: '* * * * *',
    });

    expect(scheduler.remove('to-remove')).toBe(true);
    expect(scheduler.remove('non-existent')).toBe(false);
  });

  test('should list cron jobs', () => {
    scheduler.add({
      name: 'job1',
      queue: 'test',
      data: {},
      schedule: '* * * * *',
    });
    scheduler.add({
      name: 'job2',
      queue: 'test',
      data: {},
      repeatEvery: 1000,
    });

    const list = scheduler.list();
    expect(list.length).toBe(2);
  });

  test('should get cron job by name', () => {
    scheduler.add({
      name: 'my-job',
      queue: 'test',
      data: { key: 'value' },
      schedule: '* * * * *',
    });

    const job = scheduler.get('my-job');
    expect(job?.name).toBe('my-job');
    expect(job?.data).toEqual({ key: 'value' });
  });

  test('should return stats', () => {
    scheduler.add({
      name: 'job1',
      queue: 'test',
      data: {},
      schedule: '* * * * *',
    });

    const stats = scheduler.getStats();
    expect(stats.total).toBe(1);
    expect(stats.pending).toBe(1);
  });

  test('should load cron jobs', () => {
    scheduler.load([
      {
        name: 'loaded1',
        queue: 'test',
        data: {},
        schedule: '* * * * *',
        repeatEvery: null,
        priority: 0,
        nextRun: Date.now() + 60000,
        executions: 0,
        maxLimit: null,
      },
    ]);

    expect(scheduler.list().length).toBe(1);
  });
});
