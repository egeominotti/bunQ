/**
 * Cron Timezone Support Test
 */

import { describe, test, expect } from 'bun:test';
import { getNextCronRun, validateCronExpression } from '../src/infrastructure/scheduler/cronParser';
import { CronScheduler } from '../src/infrastructure/scheduler/cronScheduler';

describe('Cron Timezone Support', () => {
  describe('cronParser', () => {
    test('validateCronExpression accepts valid timezone', () => {
      const error = validateCronExpression('0 9 * * *', 'Europe/Rome');
      expect(error).toBeNull();
    });

    test('validateCronExpression rejects an invalid timezone', () => {
      const error = validateCronExpression('0 9 * * *', 'Invalid/Timezone');
      expect(error).toContain('unknown time zone');
    });

    test('getNextCronRun uses timezone for calculation', () => {
      // Test "0 9 * * *" (9am daily) in different timezones
      // Using a fixed reference time: 2024-01-15 00:00:00 UTC
      const referenceTime = new Date('2024-01-15T00:00:00Z').getTime();

      // 9am in UTC
      const nextUtc = getNextCronRun('0 9 * * *', referenceTime);
      const nextUtcDate = new Date(nextUtc);
      expect(nextUtcDate.getUTCHours()).toBe(9);

      // 9am in Europe/Rome (UTC+1 in January)
      const nextRome = getNextCronRun('0 9 * * *', referenceTime, 'Europe/Rome');
      const nextRomeDate = new Date(nextRome);
      // 9am Rome = 8am UTC
      expect(nextRomeDate.getUTCHours()).toBe(8);

      // 9am in America/New_York (UTC-5 in January)
      const nextNy = getNextCronRun('0 9 * * *', referenceTime, 'America/New_York');
      const nextNyDate = new Date(nextNy);
      // 9am NY = 14:00 UTC
      expect(nextNyDate.getUTCHours()).toBe(14);
    });

    test('getNextCronRun without timezone uses local/UTC', () => {
      const now = Date.now();
      const next = getNextCronRun('*/5 * * * *', now); // Every 5 minutes
      expect(next).toBeGreaterThan(now);
    });
  });

  describe('CronScheduler', () => {
    test('add() accepts timezone option', () => {
      const scheduler = new CronScheduler();

      const cron = scheduler.add({
        name: 'rome-daily',
        queue: 'emails',
        data: { type: 'newsletter' },
        schedule: '0 9 * * *',
        timezone: 'Europe/Rome',
      });

      expect(cron.timezone).toBe('Europe/Rome');
      expect(cron.nextRun).toBeGreaterThan(Date.now());
    });

    test('add() works without timezone (null)', () => {
      const scheduler = new CronScheduler();

      const cron = scheduler.add({
        name: 'utc-daily',
        queue: 'emails',
        data: { type: 'report' },
        schedule: '0 9 * * *',
      });

      expect(cron.timezone).toBeNull();
    });

    test('add() handles various timezones', () => {
      const scheduler = new CronScheduler();

      // Valid IANA timezones should work
      const cron = scheduler.add({
        name: 'la-cron',
        queue: 'emails',
        data: {},
        schedule: '0 9 * * *',
        timezone: 'America/Los_Angeles',
      });

      expect(cron.timezone).toBe('America/Los_Angeles');
      expect(cron.nextRun).toBeGreaterThan(Date.now());
    });

    test('list() returns crons with timezone', () => {
      const scheduler = new CronScheduler();

      scheduler.add({
        name: 'tokyo-cron',
        queue: 'reports',
        data: {},
        schedule: '0 18 * * *',
        timezone: 'Asia/Tokyo',
      });

      const crons = scheduler.list();
      expect(crons.length).toBe(1);
      expect(crons[0].timezone).toBe('Asia/Tokyo');
    });
  });
});
