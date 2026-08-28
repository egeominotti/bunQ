import { describe, expect, test } from 'bun:test';
import { getNextCronRun } from '../src/infrastructure/scheduler/cronParser';

describe('Bun-native cron DST semantics', () => {
  test('five-field wildcard-hour schedules traverse the repeated fall-back hour', () => {
    const duringFirstRomeHour = Date.parse('2024-10-27T00:30:00.001Z');

    expect(getNextCronRun('0 * * * *', duringFirstRomeHour, 'Europe/Rome')).toBe(
      Date.parse('2024-10-27T01:00:00.000Z')
    );
  });

  test('six-field wildcard-hour schedules preserve seconds in the repeated hour', () => {
    const duringFirstRomeHour = Date.parse('2024-10-27T00:30:00.001Z');

    expect(getNextCronRun('15 0 * * * *', duringFirstRomeHour, 'Europe/Rome')).toBe(
      Date.parse('2024-10-27T01:00:15.000Z')
    );
  });

  test('fixed local times fire only in the first occurrence of a fall-back hour', () => {
    const beforeRomeOverlap = Date.parse('2024-10-26T12:00:00.000Z');
    const afterFirstOccurrence = Date.parse('2024-10-27T00:30:00.001Z');

    expect(getNextCronRun('30 2 * * *', beforeRomeOverlap, 'Europe/Rome')).toBe(
      Date.parse('2024-10-27T00:30:00.000Z')
    );
    expect(getNextCronRun('30 2 * * *', afterFirstOccurrence, 'Europe/Rome')).toBe(
      Date.parse('2024-10-28T01:30:00.000Z')
    );
  });

  test('missing spring-forward local times shift forward by the DST gap', () => {
    const beforeRomeGap = Date.parse('2024-03-30T12:00:00.000Z');

    expect(getNextCronRun('30 2 * * *', beforeRomeGap, 'Europe/Rome')).toBe(
      Date.parse('2024-03-31T01:30:00.000Z')
    );
  });
});
