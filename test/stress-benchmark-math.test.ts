import { describe, expect, test } from 'bun:test';
import { megabytesPerSecond } from '../src/benchmark/stress/common';

describe('stress benchmark units', () => {
  test('converts megabytes per millisecond to megabytes per second once', () => {
    expect(megabytesPerSecond(50, 100)).toBe(500);
  });
});
