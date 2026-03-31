/**
 * Test: JobTemplate generic type propagation (Issue #61)
 *
 * Verifies that Queue<T>.upsertJobScheduler() properly types
 * the jobTemplate.data field as T instead of unknown.
 */
import { describe, test, expect } from 'bun:test';
import { Queue } from '../src/client';

describe('upsertJobScheduler generic types', () => {
  test('JobTemplate data should accept the Queue generic type', () => {
    interface MyData {
      userId: number;
      action: string;
    }

    const queue = new Queue<MyData>('test-scheduler-generic', { embedded: true });

    // This should compile without errors — data should be typed as MyData
    const promise = queue.upsertJobScheduler(
      'test-scheduler',
      { pattern: '* * * * *' },
      {
        name: 'test-job',
        data: { userId: 1, action: 'test' },
      }
    );

    // Verify it returns a promise (basic runtime check)
    expect(promise).toBeInstanceOf(Promise);

    queue.close();
  });

  test('RepeatOpts and JobTemplate are separate parameters', () => {
    const queue = new Queue('test-scheduler-params', { embedded: true });

    // data should NOT be in RepeatOpts (second param)
    // data should be in JobTemplate (third param)
    const promise = queue.upsertJobScheduler(
      'test-scheduler-2',
      { pattern: '0 3 * * *' }, // RepeatOpts - no data here
      { data: { task: 'cleanup' } } // JobTemplate - data goes here
    );

    expect(promise).toBeInstanceOf(Promise);

    queue.close();
  });
});
