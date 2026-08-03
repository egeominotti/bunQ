/**
 * Serialization Utilities Tests
 * Tests for BigInt-safe JSON serialization and Job serialization
 */

import { describe, test, expect } from 'bun:test';
import {
  serializeJob,
  serializeJobs,
  bigIntReplacer,
  jsonStringify,
} from '../src/shared/serialization';
import { createJob, jobId, generateJobId } from '../src/domain/types/job';
import type { Job } from '../src/domain/types/job';

/** Helper: create a minimal Job with defaults via createJob */
function makeJob(overrides: Partial<Parameters<typeof createJob>[2]> = {}): Job {
  const id = generateJobId();
  return createJob(id, 'test-queue', {
    data: { message: 'hello' },
    ...overrides,
  });
}

// ---------------------------------------------------------------------------
// serializeJob
// ---------------------------------------------------------------------------
describe('serializeJob', () => {
  test('should serialize a job with all default fields', () => {
    const job = makeJob();
    const result = serializeJob(job);

    expect(result.id).toBe(job.id.toString());
    expect(result.queue).toBe('test-queue');
    expect(result.data).toEqual({ message: 'hello' });
    expect(result.priority).toBe(0);
    expect(typeof result.createdAt).toBe('number');
    expect(typeof result.runAt).toBe('number');
    expect(result.startedAt).toBeNull();
    expect(result.completedAt).toBeNull();
    expect(result.attempts).toBe(0);
    expect(result.maxAttempts).toBe(3);
    expect(result.backoff).toBe(1000);
    expect(result.ttl).toBeNull();
    expect(result.timeout).toBeNull();
    expect(result.uniqueKey).toBeNull();
    expect(result.customId).toBeNull();
    expect(result.dependsOn).toEqual([]);
    expect(result.parentId).toBeNull();
    expect(result.childrenIds).toEqual([]);
    expect(result.childrenCompleted).toBe(0);
    expect(result.tags).toEqual([]);
    expect(result.groupId).toBeNull();
    expect(result.progress).toBe(0);
    expect(result.progressMessage).toBeNull();
    expect(result.removeOnComplete).toBe(false);
    expect(result.removeOnFail).toBe(false);
    expect(typeof result.lastHeartbeat).toBe('number');
    expect(result.stallTimeout).toBeNull();
    expect(result.stallCount).toBe(0);
    expect(result.lifo).toBe(false);
  });

  test('should serialize a job with all optional fields populated', () => {
    const parentJobId = generateJobId();
    const dep1 = generateJobId();
    const dep2 = generateJobId();

    const job = makeJob({
      priority: 10,
      delay: 5000,
      maxAttempts: 5,
      backoff: 2000,
      ttl: 60000,
      timeout: 30000,
      uniqueKey: 'unique-123',
      customId: 'custom-abc',
      dependsOn: [dep1, dep2],
      parentId: parentJobId,
      tags: ['email', 'priority'],
      groupId: 'group-1',
      lifo: true,
      removeOnComplete: true,
      removeOnFail: true,
      stallTimeout: 15000,
    });

    // Manually add childrenIds to the job for test coverage
    const child1 = generateJobId();
    const child2 = generateJobId();
    job.childrenIds = [child1, child2];
    job.childrenCompleted = 1;
    job.progress = 75;
    job.progressMessage = 'Almost done';

    const result = serializeJob(job);

    expect(result.id).toBe(job.id.toString());
    expect(result.priority).toBe(10);
    expect(result.maxAttempts).toBe(5);
    expect(result.backoff).toBe(2000);
    expect(result.ttl).toBe(60000);
    expect(result.timeout).toBe(30000);
    expect(result.uniqueKey).toBe('unique-123');
    expect(result.customId).toBe('custom-abc');
    expect(result.dependsOn).toEqual([dep1.toString(), dep2.toString()]);
    expect(result.parentId).toBe(parentJobId.toString());
    expect(result.childrenIds).toEqual([child1.toString(), child2.toString()]);
    expect(result.childrenCompleted).toBe(1);
    expect(result.tags).toEqual(['email', 'priority']);
    expect(result.groupId).toBe('group-1');
    expect(result.progress).toBe(75);
    expect(result.progressMessage).toBe('Almost done');
    expect(result.removeOnComplete).toBe(true);
    expect(result.removeOnFail).toBe(true);
    expect(result.stallTimeout).toBe(15000);
    expect(result.lifo).toBe(true);
  });

  test('should convert id to string via .toString()', () => {
    const id = jobId('my-custom-id-123');
    const job = createJob(id, 'q', { data: null });
    const result = serializeJob(job);

    expect(result.id).toBe('my-custom-id-123');
    expect(typeof result.id).toBe('string');
  });

  test('should convert dependsOn array items to strings', () => {
    const dep1 = jobId('dep-aaa');
    const dep2 = jobId('dep-bbb');
    const job = makeJob({ dependsOn: [dep1, dep2] });
    const result = serializeJob(job);

    expect(result.dependsOn).toEqual(['dep-aaa', 'dep-bbb']);
    expect(Array.isArray(result.dependsOn)).toBe(true);
  });

  test('should handle empty dependsOn array', () => {
    const job = makeJob({ dependsOn: [] });
    const result = serializeJob(job);
    expect(result.dependsOn).toEqual([]);
  });

  test('should convert childrenIds array items to strings', () => {
    const job = makeJob();
    const child1 = jobId('child-1');
    const child2 = jobId('child-2');
    const child3 = jobId('child-3');
    job.childrenIds = [child1, child2, child3];

    const result = serializeJob(job);
    expect(result.childrenIds).toEqual(['child-1', 'child-2', 'child-3']);
  });

  test('should handle empty childrenIds array', () => {
    const job = makeJob();
    job.childrenIds = [];
    const result = serializeJob(job);
    expect(result.childrenIds).toEqual([]);
  });

  test('should convert parentId to string when present', () => {
    const pid = jobId('parent-xyz');
    const job = makeJob({ parentId: pid });
    const result = serializeJob(job);

    expect(result.parentId).toBe('parent-xyz');
  });

  test('should set parentId to null when not present', () => {
    const job = makeJob();
    const result = serializeJob(job);
    expect(result.parentId).toBeNull();
  });

  test('should handle empty tags array', () => {
    const job = makeJob({ tags: [] });
    const result = serializeJob(job);
    expect(result.tags).toEqual([]);
  });

  test('should preserve tags', () => {
    const job = makeJob({ tags: ['urgent', 'billing', 'retry'] });
    const result = serializeJob(job);
    expect(result.tags).toEqual(['urgent', 'billing', 'retry']);
  });

  test('should serialize date fields as numbers', () => {
    const now = 1700000000000;
    const job = createJob(generateJobId(), 'q', { data: null, delay: 1000 }, now);

    // Simulate startedAt and completedAt
    (job as any).startedAt = now + 2000;
    (job as any).completedAt = now + 5000;

    const result = serializeJob(job);

    expect(result.createdAt).toBe(now);
    expect(result.runAt).toBe(now + 1000);
    expect(result.startedAt).toBe(now + 2000);
    expect(result.completedAt).toBe(now + 5000);
  });

  test('should handle null startedAt and completedAt', () => {
    const job = makeJob();
    const result = serializeJob(job);
    expect(result.startedAt).toBeNull();
    expect(result.completedAt).toBeNull();
  });

  test('should preserve complex data payloads', () => {
    const complexData = {
      nested: { deep: { value: 42 } },
      array: [1, 'two', { three: true }],
      nullValue: null,
    };
    const job = makeJob({ data: complexData });
    const result = serializeJob(job);
    expect(result.data).toEqual(complexData);
  });

  test('should preserve null data', () => {
    const job = makeJob({ data: null });
    const result = serializeJob(job);
    expect(result.data).toBeNull();
  });

  test('should return a plain object (not a Job instance)', () => {
    const job = makeJob();
    const result = serializeJob(job);

    // Result should be a plain object with expected keys
    expect(typeof result).toBe('object');
    expect(result).not.toBeNull();
    expect(Object.keys(result)).toContain('id');
    expect(Object.keys(result)).toContain('queue');
    expect(Object.keys(result)).toContain('data');
  });

  test('should include all expected fields and no extras from the function', () => {
    const job = makeJob();
    const result = serializeJob(job);

    const expectedKeys = [
      'id',
      'queue',
      'name',
      'data',
      'priority',
      'createdAt',
      'runAt',
      'startedAt',
      'completedAt',
      'attempts',
      'maxAttempts',
      'backoff',
      'ttl',
      'timeout',
      'uniqueKey',
      'customId',
      'dependsOn',
      'parentId',
      'childrenIds',
      'childrenCompleted',
      'tags',
      'groupId',
      'progress',
      'progressMessage',
      'removeOnComplete',
      'removeOnFail',
      'lastHeartbeat',
      'stallTimeout',
      'stallCount',
      'lifo',
      'timeline',
    ];

    const resultKeys = Object.keys(result).sort();
    expect(resultKeys).toEqual(expectedKeys.sort());
  });

  test('should handle job with lifo set to true', () => {
    const job = makeJob({ lifo: true });
    const result = serializeJob(job);
    expect(result.lifo).toBe(true);
  });

  test('should serialize lastHeartbeat and stallCount', () => {
    const job = makeJob({ stallTimeout: 5000 });
    job.stallCount = 2;
    job.lastHeartbeat = 1700000005000;

    const result = serializeJob(job);
    expect(result.stallCount).toBe(2);
    expect(result.lastHeartbeat).toBe(1700000005000);
    expect(result.stallTimeout).toBe(5000);
  });
});

// ---------------------------------------------------------------------------
// serializeJobs
// ---------------------------------------------------------------------------
describe('serializeJobs', () => {
  test('should serialize an array of jobs', () => {
    const jobs = [makeJob(), makeJob(), makeJob()];
    const results = serializeJobs(jobs);

    expect(results).toHaveLength(3);
    for (let i = 0; i < 3; i++) {
      expect(results[i].id).toBe(jobs[i].id.toString());
      expect(results[i].queue).toBe('test-queue');
    }
  });

  test('should return empty array for empty input', () => {
    const results = serializeJobs([]);
    expect(results).toEqual([]);
    expect(Array.isArray(results)).toBe(true);
  });

  test('should serialize a single-element array', () => {
    const jobs = [makeJob()];
    const results = serializeJobs(jobs);
    expect(results).toHaveLength(1);
    expect(results[0].id).toBe(jobs[0].id.toString());
  });

  test('should maintain order of jobs', () => {
    const job1 = createJob(jobId('id-first'), 'q1', { data: 'a' });
    const job2 = createJob(jobId('id-second'), 'q2', { data: 'b' });
    const job3 = createJob(jobId('id-third'), 'q3', { data: 'c' });

    const results = serializeJobs([job1, job2, job3]);

    expect(results[0].id).toBe('id-first');
    expect(results[1].id).toBe('id-second');
    expect(results[2].id).toBe('id-third');
  });

  test('should serialize jobs with different configurations', () => {
    const job1 = makeJob({ priority: 1, tags: ['low'] });
    const job2 = makeJob({ priority: 100, tags: ['high'], lifo: true });

    const results = serializeJobs([job1, job2]);

    expect(results[0].priority).toBe(1);
    expect(results[0].tags).toEqual(['low']);
    expect(results[1].priority).toBe(100);
    expect(results[1].tags).toEqual(['high']);
    expect(results[1].lifo).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// bigIntReplacer
// ---------------------------------------------------------------------------
describe('bigIntReplacer', () => {
  test('should convert BigInt value to string', () => {
    const result = bigIntReplacer('key', BigInt(123));
    expect(result).toBe('123');
    expect(typeof result).toBe('string');
  });

  test('should convert BigInt(0) to string "0"', () => {
    expect(bigIntReplacer('key', BigInt(0))).toBe('0');
  });

  test('should convert negative BigInt to string', () => {
    expect(bigIntReplacer('key', BigInt(-42))).toBe('-42');
  });

  test('should convert very large BigInt to string', () => {
    const big = BigInt('9999999999999999999999999999');
    expect(bigIntReplacer('key', big)).toBe('9999999999999999999999999999');
  });

  test('should pass through string values unchanged', () => {
    expect(bigIntReplacer('key', 'hello')).toBe('hello');
  });

  test('should pass through number values unchanged', () => {
    expect(bigIntReplacer('key', 42)).toBe(42);
  });

  test('should pass through boolean values unchanged', () => {
    expect(bigIntReplacer('key', true)).toBe(true);
    expect(bigIntReplacer('key', false)).toBe(false);
  });

  test('should pass through null unchanged', () => {
    expect(bigIntReplacer('key', null)).toBeNull();
  });

  test('should pass through undefined unchanged', () => {
    expect(bigIntReplacer('key', undefined)).toBeUndefined();
  });

  test('should pass through object values unchanged', () => {
    const obj = { a: 1 };
    expect(bigIntReplacer('key', obj)).toBe(obj);
  });

  test('should pass through array values unchanged', () => {
    const arr = [1, 2, 3];
    expect(bigIntReplacer('key', arr)).toBe(arr);
  });

  test('should work regardless of key value', () => {
    expect(bigIntReplacer('', BigInt(1))).toBe('1');
    expect(bigIntReplacer('nested.key', BigInt(2))).toBe('2');
    expect(bigIntReplacer('0', BigInt(3))).toBe('3');
  });
});

// ---------------------------------------------------------------------------
// jsonStringify
// ---------------------------------------------------------------------------
describe('jsonStringify', () => {
  test('should stringify object with BigInt properties', () => {
    const obj = { id: BigInt(12345), name: 'test' };
    const result = jsonStringify(obj);
    const parsed = JSON.parse(result);

    expect(parsed.id).toBe('12345');
    expect(parsed.name).toBe('test');
  });

  test('should stringify nested BigInt values', () => {
    const obj = {
      outer: {
        inner: {
          value: BigInt(999),
        },
      },
    };
    const result = jsonStringify(obj);
    const parsed = JSON.parse(result);
    expect(parsed.outer.inner.value).toBe('999');
  });

  test('should stringify arrays with BigInt elements', () => {
    const arr = [BigInt(1), BigInt(2), BigInt(3)];
    const result = jsonStringify(arr);
    const parsed = JSON.parse(result);
    expect(parsed).toEqual(['1', '2', '3']);
  });

  test('should stringify mixed arrays with BigInt and other types', () => {
    const arr = [BigInt(10), 'hello', 42, true, null];
    const result = jsonStringify(arr);
    const parsed = JSON.parse(result);
    expect(parsed).toEqual(['10', 'hello', 42, true, null]);
  });

  test('should handle standard JSON types without modification', () => {
    const obj = {
      str: 'hello',
      num: 42,
      bool: true,
      nil: null,
      arr: [1, 2, 3],
      nested: { a: 'b' },
    };
    const result = jsonStringify(obj);
    expect(result).toBe(JSON.stringify(obj));
  });

  test('should return valid JSON string', () => {
    const obj = { id: BigInt(1), data: { key: 'value' } };
    const result = jsonStringify(obj);

    // Should not throw when parsing
    expect(() => JSON.parse(result)).not.toThrow();
  });

  test('should stringify a simple string', () => {
    expect(jsonStringify('hello')).toBe('"hello"');
  });

  test('should stringify a number', () => {
    expect(jsonStringify(42)).toBe('42');
  });

  test('should stringify null', () => {
    expect(jsonStringify(null)).toBe('null');
  });

  test('should stringify boolean', () => {
    expect(jsonStringify(true)).toBe('true');
    expect(jsonStringify(false)).toBe('false');
  });

  test('should stringify empty object', () => {
    expect(jsonStringify({})).toBe('{}');
  });

  test('should stringify empty array', () => {
    expect(jsonStringify([])).toBe('[]');
  });

  test('should handle object with multiple BigInt fields', () => {
    const obj = {
      a: BigInt(100),
      b: BigInt(200),
      c: BigInt(300),
    };
    const result = jsonStringify(obj);
    const parsed = JSON.parse(result);
    expect(parsed).toEqual({ a: '100', b: '200', c: '300' });
  });

  test('should handle deeply nested structure with BigInt', () => {
    const obj = {
      level1: {
        level2: {
          level3: {
            level4: {
              id: BigInt(42),
            },
          },
        },
      },
    };
    const result = jsonStringify(obj);
    const parsed = JSON.parse(result);
    expect(parsed.level1.level2.level3.level4.id).toBe('42');
  });

  test('should handle BigInt in array of objects', () => {
    const arr = [
      { id: BigInt(1), name: 'first' },
      { id: BigInt(2), name: 'second' },
    ];
    const result = jsonStringify(arr);
    const parsed = JSON.parse(result);
    expect(parsed[0].id).toBe('1');
    expect(parsed[1].id).toBe('2');
    expect(parsed[0].name).toBe('first');
    expect(parsed[1].name).toBe('second');
  });

  test('should not throw on BigInt (unlike plain JSON.stringify)', () => {
    const obj = { value: BigInt(123) };

    // Plain JSON.stringify throws on BigInt
    expect(() => JSON.stringify(obj)).toThrow();

    // jsonStringify should handle it gracefully
    expect(() => jsonStringify(obj)).not.toThrow();
    expect(jsonStringify(obj)).toBe('{"value":"123"}');
  });
});
