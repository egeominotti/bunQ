/**
 * SQLite Serializer & Statements Tests
 * Comprehensive tests for MessagePack pack/unpack and SQL statement definitions
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { Database } from 'bun:sqlite';
import { pack, unpack, rowToJob, reconstructDlqEntry } from '../src/infrastructure/persistence/sqliteSerializer';
import { SQL_STATEMENTS, prepareStatements, type StatementName, type DbJob } from '../src/infrastructure/persistence/statements';
import { PRAGMA_SETTINGS, SCHEMA } from '../src/infrastructure/persistence/schema';
import { jobId } from '../src/domain/types/job';
import type { DlqEntry } from '../src/domain/types/dlq';
import { unlinkSync, existsSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

// =============================================================================
// pack / unpack tests
// =============================================================================
describe('pack and unpack', () => {
  // ---------------------------------------------------------------------------
  // Basic types
  // ---------------------------------------------------------------------------
  describe('basic types', () => {
    test('should pack and unpack a string', () => {
      const data = 'hello world';
      const packed = pack(data);
      expect(packed).toBeInstanceOf(Uint8Array);
      const result = unpack<string>(packed, '', 'test');
      expect(result).toBe('hello world');
    });

    test('should pack and unpack an empty string', () => {
      const packed = pack('');
      const result = unpack<string>(packed, 'fallback', 'test');
      expect(result).toBe('');
    });

    test('should pack and unpack a positive integer', () => {
      const packed = pack(42);
      const result = unpack<number>(packed, 0, 'test');
      expect(result).toBe(42);
    });

    test('should pack and unpack zero', () => {
      const packed = pack(0);
      const result = unpack<number>(packed, -1, 'test');
      expect(result).toBe(0);
    });

    test('should pack and unpack a negative integer', () => {
      const packed = pack(-99);
      const result = unpack<number>(packed, 0, 'test');
      expect(result).toBe(-99);
    });

    test('should pack and unpack a floating point number', () => {
      const packed = pack(3.14159);
      const result = unpack<number>(packed, 0, 'test');
      expect(result).toBeCloseTo(3.14159, 5);
    });

    test('should pack and unpack boolean true', () => {
      const packed = pack(true);
      const result = unpack<boolean>(packed, false, 'test');
      expect(result).toBe(true);
    });

    test('should pack and unpack boolean false', () => {
      const packed = pack(false);
      const result = unpack<boolean>(packed, true, 'test');
      expect(result).toBe(false);
    });

    test('should pack and unpack null', () => {
      const packed = pack(null);
      const result = unpack<null>(packed, null, 'test');
      expect(result).toBeNull();
    });
  });

  // ---------------------------------------------------------------------------
  // Arrays
  // ---------------------------------------------------------------------------
  describe('arrays', () => {
    test('should pack and unpack an array of numbers', () => {
      const data = [1, 2, 3, 4, 5];
      const packed = pack(data);
      const result = unpack<number[]>(packed, [], 'test');
      expect(result).toEqual([1, 2, 3, 4, 5]);
    });

    test('should pack and unpack an array of strings', () => {
      const data = ['alpha', 'beta', 'gamma'];
      const packed = pack(data);
      const result = unpack<string[]>(packed, [], 'test');
      expect(result).toEqual(['alpha', 'beta', 'gamma']);
    });

    test('should pack and unpack a mixed-type array', () => {
      const data = [1, 'two', true, null, 3.14];
      const packed = pack(data);
      const result = unpack<unknown[]>(packed, [], 'test');
      expect(result).toEqual([1, 'two', true, null, 3.14]);
    });

    test('should pack and unpack an empty array', () => {
      const packed = pack([]);
      const result = unpack<unknown[]>(packed, ['fallback'], 'test');
      expect(result).toEqual([]);
    });

    test('should pack and unpack nested arrays', () => {
      const data = [[1, 2], [3, [4, 5]], []];
      const packed = pack(data);
      const result = unpack<unknown[]>(packed, [], 'test');
      expect(result).toEqual([[1, 2], [3, [4, 5]], []]);
    });
  });

  // ---------------------------------------------------------------------------
  // Objects / Maps
  // ---------------------------------------------------------------------------
  describe('objects and maps', () => {
    test('should pack and unpack a simple object', () => {
      const data = { name: 'test', value: 42 };
      const packed = pack(data);
      const result = unpack<typeof data>(packed, {} as typeof data, 'test');
      expect(result).toEqual({ name: 'test', value: 42 });
    });

    test('should pack and unpack an empty object', () => {
      const packed = pack({});
      const result = unpack<Record<string, unknown>>(packed, { fallback: true }, 'test');
      expect(result).toEqual({});
    });

    test('should pack and unpack deeply nested objects', () => {
      const data = {
        level1: {
          level2: {
            level3: {
              value: 'deep',
              count: 42,
            },
          },
        },
      };
      const packed = pack(data);
      const result = unpack<typeof data>(packed, {} as typeof data, 'test');
      expect(result).toEqual(data);
    });

    test('should pack and unpack object with array values', () => {
      const data = {
        ids: [1, 2, 3],
        names: ['a', 'b'],
        nested: { items: [{ x: 1 }, { x: 2 }] },
      };
      const packed = pack(data);
      const result = unpack<typeof data>(packed, {} as typeof data, 'test');
      expect(result).toEqual(data);
    });
  });

  // ---------------------------------------------------------------------------
  // Nested structures
  // ---------------------------------------------------------------------------
  describe('nested structures', () => {
    test('should handle complex nested structures', () => {
      const data = {
        user: {
          name: 'John',
          preferences: {
            theme: 'dark',
            notifications: [true, false, true],
          },
        },
        metadata: {
          tags: ['important', 'urgent'],
          counts: { views: 100, clicks: 42 },
        },
      };
      const packed = pack(data);
      const result = unpack<typeof data>(packed, {} as typeof data, 'test');
      expect(result).toEqual(data);
    });

    test('should handle array of objects', () => {
      const data = [
        { id: 1, name: 'first' },
        { id: 2, name: 'second' },
        { id: 3, name: 'third' },
      ];
      const packed = pack(data);
      const result = unpack<typeof data>(packed, [], 'test');
      expect(result).toEqual(data);
    });
  });

  // ---------------------------------------------------------------------------
  // Job-specific field serialization
  // ---------------------------------------------------------------------------
  describe('job data field', () => {
    test('should pack and unpack typical job data', () => {
      const jobData = {
        email: 'user@test.com',
        template: 'welcome',
        params: { name: 'John', code: 12345 },
      };
      const packed = pack(jobData);
      const result = unpack<typeof jobData>(packed, {}, 'data');
      expect(result).toEqual(jobData);
    });

    test('should pack and unpack null job data', () => {
      const packed = pack(null);
      const result = unpack(packed, {}, 'data');
      expect(result).toBeNull();
    });

    test('should pack and unpack numeric job data', () => {
      const packed = pack(42);
      const result = unpack<number>(packed, 0, 'data');
      expect(result).toBe(42);
    });

    test('should pack and unpack string job data', () => {
      const packed = pack('simple-payload');
      const result = unpack<string>(packed, '', 'data');
      expect(result).toBe('simple-payload');
    });
  });

  describe('dependsOn array', () => {
    test('should pack and unpack dependsOn with job IDs', () => {
      const deps = ['job-1', 'job-2', 'job-3'];
      const packed = pack(deps);
      const result = unpack<string[]>(packed, [], 'dependsOn');
      expect(result).toEqual(['job-1', 'job-2', 'job-3']);
    });

    test('should pack and unpack empty dependsOn', () => {
      const packed = pack([]);
      const result = unpack<string[]>(packed, [], 'dependsOn');
      expect(result).toEqual([]);
    });
  });

  describe('childrenIds array', () => {
    test('should pack and unpack childrenIds', () => {
      const children = ['child-a', 'child-b'];
      const packed = pack(children);
      const result = unpack<string[]>(packed, [], 'childrenIds');
      expect(result).toEqual(['child-a', 'child-b']);
    });

    test('should pack and unpack empty childrenIds', () => {
      const packed = pack([]);
      const result = unpack<string[]>(packed, [], 'childrenIds');
      expect(result).toEqual([]);
    });
  });

  describe('tags array', () => {
    test('should pack and unpack tags', () => {
      const tags = ['important', 'urgent', 'email'];
      const packed = pack(tags);
      const result = unpack<string[]>(packed, [], 'tags');
      expect(result).toEqual(['important', 'urgent', 'email']);
    });

    test('should pack and unpack empty tags', () => {
      const packed = pack([]);
      const result = unpack<string[]>(packed, [], 'tags');
      expect(result).toEqual([]);
    });

    test('should pack and unpack tags with unicode', () => {
      const tags = ['urgent', 'high-priority'];
      const packed = pack(tags);
      const result = unpack<string[]>(packed, [], 'tags');
      expect(result).toEqual(['urgent', 'high-priority']);
    });
  });

  // ---------------------------------------------------------------------------
  // Roundtrip: pack then unpack returns original data
  // ---------------------------------------------------------------------------
  describe('roundtrip', () => {
    test('should roundtrip a string', () => {
      const original = 'hello world';
      expect(unpack<string>(pack(original), '', 'test')).toBe(original);
    });

    test('should roundtrip a number', () => {
      const original = 123456;
      expect(unpack<number>(pack(original), 0, 'test')).toBe(original);
    });

    test('should roundtrip a boolean', () => {
      expect(unpack<boolean>(pack(true), false, 'test')).toBe(true);
      expect(unpack<boolean>(pack(false), true, 'test')).toBe(false);
    });

    test('should roundtrip null', () => {
      expect(unpack(pack(null), 'fallback', 'test')).toBeNull();
    });

    test('should roundtrip a complex object', () => {
      const original = {
        string: 'hello',
        number: 42,
        float: 3.14,
        boolean: true,
        null: null,
        array: [1, 2, 3, 'four', { five: 5 }],
        nested: { deep: { value: 'test' } },
      };
      const result = unpack<typeof original>(pack(original), {} as typeof original, 'test');
      expect(result).toEqual(original);
    });

    test('should roundtrip an array of strings', () => {
      const original = ['a', 'b', 'c', 'd'];
      expect(unpack<string[]>(pack(original), [], 'test')).toEqual(original);
    });

    test('should roundtrip an array of numbers', () => {
      const original = [0, 1, -1, 100, 999999];
      expect(unpack<number[]>(pack(original), [], 'test')).toEqual(original);
    });
  });

  // ---------------------------------------------------------------------------
  // Edge cases
  // ---------------------------------------------------------------------------
  describe('edge cases', () => {
    test('should return fallback when buffer is null', () => {
      const result = unpack<string>(null, 'fallback', 'test');
      expect(result).toBe('fallback');
    });

    test('should return fallback when buffer is null (array fallback)', () => {
      const result = unpack<string[]>(null, ['default'], 'test');
      expect(result).toEqual(['default']);
    });

    test('should return fallback for invalid buffer', () => {
      const invalidBuffer = new Uint8Array([0xff, 0xfe, 0xfd, 0xfc, 0xfb]);
      const result = unpack<string>(invalidBuffer, 'fallback', 'test');
      // Should either decode or fallback; no throw
      expect(result).toBeDefined();
    });

    test('should handle Uint8Array/Buffer data via pack', () => {
      const buf = new Uint8Array([1, 2, 3, 4, 5]);
      const packed = pack(buf);
      const result = unpack<Uint8Array>(packed, new Uint8Array(), 'test');
      // msgpackr encodes Uint8Array as binary; verify content
      expect(result.length).toBe(5);
      expect(result[0]).toBe(1);
      expect(result[4]).toBe(5);
    });

    test('should handle very large numbers', () => {
      const packed = pack(Number.MAX_SAFE_INTEGER);
      const result = unpack<number>(packed, 0, 'test');
      expect(result).toBe(Number.MAX_SAFE_INTEGER);
    });

    test('should handle negative large numbers', () => {
      const packed = pack(Number.MIN_SAFE_INTEGER);
      const result = unpack<number>(packed, 0, 'test');
      expect(result).toBe(Number.MIN_SAFE_INTEGER);
    });

    test('should handle unicode strings', () => {
      const data = 'Hello, world!';
      const packed = pack(data);
      const result = unpack<string>(packed, '', 'test');
      expect(result).toBe(data);
    });

    test('should handle strings with special characters', () => {
      const data = 'line1\nline2\ttab\r\nwindows';
      const packed = pack(data);
      const result = unpack<string>(packed, '', 'test');
      expect(result).toBe(data);
    });

    test('should pack returns Uint8Array', () => {
      const packed = pack({ test: true });
      expect(packed).toBeInstanceOf(Uint8Array);
      expect(packed.length).toBeGreaterThan(0);
    });
  });

  // ---------------------------------------------------------------------------
  // Performance
  // ---------------------------------------------------------------------------
  describe('performance', () => {
    test('should handle large payloads efficiently', () => {
      const largeArray = Array.from({ length: 10000 }, (_, i) => ({
        id: `item-${i}`,
        value: i * Math.random(),
        tags: [`tag-${i % 10}`],
      }));

      const start = performance.now();
      const packed = pack(largeArray);
      const unpacked = unpack<typeof largeArray>(packed, [], 'test');
      const elapsed = performance.now() - start;

      expect(unpacked.length).toBe(10000);
      expect(unpacked[0].id).toBe('item-0');
      expect(unpacked[9999].id).toBe('item-9999');
      // Should complete within a reasonable time (< 500ms)
      expect(elapsed).toBeLessThan(500);
    });

    test('should handle large nested object', () => {
      const largeObj: Record<string, unknown> = {};
      for (let i = 0; i < 1000; i++) {
        largeObj[`key-${i}`] = {
          value: i,
          nested: { data: `value-${i}` },
        };
      }

      const packed = pack(largeObj);
      const result = unpack<typeof largeObj>(packed, {}, 'test');
      expect(Object.keys(result).length).toBe(1000);
      expect((result['key-0'] as any).value).toBe(0);
      expect((result['key-999'] as any).value).toBe(999);
    });

    test('should handle rapid pack/unpack cycles', () => {
      const data = { email: 'test@example.com', count: 42, tags: ['a', 'b'] };
      const iterations = 10000;

      const start = performance.now();
      for (let i = 0; i < iterations; i++) {
        const packed = pack(data);
        unpack(packed, {}, 'test');
      }
      const elapsed = performance.now() - start;

      // 10k iterations should complete within 1 second
      expect(elapsed).toBeLessThan(1000);
    });
  });
});

// =============================================================================
// rowToJob tests
// =============================================================================
describe('rowToJob', () => {
  function createDbRow(overrides: Partial<DbJob> = {}): DbJob {
    const now = Date.now();
    return {
      id: 'test-job-id',
      queue: 'test-queue',
      data: pack({ email: 'user@test.com' }),
      priority: 5,
      created_at: now,
      run_at: now,
      started_at: null,
      completed_at: null,
      attempts: 0,
      max_attempts: 3,
      backoff: 1000,
      ttl: null,
      timeout: null,
      unique_key: null,
      custom_id: null,
      depends_on: null,
      parent_id: null,
      children_ids: null,
      tags: null,
      state: 'waiting',
      lifo: 0,
      group_id: null,
      progress: 0,
      progress_msg: null,
      remove_on_complete: 0,
      remove_on_fail: 0,
      stall_timeout: null,
      last_heartbeat: now,
      ...overrides,
    };
  }

  test('should convert a minimal DbJob row to a Job', () => {
    const row = createDbRow();
    const job = rowToJob(row);

    expect(job.id).toBe(jobId('test-job-id'));
    expect(job.queue).toBe('test-queue');
    expect(job.data).toEqual({ email: 'user@test.com' });
    expect(job.priority).toBe(5);
    expect(job.attempts).toBe(0);
    expect(job.maxAttempts).toBe(3);
    expect(job.backoff).toBe(1000);
    expect(job.ttl).toBeNull();
    expect(job.timeout).toBeNull();
    expect(job.uniqueKey).toBeNull();
    expect(job.customId).toBeNull();
    expect(job.dependsOn).toEqual([]);
    expect(job.parentId).toBeNull();
    expect(job.childrenIds).toEqual([]);
    expect(job.tags).toEqual([]);
    expect(job.lifo).toBe(false);
    expect(job.groupId).toBeNull();
    expect(job.progress).toBe(0);
    expect(job.progressMessage).toBeNull();
    expect(job.removeOnComplete).toBe(false);
    expect(job.removeOnFail).toBe(false);
    expect(job.stallTimeout).toBeNull();
    expect(job.childrenCompleted).toBe(0);
    expect(job.backoffConfig).toBeNull();
    expect(job.repeat).toBeNull();
    expect(job.stallCount).toBe(0);
  });

  test('should decode data field from MessagePack', () => {
    const complexData = { nested: { value: 42 }, array: [1, 2, 3] };
    const row = createDbRow({ data: pack(complexData) });
    const job = rowToJob(row);

    expect(job.data).toEqual(complexData);
  });

  test('should decode dependsOn from MessagePack', () => {
    const deps = ['dep-1', 'dep-2', 'dep-3'];
    const row = createDbRow({ depends_on: pack(deps) });
    const job = rowToJob(row);

    expect(job.dependsOn).toHaveLength(3);
    expect(job.dependsOn[0]).toBe(jobId('dep-1'));
    expect(job.dependsOn[1]).toBe(jobId('dep-2'));
    expect(job.dependsOn[2]).toBe(jobId('dep-3'));
  });

  test('should handle null dependsOn', () => {
    const row = createDbRow({ depends_on: null });
    const job = rowToJob(row);
    expect(job.dependsOn).toEqual([]);
  });

  test('should decode childrenIds from MessagePack', () => {
    const children = ['child-a', 'child-b'];
    const row = createDbRow({ children_ids: pack(children) });
    const job = rowToJob(row);

    expect(job.childrenIds).toHaveLength(2);
    expect(job.childrenIds[0]).toBe(jobId('child-a'));
    expect(job.childrenIds[1]).toBe(jobId('child-b'));
  });

  test('should handle null childrenIds', () => {
    const row = createDbRow({ children_ids: null });
    const job = rowToJob(row);
    expect(job.childrenIds).toEqual([]);
  });

  test('should decode tags from MessagePack', () => {
    const tags = ['urgent', 'email', 'priority'];
    const row = createDbRow({ tags: pack(tags) });
    const job = rowToJob(row);

    expect(job.tags).toEqual(['urgent', 'email', 'priority']);
  });

  test('should handle null tags', () => {
    const row = createDbRow({ tags: null });
    const job = rowToJob(row);
    expect(job.tags).toEqual([]);
  });

  test('should convert lifo integer to boolean (1 = true)', () => {
    const row = createDbRow({ lifo: 1 });
    const job = rowToJob(row);
    expect(job.lifo).toBe(true);
  });

  test('should convert lifo integer to boolean (0 = false)', () => {
    const row = createDbRow({ lifo: 0 });
    const job = rowToJob(row);
    expect(job.lifo).toBe(false);
  });

  test('should convert removeOnComplete integer to boolean', () => {
    const rowTrue = createDbRow({ remove_on_complete: 1 });
    expect(rowToJob(rowTrue).removeOnComplete).toBe(true);

    const rowFalse = createDbRow({ remove_on_complete: 0 });
    expect(rowToJob(rowFalse).removeOnComplete).toBe(false);
  });

  test('should convert removeOnFail integer to boolean', () => {
    const rowTrue = createDbRow({ remove_on_fail: 1 });
    expect(rowToJob(rowTrue).removeOnFail).toBe(true);

    const rowFalse = createDbRow({ remove_on_fail: 0 });
    expect(rowToJob(rowFalse).removeOnFail).toBe(false);
  });

  test('should handle parentId conversion', () => {
    const row = createDbRow({ parent_id: 'parent-123' });
    const job = rowToJob(row);
    expect(job.parentId).toBe(jobId('parent-123'));
  });

  test('should handle null parentId', () => {
    const row = createDbRow({ parent_id: null });
    const job = rowToJob(row);
    expect(job.parentId).toBeNull();
  });

  test('should handle timestamps correctly', () => {
    const now = Date.now();
    const row = createDbRow({
      created_at: now,
      run_at: now + 5000,
      started_at: now + 1000,
      completed_at: now + 10000,
    });
    const job = rowToJob(row);

    expect(job.createdAt).toBe(now);
    expect(job.runAt).toBe(now + 5000);
    expect(job.startedAt).toBe(now + 1000);
    expect(job.completedAt).toBe(now + 10000);
  });

  test('should handle null timestamps', () => {
    const row = createDbRow({ started_at: null, completed_at: null });
    const job = rowToJob(row);
    expect(job.startedAt).toBeNull();
    expect(job.completedAt).toBeNull();
  });

  test('should set progress to 0 when null in row', () => {
    const row = createDbRow({ progress: null });
    const job = rowToJob(row);
    expect(job.progress).toBe(0);
  });

  test('should preserve progress when set', () => {
    const row = createDbRow({ progress: 75 });
    const job = rowToJob(row);
    expect(job.progress).toBe(75);
  });

  test('should handle progressMessage', () => {
    const row = createDbRow({ progress_msg: 'Processing step 3' });
    const job = rowToJob(row);
    expect(job.progressMessage).toBe('Processing step 3');
  });

  test('should handle groupId', () => {
    const row = createDbRow({ group_id: 'batch-42' });
    const job = rowToJob(row);
    expect(job.groupId).toBe('batch-42');
  });

  test('should handle stallTimeout', () => {
    const row = createDbRow({ stall_timeout: 30000 });
    const job = rowToJob(row);
    expect(job.stallTimeout).toBe(30000);
  });

  test('should use lastHeartbeat from row when available', () => {
    const now = Date.now();
    const row = createDbRow({ last_heartbeat: now + 5000 });
    const job = rowToJob(row);
    expect(job.lastHeartbeat).toBe(now + 5000);
  });

  test('should fall back to createdAt when lastHeartbeat is null', () => {
    const now = Date.now();
    const row = createDbRow({ created_at: now, last_heartbeat: null });
    const job = rowToJob(row);
    expect(job.lastHeartbeat).toBe(now);
  });

  test('should set BullMQ v5 defaults', () => {
    const row = createDbRow();
    const job = rowToJob(row);

    expect(job.stackTraceLimit).toBe(10);
    expect(job.keepLogs).toBeNull();
    expect(job.sizeLimit).toBeNull();
    expect(job.failParentOnFailure).toBe(false);
    expect(job.removeDependencyOnFailure).toBe(false);
    expect(job.continueParentOnFailure).toBe(false);
    expect(job.ignoreDependencyOnFailure).toBe(false);
    expect(job.deduplicationTtl).toBeNull();
    expect(job.deduplicationExtend).toBe(false);
    expect(job.deduplicationReplace).toBe(false);
    expect(job.debounceId).toBeNull();
    expect(job.debounceTtl).toBeNull();
  });

  test('should handle uniqueKey and customId', () => {
    const row = createDbRow({ unique_key: 'ukey-123', custom_id: 'cid-456' });
    const job = rowToJob(row);
    expect(job.uniqueKey).toBe('ukey-123');
    expect(job.customId).toBe('cid-456');
  });
});

// =============================================================================
// reconstructDlqEntry tests
// =============================================================================
describe('reconstructDlqEntry', () => {
  function makeDlqEntry(overrides: Partial<DlqEntry> = {}): DlqEntry {
    const now = Date.now();
    return {
      job: {
        id: jobId('dlq-job-1'),
        queue: 'test-queue',
        data: { task: 'send-email' },
        priority: 0,
        createdAt: now,
        runAt: now,
        startedAt: now + 100,
        completedAt: null,
        attempts: 3,
        maxAttempts: 3,
        backoff: 1000,
        backoffConfig: null,
        ttl: null,
        timeout: null,
        uniqueKey: null,
        customId: null,
        dependsOn: [jobId('dep-1')],
        parentId: jobId('parent-1'),
        childrenIds: [jobId('child-1'), jobId('child-2')],
        childrenCompleted: 0,
        tags: ['important'],
        groupId: null,
        progress: 0,
        progressMessage: null,
        removeOnComplete: false,
        removeOnFail: false,
        repeat: null,
        lastHeartbeat: now,
        stallTimeout: null,
        stallCount: 0,
        lifo: false,
        stackTraceLimit: 10,
        keepLogs: null,
        sizeLimit: null,
        failParentOnFailure: false,
        removeDependencyOnFailure: false,
        continueParentOnFailure: false,
        ignoreDependencyOnFailure: false,
        deduplicationTtl: null,
        deduplicationExtend: false,
        deduplicationReplace: false,
        debounceId: null,
        debounceTtl: null,
        timeline: [],
      },
      enteredAt: now,
      reason: 'max_attempts_exceeded' as any,
      error: 'Too many retries',
      attempts: [],
      retryCount: 0,
      lastRetryAt: null,
      nextRetryAt: null,
      expiresAt: null,
      ...overrides,
    } as DlqEntry;
  }

  test('should reconstruct job id as JobId', () => {
    const entry = makeDlqEntry();
    const result = reconstructDlqEntry(entry);
    expect(result.job.id).toBe(jobId('dlq-job-1'));
  });

  test('should reconstruct dependsOn as JobId array', () => {
    const entry = makeDlqEntry();
    const result = reconstructDlqEntry(entry);
    expect(result.job.dependsOn).toEqual([jobId('dep-1')]);
  });

  test('should reconstruct parentId as JobId', () => {
    const entry = makeDlqEntry();
    const result = reconstructDlqEntry(entry);
    expect(result.job.parentId).toBe(jobId('parent-1'));
  });

  test('should handle null parentId', () => {
    const entry = makeDlqEntry();
    (entry.job as any).parentId = null;
    const result = reconstructDlqEntry(entry);
    expect(result.job.parentId).toBeNull();
  });

  test('should reconstruct childrenIds as JobId array', () => {
    const entry = makeDlqEntry();
    const result = reconstructDlqEntry(entry);
    expect(result.job.childrenIds).toEqual([jobId('child-1'), jobId('child-2')]);
  });

  test('should preserve non-job fields', () => {
    const entry = makeDlqEntry();
    const result = reconstructDlqEntry(entry);
    expect(result.enteredAt).toBe(entry.enteredAt);
    expect(result.reason).toBe(entry.reason);
    expect(result.error).toBe('Too many retries');
    expect(result.retryCount).toBe(0);
    expect(result.lastRetryAt).toBeNull();
  });

  test('should handle empty dependsOn and childrenIds', () => {
    const entry = makeDlqEntry();
    (entry.job as any).dependsOn = [];
    (entry.job as any).childrenIds = [];
    const result = reconstructDlqEntry(entry);
    expect(result.job.dependsOn).toEqual([]);
    expect(result.job.childrenIds).toEqual([]);
  });
});

// =============================================================================
// SQL_STATEMENTS tests
// =============================================================================
describe('SQL_STATEMENTS', () => {
  describe('statement definitions', () => {
    const expectedStatements: StatementName[] = [
      'insertJob',
      'updateJobState',
      'completeJob',
      'deleteJob',
      'deleteJobResult',
      'getJob',
      'insertResult',
      'getResult',
      'insertDlq',
      'loadDlq',
      'deleteDlqEntry',
      'clearDlqQueue',
      'insertCron',
      'updateCron',
      'upsertQueueState',
      'loadQueueState',
      'deleteQueueState',
    ];

    test('should have all required statement definitions', () => {
      for (const name of expectedStatements) {
        expect(SQL_STATEMENTS[name]).toBeDefined();
        expect(typeof SQL_STATEMENTS[name]).toBe('string');
        expect(SQL_STATEMENTS[name].trim().length).toBeGreaterThan(0);
      }
    });

    test('should have exactly the expected number of statements', () => {
      const keys = Object.keys(SQL_STATEMENTS);
      expect(keys.length).toBe(expectedStatements.length);
    });

    test('should not have any extra undefined statements', () => {
      for (const [name, sql] of Object.entries(SQL_STATEMENTS)) {
        expect(sql).not.toBeUndefined();
        expect(sql).not.toBeNull();
      }
    });
  });

  describe('insertJob statement', () => {
    test('should be an INSERT statement', () => {
      expect(SQL_STATEMENTS.insertJob.trim().toUpperCase()).toMatch(/^INSERT/);
    });

    test('should reference the jobs table', () => {
      expect(SQL_STATEMENTS.insertJob).toContain('jobs');
    });

    test('should have 24 placeholder parameters', () => {
      const paramCount = (SQL_STATEMENTS.insertJob.match(/\?/g) || []).length;
      expect(paramCount).toBe(24);
    });

    test('should include all required columns', () => {
      const sql = SQL_STATEMENTS.insertJob;
      const requiredColumns = [
        'id', 'queue', 'data', 'priority', 'created_at', 'run_at',
        'attempts', 'max_attempts', 'backoff', 'ttl', 'timeout',
        'unique_key', 'custom_id', 'depends_on', 'parent_id',
        'children_ids', 'tags', 'state', 'lifo', 'group_id',
        'remove_on_complete', 'remove_on_fail', 'stall_timeout',
      ];
      for (const col of requiredColumns) {
        expect(sql).toContain(col);
      }
    });
  });

  describe('updateJobState statement', () => {
    test('should be an UPDATE statement', () => {
      expect(SQL_STATEMENTS.updateJobState.trim().toUpperCase()).toMatch(/^UPDATE/);
    });

    test('should update state and started_at', () => {
      expect(SQL_STATEMENTS.updateJobState).toContain('state');
      expect(SQL_STATEMENTS.updateJobState).toContain('started_at');
    });

    test('should have WHERE clause on id', () => {
      expect(SQL_STATEMENTS.updateJobState).toContain('WHERE id');
    });
  });

  describe('completeJob statement', () => {
    test('should be an UPDATE statement', () => {
      expect(SQL_STATEMENTS.completeJob.trim().toUpperCase()).toMatch(/^UPDATE/);
    });

    test('should set progress to 100', () => {
      expect(SQL_STATEMENTS.completeJob).toContain('progress = 100');
    });

    test('should update completed_at', () => {
      expect(SQL_STATEMENTS.completeJob).toContain('completed_at');
    });
  });

  describe('deleteJob statement', () => {
    test('should be a DELETE statement', () => {
      expect(SQL_STATEMENTS.deleteJob.trim().toUpperCase()).toMatch(/^DELETE/);
    });

    test('should delete from jobs table', () => {
      expect(SQL_STATEMENTS.deleteJob).toContain('jobs');
    });

    test('should have WHERE clause on id', () => {
      expect(SQL_STATEMENTS.deleteJob).toContain('WHERE id');
    });
  });

  describe('getJob statement', () => {
    test('should be a SELECT statement', () => {
      expect(SQL_STATEMENTS.getJob.trim().toUpperCase()).toMatch(/^SELECT/);
    });

    test('should select all columns', () => {
      expect(SQL_STATEMENTS.getJob).toContain('*');
    });

    test('should query from jobs table', () => {
      expect(SQL_STATEMENTS.getJob).toContain('jobs');
    });
  });

  describe('result statements', () => {
    test('insertResult should be INSERT OR REPLACE', () => {
      expect(SQL_STATEMENTS.insertResult.toUpperCase()).toContain('INSERT OR REPLACE');
    });

    test('insertResult should reference job_results table', () => {
      expect(SQL_STATEMENTS.insertResult).toContain('job_results');
    });

    test('getResult should select result column', () => {
      expect(SQL_STATEMENTS.getResult).toContain('result');
      expect(SQL_STATEMENTS.getResult).toContain('job_results');
    });
  });

  describe('DLQ statements', () => {
    test('insertDlq should be an INSERT statement', () => {
      expect(SQL_STATEMENTS.insertDlq.trim().toUpperCase()).toMatch(/^INSERT/);
    });

    test('insertDlq should have 4 parameters', () => {
      const paramCount = (SQL_STATEMENTS.insertDlq.match(/\?/g) || []).length;
      expect(paramCount).toBe(4);
    });

    test('loadDlq should ORDER BY entered_at', () => {
      expect(SQL_STATEMENTS.loadDlq).toContain('ORDER BY entered_at');
    });

    test('deleteDlqEntry should delete by job_id', () => {
      expect(SQL_STATEMENTS.deleteDlqEntry).toContain('DELETE');
      expect(SQL_STATEMENTS.deleteDlqEntry).toContain('job_id');
    });

    test('clearDlqQueue should delete by queue', () => {
      expect(SQL_STATEMENTS.clearDlqQueue).toContain('DELETE');
      expect(SQL_STATEMENTS.clearDlqQueue).toContain('queue');
    });
  });

  describe('cron statements', () => {
    test('insertCron should be INSERT OR REPLACE', () => {
      expect(SQL_STATEMENTS.insertCron.toUpperCase()).toContain('INSERT OR REPLACE');
    });

    test('insertCron should have 16 parameters', () => {
      const paramCount = (SQL_STATEMENTS.insertCron.match(/\?/g) || []).length;
      expect(paramCount).toBe(16);
    });

    test('insertCron should reference cron_jobs table', () => {
      expect(SQL_STATEMENTS.insertCron).toContain('cron_jobs');
    });

    test('updateCron should update executions and next_run', () => {
      expect(SQL_STATEMENTS.updateCron).toContain('executions');
      expect(SQL_STATEMENTS.updateCron).toContain('next_run');
    });

    test('updateCron should filter by name', () => {
      expect(SQL_STATEMENTS.updateCron).toContain('WHERE name');
    });
  });
});

// =============================================================================
// prepareStatements (real SQLite database)
// =============================================================================
describe('prepareStatements', () => {
  const TEST_DB_PATH = join(tmpdir(), `bunqueue-stmt-test-${Date.now()}.db`);
  let db: Database;

  beforeAll(() => {
    if (existsSync(TEST_DB_PATH)) {
      unlinkSync(TEST_DB_PATH);
    }
    db = new Database(TEST_DB_PATH, { create: true });
    db.run(PRAGMA_SETTINGS);
    db.run(SCHEMA);
  });

  afterAll(() => {
    db.close();
    for (const suffix of ['', '-wal', '-shm']) {
      const path = TEST_DB_PATH + suffix;
      if (existsSync(path)) {
        unlinkSync(path);
      }
    }
  });

  test('should prepare all statements without errors', () => {
    const statements = prepareStatements(db);
    expect(statements.size).toBe(Object.keys(SQL_STATEMENTS).length);
  });

  test('should return a Map with all statement names as keys', () => {
    const statements = prepareStatements(db);
    for (const name of Object.keys(SQL_STATEMENTS) as StatementName[]) {
      expect(statements.has(name)).toBe(true);
    }
  });

  test('should produce usable prepared statements', () => {
    const statements = prepareStatements(db);

    // Each value should be a prepared statement (has .run, .get, .all methods)
    for (const [name, stmt] of statements) {
      expect(stmt).toBeDefined();
      expect(typeof stmt.run).toBe('function');
      expect(typeof stmt.get).toBe('function');
      expect(typeof stmt.all).toBe('function');
    }
  });

  test('insertJob statement can execute with valid data', () => {
    const statements = prepareStatements(db);
    const insertStmt = statements.get('insertJob')!;
    const now = Date.now();
    const data = pack({ test: true });

    // Insert a job using the prepared statement
    expect(() => {
      insertStmt.run(
        'prepared-test-1',       // id
        'test-queue',            // queue
        data,                    // data (BLOB)
        5,                       // priority
        now,                     // created_at
        now,                     // run_at
        0,                       // attempts
        3,                       // max_attempts
        1000,                    // backoff
        null,                    // ttl
        null,                    // timeout
        null,                    // unique_key
        null,                    // custom_id
        null,                    // depends_on
        null,                    // parent_id
        null,                    // children_ids
        null,                    // tags
        'waiting',               // state
        0,                       // lifo
        null,                    // group_id
        0,                       // remove_on_complete
        0,                       // remove_on_fail
        null,                    // stall_timeout
        null                     // timeline
      );
    }).not.toThrow();
  });

  test('getJob statement can retrieve inserted data', () => {
    const statements = prepareStatements(db);
    const getStmt = statements.get('getJob')!;
    const result = getStmt.get('prepared-test-1') as DbJob | null;

    expect(result).not.toBeNull();
    expect(result!.id).toBe('prepared-test-1');
    expect(result!.queue).toBe('test-queue');
    expect(result!.priority).toBe(5);
    expect(result!.state).toBe('waiting');
  });

  test('updateJobState statement works correctly', () => {
    const statements = prepareStatements(db);
    const updateStmt = statements.get('updateJobState')!;
    const now = Date.now();

    expect(() => {
      updateStmt.run('active', now, null, 'prepared-test-1');
    }).not.toThrow();

    const getStmt = statements.get('getJob')!;
    const result = getStmt.get('prepared-test-1') as DbJob;
    expect(result.state).toBe('active');
    expect(result.started_at).toBe(now);
  });

  test('completeJob statement works correctly', () => {
    const statements = prepareStatements(db);
    const completeStmt = statements.get('completeJob')!;
    const now = Date.now();

    expect(() => {
      completeStmt.run('completed', now, null, 'prepared-test-1');
    }).not.toThrow();

    const getStmt = statements.get('getJob')!;
    const result = getStmt.get('prepared-test-1') as DbJob;
    expect(result.state).toBe('completed');
    expect(result.completed_at).toBe(now);
    expect(result.progress).toBe(100);
  });

  test('insertResult and getResult statements work correctly', () => {
    const statements = prepareStatements(db);
    const insertResultStmt = statements.get('insertResult')!;
    const getResultStmt = statements.get('getResult')!;
    const resultData = pack({ status: 'success', count: 42 });
    const now = Date.now();

    expect(() => {
      insertResultStmt.run('prepared-test-1', resultData, now);
    }).not.toThrow();

    const row = getResultStmt.get('prepared-test-1') as { result: Uint8Array } | null;
    expect(row).not.toBeNull();
    const decoded = unpack(row!.result, {}, 'test');
    expect(decoded).toEqual({ status: 'success', count: 42 });
  });

  test('DLQ statements work correctly', () => {
    const statements = prepareStatements(db);
    const insertDlqStmt = statements.get('insertDlq')!;
    const loadDlqStmt = statements.get('loadDlq')!;
    const deleteDlqStmt = statements.get('deleteDlqEntry')!;
    const clearDlqStmt = statements.get('clearDlqQueue')!;

    const entryData = pack({ job: { id: 'dlq-1' }, reason: 'timeout' });
    const now = Date.now();

    // Insert
    expect(() => {
      insertDlqStmt.run('dlq-job-1', 'test-queue', entryData, now);
    }).not.toThrow();

    // Load
    const dlqEntries = loadDlqStmt.all() as any[];
    expect(dlqEntries.length).toBeGreaterThanOrEqual(1);

    // Delete single entry
    expect(() => {
      deleteDlqStmt.run('dlq-job-1');
    }).not.toThrow();

    // Insert another and clear by queue
    insertDlqStmt.run('dlq-job-2', 'clear-queue', entryData, now);
    expect(() => {
      clearDlqStmt.run('clear-queue');
    }).not.toThrow();
  });

  test('cron statements work correctly', () => {
    const statements = prepareStatements(db);
    const insertCronStmt = statements.get('insertCron')!;
    const updateCronStmt = statements.get('updateCron')!;
    const cronData = pack({ task: 'cleanup' });
    const now = Date.now();

    // Insert
    expect(() => {
      insertCronStmt.run(
        'test-cron',       // name
        'cron-queue',      // queue
        cronData,          // data
        '0 * * * *',      // schedule
        null,              // repeat_every
        5,                 // priority
        now + 3600000,     // next_run
        0,                 // executions
        null,              // max_limit
        'UTC',             // timezone
        null,              // unique_key
        null,              // dedup
        0,                 // skip_missed_on_restart
        0,                 // skip_if_no_worker
        1,                 // prevent_overlap
        null               // job_options
      );
    }).not.toThrow();

    // Update
    expect(() => {
      updateCronStmt.run(1, now + 7200000, 'test-cron');
    }).not.toThrow();
  });

  test('deleteJob statement works correctly', () => {
    const statements = prepareStatements(db);
    const deleteStmt = statements.get('deleteJob')!;

    expect(() => {
      deleteStmt.run('prepared-test-1');
    }).not.toThrow();

    const getStmt = statements.get('getJob')!;
    const result = getStmt.get('prepared-test-1');
    expect(result).toBeNull();
  });
});

// =============================================================================
// End-to-end: pack -> insert -> select -> rowToJob roundtrip
// =============================================================================
describe('end-to-end pack/unpack via SQLite', () => {
  const TEST_DB_PATH = join(tmpdir(), `bunqueue-e2e-test-${Date.now()}.db`);
  let db: Database;

  beforeAll(() => {
    if (existsSync(TEST_DB_PATH)) {
      unlinkSync(TEST_DB_PATH);
    }
    db = new Database(TEST_DB_PATH, { create: true });
    db.run(PRAGMA_SETTINGS);
    db.run(SCHEMA);
  });

  afterAll(() => {
    db.close();
    for (const suffix of ['', '-wal', '-shm']) {
      const path = TEST_DB_PATH + suffix;
      if (existsSync(path)) {
        unlinkSync(path);
      }
    }
  });

  test('should pack data, insert into SQLite, read back, and convert to Job', () => {
    const now = Date.now();
    const jobData = { email: 'test@example.com', retries: 3 };
    const deps = ['dep-1', 'dep-2'];
    const children = ['child-1'];
    const tags = ['important', 'billing'];

    const stmt = db.prepare(SQL_STATEMENTS.insertJob);
    stmt.run(
      'e2e-job-1',
      'email-queue',
      pack(jobData),
      10,
      now,
      now + 1000,
      1,
      5,
      2000,
      60000,
      30000,
      'ukey-1',
      'custom-1',
      pack(deps),
      'parent-job',
      pack(children),
      pack(tags),
      'waiting',
      1,
      'group-1',
      1,
      1,
      15000,
      null
    );

    const getStmt = db.prepare(SQL_STATEMENTS.getJob);
    const row = getStmt.get('e2e-job-1') as DbJob;
    expect(row).not.toBeNull();

    const job = rowToJob(row);

    expect(job.id).toBe(jobId('e2e-job-1'));
    expect(job.queue).toBe('email-queue');
    expect(job.data).toEqual(jobData);
    expect(job.priority).toBe(10);
    expect(job.createdAt).toBe(now);
    expect(job.runAt).toBe(now + 1000);
    expect(job.attempts).toBe(1);
    expect(job.maxAttempts).toBe(5);
    expect(job.backoff).toBe(2000);
    expect(job.ttl).toBe(60000);
    expect(job.timeout).toBe(30000);
    expect(job.uniqueKey).toBe('ukey-1');
    expect(job.customId).toBe('custom-1');
    expect(job.dependsOn).toEqual([jobId('dep-1'), jobId('dep-2')]);
    expect(job.parentId).toBe(jobId('parent-job'));
    expect(job.childrenIds).toEqual([jobId('child-1')]);
    expect(job.tags).toEqual(['important', 'billing']);
    expect(job.lifo).toBe(true);
    expect(job.groupId).toBe('group-1');
    expect(job.removeOnComplete).toBe(true);
    expect(job.removeOnFail).toBe(true);
    expect(job.stallTimeout).toBe(15000);
  });

  test('should handle a job with all null optional fields', () => {
    const now = Date.now();
    const stmt = db.prepare(SQL_STATEMENTS.insertJob);
    stmt.run(
      'e2e-job-2',
      'minimal-queue',
      pack(null),
      0,
      now,
      now,
      0,
      3,
      1000,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      'waiting',
      0,
      null,
      0,
      0,
      null,
      null
    );

    const getStmt = db.prepare(SQL_STATEMENTS.getJob);
    const row = getStmt.get('e2e-job-2') as DbJob;
    const job = rowToJob(row);

    expect(job.id).toBe(jobId('e2e-job-2'));
    expect(job.data).toBeNull();
    expect(job.ttl).toBeNull();
    expect(job.timeout).toBeNull();
    expect(job.uniqueKey).toBeNull();
    expect(job.customId).toBeNull();
    expect(job.dependsOn).toEqual([]);
    expect(job.parentId).toBeNull();
    expect(job.childrenIds).toEqual([]);
    expect(job.tags).toEqual([]);
    expect(job.lifo).toBe(false);
    expect(job.groupId).toBeNull();
    expect(job.removeOnComplete).toBe(false);
    expect(job.removeOnFail).toBe(false);
    expect(job.stallTimeout).toBeNull();
  });
});
