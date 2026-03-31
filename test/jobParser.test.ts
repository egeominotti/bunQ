/**
 * Tests for src/client/worker/jobParser.ts
 * Covers parseJobFromResponse: valid parsing, defaults, nullable fields,
 * array handling, edge cases, and all property assignments.
 */

import { describe, test, expect } from 'bun:test';
import { parseJobFromResponse } from '../src/client/worker/jobParser';
import { jobId } from '../src/domain/types/job';

// ============================================================================
// Helpers
// ============================================================================

/** Build a full TCP response payload with all fields populated */
function makeFullResponse(): Record<string, unknown> {
  return {
    id: 'job-abc-123',
    data: { email: 'user@test.com', count: 5 },
    priority: 10,
    createdAt: 1700000000000,
    runAt: 1700000001000,
    startedAt: 1700000002000,
    attempts: 2,
    maxAttempts: 5,
    backoff: 2000,
    ttl: 60000,
    timeout: 30000,
    uniqueKey: 'unique-abc',
    customId: 'custom-xyz',
    progress: 75,
    progressMessage: 'Processing...',
    dependsOn: ['dep-1', 'dep-2'],
    parentId: 'parent-1',
    childrenIds: ['child-a', 'child-b'],
    childrenCompleted: 1,
    tags: ['urgent', 'billing'],
    groupId: 'group-alpha',
    removeOnComplete: true,
  };
}

// ============================================================================
// parseJobFromResponse
// ============================================================================

describe('parseJobFromResponse', () => {
  // --------------------------------------------------------------------------
  // Valid response with all fields present
  // --------------------------------------------------------------------------

  describe('valid response with all fields', () => {
    test('should parse id via jobId branded type', () => {
      const result = parseJobFromResponse(makeFullResponse(), 'my-queue');
      expect(result.id).toBe(jobId('job-abc-123'));
    });

    test('should use the provided queueName, not data from response', () => {
      const result = parseJobFromResponse(makeFullResponse(), 'override-queue');
      expect(result.queue).toBe('override-queue');
    });

    test('should pass data through as-is', () => {
      const result = parseJobFromResponse(makeFullResponse(), 'q');
      expect(result.data).toEqual({ email: 'user@test.com', count: 5 });
    });

    test('should parse numeric fields from response', () => {
      const result = parseJobFromResponse(makeFullResponse(), 'q');
      expect(result.priority).toBe(10);
      expect(result.createdAt).toBe(1700000000000);
      expect(result.runAt).toBe(1700000001000);
      expect(result.startedAt).toBe(1700000002000);
      expect(result.attempts).toBe(2);
      expect(result.maxAttempts).toBe(5);
      expect(result.backoff).toBe(2000);
      expect(result.ttl).toBe(60000);
      expect(result.timeout).toBe(30000);
      expect(result.progress).toBe(75);
      expect(result.childrenCompleted).toBe(1);
    });

    test('should parse string fields from response', () => {
      const result = parseJobFromResponse(makeFullResponse(), 'q');
      expect(result.uniqueKey).toBe('unique-abc');
      expect(result.customId).toBe('custom-xyz');
      expect(result.progressMessage).toBe('Processing...');
      expect(result.parentId).toBe('parent-1');
      expect(result.groupId).toBe('group-alpha');
    });

    test('should parse array fields from response', () => {
      const result = parseJobFromResponse(makeFullResponse(), 'q');
      expect(result.dependsOn).toEqual(['dep-1', 'dep-2']);
      expect(result.childrenIds).toEqual(['child-a', 'child-b']);
      expect(result.tags).toEqual(['urgent', 'billing']);
    });

    test('should parse boolean removeOnComplete from response', () => {
      const result = parseJobFromResponse(makeFullResponse(), 'q');
      expect(result.removeOnComplete).toBe(true);
    });
  });

  // --------------------------------------------------------------------------
  // Default values for missing/undefined fields
  // --------------------------------------------------------------------------

  describe('default values for missing fields', () => {
    const minimal: Record<string, unknown> = { id: 'min-id', data: { x: 1 } };

    test('priority defaults to 0', () => {
      const result = parseJobFromResponse(minimal, 'q');
      expect(result.priority).toBe(0);
    });

    test('createdAt defaults to Date.now()', () => {
      const before = Date.now();
      const result = parseJobFromResponse(minimal, 'q');
      const after = Date.now();
      expect(result.createdAt).toBeGreaterThanOrEqual(before);
      expect(result.createdAt).toBeLessThanOrEqual(after);
    });

    test('runAt defaults to Date.now()', () => {
      const before = Date.now();
      const result = parseJobFromResponse(minimal, 'q');
      const after = Date.now();
      expect(result.runAt).toBeGreaterThanOrEqual(before);
      expect(result.runAt).toBeLessThanOrEqual(after);
    });

    test('startedAt defaults to Date.now()', () => {
      const before = Date.now();
      const result = parseJobFromResponse(minimal, 'q');
      const after = Date.now();
      expect(result.startedAt).toBeGreaterThanOrEqual(before);
      expect(result.startedAt).toBeLessThanOrEqual(after);
    });

    test('attempts defaults to 0', () => {
      const result = parseJobFromResponse(minimal, 'q');
      expect(result.attempts).toBe(0);
    });

    test('maxAttempts defaults to 3', () => {
      const result = parseJobFromResponse(minimal, 'q');
      expect(result.maxAttempts).toBe(3);
    });

    test('backoff defaults to 1000', () => {
      const result = parseJobFromResponse(minimal, 'q');
      expect(result.backoff).toBe(1000);
    });

    test('progress defaults to 0', () => {
      const result = parseJobFromResponse(minimal, 'q');
      expect(result.progress).toBe(0);
    });

    test('childrenCompleted defaults to 0', () => {
      const result = parseJobFromResponse(minimal, 'q');
      expect(result.childrenCompleted).toBe(0);
    });

    test('removeOnComplete defaults to false', () => {
      const result = parseJobFromResponse(minimal, 'q');
      expect(result.removeOnComplete).toBe(false);
    });
  });

  // --------------------------------------------------------------------------
  // Nullable field handling
  // --------------------------------------------------------------------------

  describe('nullable fields default to null', () => {
    const minimal: Record<string, unknown> = { id: 'null-id' };

    test('completedAt is always null', () => {
      const result = parseJobFromResponse(minimal, 'q');
      expect(result.completedAt).toBeNull();
    });

    test('ttl defaults to null when missing', () => {
      const result = parseJobFromResponse(minimal, 'q');
      expect(result.ttl).toBeNull();
    });

    test('timeout defaults to null when missing', () => {
      const result = parseJobFromResponse(minimal, 'q');
      expect(result.timeout).toBeNull();
    });

    test('uniqueKey defaults to null when missing', () => {
      const result = parseJobFromResponse(minimal, 'q');
      expect(result.uniqueKey).toBeNull();
    });

    test('customId defaults to null when missing', () => {
      const result = parseJobFromResponse(minimal, 'q');
      expect(result.customId).toBeNull();
    });

    test('progressMessage defaults to null when missing', () => {
      const result = parseJobFromResponse(minimal, 'q');
      expect(result.progressMessage).toBeNull();
    });

    test('parentId defaults to null when missing', () => {
      const result = parseJobFromResponse(minimal, 'q');
      expect(result.parentId).toBeNull();
    });

    test('groupId defaults to null when missing', () => {
      const result = parseJobFromResponse(minimal, 'q');
      expect(result.groupId).toBeNull();
    });

    test('stallTimeout is always null', () => {
      const result = parseJobFromResponse(minimal, 'q');
      expect(result.stallTimeout).toBeNull();
    });

    test('repeat is always null', () => {
      const result = parseJobFromResponse(minimal, 'q');
      expect(result.repeat).toBeNull();
    });
  });

  // --------------------------------------------------------------------------
  // Hardcoded / constant fields
  // --------------------------------------------------------------------------

  describe('hardcoded constant fields', () => {
    test('lifo is always false', () => {
      const resp = { ...makeFullResponse(), lifo: true };
      const result = parseJobFromResponse(resp, 'q');
      expect(result.lifo).toBe(false);
    });

    test('removeOnFail is always false', () => {
      const resp = { ...makeFullResponse(), removeOnFail: true };
      const result = parseJobFromResponse(resp, 'q');
      expect(result.removeOnFail).toBe(false);
    });

    test('stallCount is always 0', () => {
      const resp = { ...makeFullResponse(), stallCount: 5 };
      const result = parseJobFromResponse(resp, 'q');
      expect(result.stallCount).toBe(0);
    });

    test('lastHeartbeat is set to current time', () => {
      const before = Date.now();
      const result = parseJobFromResponse({ id: 'hb' }, 'q');
      const after = Date.now();
      expect(result.lastHeartbeat).toBeGreaterThanOrEqual(before);
      expect(result.lastHeartbeat).toBeLessThanOrEqual(after);
    });
  });

  // --------------------------------------------------------------------------
  // Array parsing (non-array inputs fallback to empty array)
  // --------------------------------------------------------------------------

  describe('array field fallback behavior', () => {
    test('dependsOn falls back to empty array for non-array', () => {
      const resp = { id: 'arr1', dependsOn: 'not-an-array' };
      const result = parseJobFromResponse(resp, 'q');
      expect(result.dependsOn).toEqual([]);
    });

    test('dependsOn falls back to empty array for null', () => {
      const resp = { id: 'arr2', dependsOn: null };
      const result = parseJobFromResponse(resp, 'q');
      expect(result.dependsOn).toEqual([]);
    });

    test('dependsOn falls back to empty array for number', () => {
      const resp = { id: 'arr3', dependsOn: 42 };
      const result = parseJobFromResponse(resp, 'q');
      expect(result.dependsOn).toEqual([]);
    });

    test('childrenIds falls back to empty array for non-array', () => {
      const resp = { id: 'arr4', childrenIds: 'not-an-array' };
      const result = parseJobFromResponse(resp, 'q');
      expect(result.childrenIds).toEqual([]);
    });

    test('tags falls back to empty array for non-array', () => {
      const resp = { id: 'arr5', tags: { not: 'array' } };
      const result = parseJobFromResponse(resp, 'q');
      expect(result.tags).toEqual([]);
    });

    test('dependsOn preserves valid array', () => {
      const resp = { id: 'arr6', dependsOn: ['a', 'b'] };
      const result = parseJobFromResponse(resp, 'q');
      expect(result.dependsOn).toEqual(['a', 'b']);
    });

    test('childrenIds preserves valid array', () => {
      const resp = { id: 'arr7', childrenIds: ['c1'] };
      const result = parseJobFromResponse(resp, 'q');
      expect(result.childrenIds).toEqual(['c1']);
    });

    test('tags preserves valid array', () => {
      const resp = { id: 'arr8', tags: ['tag1', 'tag2', 'tag3'] };
      const result = parseJobFromResponse(resp, 'q');
      expect(result.tags).toEqual(['tag1', 'tag2', 'tag3']);
    });

    test('empty arrays are preserved', () => {
      const resp = { id: 'arr9', dependsOn: [], childrenIds: [], tags: [] };
      const result = parseJobFromResponse(resp, 'q');
      expect(result.dependsOn).toEqual([]);
      expect(result.childrenIds).toEqual([]);
      expect(result.tags).toEqual([]);
    });
  });

  // --------------------------------------------------------------------------
  // Edge cases
  // --------------------------------------------------------------------------

  describe('edge cases', () => {
    test('empty response object produces a job with undefined id', () => {
      const result = parseJobFromResponse({} as Record<string, unknown>, 'q');
      // jobId(undefined as string) yields undefined
      expect(result.id).toBeUndefined();
      expect(result.queue).toBe('q');
      expect(result.priority).toBe(0);
      expect(result.attempts).toBe(0);
      expect(result.dependsOn).toEqual([]);
    });

    test('data can be null', () => {
      const result = parseJobFromResponse({ id: 'n1', data: null }, 'q');
      expect(result.data).toBeNull();
    });

    test('data can be a primitive', () => {
      const result = parseJobFromResponse({ id: 'p1', data: 42 }, 'q');
      expect(result.data).toBe(42);
    });

    test('data can be a string', () => {
      const result = parseJobFromResponse({ id: 's1', data: 'hello' }, 'q');
      expect(result.data).toBe('hello');
    });

    test('data can be an array', () => {
      const result = parseJobFromResponse({ id: 'a1', data: [1, 2, 3] }, 'q');
      expect(result.data).toEqual([1, 2, 3]);
    });

    test('data can be undefined (missing)', () => {
      const result = parseJobFromResponse({ id: 'u1' }, 'q');
      expect(result.data).toBeUndefined();
    });

    test('undefined numeric fields use defaults', () => {
      const resp = { id: 'undef', priority: undefined, attempts: undefined };
      const result = parseJobFromResponse(resp, 'q');
      expect(result.priority).toBe(0);
      expect(result.attempts).toBe(0);
    });

    test('id is passed through jobId branded constructor', () => {
      const result = parseJobFromResponse({ id: 'branded-test' }, 'q');
      expect(result.id).toBe('branded-test');
      expect(typeof result.id).toBe('string');
    });

    test('queue name with special characters', () => {
      const result = parseJobFromResponse({ id: 'sp1' }, 'my:queue/path.v2');
      expect(result.queue).toBe('my:queue/path.v2');
    });

    test('empty string queue name', () => {
      const result = parseJobFromResponse({ id: 'eq' }, '');
      expect(result.queue).toBe('');
    });

    test('numeric id cast to string via jobId', () => {
      const resp = { id: 12345 };
      const result = parseJobFromResponse(resp, 'q');
      // jobId casts to string, so the branded type wraps whatever is passed
      expect(result.id).toBe(12345 as unknown as string);
    });
  });

  // --------------------------------------------------------------------------
  // All 18+ property assignments verified in a single comprehensive test
  // --------------------------------------------------------------------------

  describe('comprehensive property assignment check', () => {
    test('all properties are set correctly from a full response', () => {
      const before = Date.now();
      const resp = makeFullResponse();
      const result = parseJobFromResponse(resp, 'test-queue');
      const after = Date.now();

      // From response
      expect(result.id).toBe(jobId('job-abc-123'));
      expect(result.queue).toBe('test-queue');
      expect(result.data).toEqual({ email: 'user@test.com', count: 5 });
      expect(result.priority).toBe(10);
      expect(result.createdAt).toBe(1700000000000);
      expect(result.runAt).toBe(1700000001000);
      expect(result.startedAt).toBe(1700000002000);
      expect(result.attempts).toBe(2);
      expect(result.maxAttempts).toBe(5);
      expect(result.backoff).toBe(2000);
      expect(result.ttl).toBe(60000);
      expect(result.timeout).toBe(30000);
      expect(result.uniqueKey).toBe('unique-abc');
      expect(result.customId).toBe('custom-xyz');
      expect(result.progress).toBe(75);
      expect(result.progressMessage).toBe('Processing...');
      expect(result.dependsOn).toEqual(['dep-1', 'dep-2']);
      expect(result.parentId).toBe('parent-1');
      expect(result.childrenIds).toEqual(['child-a', 'child-b']);
      expect(result.childrenCompleted).toBe(1);
      expect(result.tags).toEqual(['urgent', 'billing']);
      expect(result.groupId).toBe('group-alpha');
      expect(result.removeOnComplete).toBe(true);

      // Hardcoded values
      expect(result.completedAt).toBeNull();
      expect(result.lifo).toBe(false);
      expect(result.removeOnFail).toBe(false);
      expect(result.stallCount).toBe(0);
      expect(result.stallTimeout).toBeNull();
      expect(result.repeat).toBeNull();
      expect(result.lastHeartbeat).toBeGreaterThanOrEqual(before);
      expect(result.lastHeartbeat).toBeLessThanOrEqual(after);
    });
  });
});
