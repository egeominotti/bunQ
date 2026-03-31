/**
 * Hash Utilities Tests
 */

import { describe, test, expect } from 'bun:test';
import {
  fnv1a,
  shardIndex,
  processingShardIndex,
  uuid,
  constantTimeEqual,
  SHARD_COUNT,
} from '../src/shared/hash';

describe('fnv1a', () => {
  test('should return consistent hash for same input', () => {
    const hash1 = fnv1a('test');
    const hash2 = fnv1a('test');
    expect(hash1).toBe(hash2);
  });

  test('should return different hash for different input', () => {
    const hash1 = fnv1a('test1');
    const hash2 = fnv1a('test2');
    expect(hash1).not.toBe(hash2);
  });

  test('should return unsigned 32-bit integer', () => {
    const hash = fnv1a('test');
    expect(hash).toBeGreaterThanOrEqual(0);
    expect(hash).toBeLessThanOrEqual(0xffffffff);
  });
});

describe('shardIndex', () => {
  test('should return index within shard count', () => {
    for (let i = 0; i < 100; i++) {
      const idx = shardIndex(`queue-${i}`);
      expect(idx).toBeGreaterThanOrEqual(0);
      expect(idx).toBeLessThan(SHARD_COUNT);
    }
  });

  test('should return consistent index', () => {
    const idx1 = shardIndex('emails');
    const idx2 = shardIndex('emails');
    expect(idx1).toBe(idx2);
  });

  test('should distribute across shards', () => {
    const distribution = new Set<number>();
    for (let i = 0; i < 1000; i++) {
      distribution.add(shardIndex(`queue-${i}`));
    }
    // Should use most shards
    expect(distribution.size).toBeGreaterThan(SHARD_COUNT / 2);
  });
});

describe('processingShardIndex', () => {
  test('should return index within shard count', () => {
    for (let i = 0; i < 100; i++) {
      const idx = processingShardIndex(`shard-test-${i}`);
      expect(idx).toBeGreaterThanOrEqual(0);
      expect(idx).toBeLessThan(SHARD_COUNT);
    }
  });

  test('should be deterministic', () => {
    const idx1 = processingShardIndex('shard-test-12345');
    const idx2 = processingShardIndex('shard-test-12345');
    expect(idx1).toBe(idx2);
  });
});

describe('uuid', () => {
  test('should generate valid UUID v7 format', () => {
    const id = uuid();
    // UUID v7 format: version 7 at position 14, variant 8/9/a/b at position 19
    const pattern = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
    expect(id).toMatch(pattern);
  });

  test('should generate unique UUIDs', () => {
    const ids = new Set<string>();
    for (let i = 0; i < 1000; i++) {
      ids.add(uuid());
    }
    expect(ids.size).toBe(1000);
  });

  test('should generate time-ordered UUIDs', () => {
    const id1 = uuid();
    const id2 = uuid();
    // UUID v7 is lexicographically sortable by time
    expect(id1 < id2).toBe(true);
  });
});

describe('constantTimeEqual', () => {
  test('should return true for equal strings', () => {
    expect(constantTimeEqual('test', 'test')).toBe(true);
    expect(constantTimeEqual('', '')).toBe(true);
    expect(constantTimeEqual('abc123', 'abc123')).toBe(true);
  });

  test('should return false for different strings', () => {
    expect(constantTimeEqual('test', 'Test')).toBe(false);
    expect(constantTimeEqual('test', 'test1')).toBe(false);
    expect(constantTimeEqual('a', 'b')).toBe(false);
  });

  test('should return false for different lengths', () => {
    expect(constantTimeEqual('test', 'tes')).toBe(false);
    expect(constantTimeEqual('a', 'ab')).toBe(false);
  });
});
