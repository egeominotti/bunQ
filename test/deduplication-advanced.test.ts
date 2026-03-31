/**
 * Advanced Deduplication Tests
 * Tests TTL, extend, and replace deduplication strategies
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { Queue } from '../src/client/queue/queue';
import { getSharedManager } from '../src/client/manager';

const sleep = (ms: number) => Bun.sleep(ms);

describe('Advanced Deduplication', () => {
  const QUEUE_NAME = 'test-dedup';

  beforeEach(async () => {
    // Clean up
    const manager = getSharedManager();
    manager.drain(QUEUE_NAME);
  });

  describe('Basic unique key (default behavior)', () => {
    test('returns existing job on duplicate jobId (BullMQ-style)', async () => {
      const queue = new Queue<{ msg: string }>(QUEUE_NAME, { embedded: true });

      const job1 = await queue.add('job1', { msg: 'first' }, { jobId: 'unique-1' });

      // Second add with same key returns existing job (BullMQ-style idempotency)
      const job2 = await queue.add('job2', { msg: 'second' }, { jobId: 'unique-1' });

      // Same job ID returned
      expect(job2.id).toBe(job1.id);
    });

    test('allows different unique keys', async () => {
      const queue = new Queue<{ msg: string }>(QUEUE_NAME, { embedded: true });

      const job1 = await queue.add('job1', { msg: 'first' }, { jobId: 'unique-1' });
      const job2 = await queue.add('job2', { msg: 'second' }, { jobId: 'unique-2' });

      expect(job1.id).not.toBe(job2.id);
    });
  });

  describe('TTL-based deduplication', () => {
    test('unique key expires after TTL', async () => {
      const queue = new Queue<{ msg: string }>(QUEUE_NAME, { embedded: true });

      // Add job with uniqueKey that has 100ms TTL using dedup option
      const manager = getSharedManager();
      const job1 = await manager.push(QUEUE_NAME, {
        data: { name: 'job1', msg: 'first' },
        uniqueKey: 'ttl-key-1',
        dedup: { ttl: 100 },
      });

      expect(job1).toBeDefined();

      // Second add immediately returns existing job (BullMQ-style)
      const job2 = await manager.push(QUEUE_NAME, {
        data: { name: 'job2', msg: 'second' },
        uniqueKey: 'ttl-key-1',
      });
      expect(job2.id).toBe(job1.id);

      // Wait for TTL to expire
      await sleep(150);

      // Now it should succeed with new job (key expired)
      const job3 = await manager.push(QUEUE_NAME, {
        data: { name: 'job3', msg: 'third' },
        uniqueKey: 'ttl-key-1',
        dedup: { ttl: 100 },
      });

      expect(job3.id).not.toBe(job1.id);
    });
  });

  describe('Extend strategy', () => {
    test('extends TTL on duplicate and returns existing job', async () => {
      const manager = getSharedManager();

      // Add job with uniqueKey and TTL
      const job1 = await manager.push(QUEUE_NAME, {
        data: { name: 'job1', msg: 'first' },
        uniqueKey: 'extend-key-1',
        dedup: { ttl: 200 },
      });

      // Add duplicate with extend option - should return existing job
      const job2 = await manager.push(QUEUE_NAME, {
        data: { name: 'job2', msg: 'second' },
        uniqueKey: 'extend-key-1',
        dedup: { ttl: 200, extend: true },
      });

      // Should return the same job (not create new)
      expect(job2.id).toBe(job1.id);
    });
  });

  describe('Replace strategy', () => {
    test('replaces job on duplicate', async () => {
      const manager = getSharedManager();

      // Add first job
      const job1 = await manager.push(QUEUE_NAME, {
        data: { name: 'job1', msg: 'first', value: 1 },
        uniqueKey: 'replace-key-1',
        dedup: { ttl: 5000 },
      });

      // Add replacement
      const job2 = await manager.push(QUEUE_NAME, {
        data: { name: 'job2', msg: 'replaced', value: 2 },
        uniqueKey: 'replace-key-1',
        dedup: { ttl: 5000, replace: true },
      });

      // Should be different job IDs
      expect(job2.id).not.toBe(job1.id);

      // Original job should no longer exist
      const originalJob = await manager.getJob(job1.id);
      expect(originalJob).toBeNull();

      // New job should exist
      const newJob = await manager.getJob(job2.id);
      expect(newJob).toBeDefined();
      expect((newJob?.data as { value: number }).value).toBe(2);
    });
  });

  describe('Mixed scenarios', () => {
    test('default behavior without dedup option returns existing job', async () => {
      const manager = getSharedManager();

      const job1 = await manager.push(QUEUE_NAME, {
        data: { name: 'job1', msg: 'first' },
        uniqueKey: 'simple-key',
      });

      // Should return existing job (BullMQ-style)
      const job2 = await manager.push(QUEUE_NAME, {
        data: { name: 'job2', msg: 'second' },
        uniqueKey: 'simple-key',
      });

      expect(job1).toBeDefined();
      expect(job2.id).toBe(job1.id);
    });
  });
});
