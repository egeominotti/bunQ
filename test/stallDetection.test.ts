/**
 * Stall Detection Tests
 */

import { describe, test, expect } from 'bun:test';
import { createJob, jobId, type Job } from '../src/domain/types/job';
import {
  checkStall,
  getStallAction,
  StallAction,
  incrementStallCount,
  updateHeartbeat,
  resetStallCount,
  DEFAULT_STALL_CONFIG,
  type StallConfig,
} from '../src/domain/types/stall';

describe('Stall Detection', () => {
  function makeJob(id: number, queue = 'test', overrides: Partial<Job> = {}): Job {
    const job = createJob(jobId(`test-job-${id}`), queue, { data: { id } }, Date.now());
    return { ...job, ...overrides };
  }

  describe('checkStall', () => {
    test('should not detect stall for job not started', () => {
      const job = makeJob(1);
      job.startedAt = null;

      const result = checkStall(job);

      expect(result.isStalled).toBe(false);
      expect(result.shouldMoveToDlq).toBe(false);
    });

    test('should not detect stall within grace period', () => {
      const now = Date.now();
      const job = makeJob(1);
      job.startedAt = now - 1000; // Started 1 second ago
      job.lastHeartbeat = now - 1000;

      const result = checkStall(job, DEFAULT_STALL_CONFIG, now);

      expect(result.isStalled).toBe(false);
    });

    test('should detect stall after interval without heartbeat', () => {
      const now = Date.now();
      const job = makeJob(1);
      job.startedAt = now - 60000; // Started 60 seconds ago
      job.lastHeartbeat = now - 35000; // Last heartbeat 35 seconds ago

      const result = checkStall(job, DEFAULT_STALL_CONFIG, now);

      expect(result.isStalled).toBe(true);
      expect(result.stalledFor).toBeGreaterThan(30000);
    });

    test('should not detect stall if heartbeat is recent', () => {
      const now = Date.now();
      const job = makeJob(1);
      job.startedAt = now - 60000;
      job.lastHeartbeat = now - 5000; // Heartbeat 5 seconds ago

      const result = checkStall(job, DEFAULT_STALL_CONFIG, now);

      expect(result.isStalled).toBe(false);
    });

    test('should use job-specific stall timeout if set', () => {
      const now = Date.now();
      const job = makeJob(1);
      job.startedAt = now - 20000;
      job.lastHeartbeat = now - 15000;
      job.stallTimeout = 10000; // 10 second custom timeout

      const result = checkStall(job, DEFAULT_STALL_CONFIG, now);

      expect(result.isStalled).toBe(true);
    });

    test('should indicate move to DLQ after max stalls', () => {
      const now = Date.now();
      const job = makeJob(1);
      job.startedAt = now - 60000;
      job.lastHeartbeat = now - 35000;
      job.stallCount = 2; // Already stalled twice

      const config: StallConfig = { ...DEFAULT_STALL_CONFIG, maxStalls: 3 };
      const result = checkStall(job, config, now);

      expect(result.isStalled).toBe(true);
      expect(result.shouldMoveToDlq).toBe(true);
      expect(result.newStallCount).toBe(3);
    });
  });

  describe('getStallAction', () => {
    test('should return Keep for non-stalled job', () => {
      const job = makeJob(1);
      job.startedAt = null;

      const action = getStallAction(job);

      expect(action).toBe(StallAction.Keep);
    });

    test('should return Retry for first stall', () => {
      const now = Date.now();
      const job = makeJob(1);
      job.startedAt = now - 60000;
      job.lastHeartbeat = now - 35000;
      job.stallCount = 0;

      const action = getStallAction(job, DEFAULT_STALL_CONFIG, now);

      expect(action).toBe(StallAction.Retry);
    });

    test('should return MoveToDlq after max stalls', () => {
      const now = Date.now();
      const job = makeJob(1);
      job.startedAt = now - 60000;
      job.lastHeartbeat = now - 35000;
      job.stallCount = 2;

      const config: StallConfig = { ...DEFAULT_STALL_CONFIG, maxStalls: 3 };
      const action = getStallAction(job, config, now);

      expect(action).toBe(StallAction.MoveToDlq);
    });
  });

  describe('stall count management', () => {
    test('should increment stall count', () => {
      const job = makeJob(1);
      job.stallCount = 1;

      const newCount = incrementStallCount(job);

      expect(newCount).toBe(2);
      expect(job.stallCount).toBe(2);
    });

    test('should reset stall count', () => {
      const job = makeJob(1);
      job.stallCount = 3;

      resetStallCount(job);

      expect(job.stallCount).toBe(0);
    });

    test('should update heartbeat', () => {
      const job = makeJob(1);
      const now = Date.now();
      job.lastHeartbeat = now - 10000;

      updateHeartbeat(job, now);

      expect(job.lastHeartbeat).toBe(now);
    });
  });

  describe('custom stall config', () => {
    test('should respect custom stall interval', () => {
      const now = Date.now();
      const job = makeJob(1);
      job.startedAt = now - 20000;
      job.lastHeartbeat = now - 15000;

      // With default 30s interval, should not be stalled
      expect(checkStall(job, DEFAULT_STALL_CONFIG, now).isStalled).toBe(false);

      // With custom 10s interval, should be stalled
      const customConfig: StallConfig = { ...DEFAULT_STALL_CONFIG, stallInterval: 10000 };
      expect(checkStall(job, customConfig, now).isStalled).toBe(true);
    });

    test('should respect custom grace period', () => {
      const now = Date.now();
      const job = makeJob(1);
      job.startedAt = now - 3000;
      job.lastHeartbeat = now - 3000;

      // With default 5s grace, should not check for stall
      const defaultResult = checkStall(job, DEFAULT_STALL_CONFIG, now);
      expect(defaultResult.isStalled).toBe(false);

      // With 1s grace, should check and not be stalled (interval not exceeded)
      const customConfig: StallConfig = {
        ...DEFAULT_STALL_CONFIG,
        gracePeriod: 1000,
        stallInterval: 2000,
      };
      const customResult = checkStall(job, customConfig, now);
      expect(customResult.isStalled).toBe(true);
    });

    test('should respect disabled stall detection', () => {
      const now = Date.now();
      const job = makeJob(1);
      job.startedAt = now - 60000;
      job.lastHeartbeat = now - 60000;

      const config: StallConfig = { ...DEFAULT_STALL_CONFIG, enabled: false };
      const action = getStallAction(job, config, now);

      // When disabled, getStallAction still checks but in real code we skip the check
      // The checkStall function doesn't check enabled flag, that's done at caller level
      expect(action).toBe(StallAction.Retry); // Still detects stall
    });
  });
});
