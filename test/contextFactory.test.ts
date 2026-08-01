/**
 * ContextFactory Tests
 * Tests for building context objects from QueueManager dependencies
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import {
  ContextFactory,
  type ContextDependencies,
  type ContextCallbacks,
} from '../src/application/contextFactory';
import { Shard } from '../src/domain/queue/shard';
import { RWLock } from '../src/shared/lock';
import { LRUMap, BoundedSet, BoundedMap } from '../src/shared/lru';
import { SHARD_COUNT } from '../src/shared/hash';
import { DEFAULT_CONFIG } from '../src/application/types';
import { EventsManager } from '../src/application/eventsManager';
import { WebhookManager } from '../src/application/webhookManager';
import type { JobId } from '../src/domain/types/job';
import type { Job } from '../src/domain/types/job';
import type { JobLocation } from '../src/domain/types/queue';
import type { JobLogEntry } from '../src/domain/types/worker';
import type { JobLock } from '../src/domain/types/job';

/**
 * Creates a minimal set of ContextDependencies for testing
 * without any actual SQLite storage.
 */
function createTestDependencies(overrides?: Partial<ContextDependencies>): ContextDependencies {
  const shards: Shard[] = [];
  const shardLocks: RWLock[] = [];
  const processingShards: Map<JobId, Job>[] = [];
  const processingLocks: RWLock[] = [];

  for (let i = 0; i < SHARD_COUNT; i++) {
    shards.push(new Shard());
    shardLocks.push(new RWLock());
    processingShards.push(new Map());
    processingLocks.push(new RWLock());
  }

  const webhookManager = new WebhookManager();
  const eventsManager = new EventsManager(webhookManager);

  return {
    config: { ...DEFAULT_CONFIG },
    storage: null,
    shards,
    shardLocks,
    processingShards,
    processingLocks,
    jobIndex: new Map<JobId, JobLocation>(),
    completedJobs: new BoundedSet<JobId>(DEFAULT_CONFIG.maxCompletedJobs),
    completedJobsData: new BoundedMap<JobId, Job>(DEFAULT_CONFIG.maxCompletedJobs),
    jobResults: new BoundedMap<JobId, unknown>(DEFAULT_CONFIG.maxJobResults),
    customIdMap: new LRUMap<string, JobId>(DEFAULT_CONFIG.maxCustomIds),
    jobLogs: new LRUMap<JobId, JobLogEntry[]>(DEFAULT_CONFIG.maxJobLogs),
    jobLocks: new Map<JobId, JobLock>(),
    clientJobs: new Map<string, Set<JobId>>(),
    stalledCandidates: new Set<JobId>(),
    pendingDepChecks: new Set<JobId>(),
    queueNamesCache: new Set<string>(),
    eventsManager,
    webhookManager,
    metrics: {
      totalPushed: { value: 0n },
      totalPulled: { value: 0n },
      totalCompleted: { value: 0n },
      totalFailed: { value: 0n },
    },
    startTime: Date.now(),
    maxLogsPerJob: 100,
    ...overrides,
  };
}

/**
 * Creates a minimal set of ContextCallbacks for testing.
 */
function createTestCallbacks(overrides?: Partial<ContextCallbacks>): ContextCallbacks {
  return {
    fail: async () => {},
    registerQueueName: () => {},
    unregisterQueueName: () => {},
    onJobCompleted: () => {},
    onJobsCompleted: () => {},
    hasPendingDeps: () => false,
    onRepeat: () => {},
    ...overrides,
  };
}

describe('ContextFactory', () => {
  let deps: ContextDependencies;
  let callbacks: ContextCallbacks;
  let factory: ContextFactory;

  beforeEach(() => {
    deps = createTestDependencies();
    callbacks = createTestCallbacks();
    factory = new ContextFactory(deps, callbacks);
  });

  // ============ Constructor ============

  describe('constructor', () => {
    test('should create factory with dependencies and callbacks', () => {
      expect(factory).toBeDefined();
      expect(factory).toBeInstanceOf(ContextFactory);
    });

    test('should create factory with custom config values', () => {
      const customDeps = createTestDependencies({
        config: { ...DEFAULT_CONFIG, maxCompletedJobs: 100, dataPath: '/tmp/test.db' },
      });
      const f = new ContextFactory(customDeps, callbacks);
      const ctx = f.getBackgroundContext();
      expect(ctx.config.maxCompletedJobs).toBe(100);
      expect(ctx.config.dataPath).toBe('/tmp/test.db');
    });
  });

  // ============ getLockContext ============

  describe('getLockContext', () => {
    test('should return context with jobIndex', () => {
      const ctx = factory.getLockContext();
      expect(ctx.jobIndex).toBe(deps.jobIndex);
    });

    test('should return context with jobLocks', () => {
      const ctx = factory.getLockContext();
      expect(ctx.jobLocks).toBe(deps.jobLocks);
    });

    test('should return context with clientJobs', () => {
      const ctx = factory.getLockContext();
      expect(ctx.clientJobs).toBe(deps.clientJobs);
    });

    test('should return context with processingShards', () => {
      const ctx = factory.getLockContext();
      expect(ctx.processingShards).toBe(deps.processingShards);
      expect(ctx.processingShards.length).toBe(SHARD_COUNT);
    });

    test('should return context with processingLocks', () => {
      const ctx = factory.getLockContext();
      expect(ctx.processingLocks).toBe(deps.processingLocks);
      expect(ctx.processingLocks.length).toBe(SHARD_COUNT);
    });

    test('should return context with shards', () => {
      const ctx = factory.getLockContext();
      expect(ctx.shards).toBe(deps.shards);
      expect(ctx.shards.length).toBe(SHARD_COUNT);
    });

    test('should return context with shardLocks', () => {
      const ctx = factory.getLockContext();
      expect(ctx.shardLocks).toBe(deps.shardLocks);
      expect(ctx.shardLocks.length).toBe(SHARD_COUNT);
    });

    test('should return context with eventsManager', () => {
      const ctx = factory.getLockContext();
      expect(ctx.eventsManager).toBe(deps.eventsManager);
    });

    test('should return same references on multiple calls', () => {
      const ctx1 = factory.getLockContext();
      const ctx2 = factory.getLockContext();
      expect(ctx1.jobIndex).toBe(ctx2.jobIndex);
      expect(ctx1.shards).toBe(ctx2.shards);
    });

    test('should not include unrelated fields', () => {
      const ctx = factory.getLockContext();
      // storage is included in LockContext (added for #73 cron cleanup)
      expect((ctx as any).completedJobs).toBeUndefined();
      expect((ctx as any).jobResults).toBeUndefined();
    });
  });

  // ============ getBackgroundContext ============

  describe('getBackgroundContext', () => {
    test('should return context with config', () => {
      const ctx = factory.getBackgroundContext();
      expect(ctx.config).toBe(deps.config);
    });

    test('should return context with storage (null)', () => {
      const ctx = factory.getBackgroundContext();
      expect(ctx.storage).toBeNull();
    });

    test('should return context with shards and shardLocks', () => {
      const ctx = factory.getBackgroundContext();
      expect(ctx.shards).toBe(deps.shards);
      expect(ctx.shardLocks).toBe(deps.shardLocks);
    });

    test('should return context with processingShards and processingLocks', () => {
      const ctx = factory.getBackgroundContext();
      expect(ctx.processingShards).toBe(deps.processingShards);
      expect(ctx.processingLocks).toBe(deps.processingLocks);
    });

    test('should return context with jobIndex', () => {
      const ctx = factory.getBackgroundContext();
      expect(ctx.jobIndex).toBe(deps.jobIndex);
    });

    test('should return context with completedJobs', () => {
      const ctx = factory.getBackgroundContext();
      expect(ctx.completedJobs).toBe(deps.completedJobs);
    });

    test('should return context with jobResults', () => {
      const ctx = factory.getBackgroundContext();
      expect(ctx.jobResults).toBe(deps.jobResults);
    });

    test('should return context with customIdMap', () => {
      const ctx = factory.getBackgroundContext();
      expect(ctx.customIdMap).toBe(deps.customIdMap);
    });

    test('should return context with jobLogs', () => {
      const ctx = factory.getBackgroundContext();
      expect(ctx.jobLogs).toBe(deps.jobLogs);
    });

    test('should return context with jobLocks', () => {
      const ctx = factory.getBackgroundContext();
      expect(ctx.jobLocks).toBe(deps.jobLocks);
    });

    test('should return context with clientJobs', () => {
      const ctx = factory.getBackgroundContext();
      expect(ctx.clientJobs).toBe(deps.clientJobs);
    });

    test('should return context with stalledCandidates', () => {
      const ctx = factory.getBackgroundContext();
      expect(ctx.stalledCandidates).toBe(deps.stalledCandidates);
    });

    test('should return context with pendingDepChecks', () => {
      const ctx = factory.getBackgroundContext();
      expect(ctx.pendingDepChecks).toBe(deps.pendingDepChecks);
    });

    test('should return context with queueNamesCache', () => {
      const ctx = factory.getBackgroundContext();
      expect(ctx.queueNamesCache).toBe(deps.queueNamesCache);
    });

    test('should return context with eventsManager', () => {
      const ctx = factory.getBackgroundContext();
      expect(ctx.eventsManager).toBe(deps.eventsManager);
    });

    test('should return context with webhookManager', () => {
      const ctx = factory.getBackgroundContext();
      expect(ctx.webhookManager).toBe(deps.webhookManager);
    });

    test('should return context with metrics', () => {
      const ctx = factory.getBackgroundContext();
      expect(ctx.metrics).toBe(deps.metrics);
      expect(ctx.metrics.totalPushed.value).toBe(0n);
    });

    test('should return context with startTime', () => {
      const ctx = factory.getBackgroundContext();
      expect(ctx.startTime).toBe(deps.startTime);
      expect(typeof ctx.startTime).toBe('number');
    });

    test('should return context with fail callback', () => {
      const ctx = factory.getBackgroundContext();
      expect(typeof ctx.fail).toBe('function');
    });

    test('should call the fail callback provided', async () => {
      let failCalled = false;
      let failArgs: any[] = [];
      const customCallbacks = createTestCallbacks({
        fail: async (jobId, error) => {
          failCalled = true;
          failArgs = [jobId, error];
        },
      });
      const f = new ContextFactory(deps, customCallbacks);
      const ctx = f.getBackgroundContext();
      await ctx.fail('test-job-123' as JobId, 'some error');
      expect(failCalled).toBe(true);
      expect(failArgs[0]).toBe('test-job-123');
      expect(failArgs[1]).toBe('some error');
    });

    test('should return context with registerQueueName callback', () => {
      const ctx = factory.getBackgroundContext();
      expect(typeof ctx.registerQueueName).toBe('function');
    });

    test('should call registerQueueName callback', () => {
      let calledWith: string | undefined;
      const customCallbacks = createTestCallbacks({
        registerQueueName: (queue) => {
          calledWith = queue;
        },
      });
      const f = new ContextFactory(deps, customCallbacks);
      const ctx = f.getBackgroundContext();
      ctx.registerQueueName('my-queue');
      expect(calledWith).toBe('my-queue');
    });

    test('should return context with unregisterQueueName callback', () => {
      const ctx = factory.getBackgroundContext();
      expect(typeof ctx.unregisterQueueName).toBe('function');
    });

    test('should call unregisterQueueName callback', () => {
      let calledWith: string | undefined;
      const customCallbacks = createTestCallbacks({
        unregisterQueueName: (queue) => {
          calledWith = queue;
        },
      });
      const f = new ContextFactory(deps, customCallbacks);
      const ctx = f.getBackgroundContext();
      ctx.unregisterQueueName('old-queue');
      expect(calledWith).toBe('old-queue');
    });
  });

  // ============ getStatsContext ============

  describe('getStatsContext', () => {
    test('should return context with shards', () => {
      const ctx = factory.getStatsContext();
      expect(ctx.shards).toBe(deps.shards);
    });

    test('should return context with processingShards', () => {
      const ctx = factory.getStatsContext();
      expect(ctx.processingShards).toBe(deps.processingShards);
    });

    test('should return context with completedJobs', () => {
      const ctx = factory.getStatsContext();
      expect(ctx.completedJobs).toBe(deps.completedJobs);
    });

    test('should return context with jobIndex', () => {
      const ctx = factory.getStatsContext();
      expect(ctx.jobIndex).toBe(deps.jobIndex);
    });

    test('should return context with jobResults', () => {
      const ctx = factory.getStatsContext();
      expect(ctx.jobResults).toBe(deps.jobResults);
    });

    test('should return context with jobLogs', () => {
      const ctx = factory.getStatsContext();
      expect(ctx.jobLogs).toBe(deps.jobLogs);
    });

    test('should return context with customIdMap', () => {
      const ctx = factory.getStatsContext();
      expect(ctx.customIdMap).toBe(deps.customIdMap);
    });

    test('should return context with jobLocks', () => {
      const ctx = factory.getStatsContext();
      expect(ctx.jobLocks).toBe(deps.jobLocks);
    });

    test('should return context with clientJobs', () => {
      const ctx = factory.getStatsContext();
      expect(ctx.clientJobs).toBe(deps.clientJobs);
    });

    test('should return context with pendingDepChecks', () => {
      const ctx = factory.getStatsContext();
      expect(ctx.pendingDepChecks).toBe(deps.pendingDepChecks);
    });

    test('should return context with stalledCandidates', () => {
      const ctx = factory.getStatsContext();
      expect(ctx.stalledCandidates).toBe(deps.stalledCandidates);
    });

    test('should return context with metrics', () => {
      const ctx = factory.getStatsContext();
      expect(ctx.metrics).toBe(deps.metrics);
    });

    test('should return context with startTime', () => {
      const ctx = factory.getStatsContext();
      expect(ctx.startTime).toBe(deps.startTime);
    });

    test('should not include unrelated fields like storage or callbacks', () => {
      const ctx = factory.getStatsContext();
      expect((ctx as any).storage).toBeUndefined();
      expect((ctx as any).fail).toBeUndefined();
      expect((ctx as any).webhookManager).toBeUndefined();
    });
  });

  // ============ getPushContext ============

  describe('getPushContext', () => {
    test('should return context with storage', () => {
      const ctx = factory.getPushContext();
      expect(ctx.storage).toBe(deps.storage);
    });

    test('should return context with shards', () => {
      const ctx = factory.getPushContext();
      expect(ctx.shards).toBe(deps.shards);
    });

    test('should return context with shardLocks', () => {
      const ctx = factory.getPushContext();
      expect(ctx.shardLocks).toBe(deps.shardLocks);
    });

    test('should return context with completedJobs', () => {
      const ctx = factory.getPushContext();
      expect(ctx.completedJobs).toBe(deps.completedJobs);
    });

    test('should return context with customIdMap', () => {
      const ctx = factory.getPushContext();
      expect(ctx.customIdMap).toBe(deps.customIdMap);
    });

    test('should return context with jobIndex', () => {
      const ctx = factory.getPushContext();
      expect(ctx.jobIndex).toBe(deps.jobIndex);
    });

    test('should return context with totalPushed metric', () => {
      const ctx = factory.getPushContext();
      expect(ctx.totalPushed).toBe(deps.metrics.totalPushed);
      expect(ctx.totalPushed.value).toBe(0n);
    });

    test('should return context with broadcast function', () => {
      const ctx = factory.getPushContext();
      expect(typeof ctx.broadcast).toBe('function');
    });

    test('broadcast should be bound to eventsManager', () => {
      // Verify that broadcast is a function (bound method from eventsManager)
      const ctx = factory.getPushContext();
      // Calling broadcast should not throw
      expect(() => {
        ctx.broadcast({
          eventType: 0 as any, // EventType.Pushed
          queue: 'test',
          jobId: 'test-id' as JobId,
          timestamp: Date.now(),
        });
      }).not.toThrow();
    });

    test('should not include processing-related fields', () => {
      const ctx = factory.getPushContext();
      expect((ctx as any).processingShards).toBeUndefined();
      expect((ctx as any).processingLocks).toBeUndefined();
    });
  });

  // ============ getPullContext ============

  describe('getPullContext', () => {
    test('should return context with storage', () => {
      const ctx = factory.getPullContext();
      expect(ctx.storage).toBe(deps.storage);
    });

    test('should return context with shards', () => {
      const ctx = factory.getPullContext();
      expect(ctx.shards).toBe(deps.shards);
    });

    test('should return context with shardLocks', () => {
      const ctx = factory.getPullContext();
      expect(ctx.shardLocks).toBe(deps.shardLocks);
    });

    test('should return context with processingShards', () => {
      const ctx = factory.getPullContext();
      expect(ctx.processingShards).toBe(deps.processingShards);
    });

    test('should return context with processingLocks', () => {
      const ctx = factory.getPullContext();
      expect(ctx.processingLocks).toBe(deps.processingLocks);
    });

    test('should return context with jobIndex', () => {
      const ctx = factory.getPullContext();
      expect(ctx.jobIndex).toBe(deps.jobIndex);
    });

    test('should return context with totalPulled metric', () => {
      const ctx = factory.getPullContext();
      expect(ctx.totalPulled).toBe(deps.metrics.totalPulled);
      expect(ctx.totalPulled.value).toBe(0n);
    });

    test('should return context with broadcast function', () => {
      const ctx = factory.getPullContext();
      expect(typeof ctx.broadcast).toBe('function');
    });

    test('should not include completedJobs or jobResults', () => {
      const ctx = factory.getPullContext();
      expect((ctx as any).completedJobs).toBeUndefined();
      expect((ctx as any).jobResults).toBeUndefined();
    });
  });

  // ============ getAckContext ============

  describe('getAckContext', () => {
    test('should return context with storage', () => {
      const ctx = factory.getAckContext();
      expect(ctx.storage).toBe(deps.storage);
    });

    test('should return context with shards', () => {
      const ctx = factory.getAckContext();
      expect(ctx.shards).toBe(deps.shards);
    });

    test('should return context with shardLocks', () => {
      const ctx = factory.getAckContext();
      expect(ctx.shardLocks).toBe(deps.shardLocks);
    });

    test('should return context with processingShards', () => {
      const ctx = factory.getAckContext();
      expect(ctx.processingShards).toBe(deps.processingShards);
    });

    test('should return context with processingLocks', () => {
      const ctx = factory.getAckContext();
      expect(ctx.processingLocks).toBe(deps.processingLocks);
    });

    test('should return context with completedJobs', () => {
      const ctx = factory.getAckContext();
      expect(ctx.completedJobs).toBe(deps.completedJobs);
    });

    test('should return context with jobResults', () => {
      const ctx = factory.getAckContext();
      expect(ctx.jobResults).toBe(deps.jobResults);
    });

    test('should return context with jobIndex', () => {
      const ctx = factory.getAckContext();
      expect(ctx.jobIndex).toBe(deps.jobIndex);
    });

    test('should return context with customIdMap', () => {
      const ctx = factory.getAckContext();
      expect(ctx.customIdMap).toBe(deps.customIdMap);
    });

    test('should return context with totalCompleted metric', () => {
      const ctx = factory.getAckContext();
      expect(ctx.totalCompleted).toBe(deps.metrics.totalCompleted);
      expect(ctx.totalCompleted.value).toBe(0n);
    });

    test('should return context with totalFailed metric', () => {
      const ctx = factory.getAckContext();
      expect(ctx.totalFailed).toBe(deps.metrics.totalFailed);
      expect(ctx.totalFailed.value).toBe(0n);
    });

    test('should return context with broadcast function', () => {
      const ctx = factory.getAckContext();
      expect(typeof ctx.broadcast).toBe('function');
    });

    test('should return context with onJobCompleted callback', () => {
      const ctx = factory.getAckContext();
      expect(typeof ctx.onJobCompleted).toBe('function');
    });

    test('should call onJobCompleted callback from callbacks', () => {
      let completedId: JobId | undefined;
      const customCallbacks = createTestCallbacks({
        onJobCompleted: (id) => {
          completedId = id;
        },
      });
      const f = new ContextFactory(deps, customCallbacks);
      const ctx = f.getAckContext();
      ctx.onJobCompleted('job-42' as JobId);
      expect(completedId).toBe('job-42');
    });

    test('should return context with onJobsCompleted callback', () => {
      const ctx = factory.getAckContext();
      expect(typeof ctx.onJobsCompleted).toBe('function');
    });

    test('should call onJobsCompleted callback from callbacks', () => {
      let completedIds: JobId[] = [];
      const customCallbacks = createTestCallbacks({
        onJobsCompleted: (ids) => {
          completedIds = ids;
        },
      });
      const f = new ContextFactory(deps, customCallbacks);
      const ctx = f.getAckContext();
      ctx.onJobsCompleted!(['job-1' as JobId, 'job-2' as JobId]);
      expect(completedIds).toEqual(['job-1', 'job-2']);
    });

    test('should return context with needsBroadcast function', () => {
      const ctx = factory.getAckContext();
      expect(typeof ctx.needsBroadcast).toBe('function');
    });

    test('needsBroadcast should return false when no subscribers or webhooks', () => {
      const ctx = factory.getAckContext();
      // No subscribers registered, no webhooks
      expect(ctx.needsBroadcast!()).toBe(false);
    });

    test('should return context with hasPendingDeps callback', () => {
      const ctx = factory.getAckContext();
      expect(typeof ctx.hasPendingDeps).toBe('function');
    });

    test('hasPendingDeps should return the value from callbacks', () => {
      const customCallbacks = createTestCallbacks({
        hasPendingDeps: () => true,
      });
      const f = new ContextFactory(deps, customCallbacks);
      const ctx = f.getAckContext();
      expect(ctx.hasPendingDeps!()).toBe(true);
    });

    test('should return context with onRepeat callback', () => {
      const ctx = factory.getAckContext();
      expect(typeof ctx.onRepeat).toBe('function');
    });

    test('should call onRepeat callback from callbacks', () => {
      let repeatJob: Job | undefined;
      const customCallbacks = createTestCallbacks({
        onRepeat: (job) => {
          repeatJob = job;
        },
      });
      const f = new ContextFactory(deps, customCallbacks);
      const ctx = f.getAckContext();
      const fakeJob = { id: 'j1' as JobId, queue: 'q1' } as Job;
      ctx.onRepeat!(fakeJob);
      expect(repeatJob).toBe(fakeJob);
    });
  });

  // ============ getJobMgmtContext ============

  describe('getJobMgmtContext', () => {
    test('should return context with storage', () => {
      const ctx = factory.getJobMgmtContext();
      expect(ctx.storage).toBe(deps.storage);
    });

    test('should return context with shards', () => {
      const ctx = factory.getJobMgmtContext();
      expect(ctx.shards).toBe(deps.shards);
    });

    test('should return context with shardLocks', () => {
      const ctx = factory.getJobMgmtContext();
      expect(ctx.shardLocks).toBe(deps.shardLocks);
    });

    test('should return context with processingShards', () => {
      const ctx = factory.getJobMgmtContext();
      expect(ctx.processingShards).toBe(deps.processingShards);
    });

    test('should return context with processingLocks', () => {
      const ctx = factory.getJobMgmtContext();
      expect(ctx.processingLocks).toBe(deps.processingLocks);
    });

    test('should return context with jobIndex', () => {
      const ctx = factory.getJobMgmtContext();
      expect(ctx.jobIndex).toBe(deps.jobIndex);
    });

    test('should return context with webhookManager', () => {
      const ctx = factory.getJobMgmtContext();
      expect(ctx.webhookManager).toBe(deps.webhookManager);
    });

    test('should return context with eventsManager', () => {
      const ctx = factory.getJobMgmtContext();
      expect(ctx.eventsManager).toBe(deps.eventsManager);
    });

    test('should not include completedJobs or jobResults', () => {
      const ctx = factory.getJobMgmtContext();
      expect((ctx as any).completedJobs).toBeUndefined();
      expect((ctx as any).jobResults).toBeUndefined();
    });
  });

  // ============ getQueryContext ============

  describe('getQueryContext', () => {
    test('should return context with storage', () => {
      const ctx = factory.getQueryContext();
      expect(ctx.storage).toBe(deps.storage);
    });

    test('should return context with shards', () => {
      const ctx = factory.getQueryContext();
      expect(ctx.shards).toBe(deps.shards);
    });

    test('should return context with shardLocks', () => {
      const ctx = factory.getQueryContext();
      expect(ctx.shardLocks).toBe(deps.shardLocks);
    });

    test('should return context with processingShards', () => {
      const ctx = factory.getQueryContext();
      expect(ctx.processingShards).toBe(deps.processingShards);
    });

    test('should return context with processingLocks', () => {
      const ctx = factory.getQueryContext();
      expect(ctx.processingLocks).toBe(deps.processingLocks);
    });

    test('should return context with jobIndex', () => {
      const ctx = factory.getQueryContext();
      expect(ctx.jobIndex).toBe(deps.jobIndex);
    });

    test('should return context with completedJobs', () => {
      const ctx = factory.getQueryContext();
      expect(ctx.completedJobs).toBe(deps.completedJobs);
    });

    test('should return context with jobResults', () => {
      const ctx = factory.getQueryContext();
      expect(ctx.jobResults).toBe(deps.jobResults);
    });

    test('should return context with customIdMap', () => {
      const ctx = factory.getQueryContext();
      expect(ctx.customIdMap).toBe(deps.customIdMap);
    });

    test('should not include eventsManager or webhookManager', () => {
      const ctx = factory.getQueryContext();
      expect((ctx as any).eventsManager).toBeUndefined();
      expect((ctx as any).webhookManager).toBeUndefined();
    });
  });

  // ============ getDlqContext ============

  describe('getDlqContext', () => {
    test('should return context with shards', () => {
      const ctx = factory.getDlqContext();
      expect(ctx.shards).toBe(deps.shards);
    });

    test('should return context with jobIndex', () => {
      const ctx = factory.getDlqContext();
      expect(ctx.jobIndex).toBe(deps.jobIndex);
    });

    test('should return context with storage', () => {
      const ctx = factory.getDlqContext();
      expect(ctx.storage).toBe(deps.storage);
    });

    test('should include terminal-job auxiliary indexes', () => {
      const ctx = factory.getDlqContext();
      const keys = Object.keys(ctx);
      expect(keys).toContain('shards');
      expect(keys).toContain('jobIndex');
      expect(keys).toContain('jobResults');
      expect(keys).toContain('jobLogs');
      expect(keys).toContain('storage');
      expect(keys.length).toBe(5);
    });
  });

  // ============ getRetryCompletedContext ============

  describe('getRetryCompletedContext', () => {
    test('should return context with shards', () => {
      const ctx = factory.getRetryCompletedContext();
      expect(ctx.shards).toBe(deps.shards);
    });

    test('should return context with jobIndex', () => {
      const ctx = factory.getRetryCompletedContext();
      expect(ctx.jobIndex).toBe(deps.jobIndex);
    });

    test('should return context with storage', () => {
      const ctx = factory.getRetryCompletedContext();
      expect(ctx.storage).toBe(deps.storage);
    });

    test('should return context with completedJobs', () => {
      const ctx = factory.getRetryCompletedContext();
      expect(ctx.completedJobs).toBe(deps.completedJobs);
    });

    test('should return context with jobResults', () => {
      const ctx = factory.getRetryCompletedContext();
      expect(ctx.jobResults).toBe(deps.jobResults);
    });

    test('should extend DlqContext with completed job data and results', () => {
      const ctx = factory.getRetryCompletedContext();
      const keys = Object.keys(ctx);
      expect(keys.length).toBe(7);
      expect(keys).toContain('shards');
      expect(keys).toContain('jobIndex');
      expect(keys).toContain('jobLogs');
      expect(keys).toContain('storage');
      expect(keys).toContain('completedJobs');
      expect(keys).toContain('completedJobsData');
      expect(keys).toContain('jobResults');
    });
  });

  // ============ getLogsContext ============

  describe('getLogsContext', () => {
    test('should return context with jobIndex', () => {
      const ctx = factory.getLogsContext();
      expect(ctx.jobIndex).toBe(deps.jobIndex);
    });

    test('should return context with jobLogs', () => {
      const ctx = factory.getLogsContext();
      expect(ctx.jobLogs).toBe(deps.jobLogs);
    });

    test('should return context with maxLogsPerJob', () => {
      const ctx = factory.getLogsContext();
      expect(ctx.maxLogsPerJob).toBe(100);
    });

    test('should use custom maxLogsPerJob value', () => {
      const customDeps = createTestDependencies({ maxLogsPerJob: 50 });
      const f = new ContextFactory(customDeps, callbacks);
      const ctx = f.getLogsContext();
      expect(ctx.maxLogsPerJob).toBe(50);
    });

    test('should have exactly 3 fields', () => {
      const ctx = factory.getLogsContext();
      const keys = Object.keys(ctx);
      expect(keys.length).toBe(3);
    });
  });

  // ============ getQueueControlContext ============

  describe('getQueueControlContext', () => {
    test('should return context with shards', () => {
      const ctx = factory.getQueueControlContext();
      expect(ctx.shards).toBe(deps.shards);
    });

    test('should return context with jobIndex', () => {
      const ctx = factory.getQueueControlContext();
      expect(ctx.jobIndex).toBe(deps.jobIndex);
    });

    test('should include stores needed to clean completed/failed/active jobs', () => {
      const ctx = factory.getQueueControlContext();
      expect(ctx.processingShards).toBe(deps.processingShards);
      expect(ctx.completedJobs).toBe(deps.completedJobs);
      expect(ctx.completedJobsData).toBe(deps.completedJobsData);
      expect(ctx.jobResults).toBe(deps.jobResults);
      expect(ctx.jobLogs).toBe(deps.jobLogs);
      expect(ctx.storage).toBe(deps.storage);
    });

    test('should not include locks', () => {
      const ctx = factory.getQueueControlContext();
      expect((ctx as any).shardLocks).toBeUndefined();
      expect((ctx as any).processingLocks).toBeUndefined();
    });
  });

  // ============ Reference Identity Tests ============

  describe('reference identity across contexts', () => {
    test('shards should be the same reference across all contexts', () => {
      const push = factory.getPushContext();
      const pull = factory.getPullContext();
      const ack = factory.getAckContext();
      const lock = factory.getLockContext();
      const bg = factory.getBackgroundContext();
      const stats = factory.getStatsContext();
      const jobMgmt = factory.getJobMgmtContext();
      const query = factory.getQueryContext();
      const dlq = factory.getDlqContext();
      const retry = factory.getRetryCompletedContext();
      const queueCtrl = factory.getQueueControlContext();

      const sameRef = deps.shards;
      expect(push.shards).toBe(sameRef);
      expect(pull.shards).toBe(sameRef);
      expect(ack.shards).toBe(sameRef);
      expect(lock.shards).toBe(sameRef);
      expect(bg.shards).toBe(sameRef);
      expect(stats.shards).toBe(sameRef);
      expect(jobMgmt.shards).toBe(sameRef);
      expect(query.shards).toBe(sameRef);
      expect(dlq.shards).toBe(sameRef);
      expect(retry.shards).toBe(sameRef);
      expect(queueCtrl.shards).toBe(sameRef);
    });

    test('jobIndex should be the same reference across all contexts that use it', () => {
      const push = factory.getPushContext();
      const pull = factory.getPullContext();
      const ack = factory.getAckContext();
      const lock = factory.getLockContext();
      const bg = factory.getBackgroundContext();
      const stats = factory.getStatsContext();
      const jobMgmt = factory.getJobMgmtContext();
      const query = factory.getQueryContext();
      const dlq = factory.getDlqContext();
      const logs = factory.getLogsContext();
      const queueCtrl = factory.getQueueControlContext();

      const sameRef = deps.jobIndex;
      expect(push.jobIndex).toBe(sameRef);
      expect(pull.jobIndex).toBe(sameRef);
      expect(ack.jobIndex).toBe(sameRef);
      expect(lock.jobIndex).toBe(sameRef);
      expect(bg.jobIndex).toBe(sameRef);
      expect(stats.jobIndex).toBe(sameRef);
      expect(jobMgmt.jobIndex).toBe(sameRef);
      expect(query.jobIndex).toBe(sameRef);
      expect(dlq.jobIndex).toBe(sameRef);
      expect(logs.jobIndex).toBe(sameRef);
      expect(queueCtrl.jobIndex).toBe(sameRef);
    });

    test('mutations to shared data should be visible across contexts', () => {
      const push = factory.getPushContext();
      const pull = factory.getPullContext();

      // Mutate jobIndex through push context
      push.jobIndex.set('job-1' as JobId, { type: 'queue', shardIdx: 0, queueName: 'test' });

      // Should be visible through pull context
      expect(pull.jobIndex.has('job-1' as JobId)).toBe(true);
      expect(pull.jobIndex.get('job-1' as JobId)?.queueName).toBe('test');
    });

    test('metrics mutations should be visible across contexts', () => {
      const push = factory.getPushContext();
      const stats = factory.getStatsContext();

      // Increment pushed metric via push context
      push.totalPushed.value += 1n;

      // Should be visible through stats context
      expect(stats.metrics.totalPushed.value).toBe(1n);
    });
  });

  // ============ Edge Cases ============

  describe('edge cases', () => {
    test('factory with null storage should propagate null to all contexts', () => {
      const ctx = factory.getPushContext();
      expect(ctx.storage).toBeNull();

      const pullCtx = factory.getPullContext();
      expect(pullCtx.storage).toBeNull();

      const ackCtx = factory.getAckContext();
      expect(ackCtx.storage).toBeNull();

      const bgCtx = factory.getBackgroundContext();
      expect(bgCtx.storage).toBeNull();

      const queryCtx = factory.getQueryContext();
      expect(queryCtx.storage).toBeNull();

      const dlqCtx = factory.getDlqContext();
      expect(dlqCtx.storage).toBeNull();
    });

    test('factory with default config should use DEFAULT_CONFIG values', () => {
      const bgCtx = factory.getBackgroundContext();
      expect(bgCtx.config.maxCompletedJobs).toBe(50_000);
      expect(bgCtx.config.maxJobResults).toBe(10_000);
      expect(bgCtx.config.maxJobLogs).toBe(10_000);
      expect(bgCtx.config.maxCustomIds).toBe(50_000);
      expect(bgCtx.config.cleanupIntervalMs).toBe(10_000);
      expect(bgCtx.config.jobTimeoutCheckMs).toBe(5_000);
      expect(bgCtx.config.dependencyCheckMs).toBe(30_000);
      expect(bgCtx.config.stallCheckMs).toBe(5_000);
      expect(bgCtx.config.dlqMaintenanceMs).toBe(60_000);
    });

    test('factory should work with empty collections', () => {
      expect(deps.jobIndex.size).toBe(0);
      expect(deps.jobLocks.size).toBe(0);
      expect(deps.clientJobs.size).toBe(0);
      expect(deps.stalledCandidates.size).toBe(0);
      expect(deps.pendingDepChecks.size).toBe(0);
      expect(deps.queueNamesCache.size).toBe(0);

      // All contexts should still be creatable
      expect(() => factory.getLockContext()).not.toThrow();
      expect(() => factory.getBackgroundContext()).not.toThrow();
      expect(() => factory.getStatsContext()).not.toThrow();
      expect(() => factory.getPushContext()).not.toThrow();
      expect(() => factory.getPullContext()).not.toThrow();
      expect(() => factory.getAckContext()).not.toThrow();
      expect(() => factory.getJobMgmtContext()).not.toThrow();
      expect(() => factory.getQueryContext()).not.toThrow();
      expect(() => factory.getDlqContext()).not.toThrow();
      expect(() => factory.getRetryCompletedContext()).not.toThrow();
      expect(() => factory.getLogsContext()).not.toThrow();
      expect(() => factory.getQueueControlContext()).not.toThrow();
    });

    test('metrics should start at 0n', () => {
      const stats = factory.getStatsContext();
      expect(stats.metrics.totalPushed.value).toBe(0n);
      expect(stats.metrics.totalPulled.value).toBe(0n);
      expect(stats.metrics.totalCompleted.value).toBe(0n);
      expect(stats.metrics.totalFailed.value).toBe(0n);
    });

    test('startTime should be a recent timestamp', () => {
      const stats = factory.getStatsContext();
      const now = Date.now();
      // startTime should be within the last 5 seconds
      expect(stats.startTime).toBeGreaterThan(now - 5000);
      expect(stats.startTime).toBeLessThanOrEqual(now);
    });

    test('shard count should match SHARD_COUNT', () => {
      const ctx = factory.getPushContext();
      expect(ctx.shards.length).toBe(SHARD_COUNT);
      expect(ctx.shardLocks.length).toBe(SHARD_COUNT);
    });

    test('processing shard count should match SHARD_COUNT', () => {
      const ctx = factory.getPullContext();
      expect(ctx.processingShards.length).toBe(SHARD_COUNT);
      expect(ctx.processingLocks.length).toBe(SHARD_COUNT);
    });

    test('each context method should return a new object', () => {
      const ctx1 = factory.getPushContext();
      const ctx2 = factory.getPushContext();
      // Different objects but same field references
      expect(ctx1).not.toBe(ctx2);
      expect(ctx1.shards).toBe(ctx2.shards);
    });

    test('config without dataPath should have dataPath undefined', () => {
      const bg = factory.getBackgroundContext();
      expect(bg.config.dataPath).toBeUndefined();
    });

    test('config with dataPath should have dataPath set', () => {
      const customDeps = createTestDependencies({
        config: { ...DEFAULT_CONFIG, dataPath: '/data/test.db' },
      });
      const f = new ContextFactory(customDeps, callbacks);
      const bg = f.getBackgroundContext();
      expect(bg.config.dataPath).toBe('/data/test.db');
    });
  });

  // ============ Integration with QueueManager ============

  describe('integration: used through QueueManager', () => {
    // These tests use QueueManager directly to verify contextFactory works
    // end-to-end in its natural environment
    let qm: any; // QueueManager

    beforeEach(async () => {
      const { QueueManager } = await import('../src/application/queueManager');
      qm = new QueueManager();
    });

    afterEach(() => {
      qm?.shutdown();
    });

    test('push/pull through QueueManager should work (verifies PushContext and PullContext)', async () => {
      const job = await qm.push('test-queue', { data: { msg: 'hello' } });
      expect(job).toBeDefined();
      expect(job.queue).toBe('test-queue');

      const pulled = await qm.pull('test-queue');
      expect(pulled).not.toBeNull();
      expect(pulled.data).toEqual({ msg: 'hello' });
    });

    test('ack through QueueManager should work (verifies AckContext)', async () => {
      const job = await qm.push('ack-queue', { data: { value: 42 } });
      const pulled = await qm.pull('ack-queue');
      expect(pulled).not.toBeNull();

      // Ack should not throw
      await expect(qm.ack(pulled!.id, { success: true })).resolves.toBeUndefined();
    });

    test('getJob through QueueManager should work (verifies QueryContext)', async () => {
      const job = await qm.push('query-queue', { data: { x: 1 } });
      const found = await qm.getJob(job.id);
      expect(found).not.toBeNull();
      expect(found!.id).toBe(job.id);
    });

    test('stats through QueueManager should work (verifies StatsContext)', async () => {
      await qm.push('stats-queue', { data: {} });
      const stats = qm.getStats();
      expect(stats).toBeDefined();
      expect(typeof stats.waiting).toBe('number');
    });
  });
});
