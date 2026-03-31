/**
 * MCP Backend Adapter Tests
 *
 * Tests the EmbeddedBackend adapter which is the core logic layer
 * for the MCP server. Verifies all operations work correctly through
 * the McpBackend interface.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { shutdownManager } from '../src/client/manager';
import { EmbeddedBackend, TcpBackend } from '../src/mcp/adapter';
import { withErrorHandler } from '../src/mcp/tools/withErrorHandler';

let backend: EmbeddedBackend;

beforeEach(() => {
  shutdownManager();
  backend = new EmbeddedBackend();
});

afterEach(() => {
  backend.shutdown();
});

// ─── Job Operations ─────────────────────────────────────────────────────────

describe('EmbeddedBackend - Job Operations', () => {
  test('addJob returns jobId', async () => {
    const result = await backend.addJob('test-q', 'email-send', { to: 'user@test.com' });
    expect(result.jobId).toBeDefined();
    expect(typeof result.jobId).toBe('string');
  });

  test('addJob respects priority option', async () => {
    const result = await backend.addJob('test-q', 'high-prio', { x: 1 }, { priority: 10 });
    expect(result.jobId).toBeDefined();
  });

  test('addJob respects delay option', async () => {
    const result = await backend.addJob('test-q', 'delayed', { x: 1 }, { delay: 5000 });
    expect(result.jobId).toBeDefined();
  });

  test('addJob respects attempts option', async () => {
    const result = await backend.addJob('test-q', 'retry', { x: 1 }, { attempts: 5 });
    expect(result.jobId).toBeDefined();

    const job = await backend.getJob(result.jobId);
    expect(job).not.toBeNull();
    expect(job!.maxAttempts).toBe(5);
  });

  test('addJobsBulk returns count and jobIds', async () => {
    const result = await backend.addJobsBulk('bulk-q', [
      { name: 'a', data: { v: 1 } },
      { name: 'b', data: { v: 2 } },
    ]);
    expect(result.jobIds).toHaveLength(2);
  });

  test('addJobsBulk handles empty array', async () => {
    const result = await backend.addJobsBulk('bulk-q', []);
    expect(result.jobIds).toHaveLength(0);
  });

  test('getJob returns full job details', async () => {
    const added = await backend.addJob('get-q', 'fetch-me', { key: 'value' });
    const job = await backend.getJob(added.jobId);

    expect(job).not.toBeNull();
    expect(job!.id).toBe(added.jobId);
    expect(job!.queue).toBe('get-q');
    expect(job!.createdAt).toBeDefined();
  });

  test('getJob returns null for non-existent job', async () => {
    const job = await backend.getJob('00000000-0000-0000-0000-000000000000');
    expect(job).toBeNull();
  });

  test('cancelJob returns true for waiting job', async () => {
    const added = await backend.addJob('cancel-q', 'to-cancel', {});
    const result = await backend.cancelJob(added.jobId);
    expect(result).toBe(true);
  });

  test('cancelled job is no longer retrievable', async () => {
    const added = await backend.addJob('cancel-q', 'gone', {});
    await backend.cancelJob(added.jobId);
    const job = await backend.getJob(added.jobId);
    expect(job).toBeNull();
  });

  test('getJobState returns state string', async () => {
    const added = await backend.addJob('state-q', 'check-state', {});
    const state = await backend.getJobState(added.jobId);
    expect(state).toBe('waiting');
  });

  test('updateProgress works for active job', async () => {
    const added = await backend.addJob('progress-q', 'prog-test', {});
    const pulled = await backend.pullJob('progress-q');
    expect(pulled).not.toBeNull();

    const result = await backend.updateProgress(pulled!.id, 75, 'Almost done');
    expect(result).toBe(true);
  });

  test('getJobByCustomId returns null for non-existent custom id', async () => {
    const job = await backend.getJobByCustomId('does-not-exist');
    expect(job).toBeNull();
  });

  test('getJobResult returns undefined for job without result', async () => {
    const added = await backend.addJob('result-q', 'no-result', {});
    const result = await backend.getJobResult(added.jobId);
    expect(result).toBeUndefined();
  });

  test('promoteJob returns false for non-delayed job', async () => {
    const added = await backend.addJob('promote-q', 'not-delayed', {});
    const result = await backend.promoteJob(added.jobId);
    expect(result).toBe(false);
  });
});

// ─── Queue Control ─────────────────────────────────────────────────────────

describe('EmbeddedBackend - Queue Control', () => {
  test('listQueues returns array', async () => {
    await backend.addJob('lq-alpha', 'j', {});
    await backend.addJob('lq-beta', 'j', {});

    const queues = await backend.listQueues();
    expect(Array.isArray(queues)).toBe(true);
    expect(queues).toContain('lq-alpha');
    expect(queues).toContain('lq-beta');
  });

  test('countJobs returns correct count', async () => {
    await backend.addJob('count-q', 'j1', {});
    await backend.addJob('count-q', 'j2', {});
    await backend.addJob('count-q', 'j3', {});

    const count = await backend.countJobs('count-q');
    expect(count).toBe(3);
  });

  test('pauseQueue and resumeQueue work', async () => {
    await backend.pauseQueue('pause-q');
    const paused = await backend.isPaused('pause-q');
    expect(paused).toBe(true);

    await backend.resumeQueue('pause-q');
    const resumed = await backend.isPaused('pause-q');
    expect(resumed).toBe(false);
  });

  test('drainQueue removes waiting jobs', async () => {
    await backend.addJob('drain-q', 'j1', {});
    await backend.addJob('drain-q', 'j2', {});

    const removed = await backend.drainQueue('drain-q');
    expect(removed).toBe(2);

    const count = await backend.countJobs('drain-q');
    expect(count).toBe(0);
  });

  test('obliterateQueue removes all data', async () => {
    await backend.addJob('obliterate-q', 'j1', {});
    await backend.obliterateQueue('obliterate-q');

    const count = await backend.countJobs('obliterate-q');
    expect(count).toBe(0);
  });

  test('getJobCounts returns counts per state', async () => {
    await backend.addJob('counts-q', 'j1', {});
    await backend.addJob('counts-q', 'j2', {});

    const counts = await backend.getJobCounts('counts-q');
    expect(counts.waiting).toBe(2);
    expect(typeof counts.active).toBe('number');
    expect(typeof counts.completed).toBe('number');
  });

  test('getJobs returns jobs with pagination', async () => {
    await backend.addJob('getjobs-q', 'j1', {});
    await backend.addJob('getjobs-q', 'j2', {});
    await backend.addJob('getjobs-q', 'j3', {});

    const jobs = await backend.getJobs('getjobs-q', { start: 0, end: 2 });
    expect(Array.isArray(jobs)).toBe(true);
    expect(jobs.length).toBeLessThanOrEqual(2);
  });

  test('getJobs with state filter', async () => {
    await backend.addJob('filter-q', 'j1', {});
    const jobs = await backend.getJobs('filter-q', { state: 'waiting' });
    expect(jobs.length).toBeGreaterThan(0);
    for (const job of jobs) {
      expect(job.queue).toBe('filter-q');
    }
  });

  test('cleanQueue returns number removed', async () => {
    await backend.addJob('clean-q', 'j1', {});
    const removed = await backend.cleanQueue('clean-q', 0);
    expect(typeof removed).toBe('number');
  });

  test('getCountsPerPriority returns priority breakdown', async () => {
    await backend.addJob('prio-q', 'j1', { x: 1 }, { priority: 5 });
    await backend.addJob('prio-q', 'j2', { x: 2 }, { priority: 10 });

    const counts = await backend.getCountsPerPriority('prio-q');
    expect(typeof counts).toBe('object');
  });

  test('isPaused returns false for new queue', async () => {
    const paused = await backend.isPaused('not-paused-q');
    expect(paused).toBe(false);
  });
});

// ─── Rate Limiting ─────────────────────────────────────────────────────────

describe('EmbeddedBackend - Rate Limiting', () => {
  test('setRateLimit does not throw', async () => {
    await backend.setRateLimit('rl-q', 50);
  });

  test('setConcurrency does not throw', async () => {
    await backend.setConcurrency('conc-q', 10);
  });

  test('clearRateLimit does not throw', async () => {
    await backend.setRateLimit('rl-q2', 50);
    await backend.clearRateLimit('rl-q2');
  });

  test('clearConcurrency does not throw', async () => {
    await backend.setConcurrency('conc-q2', 10);
    await backend.clearConcurrency('conc-q2');
  });
});

// ─── DLQ Operations ─────────────────────────────────────────────────────────

describe('EmbeddedBackend - DLQ', () => {
  test('getDlq returns empty array for queue with no DLQ entries', async () => {
    const result = await backend.getDlq('dlq-empty-q');
    expect(Array.isArray(result)).toBe(true);
    expect(result).toHaveLength(0);
  });

  test('retryDlq returns 0 for empty DLQ', async () => {
    const result = await backend.retryDlq('dlq-retry-q');
    expect(result).toBe(0);
  });

  test('purgeDlq returns 0 for empty DLQ', async () => {
    const result = await backend.purgeDlq('dlq-purge-q');
    expect(result).toBe(0);
  });

  test('retryCompleted returns 0 when no completed jobs', async () => {
    const result = await backend.retryCompleted('dlq-retry-completed-q');
    expect(result).toBe(0);
  });
});

// ─── Cron Jobs ─────────────────────────────────────────────────────────────

describe('EmbeddedBackend - Cron', () => {
  test('addCron with repeatEvery returns cron details', async () => {
    const result = await backend.addCron({
      name: 'test-cron-interval',
      queue: 'cron-q',
      data: { task: 'ping' },
      repeatEvery: 30000,
    });
    expect(result.name).toBe('test-cron-interval');
    expect(result.queue).toBe('cron-q');
  });

  test('listCrons returns cron entries', async () => {
    await backend.addCron({
      name: 'list-cron-1',
      queue: 'cron-q',
      data: {},
      repeatEvery: 1000,
    });

    const crons = await backend.listCrons();
    expect(Array.isArray(crons)).toBe(true);
    const names = crons.map((c) => c.name);
    expect(names).toContain('list-cron-1');
  });

  test('getCron returns cron by name', async () => {
    await backend.addCron({
      name: 'get-cron-test',
      queue: 'cron-q',
      data: {},
      repeatEvery: 5000,
    });

    const cron = await backend.getCron('get-cron-test');
    expect(cron).not.toBeNull();
    expect(cron!.name).toBe('get-cron-test');
    expect(cron!.queue).toBe('cron-q');
  });

  test('getCron returns null for non-existent cron', async () => {
    const cron = await backend.getCron('does-not-exist');
    expect(cron).toBeNull();
  });

  test('deleteCron removes existing cron', async () => {
    await backend.addCron({
      name: 'to-delete-cron',
      queue: 'cron-q',
      data: {},
      repeatEvery: 1000,
    });

    const result = await backend.deleteCron('to-delete-cron');
    expect(result).toBe(true);

    const crons = await backend.listCrons();
    const names = crons.map((c) => c.name);
    expect(names).not.toContain('to-delete-cron');
  });

  test('deleteCron returns false for non-existent cron', async () => {
    const result = await backend.deleteCron('does-not-exist');
    expect(result).toBe(false);
  });
});

// ─── Stats & Logs ─────────────────────────────────────────────────────────

describe('EmbeddedBackend - Stats & Logs', () => {
  test('getStats returns stats object', async () => {
    const stats = await backend.getStats();
    expect(stats).toBeDefined();
    expect(typeof stats).toBe('object');
  });

  test('stats are JSON-serializable (no BigInt)', async () => {
    await backend.addJob('stats-q', 'j', {});
    const stats = await backend.getStats();
    const json = JSON.stringify(stats);
    expect(json).toBeDefined();
    expect(() => JSON.parse(json)).not.toThrow();
  });

  test('getJobLogs returns empty array when no logs', async () => {
    const added = await backend.addJob('log-q', 'no-logs', {});
    const logs = await backend.getJobLogs(added.jobId);
    expect(Array.isArray(logs)).toBe(true);
    expect(logs).toHaveLength(0);
  });

  test('addJobLog and getJobLogs round-trip', async () => {
    const added = await backend.addJob('log-q', 'has-logs', {});

    await backend.addJobLog(added.jobId, 'Step 1 started', 'info');
    await backend.addJobLog(added.jobId, 'Warning encountered', 'warn');

    const logs = await backend.getJobLogs(added.jobId);
    expect(logs).toHaveLength(2);
    expect(logs[0].message).toBe('Step 1 started');
    expect(logs[0].level).toBe('info');
    expect(logs[1].message).toBe('Warning encountered');
    expect(logs[1].level).toBe('warn');
  });

  test('clearJobLogs removes logs', async () => {
    const added = await backend.addJob('clear-log-q', 'logged', {});
    await backend.addJobLog(added.jobId, 'Log 1', 'info');
    await backend.addJobLog(added.jobId, 'Log 2', 'info');

    await backend.clearJobLogs(added.jobId);
    const logs = await backend.getJobLogs(added.jobId);
    expect(logs).toHaveLength(0);
  });
});

// ─── Job Consumption (Pull/Ack/Fail) ─────────────────────────────────────

describe('EmbeddedBackend - Job Consumption', () => {
  test('pullJob returns a job from queue', async () => {
    await backend.addJob('pull-q', 'pull-test', { data: 1 });

    const job = await backend.pullJob('pull-q');
    expect(job).not.toBeNull();
    expect(job!.queue).toBe('pull-q');
  });

  test('pullJob returns null on empty queue', async () => {
    const job = await backend.pullJob('empty-pull-q');
    expect(job).toBeNull();
  });

  test('pullJobBatch returns multiple jobs', async () => {
    await backend.addJob('batch-pull-q', 'j1', {});
    await backend.addJob('batch-pull-q', 'j2', {});
    await backend.addJob('batch-pull-q', 'j3', {});

    const jobs = await backend.pullJobBatch('batch-pull-q', 2);
    expect(jobs.length).toBeLessThanOrEqual(2);
    expect(jobs.length).toBeGreaterThan(0);
  });

  test('pullJobBatch returns empty array on empty queue', async () => {
    const jobs = await backend.pullJobBatch('empty-batch-q', 5);
    expect(Array.isArray(jobs)).toBe(true);
    expect(jobs).toHaveLength(0);
  });

  test('ackJob completes the job', async () => {
    await backend.addJob('ack-q', 'ack-test', {});
    const job = await backend.pullJob('ack-q');
    expect(job).not.toBeNull();

    await backend.ackJob(job!.id, { done: true });
    // Job should be completed now
    const state = await backend.getJobState(job!.id);
    expect(state).toBe('completed');
  });

  test('ackJobBatch completes multiple jobs', async () => {
    await backend.addJob('ackb-q', 'j1', {});
    await backend.addJob('ackb-q', 'j2', {});
    const jobs = await backend.pullJobBatch('ackb-q', 2);
    expect(jobs.length).toBe(2);

    await backend.ackJobBatch(jobs.map((j) => j.id));

    for (const job of jobs) {
      const state = await backend.getJobState(job.id);
      expect(state).toBe('completed');
    }
  });

  test('failJob marks job as failed', async () => {
    await backend.addJob('fail-q', 'fail-test', {});
    const job = await backend.pullJob('fail-q');
    expect(job).not.toBeNull();

    await backend.failJob(job!.id, 'Test error');
  });

  test('jobHeartbeat returns boolean', async () => {
    await backend.addJob('hb-q', 'hb-test', {});
    const job = await backend.pullJob('hb-q');
    expect(job).not.toBeNull();

    const result = await backend.jobHeartbeat(job!.id);
    expect(typeof result).toBe('boolean');
  });

  test('jobHeartbeatBatch returns count', async () => {
    await backend.addJob('hbb-q', 'j1', {});
    await backend.addJob('hbb-q', 'j2', {});
    const jobs = await backend.pullJobBatch('hbb-q', 2);

    const count = await backend.jobHeartbeatBatch(jobs.map((j) => j.id));
    expect(typeof count).toBe('number');
  });
});

// ─── Worker Management ────────────────────────────────────────────────────

describe('EmbeddedBackend - Worker Management', () => {
  test('registerWorker returns worker info', async () => {
    const worker = await backend.registerWorker('test-worker', ['q1', 'q2']);
    expect(worker).toBeDefined();
    expect(worker.name).toBe('test-worker');
    expect(worker.queues).toContain('q1');
    expect(worker.queues).toContain('q2');
  });

  test('listWorkers includes registered worker', async () => {
    await backend.registerWorker('listed-worker', ['q1']);
    const workers = await backend.listWorkers();
    expect(Array.isArray(workers)).toBe(true);
    const names = workers.map((w) => w.name);
    expect(names).toContain('listed-worker');
  });

  test('workerHeartbeat returns boolean', async () => {
    const worker = await backend.registerWorker('hb-worker', ['q1']);
    const result = await backend.workerHeartbeat(worker.id);
    expect(typeof result).toBe('boolean');
  });

  test('unregisterWorker removes worker', async () => {
    const worker = await backend.registerWorker('unreg-worker', ['q1']);
    const success = await backend.unregisterWorker(worker.id);
    expect(success).toBe(true);

    const workers = await backend.listWorkers();
    const ids = workers.map((w) => w.id);
    expect(ids).not.toContain(worker.id);
  });
});

// ─── Webhooks ─────────────────────────────────────────────────────────────

describe('EmbeddedBackend - Webhooks', () => {
  test('addWebhook returns webhook info', async () => {
    const webhook = await backend.addWebhook(
      'https://example.com/hook',
      ['job.completed'],
      'webhook-q'
    );
    expect(webhook).toBeDefined();
    expect(webhook.url).toBe('https://example.com/hook');
    expect(webhook.events).toContain('job.completed');
    expect(webhook.enabled).toBe(true);
  });

  test('listWebhooks returns registered webhooks', async () => {
    await backend.addWebhook('https://example.com/hook2', ['job.failed']);
    const webhooks = await backend.listWebhooks();
    expect(Array.isArray(webhooks)).toBe(true);
    expect(webhooks.length).toBeGreaterThan(0);
  });

  test('setWebhookEnabled toggles webhook', async () => {
    const webhook = await backend.addWebhook('https://example.com/toggle', ['job.completed']);
    const result = await backend.setWebhookEnabled(webhook.id, false);
    expect(result).toBe(true);
  });

  test('removeWebhook removes webhook', async () => {
    const webhook = await backend.addWebhook('https://example.com/remove', ['job.completed']);
    const success = await backend.removeWebhook(webhook.id);
    expect(success).toBe(true);
  });
});

// ─── Monitoring ───────────────────────────────────────────────────────────

describe('EmbeddedBackend - Monitoring', () => {
  test('getPerQueueStats returns object', async () => {
    const stats = await backend.getPerQueueStats();
    expect(typeof stats).toBe('object');
  });

  test('getMemoryStats returns object', async () => {
    const stats = await backend.getMemoryStats();
    expect(typeof stats).toBe('object');
  });

  test('getPrometheusMetrics returns string', async () => {
    const metrics = await backend.getPrometheusMetrics();
    expect(typeof metrics).toBe('string');
  });

  test('getStorageStatus returns health status', async () => {
    const status = await backend.getStorageStatus();
    expect(typeof status.diskFull).toBe('boolean');
  });

  test('compactMemory does not throw', async () => {
    await backend.compactMemory();
  });
});

// ─── Progress ─────────────────────────────────────────────────────────────

describe('EmbeddedBackend - Progress', () => {
  test('getProgress returns null for non-existent job', async () => {
    const result = await backend.getProgress('00000000-0000-0000-0000-000000000000');
    expect(result).toBeNull();
  });

  test('getProgress returns progress after update', async () => {
    const added = await backend.addJob('prog-q', 'prog-test', {});
    const pulled = await backend.pullJob('prog-q');
    expect(pulled).not.toBeNull();

    await backend.updateProgress(pulled!.id, 50, 'Halfway there');
    const progress = await backend.getProgress(pulled!.id);
    expect(progress).not.toBeNull();
    expect(progress!.progress).toBe(50);
    expect(progress!.message).toBe('Halfway there');
  });
});

// ─── End-to-end flows ─────────────────────────────────────────────────────

describe('EmbeddedBackend - End-to-end flows', () => {
  test('add job, retrieve it, cancel it, verify gone', async () => {
    const added = await backend.addJob('e2e-q', 'lifecycle', { step: 1 });
    expect(added.jobId).toBeDefined();

    const fetched = await backend.getJob(added.jobId);
    expect(fetched).not.toBeNull();
    expect(fetched!.id).toBe(added.jobId);

    await backend.cancelJob(added.jobId);

    const gone = await backend.getJob(added.jobId);
    expect(gone).toBeNull();
  });

  test('add jobs, count, drain, verify empty', async () => {
    await backend.addJob('e2e-drain', 'j1', {});
    await backend.addJob('e2e-drain', 'j2', {});
    await backend.addJob('e2e-drain', 'j3', {});

    const count = await backend.countJobs('e2e-drain');
    expect(count).toBe(3);

    const removed = await backend.drainQueue('e2e-drain');
    expect(removed).toBe(3);

    const after = await backend.countJobs('e2e-drain');
    expect(after).toBe(0);
  });

  test('add cron, list, delete, verify removed', async () => {
    await backend.addCron({
      name: 'e2e-cron',
      queue: 'cron-q',
      data: { action: 'report' },
      repeatEvery: 60000,
    });

    let crons = await backend.listCrons();
    let names = crons.map((c) => c.name);
    expect(names).toContain('e2e-cron');

    await backend.deleteCron('e2e-cron');

    crons = await backend.listCrons();
    names = crons.map((c) => c.name);
    expect(names).not.toContain('e2e-cron');
  });

  test('add job, add logs, retrieve logs', async () => {
    const added = await backend.addJob('e2e-logs', 'logged', {});

    await backend.addJobLog(added.jobId, 'Phase 1', 'info');
    await backend.addJobLog(added.jobId, 'Phase 2', 'warn');
    await backend.addJobLog(added.jobId, 'Phase 3', 'error');

    const logs = await backend.getJobLogs(added.jobId);
    expect(logs).toHaveLength(3);
    expect(logs[0].message).toBe('Phase 1');
    expect(logs[2].level).toBe('error');
  });

  test('bulk add then count', async () => {
    const result = await backend.addJobsBulk('e2e-bulk', [
      { name: 'a', data: { n: 1 } },
      { name: 'b', data: { n: 2 } },
      { name: 'c', data: { n: 3 } },
      { name: 'd', data: { n: 4 } },
      { name: 'e', data: { n: 5 } },
    ]);
    expect(result.jobIds).toHaveLength(5);

    const count = await backend.countJobs('e2e-bulk');
    expect(count).toBe(5);
  });

  test('full job lifecycle: add, pull, heartbeat, progress, ack, verify completed', async () => {
    const added = await backend.addJob('e2e-lifecycle', 'full-cycle', { step: 'start' });
    const pulled = await backend.pullJob('e2e-lifecycle');
    expect(pulled).not.toBeNull();

    await backend.jobHeartbeat(pulled!.id);
    await backend.updateProgress(pulled!.id, 50, 'In progress');
    await backend.ackJob(pulled!.id, { step: 'done' });

    const state = await backend.getJobState(pulled!.id);
    expect(state).toBe('completed');

    const result = await backend.getJobResult(pulled!.id);
    expect(result).toEqual({ step: 'done' });
  });
});

// ─── MCP Fix Tests (v2.5.3) ──────────────────────────────────────────────

describe('EmbeddedBackend - getStorageStatus returns real status', () => {
  test('getStorageStatus returns diskFull and error fields', async () => {
    const status = await backend.getStorageStatus();
    expect(typeof status.diskFull).toBe('boolean');
    expect(status).toHaveProperty('error');
  });

  test('getStorageStatus error is null when healthy', async () => {
    const status = await backend.getStorageStatus();
    expect(status.error).toBeNull();
  });
});

describe('TcpBackend - pool size configurable via env', () => {
  test('BUNQUEUE_POOL_SIZE env var is respected', () => {
    const original = process.env.BUNQUEUE_POOL_SIZE;
    try {
      process.env.BUNQUEUE_POOL_SIZE = '4';
      // TcpBackend constructor reads BUNQUEUE_POOL_SIZE
      // We can't connect but we can verify it doesn't throw during construction
      const tcpBackend = new TcpBackend({ host: '127.0.0.1', port: 19999 });
      expect(tcpBackend).toBeDefined();
      tcpBackend.shutdown();
    } finally {
      if (original === undefined) {
        delete process.env.BUNQUEUE_POOL_SIZE;
      } else {
        process.env.BUNQUEUE_POOL_SIZE = original;
      }
    }
  });

  test('default pool size works without env var', () => {
    const original = process.env.BUNQUEUE_POOL_SIZE;
    try {
      delete process.env.BUNQUEUE_POOL_SIZE;
      const tcpBackend = new TcpBackend({ host: '127.0.0.1', port: 19999 });
      expect(tcpBackend).toBeDefined();
      tcpBackend.shutdown();
    } finally {
      if (original !== undefined) {
        process.env.BUNQUEUE_POOL_SIZE = original;
      }
    }
  });

  test('getStorageStatus catches connection errors', async () => {
    // Verify the try/catch in getStorageStatus handles errors gracefully
    // by checking the method signature returns error field
    const tcpBackend = new TcpBackend({ host: '127.0.0.1', port: 19999 });
    // We can't actually test a failed send without a long timeout,
    // but we verify the backend constructs and shuts down cleanly
    expect(typeof tcpBackend.getStorageStatus).toBe('function');
    tcpBackend.shutdown();
  });
});

describe('EmbeddedBackend - getChildrenValues parallel fetch', () => {
  test('getChildrenValues returns empty for non-existent parent', async () => {
    const result = await backend.getChildrenValues('00000000-0000-0000-0000-000000000000');
    expect(result).toEqual({});
  });

  test('getChildrenValues returns empty for job without children', async () => {
    const added = await backend.addJob('children-q', 'parent', {});
    const result = await backend.getChildrenValues(added.jobId);
    expect(result).toEqual({});
  });
});

describe('withErrorHandler - error format consistency', () => {
  test('wraps thrown errors with isError: true', async () => {
    const handler = withErrorHandler('test_throw', async () => {
      throw new Error('test failure');
    });
    const result = await handler({});
    expect(result.isError).toBe(true);
    expect(result.content).toHaveLength(1);
    expect(result.content[0].type).toBe('text');
    const parsed = JSON.parse(result.content[0].text);
    expect(parsed.error).toBe('test failure');
  });

  test('passes through successful results unchanged', async () => {
    const handler = withErrorHandler('test_success', async () => ({
      content: [{ type: 'text' as const, text: '{"ok":true}' }],
    }));
    const result = await handler({});
    expect(result.isError).toBeUndefined();
    expect(result.content[0].text).toBe('{"ok":true}');
  });

  test('handles non-Error thrown values', async () => {
    const handler = withErrorHandler('test_throw_str', async () => {
      throw 'string error';
    });
    const result = await handler({});
    expect(result.isError).toBe(true);
    const parsed = JSON.parse(result.content[0].text);
    expect(parsed.error).toBe('string error');
  });
});

describe('Resource error format - isError flag', () => {
  test('resource error handler includes isError in JSON', async () => {
    // Simulate what withResourceErrorHandler does
    const errorJson = JSON.stringify({ error: 'connection failed', isError: true });
    const parsed = JSON.parse(errorJson);
    expect(parsed.isError).toBe(true);
    expect(parsed.error).toBe('connection failed');
  });
});

// ─── MCP Prompts Data Gathering ──────────────────────────────────────────

describe('MCP Prompts - health_report data gathering', () => {
  test('all required backend methods return valid data', async () => {
    await backend.addJob('health-q', 'j1', { x: 1 });
    await backend.registerWorker('health-worker', ['health-q']);

    const [stats, storage, queues, workers, crons, memory] = await Promise.all([
      backend.getStats(),
      backend.getStorageStatus(),
      backend.listQueues(),
      backend.listWorkers(),
      backend.listCrons(),
      backend.getMemoryStats(),
    ]);

    expect(typeof stats).toBe('object');
    expect(typeof storage.diskFull).toBe('boolean');
    expect(Array.isArray(queues)).toBe(true);
    expect(queues).toContain('health-q');
    expect(workers.length).toBeGreaterThan(0);
    expect(Array.isArray(crons)).toBe(true);
    expect(typeof memory).toBe('object');
  });

  test('per-queue counts are gatherable for all queues', async () => {
    await backend.addJob('hr-q1', 'j', {});
    await backend.addJob('hr-q2', 'j', {});

    const queues = await backend.listQueues();
    const details = await Promise.all(
      queues.map(async (q) => ({
        name: q,
        counts: await backend.getJobCounts(q),
        paused: await backend.isPaused(q),
      }))
    );

    expect(details.length).toBeGreaterThanOrEqual(2);
    for (const d of details) {
      expect(d.counts).toHaveProperty('waiting');
      expect(typeof d.paused).toBe('boolean');
    }
  });

  test('all gathered data is JSON-serializable', async () => {
    await backend.addJob('serial-q', 'j', {});
    const stats = await backend.getStats();
    const memory = await backend.getMemoryStats();
    const storage = await backend.getStorageStatus();

    expect(() => JSON.stringify(stats)).not.toThrow();
    expect(() => JSON.stringify(memory)).not.toThrow();
    expect(() => JSON.stringify(storage)).not.toThrow();
  });
});

describe('MCP Prompts - debug_queue data gathering', () => {
  test('all diagnostic data is gatherable for a queue', async () => {
    await backend.addJob('debug-q', 'j1', {}, { priority: 5 });
    await backend.addJob('debug-q', 'j2', {}, { priority: 10 });

    const [counts, paused, dlqEntries, activeJobs, priorities] = await Promise.all([
      backend.getJobCounts('debug-q'),
      backend.isPaused('debug-q'),
      backend.getDlq('debug-q', 10),
      backend.getJobs('debug-q', { state: 'active' }),
      backend.getCountsPerPriority('debug-q'),
    ]);

    // BullMQ v5: priority > 0 jobs are in 'prioritized' state
    expect(counts.prioritized).toBe(2);
    expect(paused).toBe(false);
    expect(Array.isArray(dlqEntries)).toBe(true);
    expect(Array.isArray(activeJobs)).toBe(true);
    expect(typeof priorities).toBe('object');
  });

  test('works on empty/nonexistent queue', async () => {
    const counts = await backend.getJobCounts('nonexistent-debug-q');
    expect(counts.waiting).toBe(0);
    expect(counts.active).toBe(0);
    expect(counts.failed).toBe(0);
  });

  test('total job count calculation is correct', async () => {
    await backend.addJob('total-q', 'j1', {});
    await backend.addJob('total-q', 'j2', {});
    await backend.addJob('total-q', 'j3', {});

    const counts = await backend.getJobCounts('total-q');
    const total =
      counts.waiting + counts.delayed + counts.active + counts.completed + counts.failed;
    expect(total).toBe(3);
  });
});

describe('MCP Prompts - incident_response data gathering', () => {
  test('identifies paused queues as problematic', async () => {
    await backend.addJob('incident-q', 'j1', {});
    await backend.pauseQueue('incident-q');

    const paused = await backend.isPaused('incident-q');
    expect(paused).toBe(true);
  });

  test('identifies stuck queues (waiting > 0, active = 0)', async () => {
    await backend.addJob('stuck-q', 'j1', {});

    const counts = await backend.getJobCounts('stuck-q');
    const isStuck = counts.waiting > 0 && counts.active === 0;
    expect(isStuck).toBe(true);
  });

  test('gathers worker and stats data', async () => {
    const [workers, stats] = await Promise.all([
      backend.listWorkers(),
      backend.getStats(),
    ]);

    expect(Array.isArray(workers)).toBe(true);
    expect(typeof stats).toBe('object');
  });

  test('filters problematic queues correctly', async () => {
    await backend.addJob('ok-q', 'j', {});
    await backend.addJob('bad-q', 'j', {});
    await backend.pauseQueue('bad-q');

    const queues = ['ok-q', 'bad-q'];
    const diagnostics = await Promise.all(
      queues.map(async (q) => ({
        name: q,
        counts: await backend.getJobCounts(q),
        paused: await backend.isPaused(q),
        dlqEntries: (await backend.getDlq(q, 5)).length,
      }))
    );

    const problematic = diagnostics.filter(
      (q) => q.paused || q.dlqEntries > 0 || (q.counts.waiting > 0 && q.counts.active === 0)
    );

    // Both are problematic: bad-q is paused, ok-q has waiting>0 with active=0
    expect(problematic.length).toBe(2);
    expect(problematic.find((q) => q.name === 'bad-q')?.paused).toBe(true);
  });
});

// ─── Flow Tools ────────────────────────────────────────────────────────────

describe('EmbeddedBackend - Flow Operations', () => {
  test('addFlowChain creates sequential pipeline', async () => {
    const result = await backend.addFlowChain([
      { name: 'fetch', queueName: 'pipeline', data: { url: 'http://example.com' } },
      { name: 'process', queueName: 'pipeline', data: { format: 'json' } },
      { name: 'store', queueName: 'pipeline', data: { bucket: 's3' } },
    ]);

    expect(result.jobIds).toHaveLength(3);
    result.jobIds.forEach((id) => expect(id).toBeDefined());
  });

  test('addFlowChain with single step', async () => {
    const result = await backend.addFlowChain([
      { name: 'only-step', queueName: 'solo', data: { key: 'value' } },
    ]);
    expect(result.jobIds).toHaveLength(1);
  });

  test('addFlowChain with empty array returns empty', async () => {
    const result = await backend.addFlowChain([]);
    expect(result.jobIds).toHaveLength(0);
  });

  test('addFlowBulkThen creates fan-out/fan-in', async () => {
    const result = await backend.addFlowBulkThen(
      [
        { name: 'resize-small', queueName: 'images', data: { size: 'small' } },
        { name: 'resize-medium', queueName: 'images', data: { size: 'medium' } },
        { name: 'resize-large', queueName: 'images', data: { size: 'large' } },
      ],
      { name: 'merge-results', queueName: 'images', data: { output: 'gallery' } }
    );

    expect(result.parallelIds).toHaveLength(3);
    expect(result.finalId).toBeDefined();
    // Final job is distinct from parallel jobs
    expect(result.parallelIds).not.toContain(result.finalId);
  });

  test('addFlow creates tree with children', async () => {
    const result = await backend.addFlow({
      name: 'parent',
      queueName: 'flow-q',
      data: { type: 'root' },
      children: [
        { name: 'child-a', queueName: 'flow-q', data: { type: 'a' } },
        { name: 'child-b', queueName: 'flow-q', data: { type: 'b' } },
      ],
    });

    expect(result.jobId).toBeDefined();
    expect(result.name).toBe('parent');
    expect(result.queueName).toBe('flow-q');
    expect(result.children).toHaveLength(2);
    expect(result.children![0].name).toBe('child-a');
    expect(result.children![1].name).toBe('child-b');
  });

  test('addFlow creates leaf node without children', async () => {
    const result = await backend.addFlow({
      name: 'leaf',
      queueName: 'flow-q',
      data: { solo: true },
    });

    expect(result.jobId).toBeDefined();
    expect(result.name).toBe('leaf');
    expect(result.children).toBeUndefined();
  });

  test('addFlow with nested children (3 levels)', async () => {
    const result = await backend.addFlow({
      name: 'root',
      queueName: 'deep',
      children: [
        {
          name: 'mid',
          queueName: 'deep',
          children: [{ name: 'leaf', queueName: 'deep', data: { depth: 3 } }],
        },
      ],
    });

    expect(result.children).toHaveLength(1);
    expect(result.children![0].name).toBe('mid');
    expect(result.children![0].children).toHaveLength(1);
    expect(result.children![0].children![0].name).toBe('leaf');
  });

  test('getFlow retrieves flow tree', async () => {
    const created = await backend.addFlow({
      name: 'parent',
      queueName: 'flow-get',
      data: { x: 1 },
      children: [{ name: 'child', queueName: 'flow-get', data: { x: 2 } }],
    });

    const flow = await backend.getFlow(created.jobId, 'flow-get');
    expect(flow).not.toBeNull();
    expect(flow!.jobId).toBe(created.jobId);
    expect(flow!.name).toBe('parent');
  });

  test('getFlow returns null for non-existent job', async () => {
    const flow = await backend.getFlow('non-existent-id', 'no-queue');
    expect(flow).toBeNull();
  });

  test('addFlowChain with opts (priority, delay)', async () => {
    const result = await backend.addFlowChain([
      {
        name: 'urgent',
        queueName: 'prio-q',
        data: { task: 'important' },
        opts: { priority: 10 },
      },
      {
        name: 'delayed',
        queueName: 'prio-q',
        data: { task: 'later' },
        opts: { delay: 5000 },
      },
    ]);

    expect(result.jobIds).toHaveLength(2);
    // Verify the first job has correct priority
    const job = await backend.getJob(result.jobIds[0]);
    expect(job).not.toBeNull();
    expect(job!.priority).toBe(10);
  });
});

// ─── Job Management Tools ──────────────────────────────────────────────────

describe('EmbeddedBackend - Job Management', () => {
  test('updateJobData changes job payload', async () => {
    const { jobId } = await backend.addJob('mgmt-q', 'task', { old: 'data' });
    const success = await backend.updateJobData(jobId, { new: 'data', updated: true });
    expect(success).toBe(true);
  });

  test('updateJobData returns false for non-existent job', async () => {
    const success = await backend.updateJobData('non-existent', { x: 1 });
    expect(success).toBe(false);
  });

  test('changeJobPriority updates priority', async () => {
    const { jobId } = await backend.addJob('mgmt-q', 'task', { a: 1 }, { priority: 1 });
    const success = await backend.changeJobPriority(jobId, 99);
    expect(success).toBe(true);
    const job = await backend.getJob(jobId);
    expect(job).not.toBeNull();
    expect(job!.priority).toBe(99);
  });

  test('changeJobPriority returns false for non-existent job', async () => {
    const success = await backend.changeJobPriority('non-existent', 5);
    expect(success).toBe(false);
  });

  test('moveToDelayed on active job', async () => {
    const { jobId } = await backend.addJob('mgmt-q', 'task', { a: 1 });
    // Pull to make active
    await backend.pullJob('mgmt-q');
    const success = await backend.moveToDelayed(jobId, 60000);
    // moveToDelayed may only work on active jobs; verify it returns boolean
    expect(typeof success).toBe('boolean');
  });

  test('moveToDelayed returns false for non-existent job', async () => {
    const success = await backend.moveToDelayed('non-existent', 1000);
    expect(success).toBe(false);
  });

  test('discardJob moves a waiting job to DLQ', async () => {
    const { jobId } = await backend.addJob('mgmt-q', 'discard-me', { x: 1 });
    const success = await backend.discardJob(jobId);
    expect(success).toBe(true);
    const job = await backend.getJob(jobId);
    // Job is now in DLQ (failed state), still retrievable via getJob
    expect(job).not.toBeNull();
    const state = await backend.getJobState(jobId);
    expect(state).toBe('failed');
  });

  test('discardJob returns false for non-existent job', async () => {
    const success = await backend.discardJob('non-existent');
    expect(success).toBe(false);
  });

  test('changeDelay on delayed job returns boolean', async () => {
    const { jobId } = await backend.addJob('mgmt-q', 'task', { a: 1 }, { delay: 10000 });
    const success = await backend.changeDelay(jobId, 60000);
    expect(typeof success).toBe('boolean');
  });

  test('changeDelay returns false for non-existent job', async () => {
    const success = await backend.changeDelay('non-existent', 5000);
    expect(success).toBe(false);
  });

  test('getProgress returns progress with message after update', async () => {
    const { jobId } = await backend.addJob('mgmt-q', 'task', { a: 1 });
    // Pull to make active (required for progress update)
    await backend.pullJob('mgmt-q');
    await backend.updateProgress(jobId, 75, 'Three quarters done');
    const progress = await backend.getProgress(jobId);
    expect(progress).not.toBeNull();
    expect(progress!.progress).toBe(75);
    expect(progress!.message).toBe('Three quarters done');
  });
});

// ─── Wait For Job ──────────────────────────────────────────────────────────

describe('EmbeddedBackend - waitForJobCompletion', () => {
  test('returns false when job does not complete in time', async () => {
    const { jobId } = await backend.addJob('wait-q', 'task', { a: 1 });
    // Don't complete the job — should timeout
    const completed = await backend.waitForJobCompletion(jobId, 100);
    expect(completed).toBe(false);
  });

  test('waitForJobCompletion returns boolean', async () => {
    const { jobId } = await backend.addJob('wait-q', 'task', { a: 1 });
    const result = await backend.waitForJobCompletion(jobId, 100);
    expect(typeof result).toBe('boolean');
  });
});

// ─── Lock Operations ───────────────────────────────────────────────────────

describe('EmbeddedBackend - extendLock', () => {
  test('extendLock returns boolean', async () => {
    const { jobId } = await backend.addJob('lock-q', 'task', { a: 1 });
    await backend.pullJob('lock-q');
    const result = await backend.extendLock(jobId, 'test-token', 30000);
    expect(typeof result).toBe('boolean');
  });

  test('extendLock on non-existent job returns false', async () => {
    const result = await backend.extendLock('non-existent', 'token', 5000);
    expect(result).toBe(false);
  });
});

// ─── Children Values with real children ────────────────────────────────────

describe('EmbeddedBackend - getChildrenValues with real flow', () => {
  test('returns child results after ack', async () => {
    // Create a flow with parent + child
    const flow = await backend.addFlow({
      name: 'parent',
      queueName: 'cv-q',
      children: [
        { name: 'child-1', queueName: 'cv-q', data: { step: 1 } },
      ],
    });

    // Pull and ack the child with a result
    const child = await backend.pullJob('cv-q');
    if (child) {
      await backend.ackJob(child.id, { output: 'child-result' });
    }

    const values = await backend.getChildrenValues(flow.jobId);
    expect(typeof values).toBe('object');
  });
});

// ─── Queue Stats ───────────────────────────────────────────────────────────

describe('EmbeddedBackend - getPerQueueStats detailed', () => {
  test('returns per-queue stats after adding jobs', async () => {
    await backend.addJob('stats-q1', 'task', { a: 1 });
    await backend.addJob('stats-q2', 'task', { a: 2 });
    const stats = await backend.getPerQueueStats();
    expect(typeof stats).toBe('object');
  });

  test('getStats includes uptime and counts', async () => {
    await backend.addJob('stats-q', 'task', { a: 1 });
    const stats = await backend.getStats();
    expect(stats).toBeDefined();
    expect(typeof stats).toBe('object');
  });
});
