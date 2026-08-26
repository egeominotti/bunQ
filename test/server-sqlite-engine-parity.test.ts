import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { jobId } from '../src/domain/types/job';
import { handleCommand as handleCloudCommand } from '../src/infrastructure/cloud/commandHandler';
import { handlePush, handlePushBatch } from '../src/infrastructure/server/handlers/core';
import { handleDashboardOverview } from '../src/infrastructure/server/handlers/dashboard';
import { dashboardOverviewEndpoint } from '../src/infrastructure/server/httpDashboardEndpoints';
import { routeQueueRoutes } from '../src/infrastructure/server/httpRouteQueues';
import type { HandlerContext } from '../src/infrastructure/server/types';
import { buildStatsSnapshot } from '../src/infrastructure/server/ws/snapshots';

const managers: QueueManager[] = [];
const directories: string[] = [];

function manager(): QueueManager {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-sqlite-server-parity-'));
  directories.push(directory);
  const queueManager = new QueueManager({ dataPath: join(directory, 'queue.db') });
  managers.push(queueManager);
  return queueManager;
}

function context(queueManager: QueueManager): HandlerContext {
  return { queueManager, authTokens: new Set(), authenticated: true };
}

afterEach(() => {
  for (const queueManager of managers.splice(0)) queueManager.shutdown();
  for (const directory of directories.splice(0))
    rmSync(directory, { recursive: true, force: true });
});

describe('SQLite server surface parity', () => {
  test('returns the requested Cloud job page size without changing local getJobs semantics', async () => {
    const queueManager = manager();
    const queue = 'cloud-pagination';
    for (let index = 0; index < 3; index++) {
      await queueManager.push(queue, { data: { index } });
    }
    queueManager.flushPersistence();

    const response = await handleCloudCommand(queueManager, {
      type: 'command',
      id: 'sqlite-cloud-page',
      action: 'job:list',
      queue,
      state: 'waiting',
      offset: 0,
      limit: 2,
    });

    expect(response.success).toBe(true);
    expect((response.data as { jobs: unknown[] }).jobs).toHaveLength(2);

    const zero = await handleCloudCommand(queueManager, {
      type: 'command',
      id: 'sqlite-cloud-zero-page',
      action: 'job:list',
      queue,
      state: 'waiting',
      offset: -10,
      limit: -5,
    });
    expect(zero.data).toMatchObject({ jobs: [], offset: 0, limit: 0 });

    const secondPage = await handleCloudCommand(queueManager, {
      type: 'command',
      id: 'sqlite-cloud-second-page',
      action: 'job:list',
      queue,
      state: 'waiting',
      offset: 1,
      limit: 2,
    });
    expect((secondPage.data as { jobs: unknown[] }).jobs).toHaveLength(2);
  });

  test('keeps local PUSH and reverse-order PUSHB dependency validation', async () => {
    const queueManager = manager();
    const ctx = context(queueManager);
    const parent = await queueManager.push('dependencies', { data: { parent: true } });

    expect(
      await handlePush(
        { cmd: 'PUSH', queue: 'dependencies', data: { child: true }, dependsOn: [parent.id] },
        ctx
      )
    ).toMatchObject({ ok: true });
    expect(
      await handlePushBatch(
        {
          cmd: 'PUSHB',
          queue: 'dependencies',
          jobs: [
            { customId: 'child', data: {}, dependsOn: [jobId('batch-parent')] },
            { customId: 'batch-parent', data: {} },
          ],
        },
        ctx
      )
    ).toMatchObject({ ok: true });
    expect(
      await handlePush(
        { cmd: 'PUSH', queue: 'dependencies', data: {}, dependsOn: ['missing'] },
        ctx
      )
    ).toMatchObject({ ok: false, error: 'Dependency job not found: missing' });
  });

  test('keeps local worker and cron monitoring synchronous and complete', async () => {
    const queueManager = manager();
    const queue = 'monitoring';
    const worker = queueManager.registerWorker('local-worker', [queue], 2, {
      workerId: 'local-worker-id',
    });
    queueManager.addCron({ name: 'local-cron', queue, data: {}, repeatEvery: 60_000 });

    const dashboardResponse = dashboardOverviewEndpoint(queueManager);
    const dashboardCommand = handleDashboardOverview(
      { cmd: 'DashboardOverview' },
      context(queueManager)
    );
    const snapshot = buildStatsSnapshot(queueManager);
    expect(dashboardResponse).toBeInstanceOf(Response);
    expect(dashboardCommand).not.toBeInstanceOf(Promise);
    expect(snapshot).not.toBeInstanceOf(Promise);

    const dashboard = await (dashboardResponse as Response).json();
    expect(dashboard).toMatchObject({
      workers: { total: 1, active: 1, list: [{ id: worker.id }] },
      crons: { total: 1, list: [{ name: 'local-cron' }] },
    });
    expect(snapshot).toMatchObject({ workers: { total: 1, active: 1 }, cronJobs: 1 });
    expect(dashboardCommand).toMatchObject({
      ok: true,
      data: {
        workers: { total: 1, active: 1, list: [{ id: worker.id }] },
        crons: [{ name: 'local-cron' }],
      },
    });

    const response = await routeQueueRoutes(
      new Request(`http://localhost/queues/${queue}/workers`),
      `/queues/${queue}/workers`,
      'GET',
      context(queueManager),
      new Set()
    );
    expect(await response!.json()).toMatchObject({ workers: [{ id: worker.id }] });
  });
});
