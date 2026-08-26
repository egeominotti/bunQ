import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import type { Command } from '../src/domain/types/command';
import { jobId, type JobId } from '../src/domain/types/job';
import { handlePush, handlePushBatch } from '../src/infrastructure/server/handlers/core';
import { handleDashboardOverview } from '../src/infrastructure/server/handlers/dashboard';
import { dashboardOverviewEndpoint } from '../src/infrastructure/server/httpDashboardEndpoints';
import { routeQueueRoutes } from '../src/infrastructure/server/httpRouteQueues';
import type { HandlerContext } from '../src/infrastructure/server/types';
import { buildStatsSnapshot } from '../src/infrastructure/server/ws/snapshots';
import { cleanupPostgresNamespace, pausePostgresEventStream } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-server-surfaces-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string): PostgresQueueManager {
  return new PostgresQueueManager({
    postgres: { url: postgresUrl!, namespace: value, brokerId, pollIntervalMs: 25 },
  });
}

function context(queueManager: PostgresQueueManager): HandlerContext {
  return { queueManager, authTokens: new Set(), authenticated: true };
}

async function completeParent(manager: PostgresQueueManager, queue: string): Promise<JobId> {
  const parent = await manager.push(queue, { data: { parent: true }, removeOnComplete: true });
  const claimed = await manager.pullWithLock(queue, 'parent-owner', 0);
  expect(claimed.job?.id).toBe(parent.id);
  expect(claimed.token).not.toBeNull();
  await manager.ack(parent.id, { complete: true }, claimed.token!, { removeOnComplete: true });
  expect(await manager.getJob(parent.id)).toBeNull();
  return parent.id;
}

async function pushDependent(
  manager: PostgresQueueManager,
  queue: string,
  dependencyId: JobId,
  batch: boolean
): Promise<JobId> {
  if (batch) {
    const response = await handlePushBatch(
      {
        cmd: 'PUSHB',
        queue,
        jobs: [{ data: { child: true }, dependsOn: [dependencyId] }],
      },
      context(manager)
    );
    expect(response).toMatchObject({ ok: true });
    if (!response.ok || !('ids' in response)) throw new Error('PUSHB did not return job ids');
    return response.ids[0] as JobId;
  }

  const command: Extract<Command, { cmd: 'PUSH' }> = {
    cmd: 'PUSH',
    queue,
    data: { child: true },
    dependsOn: [dependencyId],
  };
  const response = await handlePush(command, context(manager));
  expect(response).toMatchObject({ ok: true });
  if (!response.ok || !('id' in response) || !response.id) {
    throw new Error('PUSH did not return a job id');
  }
  return response.id as JobId;
}

async function assertRemoteDependencyAccepted(batch: boolean, completed: boolean): Promise<void> {
  const value = namespace(`${batch ? 'pushb' : 'push'}-${completed ? 'completed' : 'existing'}`);
  const first = manager(value, 'dependency-a');
  const second = manager(value, 'dependency-b');
  const queue = `dependency-${crypto.randomUUID()}`;
  try {
    await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
    await pausePostgresEventStream(second);
    const findMissing = second.findMissingDependenciesDurable.bind(second);
    let validationQueries = 0;
    second.findMissingDependenciesDurable = async (ids) => {
      validationQueries++;
      return await findMissing(ids);
    };
    const parentId = completed
      ? await completeParent(first, queue)
      : (await first.push(queue, { data: { parent: true } })).id;

    const childId = await pushDependent(second, queue, parentId, batch);
    expect(validationQueries).toBe(1);
    const child = await second.getJob(childId);
    expect(child?.dependsOn).toEqual([parentId]);
    expect(await second.getJobState(childId)).toBe(completed ? 'waiting' : 'waiting-children');
  } finally {
    await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
  }
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL multi-broker server surfaces', () => {
  for (const batch of [false, true]) {
    for (const completed of [false, true]) {
      test.skipIf(!postgresUrl)(
        `${batch ? 'PUSHB' : 'PUSH'} accepts a parent ${completed ? 'completed' : 'created'} on another broker`,
        async () => await assertRemoteDependencyAccepted(batch, completed)
      );
    }
  }

  test.skipIf(!postgresUrl)(
    'reads workers and crons durably for queue HTTP, dashboard, and stats snapshots',
    async () => {
      const value = namespace('monitoring');
      const first = manager(value, 'monitoring-a');
      const second = manager(value, 'monitoring-b');
      const queue = `monitoring-${crypto.randomUUID()}`;
      const workerId = `worker-${crypto.randomUUID()}`;
      const cronName = `cron-${crypto.randomUUID()}`;
      try {
        await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
        await first.registerWorkerDurable('remote-worker', [queue], 3, {
          workerId,
          clientId: 'remote-client',
        });
        await first.addCronDurable({
          name: cronName,
          queue,
          data: { remote: true },
          repeatEvery: 60_000,
        });

        const queueResponse = await routeQueueRoutes(
          new Request(`http://localhost/queues/${encodeURIComponent(queue)}/workers`),
          `/queues/${encodeURIComponent(queue)}/workers`,
          'GET',
          context(second),
          new Set()
        );
        const queueBody = (await queueResponse!.json()) as { workers: Array<{ id: string }> };
        const dashboardResponse = await dashboardOverviewEndpoint(second);
        const dashboard = (await dashboardResponse.json()) as {
          workers: { total: number; active: number; list: Array<{ id: string }> };
          crons: { total: number; list: Array<{ name: string }> };
        };
        const snapshot = (await buildStatsSnapshot(second)) as {
          workers: { total: number; active: number };
          cronJobs: number;
        };
        const commandResponse = await handleDashboardOverview(
          { cmd: 'DashboardOverview' },
          context(second)
        );
        if (!commandResponse.ok || !('data' in commandResponse)) {
          throw new Error('DashboardOverview did not return data');
        }
        const commandDashboard = commandResponse.data as {
          workers: { total: number; active: number; list: Array<{ id: string }> };
          crons: Array<{ name: string }>;
        };

        expect({
          queueWorkers: queueBody.workers.map((worker) => worker.id),
          dashboardWorkerIds: dashboard.workers.list.map((worker) => worker.id),
          dashboardWorkers: [dashboard.workers.total, dashboard.workers.active],
          dashboardCronNames: dashboard.crons.list.map((cron) => cron.name),
          dashboardCrons: dashboard.crons.total,
          snapshotWorkers: [snapshot.workers.total, snapshot.workers.active],
          snapshotCrons: snapshot.cronJobs,
          commandWorkerIds: commandDashboard.workers.list.map((worker) => worker.id),
          commandWorkers: [commandDashboard.workers.total, commandDashboard.workers.active],
          commandCronNames: commandDashboard.crons.map((cron) => cron.name),
        }).toEqual({
          queueWorkers: [workerId],
          dashboardWorkerIds: [workerId],
          dashboardWorkers: [1, 1],
          dashboardCronNames: [cronName],
          dashboardCrons: 1,
          snapshotWorkers: [1, 1],
          snapshotCrons: 1,
          commandWorkerIds: [workerId],
          commandWorkers: [1, 1],
          commandCronNames: [cronName],
        });
      } finally {
        await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'deduplicates dependency lookups and isolates them by namespace',
    async () => {
      const firstNamespace = namespace('lookup-primary');
      const secondNamespace = namespace('lookup-isolated');
      const first = manager(firstNamespace, 'lookup-a');
      const isolated = manager(secondNamespace, 'lookup-b');
      try {
        await Promise.all([first.waitUntilReady(), isolated.waitUntilReady()]);
        const parent = await first.push('lookup', { customId: 'shared-id', data: {} });
        const missing = jobId('missing-id');

        expect(
          await first.findMissingDependenciesDurable([parent.id, parent.id, missing, missing])
        ).toEqual([missing]);
        expect(await isolated.findMissingDependenciesDurable([parent.id, parent.id])).toEqual([
          parent.id,
        ]);
      } finally {
        await Promise.allSettled([first.shutdownPostgres(), isolated.shutdownPostgres()]);
      }
    }
  );
});
