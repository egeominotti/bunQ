import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import type { JobInput } from '../src/domain/types/job';
import { validatePushBatchJobs } from '../src/infrastructure/server/handlers/pushBatchValidation';
import type { HandlerContext } from '../src/infrastructure/server/types';
import { cleanupPostgresNamespace } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-pushb-validation-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL PUSHB validation hot path', () => {
  test.skipIf(!postgresUrl)(
    'does not materialize local snapshot views for dependency-free batches',
    async () => {
      const value = namespace('dependency-free');
      const manager = new PostgresQueueManager({
        postgres: {
          url: postgresUrl!,
          namespace: value,
          brokerId: 'pushb-validation-broker',
        },
      });

      try {
        await manager.waitUntilReady();
        const measured = manager as PostgresQueueManager & {
          getJobIndex(): ReturnType<PostgresQueueManager['getJobIndex']>;
          getCompletedJobs(): ReturnType<PostgresQueueManager['getCompletedJobs']>;
          getDepCompletions(): ReturnType<PostgresQueueManager['getDepCompletions']>;
        };
        const originalJobIndex = measured.getJobIndex.bind(manager);
        const originalCompleted = measured.getCompletedJobs.bind(manager);
        const originalCompletions = measured.getDepCompletions.bind(manager);
        let snapshotViews = 0;
        measured.getJobIndex = () => {
          snapshotViews++;
          return originalJobIndex();
        };
        measured.getCompletedJobs = () => {
          snapshotViews++;
          return originalCompleted();
        };
        measured.getDepCompletions = () => {
          snapshotViews++;
          return originalCompletions();
        };

        const jobs: JobInput[] = Array.from({ length: 100 }, (_, index) => ({
          data: { index },
        }));
        const error = await validatePushBatchJobs(jobs, {
          queueManager: manager,
        } as HandlerContext);

        expect(error).toBeNull();
        expect(snapshotViews).toBe(0);
      } finally {
        await manager.shutdownPostgres();
      }
    },
    15_000
  );
});
