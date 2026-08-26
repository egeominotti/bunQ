import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { createJob, jobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import {
  cleanupPostgresNamespace,
  deferred,
  postgresManagerStore,
} from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(): string {
  const value = `test-postcommit-shutdown-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function store(value: string, brokerId: string): PostgresQueueStore {
  return new PostgresQueueStore({
    url: postgresUrl!,
    namespace: value,
    brokerId,
    maxCompletedJobs: 1,
  });
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL post-commit shutdown regression', () => {
  test.skipIf(!postgresUrl)(
    'drains an admission that committed before shutdown closes the pool',
    async () => {
      const value = namespace();
      const manager = new PostgresQueueManager({
        postgres: { url: postgresUrl!, namespace: value, brokerId: 'admission-manager' },
      });
      const verifier = store(value, 'admission-verifier');
      const admissionCommitted = deferred<undefined>();
      const releaseAdmission = deferred<undefined>();
      const shutdownEntered = deferred<number>();
      let shutdown: Promise<void> | null = null;

      try {
        await Promise.all([manager.waitUntilReady(), verifier.initialize()]);
        const managerStore = postgresManagerStore(manager);
        const insert = managerStore.insert.bind(managerStore);
        managerStore.insert = async (job) => {
          const admitted = await insert(job);
          admissionCommitted.resolve(undefined);
          await releaseAdmission.promise;
          return admitted;
        };

        const managerInternals = manager as unknown as {
          operations: {
            active: number;
            closeAndDrain(): Promise<void>;
          };
        };
        const closeAndDrain = managerInternals.operations.closeAndDrain.bind(
          managerInternals.operations
        );
        managerInternals.operations.closeAndDrain = () => {
          shutdownEntered.resolve(managerInternals.operations.active);
          return closeAndDrain();
        };

        const admission = manager.push('shutdown-admission', { data: { delivery: 'once' } });
        void admission.catch(() => undefined);
        await admissionCommitted.promise;

        const [beforeShutdown] = await verifier.context.sql<{ count: number }[]>`
          SELECT COUNT(*)::int AS count
          FROM bunqueue_jobs
          WHERE namespace = ${value} AND queue = 'shutdown-admission'
        `;
        expect(beforeShutdown.count).toBe(1);

        shutdown = manager.shutdownPostgres();
        const activeAtShutdown = await shutdownEntered.promise;
        releaseAdmission.resolve(undefined);

        const job = await admission;
        await shutdown;
        expect(activeAtShutdown).toBe(1);
        expect(job.queue).toBe('shutdown-admission');

        const [afterShutdown] = await verifier.context.sql<{ count: number }[]>`
          SELECT COUNT(*)::int AS count
          FROM bunqueue_jobs
          WHERE namespace = ${value} AND queue = 'shutdown-admission'
        `;
        expect(afterShutdown.count).toBe(1);
      } finally {
        releaseAdmission.resolve(undefined);
        await Promise.allSettled([shutdown ?? manager.shutdownPostgres(), verifier.close()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'does not reject a committed ACK when shutdown wins before maintenance admission',
    async () => {
      const value = namespace();
      const primary = store(value, 'primary');
      const verifier = store(value, 'verifier');
      const maintenanceEntered = deferred<undefined>();
      const releaseMaintenance = deferred<undefined>();
      let primaryClosed = false;

      try {
        await Promise.all([primary.initialize(), verifier.initialize()]);
        const job = createJob(jobId('shutdown-ack'), 'shutdown', {
          data: {},
          removeOnComplete: true,
        });
        await primary.insert(job);
        const [claim] = await primary.claim('shutdown', 1, 'worker', 60_000);

        const mutableContext = primary.context as {
          postCommitMaintenance?: NonNullable<typeof primary.context.postCommitMaintenance>;
        };
        const runMaintenance = mutableContext.postCommitMaintenance!;
        mutableContext.postCommitMaintenance = async (subsystem, operation) => {
          maintenanceEntered.resolve(undefined);
          await releaseMaintenance.promise;
          await runMaintenance(subsystem, operation);
        };

        const completion = primary.complete(claim.job.id, claim.token, 'durable', true);
        void completion.catch(() => undefined);
        await maintenanceEntered.promise;

        const [beforeClose] = await verifier.context.sql<{ count: number }[]>`
          SELECT COUNT(*)::int AS count
          FROM bunqueue_completions
          WHERE namespace = ${value} AND job_id = ${String(claim.job.id)}
        `;
        expect(beforeClose.count).toBe(1);

        await primary.close();
        primaryClosed = true;
        releaseMaintenance.resolve(undefined);

        expect((await completion).applied).toBe(true);
        expect(await verifier.getResult(claim.job.id)).toEqual({
          found: true,
          result: 'durable',
        });
      } finally {
        releaseMaintenance.resolve(undefined);
        await Promise.allSettled([
          primaryClosed ? Promise.resolve() : primary.close(),
          verifier.close(),
        ]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'drains a committed public ACK before closing the PostgreSQL pool',
    async () => {
      const value = namespace();
      const manager = new PostgresQueueManager({
        maxCompletedJobs: 1,
        postgres: { url: postgresUrl!, namespace: value, brokerId: 'manager' },
      });
      const verifier = store(value, 'manager-verifier');
      const maintenanceEntered = deferred<undefined>();
      const releaseMaintenance = deferred<undefined>();
      let shutdown: Promise<void> | null = null;
      let shutdownComplete = false;

      try {
        await Promise.all([manager.waitUntilReady(), verifier.initialize()]);
        const job = await manager.push('manager-shutdown', {
          data: {},
          removeOnComplete: true,
        });
        const claim = await manager.pullWithLock('manager-shutdown', 'worker');
        expect(claim.job?.id).toBe(job.id);

        const managerStore = postgresManagerStore(manager);
        const mutableContext = managerStore.context as {
          postCommitMaintenance?: NonNullable<typeof managerStore.context.postCommitMaintenance>;
        };
        const runMaintenance = mutableContext.postCommitMaintenance!;
        mutableContext.postCommitMaintenance = async (subsystem, operation) => {
          maintenanceEntered.resolve(undefined);
          await releaseMaintenance.promise;
          await runMaintenance(subsystem, operation);
        };

        const acknowledgement = manager.ack(job.id, 'manager-durable', claim.token!, {
          removeOnComplete: true,
        });
        void acknowledgement.catch(() => undefined);
        await maintenanceEntered.promise;

        shutdown = manager.shutdownPostgres().then(() => {
          shutdownComplete = true;
        });
        await Promise.resolve();
        expect(shutdownComplete).toBe(false);

        releaseMaintenance.resolve(undefined);
        await expect(acknowledgement).resolves.toBeUndefined();
        await shutdown;

        expect(await verifier.getResult(job.id)).toEqual({
          found: true,
          result: 'manager-durable',
        });
      } finally {
        releaseMaintenance.resolve(undefined);
        await Promise.allSettled([shutdown ?? manager.shutdownPostgres(), verifier.close()]);
      }
    }
  );
});
