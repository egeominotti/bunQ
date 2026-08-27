import { expect, test } from 'bun:test';
import { SQL } from 'bun';
import { postgresAdvisoryLockName } from '../src/infrastructure/persistence/postgres/advisoryLocks';
import {
  cleanupPostgresNamespace,
  postgresProcessRetryDiagnostics,
  startPostgresProcessCluster,
  stopPostgresProcessCluster,
  type PostgresProcessBroker,
} from './support/postgres-process-cluster';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;

test.skipIf(!postgresUrl)(
  'captures a JSON-formatted ACKB failure after the broker output streams close',
  async () => {
    const namespace = `test-process-diagnostics-${Date.now()}-${crypto.randomUUID()}`;
    const commitLock = postgresAdvisoryLockName('event-commit', namespace);
    const blocker = new SQL(postgresUrl!, { max: 1 });
    let brokers: PostgresProcessBroker[] = [];
    let lockHeld = false;
    try {
      brokers = await startPostgresProcessCluster(postgresUrl!, namespace, 1, {
        lockTimeoutMs: 75,
        logFormat: 'json',
        logLevel: 'warn',
        poolSize: 2,
      });
      const [broker] = brokers;
      const pushed = await broker.client.send({
        cmd: 'PUSHB',
        jobs: [{ data: { diagnostic: true } }],
        queue: 'process-diagnostics',
      });
      expect(pushed.ok).toBe(true);
      const pulled = await broker.client.send({
        cmd: 'PULLB',
        count: 1,
        lockTtl: 60_000,
        owner: 'process-diagnostics-worker',
        queue: 'process-diagnostics',
      });
      expect(pulled.ok).toBe(true);
      const [job] = pulled.jobs as Array<{ id: string }>;
      const [token] = pulled.tokens as string[];

      await blocker`SELECT pg_advisory_lock(hashtextextended(${commitLock}, 0))`;
      lockHeld = true;
      const response = await broker.client.send({
        cmd: 'ACKB',
        ids: [job.id],
        results: [{ ignored: true }],
        tokens: [token],
      });
      expect(response).toMatchObject({ ok: false, error: 'Internal server error' });

      await blocker`SELECT pg_advisory_unlock(hashtextextended(${commitLock}, 0))`;
      lockHeld = false;
      await stopPostgresProcessCluster(brokers);
      const diagnostics = await postgresProcessRetryDiagnostics(brokers);
      expect(diagnostics.ackbFailures).toBe(1);
      expect(diagnostics.transactionRetries).toBe(1);
    } finally {
      if (lockHeld) {
        await blocker`SELECT pg_advisory_unlock(hashtextextended(${commitLock}, 0))`;
      }
      await stopPostgresProcessCluster(brokers);
      await blocker.close({ timeout: 5 });
      await cleanupPostgresNamespace(postgresUrl!, namespace);
    }
  },
  15_000
);
