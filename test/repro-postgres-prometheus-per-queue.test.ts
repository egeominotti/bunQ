import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { cleanupPostgresNamespace } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-prometheus-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string, maxPrometheusQueues = 100) {
  return new PostgresQueueManager({
    maxPrometheusQueues,
    postgres: { url: postgresUrl!, namespace: value, brokerId, pollIntervalMs: 25 },
  });
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
}, 30_000);

describe('PostgreSQL per-queue Prometheus metrics', () => {
  test.skipIf(!postgresUrl)(
    'exports authoritative waiting and DLQ counts from the PostgreSQL projection',
    async () => {
      const broker = manager(namespace('states'), 'metrics-states');
      try {
        await broker.waitUntilReady();
        await broker.push('emails', { data: { to: 'a@example.com' } });
        const failed = await broker.push('payments', { data: {}, maxAttempts: 1 });
        const claim = await broker.pullWithLock('payments', 'metrics-worker');
        await broker.fail(failed.id, 'terminal', claim.token!);

        const output = broker.getPrometheusMetrics();
        expect(output).toContain('bunqueue_queue_jobs_waiting{queue="emails"} 1');
        expect(output).toContain('bunqueue_queue_jobs_dlq{queue="payments"} 1');
        expect(output).toContain('bunqueue_queue_metrics_exported 2');
        expect(output).toContain('bunqueue_queue_metrics_omitted 0');
      } finally {
        await broker.shutdownPostgres();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'conserves exported and omitted queue cardinality under the configured limit',
    async () => {
      const broker = manager(namespace('cardinality'), 'metrics-cardinality', 1);
      try {
        await broker.waitUntilReady();
        await broker.push('alpha', { data: {} });
        await broker.push('beta', { data: {} });

        const output = broker.getPrometheusMetrics();
        const labelled = output
          .split('\n')
          .filter((line) => line.startsWith('bunqueue_queue_jobs_waiting{'));
        expect(labelled).toHaveLength(1);
        expect(output).toContain('bunqueue_queue_metrics_exported 1');
        expect(output).toContain('bunqueue_queue_metrics_omitted 1');
      } finally {
        await broker.shutdownPostgres();
      }
    }
  );
});
