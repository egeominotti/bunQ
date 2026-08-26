import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import { cleanupPostgresNamespace } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-remove-unprocessed-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string): PostgresQueueManager {
  return new PostgresQueueManager({
    postgres: { url: postgresUrl!, namespace: value, brokerId, pollIntervalMs: 25 },
  });
}

function store(value: string, brokerId: string): PostgresQueueStore {
  return new PostgresQueueStore({
    url: postgresUrl!,
    namespace: value,
    brokerId,
    pollIntervalMs: 25,
  });
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
}, 30_000);

describe('PostgreSQL removeUnprocessedChildren', () => {
  test.skipIf(!postgresUrl)(
    'atomically removes every direct pending state and is idempotent',
    async () => {
      const value = namespace('pending');
      const broker = manager(value, 'pending-manager');
      const verifier = store(value, 'pending-verifier');

      try {
        await Promise.all([broker.waitUntilReady(), verifier.initialize()]);
        const parent = await broker.push('pending-parent', { data: { parent: true } });
        const dependency = await broker.push('independent', { data: { independent: true } });
        const children = await Promise.all([
          broker.push('pending-waiting', { data: { state: 'waiting' } }),
          broker.push('pending-priority', { data: { state: 'prioritized' }, priority: 10 }),
          broker.push('pending-delay', { data: { state: 'delayed' }, delay: 60_000 }),
          broker.push('pending-dependency', {
            data: { state: 'waiting-children' },
            dependsOn: [dependency.id],
          }),
        ]);
        for (const child of children) await broker.updateJobParent(child.id, parent.id);

        expect(await Promise.all(children.map((child) => broker.getJobState(child.id)))).toEqual([
          'waiting',
          'prioritized',
          'delayed',
          'waiting-children',
        ]);
        await broker.removeUnprocessedChildren(parent.id);

        for (const child of children) {
          await expect(broker.getJob(child.id)).resolves.toBeNull();
          await expect(verifier.getJob(child.id)).resolves.toBeNull();
        }
        const storedParent = await verifier.getJob(parent.id);
        expect(storedParent?.state).toBe('waiting');
        expect(storedParent?.job.childrenIds).toEqual([]);
        expect(storedParent?.job.dependsOn).toEqual([]);
        await expect(verifier.getJob(dependency.id)).resolves.not.toBeNull();

        const ids = children.map((child) => String(child.id));
        const [beforeRepeat] = await verifier.context.sql<{ removed: number; promoted: number }[]>`
          SELECT
            COUNT(*) FILTER (
              WHERE event_type = 'removed' AND job_id = ANY(${verifier.context.sql.array(ids, 'TEXT')})
            )::int AS removed,
            COUNT(*) FILTER (
              WHERE event_type = 'retried' AND job_id = ${String(parent.id)}
            )::int AS promoted
          FROM bunqueue_events
          WHERE namespace = ${value}
        `;
        expect(beforeRepeat).toEqual({ removed: children.length, promoted: 1 });

        const [beforeCount] = await verifier.context.sql<{ events: number }[]>`
          SELECT COUNT(*)::int AS events FROM bunqueue_events WHERE namespace = ${value}
        `;
        await broker.removeUnprocessedChildren(parent.id);
        const [afterRepeat] = await verifier.context.sql<{ events: number }[]>`
          SELECT COUNT(*)::int AS events FROM bunqueue_events WHERE namespace = ${value}
        `;
        expect(afterRepeat.events).toBe(beforeCount.events);
      } finally {
        await Promise.allSettled([broker.shutdownPostgres(), verifier.close()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'retains active and terminal children while removing a waiting sibling',
    async () => {
      const value = namespace('retained');
      const broker = manager(value, 'retained-manager');
      const verifier = store(value, 'retained-verifier');

      try {
        await Promise.all([broker.waitUntilReady(), verifier.initialize()]);
        const parent = await broker.push('retained-parent', { data: { parent: true } });
        const active = await broker.push('retained-active', { data: { state: 'active' } });
        const completed = await broker.push('retained-completed', {
          data: { state: 'completed' },
        });
        const failed = await broker.push('retained-failed', { data: { state: 'failed' } });
        const waiting = await broker.push('retained-waiting', { data: { state: 'waiting' } });
        for (const child of [active, completed, failed, waiting]) {
          await broker.updateJobParent(child.id, parent.id);
        }

        const activeClaim = await broker.pullWithLock('retained-active', 'active-owner');
        const completedClaim = await broker.pullWithLock('retained-completed', 'complete-owner');
        const failedClaim = await broker.pullWithLock('retained-failed', 'failed-owner');
        await broker.ack(completed.id, { persisted: true }, completedClaim.token!);
        await broker.fail(failed.id, 'terminal failure', failedClaim.token!, true);
        const activeBefore = await verifier.getJob(active.id);

        await broker.removeUnprocessedChildren(parent.id);

        await expect(verifier.getJob(waiting.id)).resolves.toBeNull();
        const activeAfter = await verifier.getJob(active.id);
        expect(activeAfter?.state).toBe('active');
        expect(activeAfter?.token).toBe(activeBefore?.token);
        expect(activeAfter?.leaseUntil).toBe(activeBefore?.leaseUntil);
        expect(activeAfter?.job.parentId).toBe(parent.id);
        expect((await verifier.getJob(completed.id))?.state).toBe('completed');
        expect(await verifier.getResult(completed.id)).toEqual({
          found: true,
          result: { persisted: true },
        });
        expect((await verifier.getJob(failed.id))?.state).toBe('failed');
        expect((await verifier.getJob(failed.id))?.dlqEntry).not.toBeNull();
        const storedParent = await verifier.getJob(parent.id);
        expect(storedParent?.state).toBe('waiting-children');
        expect(storedParent?.job.childrenIds).toEqual([active.id, completed.id, failed.id]);
        expect(activeClaim.job?.id).toBe(active.id);
      } finally {
        await Promise.allSettled([broker.shutdownPostgres(), verifier.close()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'keeps a fixed point of children required by surviving consumers',
    async () => {
      const value = namespace('shared');
      const broker = manager(value, 'shared-manager');
      const verifier = store(value, 'shared-verifier');

      try {
        await Promise.all([broker.waitUntilReady(), verifier.initialize()]);
        const parent = await broker.push('shared-parent', { data: { role: 'parent' } });
        const leaf = await broker.push('shared-leaf', { data: { role: 'leaf' } });
        const consumer = await broker.push('shared-consumer', {
          data: { role: 'consumer' },
          dependsOn: [leaf.id],
        });
        await broker.updateJobParent(leaf.id, parent.id);
        await broker.updateJobParent(consumer.id, parent.id);
        const external = await broker.push('shared-external', {
          data: { role: 'external' },
          dependsOn: [consumer.id],
        });

        await broker.removeUnprocessedChildren(parent.id);

        for (const child of [leaf, consumer]) {
          const stored = await verifier.getJob(child.id);
          expect(stored).not.toBeNull();
          if (!stored) throw new Error(`Retained child ${String(child.id)} was removed`);
          expect(stored.job.parentId).toBeNull();
          expect((stored.job.data as Record<string, unknown>).__parentId).toBeUndefined();
        }
        expect((await verifier.getJob(parent.id))?.state).toBe('waiting');
        expect((await verifier.getJob(external.id))?.state).toBe('waiting-children');

        const edges = await verifier.context.sql<Array<{ job_id: string; dependency_id: string }>>`
          SELECT job_id, dependency_id
          FROM bunqueue_dependencies
          WHERE namespace = ${value}
          ORDER BY job_id, dependency_id
        `;
        expect(edges).toEqual([
          { job_id: String(consumer.id), dependency_id: String(leaf.id) },
          { job_id: String(external.id), dependency_id: String(consumer.id) },
        ]);
      } finally {
        await Promise.allSettled([broker.shutdownPostgres(), verifier.close()]);
      }
    }
  );
});
