import { expect, test } from 'bun:test';
import { SQL } from 'bun';
import { planFlows } from '../src/client/flowPlan';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import { cleanupPostgresNamespace } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;

test.skipIf(!postgresUrl)(
  'persists a nine-job flow with one ordered event batch and exact durable topology',
  async () => {
    const namespace = `test-flow-admission-batch-${Date.now()}-${crypto.randomUUID()}`;
    const leafQueue = 'flow-admission-leaf';
    const rootQueue = 'flow-admission-root';
    const suffix = crypto.randomUUID();
    const plan = planFlows([
      {
        children: Array.from({ length: 8 }, (_, index) => ({
          data: { value: index },
          name: `leaf-${index}`,
          opts: { durable: true, jobId: `${suffix}-leaf-${index}` },
          queueName: leafQueue,
        })),
        data: { kind: 'root' },
        name: 'root',
        opts: { durable: true, jobId: `${suffix}-root` },
        queueName: rootQueue,
      },
    ]).batch;
    const store = new PostgresQueueStore({
      url: postgresUrl!,
      namespace,
      brokerId: 'flow-admission-batch',
      poolSize: 2,
    });
    const observer = new SQL(postgresUrl!, { max: 1 });
    try {
      await store.initialize();
      const jobs = await store.insertFlow(plan);

      expect(jobs).toHaveLength(9);
      expect(jobs.slice(0, 8).every((job) => job.timeline.at(-1)?.state === 'waiting')).toBe(true);
      expect(jobs[8]?.timeline.at(-1)?.state).toBe('waiting-children');
      expect(new Set(jobs.map((job) => job.timeline.at(-1)?.timestamp)).size).toBe(1);

      const events = await observer<
        Array<{ event_type: string; job_id: string; occurred_at: number | string | bigint }>
      >`
        SELECT event_type, job_id, occurred_at
        FROM bunqueue_events
        WHERE namespace = ${namespace}
        ORDER BY id
      `;
      expect(events.map(({ event_type }) => event_type)).toEqual(Array(9).fill('pushed'));
      expect(events.map(({ job_id }) => job_id)).toEqual(plan.jobs.map(({ id }) => String(id)));
      expect(new Set(events.map(({ occurred_at }) => Number(occurred_at))).size).toBe(1);
      expect(Number(events[0]?.occurred_at)).toBe(jobs[0]?.timeline.at(-1)?.timestamp);

      const [durable] = await observer<
        Array<{
          dependencies: number | string | bigint;
          jobs: number | string | bigint;
          queues: number | string | bigint;
          waiting_children: number | string | bigint;
        }>
      >`
        SELECT
          (SELECT COUNT(*) FROM bunqueue_jobs WHERE namespace = ${namespace}) AS jobs,
          (SELECT COUNT(*) FROM bunqueue_jobs
           WHERE namespace = ${namespace} AND state = 'waiting-children') AS waiting_children,
          (SELECT COUNT(*) FROM bunqueue_dependencies
           WHERE namespace = ${namespace}) AS dependencies,
          (SELECT COUNT(*) FROM bunqueue_queue_state
           WHERE namespace = ${namespace}) AS queues
      `;
      expect({
        dependencies: Number(durable.dependencies),
        jobs: Number(durable.jobs),
        queues: Number(durable.queues),
        waitingChildren: Number(durable.waiting_children),
      }).toEqual({ dependencies: 8, jobs: 9, queues: 2, waitingChildren: 1 });
    } finally {
      await Promise.allSettled([store.close(), observer.close({ timeout: 5 })]);
      await cleanupPostgresNamespace(postgresUrl!, namespace);
    }
  },
  15_000
);
