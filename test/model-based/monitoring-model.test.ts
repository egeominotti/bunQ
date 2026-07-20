import { describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import { generatePrometheusMetrics, type QueueStats } from '../../src/application/metricsExporter';
import { WebhookManager } from '../../src/application/webhookManager';
import { WorkerManager } from '../../src/application/workerManager';

type WorkerModel = { activeJobs: number; concurrency: number };
type MonitoringModel = {
  workers: Map<number, WorkerModel>;
  totalProcessed: number;
  totalFailed: number;
};
type Action =
  | { type: 'register'; id: number; concurrency: number }
  | { type: 'active'; id: number; activeJobs: number }
  | { type: 'increment'; id: number }
  | { type: 'complete'; id: number }
  | { type: 'fail'; id: number }
  | { type: 'unregister'; id: number };

const EMPTY_STATS: QueueStats = {
  waiting: 0,
  prioritized: 0,
  delayed: 0,
  active: 0,
  dlq: 0,
  completed: 0,
  totalPushed: 0n,
  totalPulled: 0n,
  totalCompleted: 0n,
  totalFailed: 0n,
  uptime: 0,
  cronJobs: 0,
  cronPending: 0,
};

const action = fc.oneof<Action>(
  fc.record({
    type: fc.constant('register'),
    id: fc.integer({ min: 0, max: 7 }),
    concurrency: fc.integer({ min: 1, max: 16 }),
  }),
  fc.record({
    type: fc.constant('active'),
    id: fc.integer({ min: 0, max: 7 }),
    activeJobs: fc.integer({ min: 0, max: 16 }),
  }),
  fc.record({ type: fc.constant('increment'), id: fc.integer({ min: 0, max: 7 }) }),
  fc.record({ type: fc.constant('complete'), id: fc.integer({ min: 0, max: 7 }) }),
  fc.record({ type: fc.constant('fail'), id: fc.integer({ min: 0, max: 7 }) }),
  fc.record({ type: fc.constant('unregister'), id: fc.integer({ min: 0, max: 7 }) })
);

function metric(output: string, name: string): number {
  const line = output.split('\n').find((candidate) => candidate.startsWith(`${name} `));
  if (!line) throw new Error(`Missing metric ${name}`);
  return Number(line.slice(name.length + 1));
}

describe('monitoring state model', () => {
  test('worker metrics conserve registry, active jobs, capacity, and outcomes', () => {
    fc.assert(
      fc.property(fc.array(action, { minLength: 1, maxLength: 80 }), (actions) => {
        const real = new WorkerManager();
        const model: MonitoringModel = {
          workers: new Map(),
          totalProcessed: 0,
          totalFailed: 0,
        };

        try {
          for (const current of actions) {
            const id = `model-worker-${current.id}`;
            const expected = model.workers.get(current.id);
            switch (current.type) {
              case 'register':
                real.register(id, ['jobs'], current.concurrency, { workerId: id });
                if (expected) expected.concurrency = current.concurrency;
                else {
                  model.workers.set(current.id, {
                    activeJobs: 0,
                    concurrency: current.concurrency,
                  });
                }
                break;
              case 'active':
                real.heartbeat(id, { activeJobs: current.activeJobs });
                if (expected) expected.activeJobs = current.activeJobs;
                break;
              case 'increment':
                real.incrementActive(id);
                if (expected) expected.activeJobs++;
                break;
              case 'complete':
                real.jobCompleted(id);
                if (expected) {
                  if (expected.activeJobs > 0) expected.activeJobs--;
                  model.totalProcessed++;
                }
                break;
              case 'fail':
                real.jobFailed(id);
                if (expected) {
                  if (expected.activeJobs > 0) expected.activeJobs--;
                  model.totalFailed++;
                }
                break;
              case 'unregister':
                expect(real.unregister(id)).toBe(model.workers.delete(current.id));
                break;
            }

            const stats = real.getStats();
            expect(stats.total).toBe(model.workers.size);
            expect(stats.activeJobs).toBe(
              [...model.workers.values()].reduce((sum, worker) => sum + worker.activeJobs, 0)
            );
            expect(stats.concurrencySlots).toBe(
              [...model.workers.values()].reduce((sum, worker) => sum + worker.concurrency, 0)
            );
            expect(stats.totalProcessed).toBe(model.totalProcessed);
            expect(stats.totalFailed).toBe(model.totalFailed);
          }

          const output = generatePrometheusMetrics(EMPTY_STATS, real, new WebhookManager());
          const stats = real.getStats();
          expect(metric(output, 'bunqueue_workers_registered')).toBe(stats.total);
          expect(metric(output, 'bunqueue_worker_active_jobs')).toBe(stats.activeJobs);
          expect(metric(output, 'bunqueue_worker_concurrency_slots')).toBe(stats.concurrencySlots);
          expect(metric(output, 'bunqueue_workers_processed_total')).toBe(model.totalProcessed);
          expect(metric(output, 'bunqueue_workers_failed_total')).toBe(model.totalFailed);
        } finally {
          real.stop();
        }
      }),
      { numRuns: 500 }
    );
  });
});
