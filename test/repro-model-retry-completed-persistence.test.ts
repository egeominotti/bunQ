import { Database } from 'bun:sqlite';
import { afterEach, describe, expect, test } from 'bun:test';
import { Queue } from '../src/client';
import { jobId } from '../src/domain/types/job';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitForState,
} from './docs-guide-support';

let harness: CoreE2eHarness | null = null;

afterEach(async () => {
  await closeHarness(harness);
  harness = null;
});

for (const mode of MODES) {
  describe(`retryCompleted persistence regression [${mode}]`, () => {
    test('requeue clears completed execution metadata and result before restart', async () => {
      harness = await startHarness('model-retry-completed-persistence', mode);
      const queue = harness.queue<{ generation: number }>(`retry-completed-${mode}`);
      const firstWorker = harness.worker(
        queue.name,
        async (job) => {
          await job.updateProgress(42, 'first execution');
          return { generation: 1 };
        },
        { concurrency: 1, lockDuration: 60_000 }
      );
      firstWorker.on('error', (error) => {
        throw error;
      });

      const job = await queue.add('retry-completed', { generation: 1 }, { durable: true });
      await waitForState(queue, job.id, 'completed');
      await firstWorker.close();
      expect((await queue.getJob(job.id))?.returnvalue).toEqual({ generation: 1 });

      const retriedAt = Date.now();
      expect(await queue.retryCompletedAsync(job.id)).toBe(1);
      await waitForState(queue, job.id, 'waiting');

      const retried = await queue.getJob(job.id);
      expect(await queue.getJobState(job.id)).toBe('waiting');
      expect(retried?.progress).toBe(0);
      expect(retried?.processedOn).toBeUndefined();
      expect(retried?.finishedOn).toBeUndefined();
      expect(retried?.returnvalue).toBeUndefined();
      const internal = await harness.brokerManager().getJob(jobId(job.id));
      expect(internal).toMatchObject({
        progress: 0,
        progressMessage: null,
        startedAt: null,
        completedAt: null,
      });
      expect(internal?.lastHeartbeat).toBeGreaterThanOrEqual(retriedAt);
      expect(internal?.lastHeartbeat).toBe(internal?.runAt);

      const db = new Database(harness.dataPath, { readonly: true });
      try {
        const row = db
          .query<
            {
              completed_at: number | null;
              last_heartbeat: number;
              progress: number;
              progress_msg: string | null;
              started_at: number | null;
              state: string;
            },
            [string]
          >(
            'SELECT state, started_at, completed_at, progress, progress_msg, last_heartbeat FROM jobs WHERE id = ?'
          )
          .get(job.id);
        expect(row).toEqual({
          state: 'waiting',
          started_at: null,
          completed_at: null,
          progress: 0,
          progress_msg: null,
          last_heartbeat: internal?.lastHeartbeat,
        });
        expect(
          db
            .query<{ count: number }, [string]>(
              'SELECT COUNT(*) AS count FROM job_results WHERE job_id = ?'
            )
            .get(job.id)?.count
        ).toBe(0);
      } finally {
        db.close();
      }

      const queueName = queue.name;
      await queue.close();
      await harness.restartBroker();
      const recoveredQueue = new Queue<{ generation: number }>(queueName, harness.queueOptions());
      harness.addCleanup(() => recoveredQueue.close());
      await waitForState(recoveredQueue, job.id, 'waiting');
      expect((await recoveredQueue.getJob(job.id))?.returnvalue).toBeUndefined();

      const secondWorker = harness.worker(queueName, () => ({ generation: 2 }), {
        concurrency: 1,
        lockDuration: 60_000,
      });
      secondWorker.on('error', (error) => {
        throw error;
      });
      await waitForState(recoveredQueue, job.id, 'completed');
      expect((await recoveredQueue.getJob(job.id))?.returnvalue).toEqual({ generation: 2 });
    }, 30_000);
  });
}
