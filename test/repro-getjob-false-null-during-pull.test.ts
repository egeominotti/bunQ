/**
 * REPRO — getJob() transiently returns null for an EXISTING job during the
 * pull transition (queue -> processing), embedded mode.
 *
 * Run: bun test test/repro-getjob-false-null-during-pull.test.ts
 *
 * Root cause: pullJob/pullJobBatch (src/application/operations/pull.ts) pop the
 * job from the shard priority queue inside tryPullFromShard/tryPullBatchFromShard
 * (q.pop() at pull.ts:84) while jobIndex STILL says { type: 'queue' }. The
 * jobIndex entry is only flipped to { type: 'processing' } later, in
 * moveToProcessing/moveToProcessingBatch (pull.ts:111 / pull.ts:158), after an
 * await boundary (a separate processingLocks write-lock acquisition). A getJob()
 * issued in that window (src/application/operations/queryOperations.ts:44-55)
 * follows the stale 'queue' location, misses in the shard queue (already
 * popped), misses waitingDeps/waitingChildren, and returns null — for a job
 * that exists and is about to become active.
 *
 * User-visible invariant this violates: once getJob(id) observes null for a
 * job, the null must be PERMANENT (ids are uuidv7, never reused; removal is
 * terminal). Today a poller can observe JOB -> null -> JOB(active), which
 * defeats "poll getJob until null" as a completion signal (this is exactly
 * what de-flaking scripts/embedded/test-retry-backoff.ts test 6 tripped over:
 * the false null fired before the processor ever ran, the worker was closed,
 * and the never-processed job was found stuck 'active' afterwards).
 *
 * RED on current code (a null -> job flicker is observed within a few
 * attempts) -> GREEN once getJob never returns null for an existing job
 * (e.g. jobIndex updated atomically with the queue pop, or a fallback lookup
 * in processingShards before concluding null).
 *
 * Embedded only; DOES NOT touch src/.
 */
import { describe, test, expect } from 'bun:test';
import { Queue } from '../src/client/queue/queue';
import { Worker } from '../src/client/worker';

const ATTEMPTS = 25;
// The false-null window sits at worker startup (first pull). Processing +
// batched-ack removal complete within ~60ms, so a short watch is enough.
const FLICKER_WATCH_MS = 300;

describe('repro: getJob false null during pull transition', () => {
  test('once getJob(id) returns null, it must stay null (no null -> active flicker)', async () => {
    for (let attempt = 0; attempt < ATTEMPTS; attempt++) {
      const queueName = `repro-pull-window-${attempt}`;
      const queue = new Queue<{ v: number }>(queueName, { embedded: true });
      queue.obliterate();

      const job = await queue.add('remove-job', { v: 1 }, { removeOnComplete: true });

      const worker = new Worker<{ v: number }>(queueName, async () => ({ done: true }), {
        concurrency: 1,
        embedded: true,
      });

      try {
        // Poll until getJob returns null. With removeOnComplete this is
        // EITHER the genuine post-ack removal OR the buggy pull-transition
        // window (job popped from queue, jobIndex not yet 'processing').
        let g = await queue.getJob(job.id);
        const deadline = Date.now() + 15000;
        while (g !== null && Date.now() < deadline) {
          g = await queue.getJob(job.id);
        }
        expect(g).toBeNull();

        // A null must be permanent: the id is a uuidv7 that is never reused
        // and removeOnComplete removal is terminal. Any non-null observation
        // after a null is the bug.
        let flicker: unknown = null;
        const watchUntil = Date.now() + FLICKER_WATCH_MS;
        while (Date.now() < watchUntil) {
          const again = await queue.getJob(job.id);
          if (again !== null) {
            flicker = again;
            break;
          }
        }

        if (flicker !== null) {
          const state = await queue.getJobState(job.id);
          expect(
            `attempt ${attempt}: getJob flickered null -> job (state=${state}) — ` +
              'pull popped the job from the shard queue before jobIndex moved to processing'
          ).toBeNull();
        }
      } finally {
        await worker.close();
        queue.obliterate();
      }
    }
  }, 60000);
});
