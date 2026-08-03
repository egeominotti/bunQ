import type { Job } from '../../domain/types/job';
import { queueLog } from '../../shared/logger';
import { buildRepeatSuccessor } from '../repeatJobs';
import type { QueueManagerRuntime } from '../types';

/** Schedule one successor after a successful repeat generation. */
export function handleRepeat(runtime: QueueManagerRuntime, job: Job): void {
  const input = buildRepeatSuccessor(job);
  if (!input) return;
  const oldId = job.id;

  void runtime
    .push(job.queue, input)
    .then((newJob) => {
      runtime.repeatChain.set(oldId, newJob.id);
      if (runtime.repeatChain.size > 10_000) {
        const first = runtime.repeatChain.keys().next().value;
        if (first !== undefined) runtime.repeatChain.delete(first);
      }
    })
    .catch((error: unknown) => {
      queueLog.error('Failed to create repeat successor', {
        jobId: String(oldId),
        queue: job.queue,
        error: String(error),
      });
    });
}
