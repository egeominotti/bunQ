import { FailureReason } from '../../domain/types/dlq';
import { queueLog } from '../../shared/logger';
import type { BackgroundContext } from '../types';

export function checkJobTimeouts(ctx: BackgroundContext): void {
  const now = Date.now();
  for (const processingShard of ctx.processingShards) {
    for (const [jobId, job] of processingShard) {
      if (job.timeout && job.startedAt && now - job.startedAt > job.timeout) {
        ctx.dashboardEmit?.('job:timeout', {
          jobId: String(jobId),
          queue: job.queue,
          timeout: job.timeout,
        });
        ctx.timedOutJobs?.add(jobId);
        ctx.fail(jobId, 'Job timeout exceeded', FailureReason.Timeout).catch((error: unknown) => {
          queueLog.error('Failed to mark timed out job as failed', {
            jobId: String(jobId),
            error: String(error),
          });
        });
      }
    }
  }
}
