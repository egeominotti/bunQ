import type { QueueManager } from '../../../application/queueManager';
import { jobId as toJobId } from '../../../domain/types/job';
import type { CloudSnapshot } from '../types';

export async function collectStallDetails(
  queueManager: QueueManager
): Promise<CloudSnapshot['stallDetails']> {
  try {
    if (queueManager.getMemoryStats().stalledCandidates === 0) return [];
    const stalledIds = (queueManager as unknown as { stalledCandidates: Set<string> })
      .stalledCandidates;
    if (stalledIds.size === 0) return [];

    const now = Date.now();
    const details: CloudSnapshot['stallDetails'] = [];
    for (const id of [...stalledIds].slice(0, 20)) {
      try {
        const job = await queueManager.getJob(toJobId(id));
        if (!job) continue;
        details.push({
          jobId: id,
          queue: job.queue,
          workerId: null,
          stalledAt: job.startedAt ?? now,
          stalledForMs: now - (job.startedAt ?? now),
        });
      } catch {
        // Skip job on error.
      }
    }
    return details;
  } catch {
    return [];
  }
}
