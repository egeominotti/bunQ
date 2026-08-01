import type { QueueManager } from '../../../application/queueManager';
import { tcpLog } from '../../../shared/logger';

export async function releaseClientJobsWithRetry(
  queueManager: QueueManager,
  clientId: string,
  maxRetries = 3
): Promise<number> {
  let lastError: Error | undefined;
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await queueManager.releaseClientJobs(clientId);
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      await Bun.sleep(100 * Math.pow(2, attempt));
    }
  }
  tcpLog.error('Failed to release client jobs after retries', {
    clientId,
    error: lastError?.message,
  });
  throw lastError ?? new Error('Failed to release client jobs after retries');
}
