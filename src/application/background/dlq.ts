import { queueLog } from '../../shared/logger';
import * as dlqOperations from '../dlqManager';
import type { BackgroundContext } from '../types';

export function performDlqMaintenance(ctx: BackgroundContext): void {
  const dlqContext = {
    shards: ctx.shards,
    jobIndex: ctx.jobIndex,
    jobResults: ctx.jobResults,
    jobResultQueues: ctx.jobResultQueues,
    dependencyResults: ctx.dependencyResults,
    customIdMap: ctx.customIdMap,
    jobLogs: ctx.jobLogs,
    jobLogQueues: ctx.jobLogQueues,
    storage: ctx.storage,
  };

  for (const queueName of ctx.queueNamesCache) {
    try {
      const retried = dlqOperations.processAutoRetry(queueName, dlqContext);
      if (retried > 0) {
        ctx.dashboardEmit?.('dlq:auto-retried', { queue: queueName, count: retried });
      }
      const expired = dlqOperations.purgeExpiredDlq(queueName, dlqContext);
      if (expired > 0) {
        ctx.dashboardEmit?.('dlq:expired', { queue: queueName, count: expired });
      }
    } catch (error) {
      queueLog.error('DLQ maintenance failed', { queue: queueName, error: String(error) });
    }
  }
}
