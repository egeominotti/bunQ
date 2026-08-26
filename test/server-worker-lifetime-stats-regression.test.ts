import { describe, expect, test } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { handleListWorkers } from '../src/infrastructure/server/handlers/monitoring/workers';
import type { HandlerContext } from '../src/infrastructure/server/types';

describe('SQLite worker monitoring lifetime stats regression', () => {
  test('preserves processed and failed totals after a worker unregisters', () => {
    const queueManager = new QueueManager();
    const context: HandlerContext = {
      queueManager,
      authTokens: new Set(),
      authenticated: false,
    };
    try {
      const worker = queueManager.registerWorker('retired-worker', ['worker-stats']);
      expect(
        queueManager.workerManager.heartbeat(worker.id, { processed: 7, failed: 2, activeJobs: 0 })
      ).toBe(true);
      expect(queueManager.unregisterWorker(worker.id)).toBe(true);

      const response = handleListWorkers({ cmd: 'ListWorkers' }, context);
      expect(response).not.toBeInstanceOf(Promise);
      expect(response).toMatchObject({
        ok: true,
        data: {
          workers: [],
          stats: { totalProcessed: 7, totalFailed: 2, activeJobs: 0 },
        },
      });
    } finally {
      queueManager.shutdown();
    }
  });
});
