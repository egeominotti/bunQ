import type {
  ListWorkersCommand,
  RegisterWorkerCommand,
  UnregisterWorkerCommand,
} from '../../../../domain/types/command';
import type { Response } from '../../../../domain/types/response';
import * as response from '../../../../domain/types/response';
import type { HandlerContext } from '../../types';

const WORKER_TIMEOUT_MS = parseInt(Bun.env.WORKER_TIMEOUT_MS ?? '30000', 10);

function computeWorkerStatus(lastSeen: number, now: number): 'active' | 'stale' {
  return now - lastSeen < WORKER_TIMEOUT_MS ? 'active' : 'stale';
}

export function handleRegisterWorker(
  command: RegisterWorkerCommand,
  context: HandlerContext,
  requestId?: string
): Response {
  const worker = context.queueManager.registerWorker(
    command.name,
    command.queues,
    command.concurrency,
    {
      workerId: command.workerId,
      hostname: command.hostname,
      pid: command.pid,
      startedAt: command.startedAt,
      clientId: context.clientId,
    }
  );
  return response.data(
    {
      workerId: worker.id,
      name: worker.name,
      queues: worker.queues,
      concurrency: worker.concurrency,
      hostname: worker.hostname,
      pid: worker.pid,
      status: 'active' as const,
      registeredAt: worker.registeredAt,
      lastSeen: worker.lastSeen,
      activeJobs: worker.activeJobs,
      processedJobs: worker.processedJobs,
      failedJobs: worker.failedJobs,
      currentJob: worker.currentJob,
    },
    requestId
  );
}

export function handleUnregisterWorker(
  command: UnregisterWorkerCommand,
  context: HandlerContext,
  requestId?: string
): Response {
  return context.queueManager.unregisterWorker(command.workerId)
    ? response.data({ removed: true }, requestId)
    : response.error('Worker not found', requestId);
}

export function handleListWorkers(
  _command: ListWorkersCommand,
  context: HandlerContext,
  requestId?: string
): Response {
  const workers = context.queueManager.workerManager.list();
  const now = Date.now();
  return response.data(
    {
      workers: workers.map((worker) => ({
        id: worker.id,
        name: worker.name,
        queues: worker.queues,
        concurrency: worker.concurrency,
        hostname: worker.hostname,
        pid: worker.pid,
        status: computeWorkerStatus(worker.lastSeen, now),
        registeredAt: worker.registeredAt,
        lastSeen: worker.lastSeen,
        activeJobs: worker.activeJobs,
        processedJobs: worker.processedJobs,
        failedJobs: worker.failedJobs,
        currentJob: worker.currentJob,
        uptime: now - worker.registeredAt,
      })),
      stats: context.queueManager.workerManager.getStats(),
    },
    requestId
  );
}
