import type {
  ListWorkersCommand,
  RegisterWorkerCommand,
  UnregisterWorkerCommand,
} from '../../../../domain/types/command';
import type { Response } from '../../../../domain/types/response';
import * as response from '../../../../domain/types/response';
import type { HandlerContext } from '../../types';
import type { CreateWorkerOptions, Worker } from '../../../../domain/types/worker';

type DurableWorkerManager = HandlerContext['queueManager'] & {
  registerWorkerDurable?: (
    name: string,
    queues: string[],
    concurrency?: number,
    options?: CreateWorkerOptions
  ) => Promise<Worker>;
  unregisterWorkerDurable?: (id: string, clientId?: string) => Promise<boolean>;
  listWorkersDurable?: () => Promise<Worker[]>;
};

const WORKER_TIMEOUT_MS = parseInt(Bun.env.WORKER_TIMEOUT_MS ?? '30000', 10);

function computeWorkerStatus(lastSeen: number, now: number): 'active' | 'stale' {
  return now - lastSeen < WORKER_TIMEOUT_MS ? 'active' : 'stale';
}

export function handleRegisterWorker(
  command: RegisterWorkerCommand,
  context: HandlerContext,
  requestId?: string
): Response | Promise<Response> {
  const manager = context.queueManager as DurableWorkerManager;
  const options: CreateWorkerOptions = {
    workerId: command.workerId,
    hostname: command.hostname,
    pid: command.pid,
    startedAt: command.startedAt,
    clientId: context.clientId,
  };
  const complete = (worker: Worker): Response =>
    response.data(
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
  return manager.registerWorkerDurable
    ? manager
        .registerWorkerDurable(command.name, command.queues, command.concurrency, options)
        .then(complete)
    : complete(manager.registerWorker(command.name, command.queues, command.concurrency, options));
}

export function handleUnregisterWorker(
  command: UnregisterWorkerCommand,
  context: HandlerContext,
  requestId?: string
): Response | Promise<Response> {
  const manager = context.queueManager as DurableWorkerManager;
  const complete = (removed: boolean): Response =>
    removed
      ? response.data({ removed: true }, requestId)
      : response.error('Worker not found', requestId);
  return manager.unregisterWorkerDurable
    ? manager.unregisterWorkerDurable(command.workerId, context.clientId).then(complete)
    : complete(manager.unregisterWorker(command.workerId));
}

interface WorkerLifetimeStats {
  readonly totalProcessed: number;
  readonly totalFailed: number;
  readonly activeJobs: number;
}

function workerListResponse(
  workers: Worker[],
  requestId?: string,
  lifetime?: WorkerLifetimeStats
): Response {
  const now = Date.now();
  const active = workers.filter((worker) => computeWorkerStatus(worker.lastSeen, now) === 'active');
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
      stats: {
        total: workers.length,
        active: active.length,
        totalProcessed:
          lifetime?.totalProcessed ??
          workers.reduce((sum, worker) => sum + worker.processedJobs, 0),
        totalFailed:
          lifetime?.totalFailed ?? workers.reduce((sum, worker) => sum + worker.failedJobs, 0),
        activeJobs:
          lifetime?.activeJobs ?? workers.reduce((sum, worker) => sum + worker.activeJobs, 0),
        concurrencySlots: active.reduce((sum, worker) => sum + worker.concurrency, 0),
      },
    },
    requestId
  );
}

export function handleListWorkers(
  _command: ListWorkersCommand,
  context: HandlerContext,
  requestId?: string
): Response | Promise<Response> {
  const manager = context.queueManager as DurableWorkerManager;
  return manager.listWorkersDurable
    ? manager.listWorkersDurable().then((workers) => workerListResponse(workers, requestId))
    : workerListResponse(manager.workerManager.list(), requestId, manager.workerManager.getStats());
}
