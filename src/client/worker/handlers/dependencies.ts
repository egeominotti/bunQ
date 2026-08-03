import { jobId, type Job as InternalJob } from '../../../domain/types/job';
import { removeJobDeduplicationKey } from '../../jobDeduplication';
import { getSharedManager } from '../../manager';
import type { JobDependencies, JobDependenciesCount } from '../../types';
import type { PendingTransitionSettlement, TcpConnection } from '../types';

export function createWaitUntilFinishedHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (id: string, _queueEvents: unknown, ttl?: number) => Promise<unknown> {
  return async (id: string, _queueEvents: unknown, ttl?: number) => {
    const timeout = ttl ?? 30000;
    if (embedded) {
      const manager = getSharedManager();
      const job = await manager.getJob(jobId(id));
      if (!job) throw new Error(`Job ${id} not found`);
      if (job.completedAt) return manager.getResult(jobId(id));
      const completed = await manager.waitForJobCompletion(jobId(id), timeout);
      if (!completed) throw new Error(`waitUntilFinished timed out after ${timeout}ms`);
      return manager.getResult(jobId(id));
    }
    if (!tcp) throw new Error('waitUntilFinished: no connection');
    const response = await tcp.send({ cmd: 'WaitJob', id, timeout });
    const result = response as { completed?: boolean; result?: unknown };
    if (!result.completed) throw new Error(`waitUntilFinished timed out after ${timeout}ms`);
    return result.result;
  };
}

export function createDiscardHandler(
  embedded: boolean,
  tcp: TcpConnection | null,
  options: {
    token?: string;
    onPending?: (pending: Promise<PendingTransitionSettlement>) => void;
  } = {}
): (id: string) => void {
  let pending: Promise<PendingTransitionSettlement> | null = null;
  return (id: string) => {
    if (pending) return;
    const operation = (async (): Promise<PendingTransitionSettlement> => {
      try {
        if (embedded) {
          const applied = await getSharedManager().discard(jobId(id), options.token);
          return { status: applied ? 'applied' : 'ignored' };
        }
        if (!tcp) return { status: 'failed', error: new Error('Discard: no connection') };
        const response = await tcp.send({
          cmd: 'Discard',
          id,
          ...(options.token === undefined ? {} : { token: options.token }),
        });
        if (response.ok === true) return { status: 'applied' };
        const message = typeof response.error === 'string' ? response.error : 'Discard failed';
        if (/not found|not active|not in processing/i.test(message)) return { status: 'ignored' };
        return { status: 'failed', error: new Error(message) };
      } catch (error) {
        return {
          status: 'failed',
          error: error instanceof Error ? error : new Error(String(error)),
        };
      }
    })();
    pending = operation;
    options.onPending?.(operation);
  };
}

export function createGetDependenciesHandler(
  embedded: boolean,
  tcp: TcpConnection | null,
  internalJob: InternalJob
): (_id: string) => Promise<JobDependencies> {
  return async () => {
    const childIds = internalJob.childrenIds;
    const processed: Record<string, unknown> = {};
    const unprocessed: string[] = [];
    for (const childId of childIds) {
      let state: string = 'unknown';
      let result: unknown;
      if (embedded) {
        const manager = getSharedManager();
        state = await manager.getJobState(childId);
        if (state === 'completed') result = manager.getResult(childId);
      } else if (tcp) {
        const stateResponse = await tcp.send({ cmd: 'GetState', id: String(childId) });
        state = (stateResponse as { state?: string }).state ?? 'unknown';
        if (state === 'completed') {
          const resultResponse = await tcp.send({ cmd: 'GetResult', id: String(childId) });
          result = (resultResponse as { result?: unknown }).result;
        }
      }
      const key = `${internalJob.queue}:${String(childId)}`;
      if (state === 'completed' || state === 'failed') processed[key] = result ?? null;
      else unprocessed.push(key);
    }
    return { processed, unprocessed };
  };
}

export function createGetDependenciesCountHandler(
  embedded: boolean,
  tcp: TcpConnection | null,
  internalJob: InternalJob
): (_id: string) => Promise<JobDependenciesCount> {
  const getDependencies = createGetDependenciesHandler(embedded, tcp, internalJob);
  return async (id: string) => {
    const dependencies = await getDependencies(id);
    return {
      processed: Object.keys(dependencies.processed).length,
      unprocessed: dependencies.unprocessed.length,
    };
  };
}

export function createRemoveDeduplicationKeyHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (id: string) => Promise<boolean> {
  return (id) => removeJobDeduplicationKey(id, embedded, tcp);
}
