import { getSharedManager } from '../../manager';
import type { TcpConnectionPool } from '../../tcpPool';
import { jobId } from '../../../domain/types/job';

export async function computeDependencies(
  id: string,
  queueName: string,
  tcp: TcpConnectionPool
): Promise<{ processed: Record<string, unknown>; unprocessed: string[] }> {
  const jobRes = await tcp.send({ cmd: 'GetJob', id });
  const parent = (jobRes as { job?: { childrenIds?: string[] } }).job;
  const childIds = parent?.childrenIds ?? [];
  const processed: Record<string, unknown> = {};
  const unprocessed: string[] = [];
  for (const childId of childIds) {
    const stateRes = await tcp.send({ cmd: 'GetState', id: childId });
    const state = (stateRes as { state?: string }).state ?? 'unknown';
    const key = `${queueName}:${childId}`;
    if (state === 'completed' || state === 'failed') {
      if (state === 'completed') {
        const resultResponse = await tcp.send({ cmd: 'GetResult', id: childId });
        processed[key] = (resultResponse as { result?: unknown }).result ?? null;
      } else {
        processed[key] = null;
      }
    } else {
      unprocessed.push(key);
    }
  }
  return { processed, unprocessed };
}

export async function computeSimpleDependencies(
  id: string,
  queueName: string,
  embedded: boolean | undefined,
  tcp: TcpConnectionPool | null | undefined
): Promise<{ processed: Record<string, unknown>; unprocessed: string[] }> {
  const processed: Record<string, unknown> = {};
  const unprocessed: string[] = [];

  let childIds: string[] = [];
  if (embedded) {
    const job = await getSharedManager().getJob(jobId(id));
    childIds = (job?.childrenIds ?? []).map(String);
  } else if (tcp) {
    const jobRes = await tcp.send({ cmd: 'GetJob', id });
    const parent = (jobRes as { job?: { childrenIds?: string[] } }).job;
    childIds = (parent?.childrenIds ?? []).map(String);
  }

  for (const childId of childIds) {
    let state = 'unknown';
    let result: unknown;
    if (embedded) {
      const manager = getSharedManager();
      state = await manager.getJobState(jobId(childId));
      if (state === 'completed') result = manager.getResult(jobId(childId));
    } else if (tcp) {
      const response = await tcp.send({ cmd: 'GetState', id: childId });
      state = (response as { state?: string }).state ?? 'unknown';
      if (state === 'completed') {
        const resultResponse = await tcp.send({ cmd: 'GetResult', id: childId });
        result = (resultResponse as { result?: unknown }).result;
      }
    }
    const key = `${queueName}:${childId}`;
    if (state === 'completed' || state === 'failed') {
      processed[key] = result ?? null;
    } else {
      unprocessed.push(key);
    }
  }
  return { processed, unprocessed };
}
