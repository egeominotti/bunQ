import { jobId } from '../domain/types/job';
import { getSharedManager } from './manager';
import type { TcpConnectionPool } from './tcpPool';
import { assertFlowTcpOk } from './flowJobTypes';

export interface FlowDependencies {
  processed: Record<string, unknown>;
  unprocessed: string[];
}

async function fetchChildIds(
  id: string,
  embedded: boolean,
  tcp: TcpConnectionPool | null
): Promise<string[]> {
  if (embedded) {
    const job = await getSharedManager().getJob(jobId(id));
    if (!job) return [];
    return job.childrenIds.map(String);
  }
  if (!tcp) return [];
  const response = await tcp.send({ cmd: 'GetJob', id });
  if (response.ok !== true && response.error === 'Job not found') return [];
  assertFlowTcpOk(response, 'GetJob');
  const parent = response.job as { childrenIds?: string[] } | undefined;
  return (parent?.childrenIds ?? []).map(String);
}

async function embeddedDependency(
  fallbackQueue: string,
  childId: string
): Promise<{ key: string; state: string; result: unknown }> {
  const manager = getSharedManager();
  const child = await manager.getJob(jobId(childId));
  if (!child) throw new Error(`Flow child job ${childId} not found`);
  const state = await manager.getJobState(jobId(childId));
  return {
    key: `${child.queue || fallbackQueue}:${childId}`,
    state,
    result: state === 'completed' ? manager.getResult(jobId(childId)) : undefined,
  };
}

async function tcpDependency(
  fallbackQueue: string,
  childId: string,
  tcp: TcpConnectionPool
): Promise<{ key: string; state: string; result: unknown }> {
  const response = await tcp.send({ cmd: 'GetJob', id: childId });
  assertFlowTcpOk(response, 'GetJob');
  const child = response.job as { queue?: string; state?: string } | undefined;
  if (!child) throw new Error(`Flow child job ${childId} not found`);
  const state = child.state ?? 'unknown';
  let result: unknown;
  if (state === 'completed') {
    const resultResponse = await tcp.send({ cmd: 'GetResult', id: childId });
    assertFlowTcpOk(resultResponse, 'GetResult');
    result = resultResponse.result;
  }
  return { key: `${child.queue ?? fallbackQueue}:${childId}`, state, result };
}

/** Resolve dependency keys using the actual queue of every child. */
export async function getFlowDependencies(
  id: string,
  queueName: string,
  embedded: boolean,
  tcp: TcpConnectionPool | null
): Promise<FlowDependencies> {
  const childIds = await fetchChildIds(id, embedded, tcp);
  const processed: Record<string, unknown> = {};
  const unprocessed: string[] = [];

  for (const childId of childIds) {
    const dependency = embedded
      ? await embeddedDependency(queueName, childId)
      : tcp
        ? await tcpDependency(queueName, childId, tcp)
        : { key: `${queueName}:${childId}`, state: 'unknown', result: undefined };
    if (dependency.state === 'completed' || dependency.state === 'failed') {
      processed[dependency.key] = dependency.result ?? null;
    } else {
      unprocessed.push(dependency.key);
    }
  }
  return { processed, unprocessed };
}
