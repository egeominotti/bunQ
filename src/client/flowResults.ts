import { jobId } from '../domain/types/job';
import { getSharedManager } from './manager';
import type { TcpConnectionPool } from './tcpPool';
import { assertFlowTcpOk } from './flowJobTypes';

interface FlowResultContext {
  embedded: boolean;
  tcp: TcpConnectionPool | null;
}

export type RuntimeResult<T> = T | Promise<T>;

/** Keep the embedded call synchronous while providing the same result over TCP. */
export function getParentResult<R>(
  ctx: FlowResultContext,
  parentId: string
): RuntimeResult<R | undefined> {
  if (ctx.embedded) return getSharedManager().getResult(jobId(parentId)) as R | undefined;
  if (!ctx.tcp) return Promise.reject(new Error('FlowProducer TCP connection is unavailable'));
  return ctx.tcp.send({ cmd: 'GetResult', id: parentId }).then((response) => {
    assertFlowTcpOk(response, 'GetResult');
    return response.result as R | undefined;
  });
}

/** Resolve parent results in input order and preserve falsy completed values. */
export function getParentResults<R>(
  ctx: FlowResultContext,
  parentIds: string[]
): RuntimeResult<Map<string, R>> {
  if (ctx.embedded) {
    const manager = getSharedManager();
    const results = new Map<string, R>();
    for (const id of parentIds) {
      const result = manager.getResult(jobId(id));
      if (result !== undefined) results.set(id, result as R);
    }
    return results;
  }
  const tcp = ctx.tcp;
  if (!tcp) return Promise.reject(new Error('FlowProducer TCP connection is unavailable'));
  return Promise.all(
    parentIds.map(async (id) => {
      const response = await tcp.send({ cmd: 'GetResult', id });
      assertFlowTcpOk(response, 'GetResult');
      return [id, response.result] as const;
    })
  ).then((entries) => {
    const results = new Map<string, R>();
    for (const [id, result] of entries) {
      if (result !== undefined) results.set(id, result as R);
    }
    return results;
  });
}
