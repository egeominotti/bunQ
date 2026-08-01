import { jobId } from '../domain/types/job';
import { getSharedManager } from './manager';
import { buildFailCommand, failEmbeddedArgs } from './queue/failWire';
import { assertFlowTcpOk, type FlowJobRuntime } from './flowJobTypes';
import { removeJobDeduplicationKey } from './jobDeduplication';

/** Lifecycle and failure-inspection methods exposed by a FlowProducer Job. */
export function buildFlowJobMoveMethods(runtime: FlowJobRuntime) {
  const { id, embedded, tcp } = runtime;
  return {
    moveToCompleted: async (returnValue: unknown) => {
      if (embedded) {
        await getSharedManager().ack(jobId(id), returnValue);
        return null;
      }
      if (tcp) assertFlowTcpOk(await tcp.send({ cmd: 'ACK', id, result: returnValue }), 'ACK');
      return null;
    },
    moveToFailed: async (error: Error) => {
      if (embedded) {
        await getSharedManager().fail(jobId(id), ...failEmbeddedArgs(error));
        return;
      }
      if (tcp) assertFlowTcpOk(await tcp.send(buildFailCommand(id, error)), 'FAIL');
    },
    moveToWait: async () => {
      if (embedded) return getSharedManager().moveActiveToWait(jobId(id));
      if (!tcp) return false;
      assertFlowTcpOk(await tcp.send({ cmd: 'MoveToWait', id }), 'MoveToWait');
      return true;
    },
    moveToDelayed: async (timestamp: number) => {
      const delay = Math.max(0, timestamp - Date.now());
      if (embedded) return void (await getSharedManager().moveToDelayed(jobId(id), delay));
      if (!tcp) return;
      assertFlowTcpOk(await tcp.send({ cmd: 'MoveToDelayed', id, delay }), 'MoveToDelayed');
    },
    moveToWaitingChildren: async () => {
      if (embedded) return getSharedManager().moveToWaitingChildren(jobId(id));
      if (!tcp) return false;
      const response = await tcp.send({ cmd: 'MoveToWaitingChildren', id });
      assertFlowTcpOk(response, 'MoveToWaitingChildren');
      return true;
    },
    waitUntilFinished: async (_queueEvents: unknown, ttl?: number) => {
      const timeout = ttl ?? 30_000;
      if (embedded) {
        const manager = getSharedManager();
        const job = await manager.getJob(jobId(id));
        if (!job) throw new Error(`Job ${id} not found`);
        if (job.completedAt) return manager.getResult(jobId(id));
        if (!(await manager.waitForJobCompletion(jobId(id), timeout))) {
          throw new Error(`waitUntilFinished timed out after ${timeout}ms`);
        }
        return manager.getResult(jobId(id));
      }
      if (!tcp) throw new Error('waitUntilFinished: no connection');
      const response = await tcp.send({ cmd: 'WaitJob', id, timeout });
      assertFlowTcpOk(response, 'WaitJob');
      const typed = response as { completed?: boolean; result?: unknown };
      if (!typed.completed) throw new Error(`waitUntilFinished timed out after ${timeout}ms`);
      return typed.result;
    },
    discard: () => {
      if (embedded) void getSharedManager().discard(jobId(id));
      else if (tcp) void tcp.send({ cmd: 'Discard', id });
    },
    getFailedChildrenValues: async () => {
      if (embedded) return getSharedManager().getFailedChildrenValues(jobId(id));
      if (!tcp) return {};
      const response = await tcp.send({ cmd: 'GetFailedChildrenValues', id });
      assertFlowTcpOk(response, 'GetFailedChildrenValues');
      return (response.values as Record<string, string> | undefined) ?? {};
    },
    getIgnoredChildrenFailures: async () => {
      if (embedded) return getSharedManager().getIgnoredChildrenFailures(jobId(id));
      if (!tcp) return {};
      const response = await tcp.send({ cmd: 'GetIgnoredChildrenFailures', id });
      assertFlowTcpOk(response, 'GetIgnoredChildrenFailures');
      return (response.values as Record<string, string> | undefined) ?? {};
    },
    removeChildDependency: async () => {
      if (embedded) return getSharedManager().removeChildDependency(jobId(id));
      if (!tcp) return false;
      const response = await tcp.send({ cmd: 'RemoveChildDependency', id });
      assertFlowTcpOk(response, 'RemoveChildDependency');
      return (response.removed as boolean | undefined) ?? false;
    },
    removeDeduplicationKey: () => removeJobDeduplicationKey(id, embedded, tcp),
    removeUnprocessedChildren: async () => {
      if (embedded) return void (await getSharedManager().removeUnprocessedChildren(jobId(id)));
      if (!tcp) return;
      assertFlowTcpOk(
        await tcp.send({ cmd: 'RemoveUnprocessedChildren', id }),
        'RemoveUnprocessedChildren'
      );
    },
  };
}
