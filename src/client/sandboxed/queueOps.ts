/**
 * Queue Operations for SandboxedWorker
 * Provides a unified interface for embedded and TCP mode queue operations
 */

import type { Job as DomainJob, JobId } from '../../domain/types/job';
import { jobId } from '../../domain/types/job';
import type { SharedManager } from '../manager';
import type { TcpConnectionPool } from '../tcpPool';
import { parseJobFromResponse } from '../worker/jobParser';
import { outcomeWasApplied } from '../worker/ackOutcome';

/** Unified queue operations interface */
export interface QueueOps {
  pull(
    queue: string,
    workerId: string,
    timeout: number
  ): Promise<{ job: DomainJob | null; token: string | null }>;
  ack(id: JobId, result: unknown, token?: string): Promise<boolean>;
  fail(id: JobId, error: string, token?: string): Promise<boolean>;
  updateProgress(id: JobId, progress: number): Promise<void>;
  addLog(id: JobId, message: string): void;
  sendHeartbeat(ids: string[], tokens: string[]): Promise<void>;
  countWaiting(queue: string): Promise<number>;
}

/** Create embedded mode operations using shared QueueManager */
export function createEmbeddedOps(manager: SharedManager): QueueOps {
  return {
    pull: (queue, workerId, timeout) => manager.pullWithLock(queue, workerId, timeout),
    ack: async (id, result, token) => {
      const outcome = await manager.ack(id, result, token);
      return outcome?.applied !== false;
    },
    fail: async (id, error, token) => {
      const outcome = await manager.fail(id, error, token);
      return outcome?.applied !== false;
    },
    updateProgress: async (id, progress) => {
      await manager.updateProgress(id, progress);
    },
    addLog: (id, message) => {
      manager.addLog(id, message);
    },
    sendHeartbeat: (ids, tokens) => {
      for (let i = 0; i < ids.length; i++) {
        manager.jobHeartbeat(jobId(ids[i]), tokens[i]);
      }
      return Promise.resolve();
    },
    countWaiting: (queue) => {
      const counts = manager.getQueueJobCounts(queue);
      return Promise.resolve(counts.waiting + counts.delayed);
    },
  };
}

/** Create TCP mode operations using connection pool */
export function createTcpOps(tcp: TcpConnectionPool): QueueOps {
  return {
    async pull(queue, workerId, timeout) {
      const res = await tcp.send({ cmd: 'PULL', queue, owner: workerId, timeout });
      if (!res.ok || !res.job) return { job: null, token: null };
      return {
        job: parseJobFromResponse(res.job as Record<string, unknown>, queue),
        token: (res.token as string | null | undefined) ?? null,
      };
    },
    async ack(id, result, token) {
      const response = await tcp.send({
        cmd: 'ACK',
        id: String(id),
        result,
        token: token ?? undefined,
      });
      if (response.ok !== true) {
        throw new Error(typeof response.error === 'string' ? response.error : 'ACK failed');
      }
      return outcomeWasApplied(response.data);
    },
    async fail(id, error, token) {
      const response = await tcp.send({
        cmd: 'FAIL',
        id: String(id),
        error,
        token: token ?? undefined,
      });
      if (response.ok !== true) {
        throw new Error(typeof response.error === 'string' ? response.error : 'FAIL failed');
      }
      return outcomeWasApplied(response.data);
    },
    async updateProgress(id, progress) {
      await tcp.send({ cmd: 'Progress', id: String(id), progress });
    },
    addLog(id, message) {
      tcp.send({ cmd: 'AddLog', id: String(id), message }).catch(() => {
        // Logging is intentionally best-effort and must not fail job processing.
      });
    },
    async sendHeartbeat(ids, tokens) {
      if (ids.length === 0) return;
      if (ids.length === 1) {
        await tcp.send({ cmd: 'JobHeartbeat', id: ids[0], token: tokens[0] || undefined });
      } else {
        await tcp.send({ cmd: 'JobHeartbeatB', ids, tokens });
      }
    },
    async countWaiting(queue) {
      const res = await tcp.send({ cmd: 'GetJobCounts', queue });
      if (!res.ok) return 0;
      const counts = res.counts as Record<string, number> | undefined;
      return (counts?.waiting ?? 0) + (counts?.delayed ?? 0);
    },
  };
}
