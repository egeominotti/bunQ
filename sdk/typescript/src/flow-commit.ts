import type { ConnectionLike } from './connection-types.js';
import type { AtomicFlowJobInput } from './flow-plan.js';
import type { JobRaw } from './job.js';
import type { DataResponse } from './responses.js';

interface PushFlowResult {
  jobs: JobRaw[];
}

export function validateFlowSnapshots(
  jobs: AtomicFlowJobInput[],
  snapshots: unknown
): Map<string, JobRaw> {
  if (!Array.isArray(snapshots) || snapshots.length !== jobs.length) {
    throw new Error('Invalid PUSHF response: committed job snapshots are missing');
  }
  const expected = new Map(jobs.map((job) => [job.id, job.queue]));
  const byId = new Map<string, JobRaw>();
  for (const value of snapshots) {
    if (!value || typeof value !== 'object') {
      throw new Error('Invalid PUSHF response: job snapshot is invalid');
    }
    const snapshot = value as JobRaw;
    if (typeof snapshot.id !== 'string' || !expected.has(snapshot.id)) {
      throw new Error('Invalid PUSHF response: committed job IDs do not match the request');
    }
    if (byId.has(snapshot.id) || snapshot.queue !== expected.get(snapshot.id)) {
      throw new Error('Invalid PUSHF response: committed job IDs do not match the request');
    }
    byId.set(snapshot.id, snapshot);
  }
  return byId;
}

export async function commitFlow(
  connection: ConnectionLike,
  jobs: AtomicFlowJobInput[]
): Promise<Map<string, JobRaw>> {
  if (jobs.length === 0) return new Map();
  const response = await connection.call<DataResponse<PushFlowResult>>({
    cmd: 'PUSHF',
    jobs,
  });
  return validateFlowSnapshots(jobs, response.data?.jobs);
}
