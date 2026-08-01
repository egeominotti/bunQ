import type { Job, JobOptions, Queue } from '../../src/client';
import { jobId } from '../../src/domain/types/job';
import type { TcpHarness } from './tcp-harness';

export interface FailedDlqJob<T> {
  job: Job<T>;
  id: string;
}

export async function createFailedDlqJob<T>(
  harness: TcpHarness,
  queue: Queue<T>,
  queueName: string,
  data: T,
  options: JobOptions = {}
): Promise<FailedDlqJob<T>> {
  const added = await queue.add('failed-task', data, {
    ...options,
    attempts: 1,
    backoff: 0,
  });
  const pulled = await harness.manager.pull(queueName);
  if (!pulled || String(pulled.id) !== added.id) {
    throw new Error(`Expected ${added.id} to be pulled from ${queueName}`);
  }
  await harness.manager.fail(pulled.id, 'e2e failure');
  const entry = (await queue.getDlqAsync()).find((candidate) => candidate.job.id === added.id);
  if (!entry) throw new Error(`Expected ${added.id} in the DLQ for ${queueName}`);
  return { job: entry.job, id: added.id };
}

export function retryDirect(harness: TcpHarness, queueName: string, id: string): void {
  const count = harness.manager.retryDlq(queueName, jobId(id));
  if (count !== 1) throw new Error(`Expected ${id} to leave the DLQ`);
}

export async function activateDirect(
  harness: TcpHarness,
  queueName: string,
  id: string
): Promise<void> {
  retryDirect(harness, queueName, id);
  const pulled = await harness.manager.pull(queueName);
  if (!pulled || String(pulled.id) !== id) {
    throw new Error(`Expected ${id} to become active in ${queueName}`);
  }
}

export async function waitForState(
  queue: Queue<unknown>,
  id: string,
  expected: string,
  timeoutMs = 1_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if ((await queue.getJobState(id)) === expected) return;
    await Bun.sleep(5);
  }
  throw new Error(`Job ${id} did not reach state ${expected}`);
}
