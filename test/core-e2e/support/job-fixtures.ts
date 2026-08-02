import type { Job, JobOptions, Queue } from '../../../src/client';
import { jobId } from '../../../src/domain/types/job';
import type { CoreE2eHarness } from './harness';
import { ensure } from './tracker';

export async function createFailedJob<T>(
  harness: CoreE2eHarness,
  queue: Queue<T>,
  data: T,
  options: JobOptions = {}
): Promise<Job<T>> {
  const added = await queue.add('failed', data, {
    ...options,
    attempts: 1,
    backoff: 0,
    durable: true,
  });
  const pulled = await harness.brokerManager().pull(queue.name);
  ensure(pulled?.id === jobId(added.id), `broker did not activate ${added.id}`);
  await harness.brokerManager().fail(pulled.id, 'fixture failure');
  const failed = await queue.getJob(added.id);
  ensure(failed, `failed job ${added.id} could not be read`);
  return failed;
}

export function retryFailedJob(harness: CoreE2eHarness, queueName: string, id: string): void {
  const count = harness.brokerManager().retryDlq(queueName, jobId(id));
  ensure(count === 1, `broker did not retry ${id}`);
}

export async function activateFailedJob(
  harness: CoreE2eHarness,
  queueName: string,
  id: string
): Promise<string> {
  retryFailedJob(harness, queueName, id);
  const lease = await harness.brokerManager().pullWithLock(queueName, 'core-e2e-worker', 0, 30_000);
  ensure(lease.job?.id === jobId(id), `broker did not lease ${id}`);
  ensure(lease.token, `broker did not issue a lock token for ${id}`);
  return lease.token;
}
