import { expect } from 'bun:test';
import type { QueueModel } from './queue-model-harness';

function isReadyState(state: string): boolean {
  return state === 'waiting' || state === 'prioritized';
}

interface InternalCollections {
  completedJobs: number;
  jobIndex: number;
  jobLocks: number;
  processingTotal: number;
  queuedTotal: number;
  waitingDepsTotal: number;
}

export async function assertInternalCollections(port: number, model: QueueModel): Promise<void> {
  const response = await fetch(`http://127.0.0.1:${port + 1}/stats`);
  expect(response.ok, 'HTTP stats endpoint').toBe(true);
  const body = (await response.json()) as { collections?: InternalCollections };
  const collections = body.collections;
  expect(collections, 'internal collection telemetry').toBeDefined();

  const jobs = [...model.jobs.values()];
  expect(collections!.jobIndex, 'jobIndex cardinality').toBe(model.jobs.size);
  expect(collections!.queuedTotal, 'ShardCounters queued cardinality').toBe(
    jobs.filter((job) => isReadyState(job.state) || job.state === 'delayed').length
  );
  expect(collections!.processingTotal, 'processing shard cardinality').toBe(
    jobs.filter((job) => job.state === 'active').length
  );
  if (model.concurrency !== null) {
    expect(
      collections!.processingTotal,
      'active jobs respect configured concurrency'
    ).toBeLessThanOrEqual(model.concurrency);
  }
  expect(collections!.waitingDepsTotal, 'dependency index cardinality').toBe(
    jobs.filter((job) => job.state === 'waiting-children').length
  );
  expect(collections!.completedJobs, 'completed index cardinality').toBe(
    jobs.filter((job) => job.state === 'completed').length
  );
  expect(collections!.jobLocks, 'live lock cardinality').toBe(
    jobs.filter((job) => job.state === 'active').length
  );
}

export async function assertAggregateCounts(
  send: (command: Record<string, unknown>) => Promise<Record<string, unknown>>,
  queue: string,
  model: QueueModel
): Promise<void> {
  const states = {
    active: 0,
    completed: 0,
    delayed: 0,
    failed: 0,
    prioritized: 0,
    waiting: 0,
    'waiting-children': 0,
  };
  const priorities: Record<string, number> = {};
  for (const job of model.jobs.values()) {
    states[job.state]++;
    if (isReadyState(job.state) || job.state === 'delayed') {
      priorities[job.priority] = (priorities[job.priority] ?? 0) + 1;
    }
  }
  const queueCount = await send({ cmd: 'Count', queue });
  expect(queueCount.count, 'runnable queue count').toBe(
    states.waiting + states.prioritized + states.delayed
  );
  const response = await send({ cmd: 'GetJobCounts', queue });
  const counts = response.counts as Record<string, number>;
  expect(counts.active, 'active count').toBe(states.active);
  expect(counts.completed, 'completed count').toBe(states.completed);
  expect(counts.delayed, 'delayed count').toBe(states.delayed);
  expect(counts.failed, 'failed count').toBe(states.failed);
  expect(counts.paused, 'paused count').toBe(
    model.paused ? states.waiting + states.prioritized : 0
  );
  expect(counts.waiting, 'waiting count').toBe(model.paused ? 0 : states.waiting);
  expect(counts.prioritized, 'prioritized count').toBe(model.paused ? 0 : states.prioritized);
  expect(counts['waiting-children'], 'waiting-children count').toBe(states['waiting-children']);
  expect(
    counts.waiting +
      counts.prioritized +
      counts.delayed +
      counts.active +
      counts.completed +
      counts.failed +
      counts['waiting-children'] +
      counts.paused,
    'observable state conservation'
  ).toBe(model.jobs.size);
  const priorityResponse = await send({ cmd: 'GetCountsPerPriority', queue });
  expect(priorityResponse.counts, 'counts per priority').toEqual(priorities);
}
