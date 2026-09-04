import { afterEach, describe, expect, test } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { pushFlowBatch } from '../src/application/operations/flowPush';
import type { PushContext } from '../src/application/operations/pushContext';
import { jobId } from '../src/domain/types/job';
import type { JobEvent } from '../src/domain/types/queue';

describe('Flow telemetry batch routing', () => {
  let manager: QueueManager | undefined;

  afterEach(() => manager?.shutdown());

  test('falls back to ordered scalar events when batch broadcast is unavailable', async () => {
    manager = new QueueManager();
    const factory = (manager as unknown as { contextFactory: { getPushContext(): PushContext } })
      .contextFactory;
    const context = factory.getPushContext();
    const observed: JobEvent[] = [];
    context.broadcastBatch = undefined;
    context.broadcast = (event) => observed.push(event);
    const ids = Array.from({ length: 5 }, (_, index) => jobId(`flow-fallback-${index}`));

    const result = await pushFlowBatch(
      {
        jobs: ids.map((id, index) => ({
          id,
          queue: 'flow-fallback',
          input: { data: { index } },
        })),
      },
      context
    );

    expect(result.jobs.map((job) => job.id)).toEqual(ids);
    expect(observed.map((event) => event.jobId)).toEqual(ids);
    expect(new Set(observed.map((event) => event.timestamp)).size).toBe(1);
  });
});
