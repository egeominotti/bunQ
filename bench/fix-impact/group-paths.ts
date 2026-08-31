import { elapsedMs, type Harness } from './harness';

interface QueuePathSamples {
  push: number[];
  pull: number[];
  correct: boolean[];
}

function addPathResults(
  harness: Harness,
  prefix: string,
  workload: Record<string, number | string | boolean>,
  samples: QueuePathSamples,
  note: string
): void {
  const correctSamples = samples.correct.filter(Boolean).length;
  const allCorrect = samples.correct.every(Boolean);
  for (const [operation, timings] of [
    ['push-batch', samples.push],
    ['pull-batch', samples.pull],
  ] as const) {
    harness.addResult({
      id: `${prefix}-${operation}`,
      category: 'scheduling',
      operation,
      workload,
      samples: timings,
      correctness: {
        correctSamples,
        totalSamples: samples.correct.length,
        allCorrect,
      },
      comparable: allCorrect,
      note,
    });
  }
}

/** Measure the ordinary queue path that must not pay grouped-index duplication. */
export async function benchmarkUngroupedPath(harness: Harness): Promise<void> {
  const { QueueManager } = harness.runtime;
  const { ungroupedJobs, queuePathSamples } = harness.profile;
  const manager = new QueueManager();
  const samples: QueuePathSamples = { push: [], pull: [], correct: [] };
  try {
    for (let sample = 0; sample < queuePathSamples; sample++) {
      const queue = `bench-ungrouped-${sample}`;
      const base = Date.now() - ungroupedJobs - 10_000;
      const inputs = Array.from({ length: ungroupedJobs }, (_, index) => ({
        data: { index },
        timestamp: base + index,
      }));
      let startedAt = Bun.nanoseconds();
      await manager.pushBatch(queue, inputs);
      samples.push.push(elapsedMs(startedAt));

      startedAt = Bun.nanoseconds();
      const jobs = await manager.pullBatch(queue, ungroupedJobs);
      samples.pull.push(elapsedMs(startedAt));
      samples.correct.push(
        jobs.length === ungroupedJobs &&
          jobs.every((job, index) => (job.data as { index: number }).index === index)
      );
    }
  } finally {
    manager.shutdown();
  }
  addPathResults(
    harness,
    'queue-ungrouped',
    { jobsPerSample: ungroupedJobs },
    samples,
    'Fresh plain queue per sample; admission and a complete batch claim are timed separately.'
  );
}

/** Measure a tenant-shaped queue with plain work and one serial claim per group. */
export async function benchmarkMixedGroupPath(harness: Harness): Promise<void> {
  const { QueueManager } = harness.runtime;
  const { mixedJobs, mixedGroups, queuePathSamples } = harness.profile;
  const manager = new QueueManager();
  const samples: QueuePathSamples = { push: [], pull: [], correct: [] };
  const plainJobs = Math.floor(mixedJobs / 2);
  const groupedJobs = mixedJobs - plainJobs;
  try {
    for (let sample = 0; sample < queuePathSamples; sample++) {
      const queue = `bench-mixed-groups-${sample}`;
      const base = Date.now() - mixedJobs - 10_000;
      const inputs = Array.from({ length: mixedJobs }, (_, index) => {
        const grouped = index % 2 === 1;
        return {
          data: { grouped, index },
          timestamp: base + index,
          ...(grouped && { groupId: `tenant-${Math.floor(index / 2) % mixedGroups}` }),
        };
      });
      let startedAt = Bun.nanoseconds();
      await manager.pushBatch(queue, inputs);
      samples.push.push(elapsedMs(startedAt));

      startedAt = Bun.nanoseconds();
      const jobs = await manager.pullBatch(queue, plainJobs + mixedGroups, 0, undefined, {
        concurrency: 1,
      });
      samples.pull.push(elapsedMs(startedAt));
      const expectedPlain: Array<{ index: number; groupId: null }> = [];
      const expectedGroupHeads = new Map<string, { index: number; groupId: string }>();
      for (const input of inputs) {
        const data = input.data as { grouped: boolean; index: number };
        if (input.groupId === undefined) {
          expectedPlain.push({ index: data.index, groupId: null });
        } else if (!expectedGroupHeads.has(input.groupId)) {
          expectedGroupHeads.set(input.groupId, { index: data.index, groupId: input.groupId });
        }
      }
      const expectedOrder = [...expectedPlain, ...expectedGroupHeads.values()];
      samples.correct.push(
        expectedOrder.length === plainJobs + mixedGroups &&
          jobs.length === expectedOrder.length &&
          jobs.every((job, index) => {
            const expected = expectedOrder[index];
            const data = job.data as { grouped: boolean; index: number };
            return (
              data.index === expected.index &&
              data.grouped === (expected.groupId !== null) &&
              (expected.groupId === null
                ? job.groupId === null || job.groupId === undefined
                : job.groupId === expected.groupId)
            );
          })
      );
    }
  } finally {
    manager.shutdown();
  }
  addPathResults(
    harness,
    'queue-mixed-groups',
    { jobsPerSample: mixedJobs, groupedJobs, groups: mixedGroups, plainJobs },
    samples,
    'Half plain work and half grouped work; the claim takes every plain job and one serial turn per tenant.'
  );
}
