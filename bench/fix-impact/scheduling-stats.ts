import { elapsedMs, type Harness } from './harness';

export async function benchmarkHeadOfLine(harness: Harness): Promise<void> {
  const { QueueManager } = harness.runtime;
  const { holBlockedJobs, holSamples } = harness.profile;
  const manager = new QueueManager();
  const samples: number[] = [];
  const returnedLabels: Array<string | null> = [];
  try {
    for (let sample = 0; sample < holSamples; sample++) {
      const queue = `bench-hol-${sample}`;
      const base = Date.now() - holBlockedJobs - 10_000;
      const inputs: Array<Record<string, unknown>> = [
        { data: { label: 'A1' }, groupId: 'group-a', timestamp: base },
      ];
      for (let i = 0; i < holBlockedJobs; i++) {
        inputs.push({
          data: { label: `A${i + 2}` },
          groupId: 'group-a',
          timestamp: base + i + 1,
        });
      }
      inputs.push({
        data: { label: 'B1' },
        groupId: 'group-b',
        timestamp: base + holBlockedJobs + 1,
      });
      await manager.pushBatch(queue, inputs);
      await manager.pull(queue, 0, undefined, { concurrency: 1 });

      const startedAt = Bun.nanoseconds();
      const second = await manager.pull(queue, 0, undefined, { concurrency: 1 });
      samples.push(elapsedMs(startedAt));
      returnedLabels.push(second ? (second.data as { label: string }).label : null);
    }
    harness.addResult({
      id: 'pull-group-head-of-line',
      category: 'scheduling',
      operation: 'pull past active FIFO group',
      workload: { blockedGroupJobsAhead: holBlockedJobs },
      samples,
      correctness: {
        returnedLabels,
        eligibleJobReturnedEveryTime: returnedLabels.every((value) => value === 'B1'),
      },
      comparable: true,
      note: 'Models one saturated tenant ahead of another ready tenant with explicit serial group capacity.',
    });
  } finally {
    manager.shutdown();
  }
}

export async function benchmarkStats(harness: Harness): Promise<void> {
  const { QueueManager } = harness.runtime;
  const { statsJobs, statsQueues, statsSamples } = harness.profile;
  const manager = new QueueManager();
  const jobsPerQueue = Math.floor(statsJobs / statsQueues);
  const totalJobs = jobsPerQueue * statsQueues;
  const base = Date.now() - totalJobs - 10_000;
  try {
    for (let queueIndex = 0; queueIndex < statsQueues; queueIndex++) {
      const inputs = Array.from({ length: jobsPerQueue }, (_, jobIndex) => {
        const globalIndex = queueIndex * jobsPerQueue + jobIndex;
        return {
          data: { globalIndex },
          priority: globalIndex % 3 === 0 ? 10 : 0,
          delay: globalIndex % 10 === 0 ? 3_600_000 : 0,
          timestamp: base + globalIndex,
        };
      });
      await manager.pushBatch(`bench-stats-${queueIndex}`, inputs);
    }

    manager.getStats();
    manager.getQueuesSummary();
    const statsSamplesMs: number[] = [];
    const summarySamplesMs: number[] = [];
    let statsSnapshot: Record<string, number | bigint> = {};
    let summarySnapshot = manager.getQueuesSummary();
    for (let i = 0; i < statsSamples; i++) {
      let startedAt = Bun.nanoseconds();
      statsSnapshot = manager.getStats();
      statsSamplesMs.push(elapsedMs(startedAt));

      startedAt = Bun.nanoseconds();
      summarySnapshot = manager.getQueuesSummary();
      summarySamplesMs.push(elapsedMs(startedAt));
      harness.sink ^= summarySnapshot.length;
    }

    const globalReady =
      Number(statsSnapshot.waiting) +
      Number(statsSnapshot.prioritized) +
      Number(statsSnapshot.delayed);
    harness.addResult({
      id: 'stats-global',
      category: 'statistics',
      operation: 'getStats',
      workload: { queues: statsQueues, jobs: totalJobs },
      samples: statsSamplesMs,
      correctness: { globalReady, expectedReady: totalJobs, exact: globalReady === totalJobs },
      comparable: true,
    });

    const summaryReady = summarySnapshot.reduce(
      (total, queue) =>
        total +
        queue.counts.waiting +
        (queue.counts.prioritized ?? 0) +
        queue.counts.active +
        queue.counts.completed +
        queue.counts.failed +
        queue.counts.delayed,
      0
    );
    const priorityFieldPresent = summarySnapshot.every(
      (queue) => typeof queue.counts.prioritized === 'number'
    );
    harness.addResult({
      id: 'stats-queues-summary',
      category: 'statistics',
      operation: 'getQueuesSummary',
      workload: { queues: statsQueues, jobs: totalJobs, includesPrioritizedJobs: true },
      samples: summarySamplesMs,
      correctness: {
        queuesReturned: summarySnapshot.length,
        priorityFieldPresent,
        summaryReady,
        expectedReady: totalJobs,
        exact:
          summarySnapshot.length === statsQueues &&
          priorityFieldPresent &&
          summaryReady === totalJobs,
      },
      comparable: true,
    });
  } finally {
    manager.shutdown();
  }
}
