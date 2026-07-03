import type { SimulatorEngine } from './engine';
import { SHARD_COUNT } from './hash';
import type { LaneView, QueueView, ShardView, SimJob, Snapshot, WorkerView } from './types';

const LANE_CAP = 24;
const SERIES_BUCKETS = 60; // one bucket per sim-second
const RATE_WINDOW_MS = 5000;
const FLASH_MS = 400;

// Assemble the immutable view the React UI renders. Called on a short
// polling interval, so it stays O(jobs) with small constants and caps
// every list it returns.
export function buildSnapshot(engine: SimulatorEngine): Snapshot {
  const { simTime } = engine;

  const waiting: SimJob[] = [];
  const delayed: SimJob[] = [];
  const active: SimJob[] = [];
  const completed: SimJob[] = [];
  const dlq: SimJob[] = [];
  const queues: QueueView[] = [];

  let pushed = 0;
  let completedTotal = 0;
  let failed = 0;
  let retried = 0;
  let dead = 0;

  const shards: ShardView[] = Array.from({ length: SHARD_COUNT }, (_, index) => ({
    index,
    load: 0,
    queues: [],
    flash: false,
  }));

  for (const q of engine.queues.values()) {
    waiting.push(...q.waiting.toArray());
    delayed.push(...q.delayed.toArray());
    active.push(...q.active.values());
    completed.push(...q.completed);
    dlq.push(...q.dlq);

    pushed += q.counters.pushed;
    completedTotal += q.counters.completed;
    failed += q.counters.failed;
    retried += q.counters.retried;
    dead += q.counters.dead;

    queues.push({
      name: q.name,
      shardIndex: q.shardIndex,
      paused: q.paused,
      rateLimit: q.rateLimit,
      waiting: q.waiting.size,
      delayed: q.delayed.size,
      active: q.active.size,
      completed: q.counters.completed,
      dlq: q.dlq.length,
    });

    const shard = shards[q.shardIndex];
    shard.load += q.depth;
    shard.queues.push(q.name);
    if (simTime - q.lastPushAt < FLASH_MS) shard.flash = true;
  }

  // Display order mirrors processing order.
  waiting.sort((a, b) => (a.priority !== b.priority ? b.priority - a.priority : a.id - b.id));
  delayed.sort((a, b) => a.runAt - b.runAt);
  active.sort((a, b) => (a.startedAt ?? 0) - (b.startedAt ?? 0));
  completed.sort((a, b) => (b.finishedAt ?? 0) - (a.finishedAt ?? 0));
  dlq.sort((a, b) => (b.finishedAt ?? 0) - (a.finishedAt ?? 0));

  const workers: WorkerView[] = engine.workers.map((w) => ({
    id: w.id,
    queue: w.queue,
    concurrency: w.concurrency,
    activeCount: w.active.length,
    processed: w.processed,
    failed: w.failed,
    status: w.status,
  }));

  return {
    simTime,
    speed: engine.speed,
    running: engine.running,
    failureRate: engine.failureRate,
    totals: {
      pushed,
      waiting: waiting.length,
      delayed: delayed.length,
      active: active.length,
      completed: completedTotal,
      failed,
      retried,
      dead,
      queues: queues.length,
      workers: workers.filter((w) => w.status !== 'stopped').length,
      jobsPerSec: recentRate(engine.completionLog, simTime),
    },
    lanes: {
      waiting: lane(waiting),
      delayed: lane(delayed),
      active: lane(active),
      completed: lane(completed, completedTotal),
      dlq: lane(dlq),
    },
    queues: queues.sort((a, b) => a.name.localeCompare(b.name)),
    shards,
    workers,
    events: [...engine.events],
    series: throughputSeries(engine.completionLog, simTime),
  };
}

function lane(jobs: SimJob[], total = jobs.length): LaneView {
  return { jobs: jobs.slice(0, LANE_CAP), total };
}

function recentRate(log: number[], simTime: number): number {
  const cutoff = simTime - RATE_WINDOW_MS;
  let count = 0;
  for (let i = log.length - 1; i >= 0 && log[i] > cutoff; i--) count++;
  const windowSec = Math.min(simTime, RATE_WINDOW_MS) / 1000;
  return windowSec > 0.5 ? count / windowSec : 0;
}

function throughputSeries(log: number[], simTime: number): number[] {
  const buckets = new Array<number>(SERIES_BUCKETS).fill(0);
  const currentBucket = Math.floor(simTime / 1000);
  const oldest = currentBucket - SERIES_BUCKETS + 1;
  for (let i = log.length - 1; i >= 0; i--) {
    const bucket = Math.floor(log[i] / 1000);
    if (bucket < oldest) break;
    buckets[bucket - oldest]++;
  }
  return buckets;
}
