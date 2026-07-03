// Shared types for the in-browser bunqueue simulator.
// The simulator mirrors real bunqueue semantics (sharding, priorities,
// retry with exponential backoff, DLQ, pause/drain, rate limiting) with
// an in-memory engine driven by a controllable simulation clock.

export type JobState = 'waiting' | 'delayed' | 'active' | 'completed' | 'dlq';

export interface SimJob {
  id: number;
  queue: string;
  name: string;
  priority: number; // higher = processed sooner
  state: JobState;
  attemptsMade: number; // failed attempts so far
  maxAttempts: number;
  createdAt: number; // sim-time ms
  runAt: number; // sim-time ms when eligible (delay or retry backoff)
  startedAt: number | null;
  finishedAt: number | null;
  duration: number; // sampled processing duration (sim-time ms)
  progress: number; // 0..1 while active
  workerId: string | null;
  failedReason: string | null;
  lastBackoff: number; // last retry backoff applied, for display
}

export type WorkerStatus = 'running' | 'stopping' | 'stopped';

export interface WorkerSim {
  id: string;
  queue: string;
  concurrency: number;
  active: SimJob[];
  processed: number;
  failed: number;
  status: WorkerStatus;
}

export type SimEventType =
  | 'push'
  | 'done'
  | 'retry'
  | 'dlq'
  | 'pause'
  | 'resume'
  | 'drain'
  | 'retry-dlq'
  | 'rate-limit'
  | 'worker'
  | 'scenario';

export interface SimEvent {
  id: number;
  t: number; // sim-time ms
  type: SimEventType;
  msg: string;
}

export interface PushOptions {
  priority?: number;
  delay?: number;
  maxAttempts?: number;
}

export interface QueueView {
  name: string;
  shardIndex: number;
  paused: boolean;
  rateLimit: number; // jobs/sec, 0 = off
  waiting: number;
  delayed: number;
  active: number;
  completed: number;
  dlq: number;
}

export interface ShardView {
  index: number;
  load: number; // waiting + delayed + active across its queues
  queues: string[];
  flash: boolean; // a push landed here very recently
}

export interface WorkerView {
  id: string;
  queue: string;
  concurrency: number;
  activeCount: number;
  processed: number;
  failed: number;
  status: WorkerStatus;
}

export type LaneId = 'waiting' | 'delayed' | 'active' | 'completed' | 'dlq';

export interface LaneView {
  jobs: SimJob[]; // capped for display
  total: number; // real count
}

export interface Totals {
  pushed: number;
  waiting: number;
  delayed: number;
  active: number;
  completed: number;
  failed: number; // failed attempts (includes retried)
  retried: number;
  dead: number; // jobs that ended in the DLQ
  queues: number;
  workers: number;
  jobsPerSec: number; // completions over the last 5s of sim-time
}

export interface Snapshot {
  simTime: number;
  speed: number;
  running: boolean;
  failureRate: number; // 0..1
  totals: Totals;
  lanes: Record<LaneId, LaneView>;
  queues: QueueView[];
  shards: ShardView[];
  workers: WorkerView[];
  events: SimEvent[]; // newest first
  series: number[]; // completions/sec, one bucket per sim-second, oldest first
}
