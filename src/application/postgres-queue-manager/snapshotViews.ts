import type { DlqEntry, DlqFilter } from '../../domain/types/dlq';
import type { Job, JobId } from '../../domain/types/job';
import type {
  PostgresCounts,
  PostgresJobState,
  PostgresQueueState,
  PostgresStoredJob,
} from '../../infrastructure/persistence/postgres';

const EMPTY_COUNTS: PostgresCounts = {
  waiting: 0,
  prioritized: 0,
  delayed: 0,
  active: 0,
  completed: 0,
  failed: 0,
  waitingChildren: 0,
};

function stateMatches(
  state: PostgresJobState,
  requested: readonly string[] | null,
  paused: boolean
): boolean {
  if (!requested) return true;
  if (requested.includes('paused') && paused) {
    if (state === 'waiting' || state === 'prioritized') return true;
  }
  if (paused && (state === 'waiting' || state === 'prioritized')) return false;
  return requested.includes(state);
}

function compareIds(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

export function findSnapshotJob(
  jobs: ReadonlyMap<JobId, PostgresStoredJob>,
  predicate: (job: Job) => boolean
): Job | null {
  for (const row of jobs.values()) if (predicate(row.job)) return row.job;
  return null;
}

export function snapshotJobIds(
  jobs: ReadonlyMap<JobId, PostgresStoredJob>,
  queue: string,
  states?: readonly PostgresJobState[]
): JobId[] {
  return [...jobs.values()].flatMap((row) =>
    row.job.queue === queue && (!states || states.includes(row.state)) ? [row.job.id] : []
  );
}

export function listSnapshotJobs(
  jobs: ReadonlyMap<JobId, PostgresStoredJob>,
  queues: ReadonlyMap<string, PostgresQueueState>,
  queue: string,
  options: { state?: string | string[]; start?: number; end?: number; asc?: boolean }
): Job[] {
  const requested =
    options.state === undefined || (Array.isArray(options.state) && options.state.length === 0)
      ? null
      : Array.isArray(options.state)
        ? options.state
        : [options.state];
  const paused = queues.get(queue)?.paused ?? false;
  const rows = [...jobs.values()].filter(
    (row) => row.job.queue === queue && stateMatches(row.state, requested, paused)
  );
  const asc = options.asc ?? true;
  rows.sort((left, right) => {
    const created = left.job.createdAt - right.job.createdAt;
    if (created !== 0) return asc ? created : -created;
    return asc
      ? compareIds(String(left.job.id), String(right.job.id))
      : compareIds(String(right.job.id), String(left.job.id));
  });
  const start = Math.max(0, options.start ?? 0);
  const end = Math.max(start, options.end ?? 100);
  return rows.slice(start, end).map((row) => row.job);
}

export function countSnapshotJobs(
  jobs: ReadonlyMap<JobId, PostgresStoredJob>,
  queue?: string
): PostgresCounts {
  const counts = { ...EMPTY_COUNTS };
  for (const row of jobs.values()) {
    if (queue && row.job.queue !== queue) continue;
    if (row.state === 'waiting-children') counts.waitingChildren++;
    else counts[row.state]++;
  }
  return counts;
}

export function countSnapshotJobsByQueue(
  jobs: ReadonlyMap<JobId, PostgresStoredJob>,
  knownQueues: ReadonlySet<string>
): Map<string, PostgresCounts> {
  const counts = new Map([...knownQueues].map((queue) => [queue, { ...EMPTY_COUNTS }]));
  for (const row of jobs.values()) {
    let queueCounts = counts.get(row.job.queue);
    if (!queueCounts) {
      queueCounts = { ...EMPTY_COUNTS };
      counts.set(row.job.queue, queueCounts);
    }
    if (row.state === 'waiting-children') queueCounts.waitingChildren++;
    else queueCounts[row.state]++;
  }
  return new Map([...counts].sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0)));
}

export function countSnapshotPriorities(
  jobs: ReadonlyMap<JobId, PostgresStoredJob>,
  queue: string
): Record<number, number> {
  const counts: Record<number, number> = {};
  for (const row of jobs.values()) {
    if (row.job.queue !== queue || !['waiting', 'prioritized', 'delayed'].includes(row.state)) {
      continue;
    }
    counts[row.job.priority] = (counts[row.job.priority] ?? 0) + 1;
  }
  return counts;
}

export function listSnapshotDlq(
  jobs: ReadonlyMap<JobId, PostgresStoredJob>,
  queue: string,
  filter?: DlqFilter
): DlqEntry[] {
  const now = Date.now();
  let entries = [...jobs.values()].flatMap((row) =>
    row.job.queue === queue && row.state === 'failed' && row.dlqEntry ? [row.dlqEntry] : []
  );
  if (filter?.reason) entries = entries.filter((entry) => entry.reason === filter.reason);
  const olderThan = filter?.olderThan;
  const newerThan = filter?.newerThan;
  if (olderThan) entries = entries.filter((entry) => entry.enteredAt < olderThan);
  if (newerThan) entries = entries.filter((entry) => entry.enteredAt > newerThan);
  if (filter?.expired !== undefined) {
    entries = entries.filter(
      (entry) => (entry.expiresAt !== null && entry.expiresAt <= now) === filter.expired
    );
  }
  const offset = Math.max(0, filter?.offset ?? 0);
  return entries
    .sort(
      (left, right) =>
        right.enteredAt - left.enteredAt || compareIds(String(right.job.id), String(left.job.id))
    )
    .slice(offset, offset + Math.max(0, filter?.limit ?? entries.length));
}
