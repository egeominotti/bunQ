import { expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import fc from 'fast-check';
import { SHARD_COUNT, shardIndex } from '../../src/shared/hash';
import { startModelBroker, stopModelBroker, type StartedModelBroker } from './queue-model-broker';

const DEFAULT_RUNS = 24;
const DEFAULT_OPERATIONS = 30;

type Operation =
  | { kind: 'push'; queue: number }
  | { kind: 'complete'; queue: number }
  | { kind: 'pause'; queue: number }
  | { kind: 'resume'; queue: number }
  | { kind: 'compact' }
  | { kind: 'restart' };

interface ModeledJob {
  payload: number;
  state: 'waiting' | 'completed';
}

interface QueueState {
  jobs: Map<string, ModeledJob>;
  paused: boolean;
}

interface JobRow {
  id: string;
  queue: string;
  state: string;
}

interface Collections {
  completedJobs: number;
  jobIndex: number;
  jobLocks: number;
  processingTotal: number;
  queuedTotal: number;
}

class CrossQueueHarness {
  readonly dbPath: string;
  readonly queues: string[];
  private broker: StartedModelBroker | null = null;

  private constructor(runId: string) {
    this.dbPath = join(tmpdir(), `bunqueue-cross-model-${process.pid}-${runId}.db`);
    this.queues = chooseQueues(runId);
  }

  static async create(runId: string): Promise<CrossQueueHarness> {
    const harness = new CrossQueueHarness(runId);
    await harness.start();
    return harness;
  }

  async send(command: Record<string, unknown>): Promise<Record<string, unknown>> {
    if (!this.broker) throw new Error('cross-queue model broker is not connected');
    return this.broker.client.send(command);
  }

  async restart(): Promise<void> {
    const port = this.broker?.port;
    await this.stop();
    await Bun.sleep(25);
    this.broker = await startModelBroker(this.dbPath, port);
  }

  async assertConsistent(model: Map<string, QueueState>): Promise<void> {
    const expected = new Map<string, { queue: string; state: string }>();
    let waiting = 0;
    let completed = 0;

    for (const [queue, queueState] of model) {
      const paused = await this.send({ cmd: 'IsPaused', queue });
      expect(paused.paused, `paused state for ${queue}`).toBe(queueState.paused);
      const response = await this.send({ cmd: 'GetJobCounts', queue });
      const counts = response.counts as Record<string, number>;
      const queueWaiting = [...queueState.jobs.values()].filter(
        (job) => job.state === 'waiting'
      ).length;
      const queueCompleted = queueState.jobs.size - queueWaiting;
      expect(counts.waiting, `waiting count for ${queue}`).toBe(
        queueState.paused ? 0 : queueWaiting
      );
      expect(counts.paused, `paused count for ${queue}`).toBe(queueState.paused ? queueWaiting : 0);
      expect(counts.completed, `completed count for ${queue}`).toBe(queueCompleted);
      expect(counts.active, `active count for ${queue}`).toBe(0);
      expect(counts.failed, `failed count for ${queue}`).toBe(0);
      waiting += queueWaiting;
      completed += queueCompleted;

      for (const [id, job] of queueState.jobs) {
        expected.set(id, { queue, state: job.state });
        const state = await this.send({ cmd: 'GetState', id });
        expect(state.state, `API state for ${id}`).toBe(job.state);
        const fetched = await this.send({ cmd: 'GetJob', id });
        const apiJob = fetched.job as { data?: { payload?: number }; queue?: string } | null;
        expect(apiJob?.queue, `API queue ownership for ${id}`).toBe(queue);
        expect(apiJob?.data?.payload, `API payload for ${id}`).toBe(job.payload);
      }
    }

    const db = new Database(this.dbPath, { readonly: true });
    try {
      const rows = db.query<JobRow, []>('SELECT id, queue, state FROM jobs ORDER BY id').all();
      expect(rows.length, 'SQLite job conservation across queues').toBe(expected.size);
      for (const row of rows) {
        expect(expected.get(row.id), `modeled SQLite row ${row.id}`).toEqual({
          queue: row.queue,
          state: row.state,
        });
      }
    } finally {
      db.close();
    }

    if (!this.broker) throw new Error('cross-queue model broker is not connected');
    const stats = await fetch(`http://127.0.0.1:${this.broker.port + 1}/stats`);
    expect(stats.ok, 'HTTP stats endpoint').toBe(true);
    const body = (await stats.json()) as { collections?: Collections };
    expect(body.collections?.jobIndex, 'global jobIndex cardinality').toBe(expected.size);
    expect(body.collections?.queuedTotal, 'global queued cardinality').toBe(waiting);
    expect(body.collections?.completedJobs, 'global completed cardinality').toBe(completed);
    expect(body.collections?.processingTotal, 'global processing cardinality').toBe(0);
    expect(body.collections?.jobLocks, 'global lock cardinality').toBe(0);
  }

  async dispose(): Promise<void> {
    await this.stop();
    for (const suffix of ['', '-wal', '-shm']) {
      const path = `${this.dbPath}${suffix}`;
      if (existsSync(path)) rmSync(path, { force: true });
    }
  }

  private async start(): Promise<void> {
    this.broker = await startModelBroker(this.dbPath);
  }

  private async stop(): Promise<void> {
    if (!this.broker) return;
    await stopModelBroker(this.broker);
    this.broker = null;
  }
}

export async function runCrossQueueInvariantCampaign(): Promise<void> {
  const seed = optionalInteger(Bun.env.BUNQUEUE_MODEL_SEED);
  const numRuns = optionalInteger(Bun.env.BUNQUEUE_CROSS_QUEUE_RUNS) ?? DEFAULT_RUNS;
  const maxLength = optionalInteger(Bun.env.BUNQUEUE_CROSS_QUEUE_COMMANDS) ?? DEFAULT_OPERATIONS;
  let run = 0;

  await fc.assert(
    fc.asyncProperty(
      fc.array(operationArbitrary(), { minLength: 1, maxLength }),
      async (operations) => {
        const harness = await CrossQueueHarness.create(`${seed ?? 'random'}-${run++}`);
        const model = new Map(
          harness.queues.map((queue) => [queue, { jobs: new Map(), paused: false }])
        );
        let sequence = 0;
        try {
          for (const operation of operations) {
            await applyOperation(harness, model, operation, sequence++);
            await harness.assertConsistent(model);
          }
        } finally {
          await harness.dispose();
        }
      }
    ),
    {
      endOnFailure: true,
      interruptAfterTimeLimit: 60000,
      numRuns,
      seed,
      verbose: 2,
    }
  );
}

function operationArbitrary(): fc.Arbitrary<Operation> {
  const queue = fc.integer({ min: 0, max: 3 });
  return fc.oneof(
    { weight: 5, arbitrary: queue.map((value) => ({ kind: 'push', queue: value })) },
    { weight: 4, arbitrary: queue.map((value) => ({ kind: 'complete', queue: value })) },
    { weight: 1, arbitrary: queue.map((value) => ({ kind: 'pause', queue: value })) },
    { weight: 1, arbitrary: queue.map((value) => ({ kind: 'resume', queue: value })) },
    { weight: 1, arbitrary: fc.constant({ kind: 'compact' }) },
    { weight: 1, arbitrary: fc.constant({ kind: 'restart' }) }
  );
}

async function applyOperation(
  harness: CrossQueueHarness,
  model: Map<string, QueueState>,
  operation: Operation,
  sequence: number
): Promise<void> {
  if (operation.kind === 'restart') return harness.restart();
  if (operation.kind === 'compact') {
    expect((await harness.send({ cmd: 'CompactMemory' })).ok).toBe(true);
    return;
  }

  const queue = harness.queues[operation.queue]!;
  const state = model.get(queue)!;
  if (operation.kind === 'pause' || operation.kind === 'resume') {
    expect(
      (await harness.send({ cmd: operation.kind === 'pause' ? 'Pause' : 'Resume', queue })).ok
    ).toBe(true);
    state.paused = operation.kind === 'pause';
    return;
  }
  if (operation.kind === 'push') {
    const id = `cross-${operation.queue}-${sequence}`;
    const response = await harness.send({
      cmd: 'PUSH',
      data: { payload: sequence },
      durable: true,
      jobId: id,
      queue,
    });
    expect(response.id).toBe(id);
    state.jobs.set(id, { payload: sequence, state: 'waiting' });
    return;
  }

  const response = await harness.send({
    cmd: 'PULL',
    lockTtl: 60000,
    owner: 'cross-model-worker',
    queue,
    timeout: 0,
  });
  const job = response.job as { id?: string } | null;
  const canPull =
    !state.paused && [...state.jobs.values()].some((item) => item.state === 'waiting');
  expect(job !== null, `pull availability for ${queue}`).toBe(canPull);
  if (!job?.id) return;
  expect(state.jobs.get(job.id)?.state, `queue-local ownership for ${job.id}`).toBe('waiting');
  const ack = await harness.send({ cmd: 'ACK', id: job.id, token: response.token });
  expect(ack.ok).toBe(true);
  state.jobs.get(job.id)!.state = 'completed';
}

function chooseQueues(runId: string): string[] {
  const byShard = new Map<number, string[]>();
  for (let candidate = 0; candidate < 10000; candidate++) {
    const queue = `cross-${runId}-${candidate}`;
    const shard = shardIndex(queue);
    const names = byShard.get(shard) ?? [];
    names.push(queue);
    byShard.set(shard, names);
    const allNames = [...byShard.values()].flat();
    if (allNames.length < 4) continue;
    const sameShard = [...byShard.values()].find((entries) => entries.length >= 2);
    if (!sameShard) continue;
    const others = allNames.filter((name) => !sameShard.slice(0, 2).includes(name));
    const different = others.find((name) => shardIndex(name) !== shardIndex(sameShard[0]!));
    if (SHARD_COUNT > 1 && !different) continue;
    const third = different ?? others[0]!;
    const fourth = others.find((name) => name !== third)!;
    return [sameShard[0]!, sameShard[1]!, third, fourth];
  }
  throw new Error('unable to choose same-shard and cross-shard queue names');
}

function optionalInteger(value: string | undefined): number | undefined {
  if (value === undefined || value === '') return undefined;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) throw new Error(`expected safe integer, received ${value}`);
  return parsed;
}
