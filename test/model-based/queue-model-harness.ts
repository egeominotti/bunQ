import { expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { unpack } from 'msgpackr';
import { assertAggregateCounts, assertInternalCollections } from './queue-model-observability';
import { startModelBroker, stopModelBroker, type StartedModelBroker } from './queue-model-broker';

export type ModelState =
  | 'waiting'
  | 'prioritized'
  | 'waiting-children'
  | 'delayed'
  | 'active'
  | 'completed'
  | 'failed';

export interface ModelJob {
  attempts: number;
  diskState: 'waiting' | 'delayed' | 'active' | 'completed';
  generation: number;
  maxAttempts: number;
  priority: number;
  stallCount: number;
  state: ModelState;
}

export interface QueueModel {
  accepted: number;
  concurrency: number | null;
  generations: Map<string, number>;
  jobs: Map<string, ModelJob>;
  paused: boolean;
  rateLimit: number | null;
  rateRemaining: number;
  removed: number;
  terminalGenerations: Set<string>;
}

export function readyState(priority: number): 'waiting' | 'prioritized' {
  return priority > 0 ? 'prioritized' : 'waiting';
}

export function isReady(job: Readonly<ModelJob>): boolean {
  return job.state === 'waiting' || job.state === 'prioritized';
}

interface JobRow {
  attempts: number;
  data: Uint8Array;
  id: string;
  max_attempts: number;
  priority: number;
  stall_count: number;
  state: string;
}

export class RealQueue {
  readonly dbPath: string;
  port: number;
  readonly queue = 'model-based';
  readonly tokens = new Map<string, string>();
  private broker: StartedModelBroker | null = null;

  private constructor(runId: string, initialPort?: number) {
    this.dbPath = join(tmpdir(), `bunqueue-model-${process.pid}-${runId}.db`);
    this.port = initialPort ?? 0;
  }

  static async create(runId: string, options: { initialPort?: number } = {}): Promise<RealQueue> {
    const real = new RealQueue(runId, options.initialPort);
    await real.start();
    return real;
  }

  async send(command: Record<string, unknown>): Promise<Record<string, unknown>> {
    if (!this.broker) throw new Error('model client is not connected');
    return this.broker.client.send(command);
  }

  async crashRestart(): Promise<void> {
    await this.kill();
    this.tokens.clear();
    await this.start();
  }

  async dispose(): Promise<void> {
    await this.kill();
    cleanDb(this.dbPath);
  }

  async assertConsistent(model: QueueModel): Promise<void> {
    expect(model.accepted - model.removed, 'accepted minus explicitly removed lifecycles').toBe(
      model.jobs.size
    );
    expect(model.rateRemaining, 'rate-limit tokens never go negative').toBeGreaterThanOrEqual(0);
    if (model.rateLimit !== null) {
      expect(model.rateRemaining, 'rate-limit tokens stay within capacity').toBeLessThanOrEqual(
        model.rateLimit
      );
    }
    const paused = await this.send({ cmd: 'IsPaused', queue: this.queue });
    expect(paused.paused, 'queue paused state').toBe(model.paused);
    const expectedRows = new Set<string>();
    const expectedDlq = new Set<string>();
    for (const [id, job] of model.jobs) {
      const response = await this.send({ cmd: 'GetState', id });
      expect(response.state, `API state for ${id}`).toBe(job.state);
      const jobResponse = await this.send({ cmd: 'GetJob', id });
      const apiJob = jobResponse.job as {
        attempts?: number;
        data?: { generation?: number };
        maxAttempts?: number;
        priority?: number;
        stallCount?: number;
      };
      expect(apiJob.data?.generation, `API generation for ${id}`).toBe(job.generation);
      expect(apiJob.priority, `API priority for ${id}`).toBe(job.priority);
      expect(apiJob.attempts, `API attempts for ${id}`).toBe(job.attempts);
      expect(apiJob.maxAttempts, `API maxAttempts for ${id}`).toBe(job.maxAttempts);
      expect(apiJob.stallCount, `API stallCount for ${id}`).toBe(job.stallCount);
      expect(job.attempts, `attempt bound for ${id}`).toBeLessThanOrEqual(job.maxAttempts);
      expect(job.stallCount, `stall bound for ${id}`).toBeLessThanOrEqual(3);
      if (job.state === 'failed') expectedDlq.add(id);
      else expectedRows.add(id);
    }
    await assertAggregateCounts((command) => this.send(command), this.queue, model);

    const db = new Database(this.dbPath, { readonly: true });
    try {
      const rows = db
        .query<JobRow, [string]>(
          'SELECT id, data, priority, attempts, max_attempts, stall_count, state FROM jobs WHERE queue = ? ORDER BY id'
        )
        .all(this.queue);
      const dlq = db.query<{ job_id: string }, []>('SELECT job_id FROM dlq ORDER BY job_id').all();
      const queueState = db
        .query<
          {
            concurrency_limit: number | null;
            paused: number;
            rate_limit: number | null;
            rate_limit_duration: number | null;
          },
          [string]
        >(
          'SELECT paused, concurrency_limit, rate_limit, rate_limit_duration FROM queue_state WHERE name = ?'
        )
        .get(this.queue);
      expect(queueState?.paused ?? 0, 'persisted queue paused state').toBe(model.paused ? 1 : 0);
      expect(queueState?.concurrency_limit ?? null, 'persisted concurrency limit').toBe(
        model.concurrency
      );
      expect(queueState?.rate_limit ?? null, 'persisted rate limit').toBe(model.rateLimit);
      expect(queueState?.rate_limit_duration ?? null, 'persisted rate-limit window').toBe(
        model.rateLimit === null ? null : 600000
      );
      expect(new Set(rows.map((row) => row.id)), 'SQLite jobs membership').toEqual(expectedRows);
      expect(new Set(dlq.map((row) => row.job_id)), 'SQLite DLQ membership').toEqual(expectedDlq);
      expect(dlq.length, 'exactly one SQLite DLQ entry per failed job').toBe(expectedDlq.size);

      for (const row of rows) {
        const expected = model.jobs.get(row.id);
        expect(expected, `model entry for SQLite row ${row.id}`).toBeDefined();
        expect(row.state, `SQLite state for ${row.id}`).toBe(expected!.diskState);
        expect(row.priority, `SQLite priority for ${row.id}`).toBe(expected!.priority);
        expect(row.attempts, `SQLite attempts for ${row.id}`).toBe(expected!.attempts);
        expect(row.max_attempts, `SQLite maxAttempts for ${row.id}`).toBe(expected!.maxAttempts);
        expect(row.stall_count, `SQLite stallCount for ${row.id}`).toBe(expected!.stallCount);
        const payload = unpack(row.data) as { generation?: number };
        expect(payload.generation, `SQLite generation for ${row.id}`).toBe(expected!.generation);
      }
    } finally {
      db.close();
    }

    const active = [...model.jobs].filter(([, job]) => job.state === 'active').map(([id]) => id);
    expect(new Set(this.tokens.keys()), 'one live token per active job').toEqual(new Set(active));
    await assertInternalCollections(this.port, model);
  }

  private async start(): Promise<void> {
    this.broker = await startModelBroker(this.dbPath, this.port || undefined);
    this.port = this.broker.port;
  }

  private async kill(): Promise<void> {
    if (this.broker) {
      await stopModelBroker(this.broker);
      this.broker = null;
    }
    await Bun.sleep(50);
  }
}

export function terminalGeneration(id: string, job: Readonly<ModelJob>): string {
  return `${id}:${job.generation}`;
}

function cleanDb(path: string): void {
  for (const suffix of ['', '-wal', '-shm']) {
    const candidate = `${path}${suffix}`;
    if (existsSync(candidate)) rmSync(candidate, { force: true });
  }
}
