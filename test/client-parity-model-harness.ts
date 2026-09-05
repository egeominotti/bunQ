import { expect } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import * as native from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer } from '../src/infrastructure/server/tcp';

export type Operation = {
  kind: 'add' | 'bulk' | 'update' | 'priority' | 'remove' | 'pause' | 'resume' | 'group' | 'query';
  slot: number;
  value: number;
  priority: number;
};
type Data = { slot: number; value: number };
type ModelJob = { data: Data; priority: number };
export type Model = {
  jobs: Map<number, ModelJob>;
  paused: boolean;
  accepted: number;
  removed: number;
  groups: Map<number, number>;
};

export function model(): Model {
  return { jobs: new Map(), paused: false, accepted: 0, removed: 0, groups: new Map() };
}

/** Apply a contract model independently of either implementation's responses. */
export function advance(state: Model, operation: Operation): void {
  const { kind, slot, value, priority } = operation;
  if (kind === 'add' || kind === 'bulk') {
    for (const target of kind === 'bulk' ? [slot, (slot + 1) % 6] : [slot]) {
      if (!state.jobs.has(target)) {
        state.jobs.set(target, { data: { slot: target, value }, priority });
        state.accepted++;
      }
    }
  } else if (kind === 'remove') {
    if (state.jobs.delete(slot)) state.removed++;
  } else if (kind === 'update') {
    const job = state.jobs.get(slot);
    if (job) job.data = { slot, value };
  } else if (kind === 'priority') {
    const job = state.jobs.get(slot);
    if (job) job.priority = priority;
  } else if (kind === 'pause' || kind === 'resume') state.paused = kind === 'pause';
  else if (kind === 'group') state.groups.set(slot % 2, priority + 1);
}

export class ParityBroker {
  private readonly directory = mkdtempSync(join(tmpdir(), 'bunqueue-client-property-'));
  private readonly manager = new QueueManager({ dataPath: join(this.directory, 'broker.db') });
  private readonly server = createTcpServer(this.manager, { hostname: '127.0.0.1', port: 0 });
  private readonly queue: native.Queue<Data>;
  private readonly ids = new Map<number, string>();

  constructor(client: typeof native) {
    this.queue = new client.Queue(`property-${crypto.randomUUID()}`, {
      embedded: false,
      connection: {
        host: '127.0.0.1',
        port: this.server.server.port,
        poolSize: 1,
        pingInterval: 0,
      },
      autoBatch: { enabled: false },
    });
  }

  private remember(job: native.Job<Data>, slot: number): number {
    const previous = this.ids.get(slot);
    if (previous) expect(job.id).toBe(previous);
    this.ids.set(slot, job.id);
    expect(new Set(this.ids.values()).size).toBe(this.ids.size);
    return slot;
  }

  async execute(operation: Operation): Promise<unknown> {
    const { kind, slot, value, priority } = operation;
    const options = (target: number) => ({
      jobId: `logical-${target}`,
      priority,
      group: { id: `group-${target % 2}` },
    });
    if (kind === 'add') {
      return this.remember(await this.queue.add('task', { slot, value }, options(slot)), slot);
    }
    if (kind === 'bulk') {
      const slots = [slot, (slot + 1) % 6];
      const jobs = await this.queue.addBulk(
        slots.map((target) => ({
          name: 'task',
          data: { slot: target, value },
          opts: options(target),
        }))
      );
      expect(jobs.length).toBe(slots.length);
      return jobs.map((job, index) => {
        const target = slots[index];
        if (target === undefined) throw new Error('Unexpected bulk result index');
        return this.remember(job, target);
      });
    }
    if (kind === 'pause' || kind === 'resume') {
      return kind === 'pause' ? this.queue.pauseAsync() : this.queue.resumeAsync();
    }
    if (kind === 'group') {
      const group = `group-${slot % 2}`;
      await this.queue.setGroupConcurrency(group, priority + 1);
      await this.queue.setGroupRateLimit(group, priority + 1, 1000);
      const concurrency = await this.queue.getGroupConcurrency(group);
      const rate = await this.queue.getGroupRateLimit(group);
      expect(concurrency).toBe(priority + 1);
      expect(rate).toEqual({ max: priority + 1, duration: 1000 });
      return { concurrency, rate };
    }
    const id = this.ids.get(slot);
    if (!id) return null;
    const job = await this.queue.getJob(id);
    expect(job).not.toBeNull();
    if (!job) throw new Error('Tracked job disappeared');
    if (kind === 'update') return job.updateData({ slot, value });
    if (kind === 'priority') return job.changePriority({ priority });
    if (kind === 'remove') {
      await this.queue.removeAsync(id);
      expect(await this.queue.getJob(id)).toBeNull();
      this.ids.delete(slot);
      return null;
    }
    return { slot, state: await job.getState(), data: job.data, priority: job.opts.priority };
  }

  async snapshot(expected: Model) {
    expect(expected.accepted - expected.removed).toBe(expected.jobs.size);
    expect(this.ids.size).toBe(expected.jobs.size);
    const counts = await this.queue.getJobCountsAsync();
    expect(Object.values(counts).reduce((sum, value) => sum + value, 0)).toBe(expected.jobs.size);
    const brokerCounts = this.manager.getQueueJobCounts(this.queue.name);
    expect(Object.values(brokerCounts).reduce((sum, value) => sum + value, 0)).toBe(
      expected.jobs.size
    );
    expect(await this.queue.isPausedAsync()).toBe(expected.paused);
    const jobs = [];
    for (const [slot, reference] of [...expected.jobs].sort(([left], [right]) => left - right)) {
      const id = this.ids.get(slot);
      if (!id) throw new Error(`Missing logical ID ${slot}`);
      const job = await this.queue.getJob(id);
      expect(job?.data).toEqual(reference.data);
      expect(job?.opts.priority ?? 0).toBe(reference.priority);
      const state = await this.queue.getJobState(id);
      expect(['waiting', 'prioritized', 'paused']).toContain(state);
      // Only physical IDs/queue names and timestamps are excluded. The logical
      // slot maps each physical ID explicitly, with uniqueness checked above.
      jobs.push({
        slot,
        state,
        name: job?.name,
        data: job?.data,
        priority: job?.opts.priority ?? 0,
        progress: job?.progress,
        attemptsMade: job?.attemptsMade,
        group: job?.opts.group,
      });
    }
    const groups = [];
    for (const group of [0, 1]) {
      const name = `group-${group}`;
      const count = await this.queue.getGroupJobsCount(name);
      expect(count).toBe([...expected.jobs.keys()].filter((slot) => slot % 2 === group).length);
      const concurrency = await this.queue.getGroupConcurrency(name);
      const maximum = expected.groups.get(group);
      expect(concurrency).toBe(maximum ?? null);
      const rate = await this.queue.getGroupRateLimit(name);
      expect(rate).toEqual(maximum === undefined ? null : { max: maximum, duration: 1000 });
      groups.push({ group, count, concurrency, rate });
    }
    return { jobs, counts, brokerCounts, groups, paused: expected.paused };
  }

  async close(): Promise<void> {
    try {
      await this.queue.close();
    } finally {
      this.server.stop();
      this.manager.shutdown();
      rmSync(this.directory, { recursive: true, force: true });
    }
  }
}
