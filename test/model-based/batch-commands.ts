import { expect } from 'bun:test';
import type { AsyncCommand, Arbitrary } from 'fast-check';
import fc from 'fast-check';
import { MODEL_FLOW_CHILD_ID, MODEL_FLOW_PARENT_ID, MODEL_JOB_IDS } from './model-ids';
import { QueueCommand } from './queue-command';
import type { ModelJob, QueueModel } from './queue-model-harness';
import { isReady, readyState, terminalGeneration, type RealQueue } from './queue-model-harness';

function isUnfinished(job: ModelJob | undefined): boolean {
  return job !== undefined && (isReady(job) || job.state === 'delayed' || job.state === 'active');
}

class PushBatchCommand extends QueueCommand {
  constructor(
    private readonly firstSlot: number,
    private readonly secondSlot: number,
    private readonly priority: number
  ) {
    super();
  }

  check(): boolean {
    return true;
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const slots = [this.firstSlot, this.secondSlot];
    const prepared = slots.map((slot, index) => {
      const id = MODEL_JOB_IDS[slot]!;
      const previous = model.jobs.get(id);
      const generation = isUnfinished(previous)
        ? previous!.generation
        : (model.generations.get(id) ?? 0) + 1;
      return { generation, id, index, previous };
    });
    const response = await real.send({
      cmd: 'PUSHB',
      jobs: prepared.map(({ generation, id, index }) => ({
        customId: id,
        data: { generation },
        delay: index === 1 ? 60000 : 0,
        durable: true,
        priority: this.priority,
      })),
      queue: real.queue,
    });
    expect(response.ids).toEqual(prepared.map(({ id }) => id));
    for (const { generation, id, index } of prepared) {
      if (isUnfinished(model.jobs.get(id))) continue;
      model.generations.set(id, generation);
      const previous = model.jobs.get(id);
      model.accepted++;
      if (previous) model.removed++;
      model.jobs.set(id, {
        attempts: 0,
        diskState: index === 1 ? 'delayed' : readyState(this.priority),
        generation,
        maxAttempts: 3,
        priority: this.priority,
        progress: 0,
        progressMessage: null,
        stallCount: 0,
        state: index === 1 ? 'delayed' : readyState(this.priority),
      });
    }
    await this.verify(model, real);
  }

  toString(): string {
    return `pushBatch(${MODEL_JOB_IDS[this.firstSlot]},${MODEL_JOB_IDS[this.secondSlot]},p=${this.priority})`;
  }
}

class PullBatchCommand extends QueueCommand {
  check(model: Readonly<QueueModel>): boolean {
    const active = [...model.jobs.values()].filter((job) => job.state === 'active').length;
    return (
      !model.paused &&
      (model.concurrency === null || active < model.concurrency) &&
      (model.rateLimit === null || model.rateRemaining > 0) &&
      [...model.jobs.values()].some(isReady)
    );
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const readyCount = [...model.jobs.values()].filter(isReady).length;
    const active = [...model.jobs.values()].filter((job) => job.state === 'active').length;
    const capacity = model.concurrency === null ? 2 : Math.max(0, model.concurrency - active);
    const rateCapacity = model.rateLimit === null ? 2 : model.rateRemaining;
    const response = await real.send({
      cmd: 'PULLB',
      count: 2,
      lockTtl: 60000,
      owner: 'model-batch-worker',
      queue: real.queue,
      timeout: 0,
    });
    const jobs = response.jobs as {
      data?: { generation?: number };
      id: string;
      priority: number;
    }[];
    const tokens = response.tokens as string[];
    expect(jobs).toHaveLength(Math.min(2, readyCount, capacity, rateCapacity));
    expect(tokens).toHaveLength(jobs.length);
    for (let index = 0; index < jobs.length; index++) {
      const observed = jobs[index]!;
      const maxPriority = Math.max(
        ...[...model.jobs.values()].filter(isReady).map((job) => job.priority)
      );
      const modeled = model.jobs.get(observed.id)!;
      expect(isReady(modeled)).toBe(true);
      expect(observed.priority).toBe(maxPriority);
      expect(observed.data?.generation).toBe(modeled.generation);
      expect(model.terminalGenerations.has(terminalGeneration(observed.id, modeled))).toBe(false);
      expect(tokens[index]!.length).toBeGreaterThan(0);
      modeled.state = 'active';
      modeled.diskState = 'active';
      real.tokens.set(observed.id, tokens[index]!);
    }
    if (model.rateLimit !== null) model.rateRemaining -= jobs.length;
    await this.verify(model, real);
  }

  toString(): string {
    return 'pullBatch(2)';
  }
}

class AckBatchCommand extends QueueCommand {
  check(model: Readonly<QueueModel>): boolean {
    return [...model.jobs.values()].some((job) => job.state === 'active');
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const active = [...model.jobs].filter(([, job]) => job.state === 'active');
    const ids = active.map(([id]) => id);
    const response = await real.send({
      cmd: 'ACKB',
      ids,
      results: active.map(([, job]) => ({ generation: job.generation })),
      tokens: ids.map((id) => real.tokens.get(id)),
    });
    expect(response.ok).toBe(true);
    expect(response.count).toBeUndefined();
    expect(response.data).toBeUndefined();
    for (const [id, job] of active) {
      job.state = 'completed';
      job.diskState = 'completed';
      job.progress = 100;
      model.terminalGenerations.add(terminalGeneration(id, job));
      real.tokens.delete(id);
    }
    if (ids.includes(MODEL_FLOW_CHILD_ID)) {
      const parent = model.jobs.get(MODEL_FLOW_PARENT_ID);
      if (parent?.state === 'waiting-children') {
        parent.state = readyState(parent.priority);
        parent.diskState = readyState(parent.priority);
      }
    }
    await this.verify(model, real);
  }

  toString(): string {
    return 'ackBatch(all-active)';
  }
}

class HeartbeatBatchCommand extends QueueCommand {
  check(model: Readonly<QueueModel>): boolean {
    return [...model.jobs.values()].some((job) => job.state === 'active');
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const ids = [...model.jobs].filter(([, job]) => job.state === 'active').map(([id]) => id);
    const response = await real.send({
      cmd: 'JobHeartbeatB',
      ids,
      tokens: ids.map((id) => real.tokens.get(id)),
    });
    expect((response.data as { count?: number }).count).toBe(ids.length);
    await this.verify(model, real);
  }

  toString(): string {
    return 'heartbeatBatch(all-active)';
  }
}

export function batchCommandArbitraries(): Arbitrary<AsyncCommand<QueueModel, RealQueue>>[] {
  const slot = fc.integer({ min: 0, max: MODEL_JOB_IDS.length - 1 });
  return [
    fc
      .tuple(slot, slot, fc.integer({ min: 0, max: 3 }))
      .map(([first, second, priority]) => new PushBatchCommand(first, second, priority)),
    fc.constant(new PullBatchCommand()),
    fc.constant(new AckBatchCommand()),
    fc.constant(new HeartbeatBatchCommand()),
  ];
}
