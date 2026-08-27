import { expect } from 'bun:test';
import type { AsyncCommand, Arbitrary } from 'fast-check';
import fc from 'fast-check';
import { MODEL_JOB_IDS } from './model-ids';
import { QueueCommand } from './queue-command';
import type { ModelState, QueueModel } from './queue-model-harness';
import { isReady, readyState, terminalGeneration, type RealQueue } from './queue-model-harness';

class ChangePriorityCommand extends QueueCommand {
  constructor(
    private readonly slot: number,
    private readonly priority: number
  ) {
    super();
  }

  check(model: Readonly<QueueModel>): boolean {
    const job = model.jobs.get(MODEL_JOB_IDS[this.slot]!);
    return job !== undefined && (isReady(job) || job.state === 'delayed');
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const id = MODEL_JOB_IDS[this.slot]!;
    const response = await real.send({ cmd: 'ChangePriority', id, priority: this.priority });
    expect(response.ok).toBe(true);
    const job = model.jobs.get(id)!;
    job.priority = this.priority;
    if (isReady(job)) job.state = readyState(this.priority);
    await this.verify(model, real);
  }

  toString(): string {
    return `changePriority(${MODEL_JOB_IDS[this.slot]},${this.priority})`;
  }
}

export class MoveToDelayedCommand extends QueueCommand {
  constructor(
    private readonly slot: number,
    private readonly change: boolean
  ) {
    super();
  }

  check(model: Readonly<QueueModel>): boolean {
    const job = model.jobs.get(MODEL_JOB_IDS[this.slot]!);
    return job !== undefined && (isReady(job) || job.state === 'delayed' || job.state === 'active');
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const id = MODEL_JOB_IDS[this.slot]!;
    const previousState = model.jobs.get(id)!.state;
    const response = await real.send({
      cmd: this.change ? 'ChangeDelay' : 'MoveToDelayed',
      delay: 60000,
      id,
      ...activeLease(previousState, real, id),
    });
    expect(response.ok).toBe(true);
    const job = model.jobs.get(id)!;
    job.state = 'delayed';
    job.diskState = 'delayed';
    real.tokens.delete(id);
    await this.verify(model, real);
  }

  toString(): string {
    return `${this.change ? 'changeDelay' : 'moveToDelayed'}(${MODEL_JOB_IDS[this.slot]})`;
  }
}

export class MoveToWaitCommand extends QueueCommand {
  constructor(private readonly slot: number) {
    super();
  }

  check(model: Readonly<QueueModel>): boolean {
    const state = model.jobs.get(MODEL_JOB_IDS[this.slot]!)?.state;
    return state === 'active' || state === 'delayed' || state === 'failed';
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const id = MODEL_JOB_IDS[this.slot]!;
    const previousState = model.jobs.get(id)!.state;
    const response = await real.send({
      cmd: 'MoveToWait',
      id,
      ...activeLease(previousState, real, id),
    });
    expect(response.ok).toBe(true);
    const job = model.jobs.get(id)!;
    job.state = readyState(job.priority);
    job.diskState = 'waiting';
    if (previousState === 'failed') {
      job.attempts = 0;
      job.stallCount = 0;
      model.terminalGenerations.delete(terminalGeneration(id, job));
    }
    real.tokens.delete(id);
    await this.verify(model, real);
  }

  toString(): string {
    return `moveToWait(${MODEL_JOB_IDS[this.slot]})`;
  }
}

function activeLease(state: ModelState, real: RealQueue, id: string): { token?: string } {
  if (state !== 'active') return {};
  const token = real.tokens.get(id);
  if (!token) throw new Error(`active model job "${id}" has no lease token`);
  return { token };
}

class UpdateDataCommand extends QueueCommand {
  constructor(private readonly slot: number) {
    super();
  }

  check(model: Readonly<QueueModel>): boolean {
    const job = model.jobs.get(MODEL_JOB_IDS[this.slot]!);
    return job !== undefined && job.state !== 'completed' && job.state !== 'failed';
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const id = MODEL_JOB_IDS[this.slot]!;
    const job = model.jobs.get(id)!;
    const generation = (model.generations.get(id) ?? job.generation) + 1;
    const response = await real.send({ cmd: 'Update', data: { generation }, id });
    expect(response.ok).toBe(true);
    job.generation = generation;
    model.generations.set(id, generation);
    await this.verify(model, real);
  }

  toString(): string {
    return `updateData(${MODEL_JOB_IDS[this.slot]})`;
  }
}

class ProgressCommand extends QueueCommand {
  constructor(
    private readonly slot: number,
    private readonly progress: number
  ) {
    super();
  }

  check(model: Readonly<QueueModel>): boolean {
    return model.jobs.get(MODEL_JOB_IDS[this.slot]!)?.state === 'active';
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const id = MODEL_JOB_IDS[this.slot]!;
    const message = `model-progress-${this.progress}`;
    const response = await real.send({ cmd: 'Progress', id, message, progress: this.progress });
    expect(response.ok).toBe(true);
    const observed = await real.send({ cmd: 'GetProgress', id });
    expect(observed.progress).toBe(this.progress);
    expect(observed.message).toBe(message);
    model.jobs.get(id)!.progress = this.progress;
    model.jobs.get(id)!.progressMessage = message;
    await this.verify(model, real);
  }

  toString(): string {
    return `progress(${MODEL_JOB_IDS[this.slot]},${this.progress})`;
  }
}

class RemoveCommand extends QueueCommand {
  constructor(
    private readonly slot: number,
    private readonly discard: boolean
  ) {
    super();
  }

  check(model: Readonly<QueueModel>): boolean {
    const job = model.jobs.get(MODEL_JOB_IDS[this.slot]!);
    if (!job || job.state === 'completed' || job.state === 'failed') return false;
    return this.discard || isReady(job) || job.state === 'delayed';
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const id = MODEL_JOB_IDS[this.slot]!;
    const previousState = model.jobs.get(id)!.state;
    const response = await real.send({
      cmd: this.discard ? 'Discard' : 'Cancel',
      id,
      ...(this.discard ? activeLease(previousState, real, id) : {}),
    });
    expect(response.ok).toBe(true);
    if (this.discard) {
      model.jobs.get(id)!.state = 'failed';
      model.terminalGenerations.add(terminalGeneration(id, model.jobs.get(id)!));
    } else {
      model.jobs.delete(id);
      model.removed++;
    }
    real.tokens.delete(id);
    await this.verify(model, real);
  }

  toString(): string {
    return `${this.discard ? 'discard' : 'cancel'}(${MODEL_JOB_IDS[this.slot]})`;
  }
}

class RetryTerminalCommand extends QueueCommand {
  constructor(
    private readonly slot: number,
    private readonly completed: boolean
  ) {
    super();
  }

  check(model: Readonly<QueueModel>): boolean {
    return (
      model.jobs.get(MODEL_JOB_IDS[this.slot]!)?.state === (this.completed ? 'completed' : 'failed')
    );
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const id = MODEL_JOB_IDS[this.slot]!;
    const response = await real.send(
      this.completed
        ? { cmd: 'RetryCompleted', id, queue: real.queue }
        : { cmd: 'RetryDlq', jobId: id, queue: real.queue }
    );
    expect(response.count).toBe(1);
    const job = model.jobs.get(id)!;
    job.state = readyState(job.priority);
    job.diskState = 'waiting';
    job.attempts = 0;
    job.stallCount = 0;
    if (this.completed) job.progress = 0;
    model.terminalGenerations.delete(terminalGeneration(id, job));
    await this.verify(model, real);
  }

  toString(): string {
    return `${this.completed ? 'retryCompleted' : 'retryDlq'}(${MODEL_JOB_IDS[this.slot]})`;
  }
}

class RemoveDlqCommand extends QueueCommand {
  constructor(private readonly slot: number) {
    super();
  }

  check(model: Readonly<QueueModel>): boolean {
    return model.jobs.get(MODEL_JOB_IDS[this.slot]!)?.state === 'failed';
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const id = MODEL_JOB_IDS[this.slot]!;
    const job = model.jobs.get(id)!;
    const response = await real.send({ cmd: 'RemoveDlqJob', jobId: id, queue: real.queue });
    expect(response.ok).toBe(true);
    expect((response.data as { removed?: boolean }).removed).toBe(true);
    const repeated = await real.send({ cmd: 'RemoveDlqJob', jobId: id, queue: real.queue });
    expect(repeated.ok).toBe(true);
    expect((repeated.data as { removed?: boolean }).removed).toBe(false);
    model.terminalGenerations.delete(terminalGeneration(id, job));
    model.jobs.delete(id);
    model.removed++;
    real.tokens.delete(id);
    await this.verify(model, real);
  }

  toString(): string {
    return `removeDlq(${MODEL_JOB_IDS[this.slot]})`;
  }
}

export function jobManagementCommandArbitraries(): Arbitrary<
  AsyncCommand<QueueModel, RealQueue>
>[] {
  const slot = fc.integer({ min: 0, max: MODEL_JOB_IDS.length - 1 });
  return [
    fc
      .tuple(slot, fc.integer({ min: 0, max: 3 }))
      .map(([value, priority]) => new ChangePriorityCommand(value, priority)),
    slot.map((value) => new MoveToDelayedCommand(value, false)),
    slot.map((value) => new MoveToDelayedCommand(value, true)),
    slot.map((value) => new MoveToWaitCommand(value)),
    slot.map((value) => new UpdateDataCommand(value)),
    fc
      .tuple(slot, fc.integer({ min: 0, max: 100 }))
      .map(([value, progress]) => new ProgressCommand(value, progress)),
    slot.map((value) => new RemoveCommand(value, false)),
    slot.map((value) => new RemoveCommand(value, true)),
    slot.map((value) => new RetryTerminalCommand(value, false)),
    slot.map((value) => new RetryTerminalCommand(value, true)),
    slot.map((value) => new RemoveDlqCommand(value)),
  ];
}
